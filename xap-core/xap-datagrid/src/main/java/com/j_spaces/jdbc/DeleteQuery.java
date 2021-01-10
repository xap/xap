/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.j_spaces.jdbc;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.exceptions.BatchQueryException;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.security.AccessDeniedException;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;
import com.gigaspaces.security.service.SecurityContext;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceContextHelper;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.executor.QueryExecutor;
import com.j_spaces.jdbc.parser.ExpNode;
import com.j_spaces.jdbc.parser.RowNumNode;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles the DELETE query logic.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class DeleteQuery extends AbstractDMLQuery {
    private static final long serialVersionUID = 1L;
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    public DeleteQuery() {
        super();
    }

    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn) throws SQLException {
        //delete is quite easy to do. first we call prepare on the values if needed,
        //then we get all relevant elements, and then we call the takeMultiple method
        //with the selected entries
        _executor = new QueryExecutor(this);
        ResponsePacket packet = new ResponsePacket();
        try {

            if (getSecurityInterceptor() != null) {
                SpaceContext spaceContext = getSession().getConnectionContext().getSpaceContext();
                SecurityContext securityContext = SpaceContextHelper.getSecurityContext(spaceContext);
                getSecurityInterceptor().intercept(securityContext, SpacePrivilege.TAKE, getTableName());
            }

            prepare(space, txn);

            Collection<IEntryPacket> entries;

            // use the exclusive read lock if possible to make sure that all the objects that matched the query can be deleted
            // if not possible - use the default read mode
            int readModifier = txn == null ? ReadModifiers.REPEATABLE_READ : ReadModifiers.EXCLUSIVE_READ_LOCK;
            if (expTree != null) {

                // Handle complex queries
                if (expTree.getTemplate() == null || expTree.getTemplate().isComplex() || (rownum != null && rownum.getStartIndex() > 1)) {
                    int max = getRownumLimit();
                    entries = _executor.execute(space, txn, readModifier, max);

                }
                // Handle queries that won't return anything
                else if (expTree.getTemplate().isAlwaysEmpty()) {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Logical error - query is always empty - fix your SQL syntax");
                    }
                    entries = new ArrayList<IEntryPacket>();
                }
                //if query was translated to a single space template
                //  it can be optimized
                else {
                    // Execute the take query
                    return executeDelete(expTree.getTemplate(), space, txn);
                }
            } else //no where clause, take everything
            {
                if (rownum != null && rownum.getStartIndex() > 1) {
                    int max = getRownumLimit();

                    entries = executeExclusiveReadLock(new QueryTemplatePacket(getTableData(), _queryResultType), space, txn, max, readModifier);

                } else {
                    // Execute the take query
                    return executeDelete(new QueryTemplatePacket(getTableData(), _queryResultType), space, txn);
                }
            }

            filterByRownum(entries);

            performTakeOneByOne(space, txn, packet, entries);
        } catch (AccessDeniedException e) {
            throw e;
        } catch (BatchQueryException e) {
            throw e;
        } catch (Exception e) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Error occurred on delete", e);
            }
            SQLException ex = new SQLException("Delete failed; Cause: " + e, "GSP", -104);
            ex.initCause(e);
            throw ex;
        }
        return packet;
    }

    /**
     * @param space
     * @param txn
     * @param packet
     * @param entries
     * @throws UnusableEntryException
     * @throws TransactionException
     * @throws InterruptedException
     * @throws RemoteException
     */
    private void performTakeOneByOne(IJSpace space, Transaction txn,
                                     ResponsePacket packet, Collection<IEntryPacket> entries)
            throws UnusableEntryException, TransactionException,
            InterruptedException, RemoteException {
        Iterator<IEntryPacket> iter = entries.iterator();
        int index = 0;
        String[] multipleUids = new String[entries.size()];
        //delete one by one with the uid and the version id in order to keep consistency
        while (iter.hasNext()) {
            IEntryPacket entry = iter.next();
            multipleUids[index] = entry.getUID();
            index++;

            iter.remove();
        }

        QueryTemplatePacket templatePacket = new QueryTemplatePacket(getTableData(), _queryResultType);
        templatePacket.setMultipleUIDs(multipleUids);
        templatePacket.setOperationID(getOperationID());
        IEntryPacket[] deleted = (IEntryPacket[]) space.takeMultiple(templatePacket, txn, Integer.MAX_VALUE);
        packet.setResultSet(Arrays.asList(deleted));
        packet.setIntResult(deleted.length);
    }

    public ArrayList<IEntryPacket> executeExclusiveReadLock(QueryTemplatePacket template,
                                                            IJSpace space, Transaction txn, int max, int modifiers) throws SQLException {
        IEntryPacket[] result = null;
        try {
            result = (IEntryPacket[]) space.readMultiple(template, txn, max, modifiers);

        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.getMessage(), e);
            }
            throw new SQLException("Failed to execute readMultiple: "
                    + e, "GSP", -111);
        }

        ArrayList<IEntryPacket> entries = new ArrayList<IEntryPacket>();

        if (result != null) {
            for (int i = 0; i < result.length; i++) {
                entries.add(result[i]);
            }
        }

        return entries;
    }

    /**
     * Execute delete operation with given template
     */
    private ResponsePacket executeDelete(QueryTemplatePacket template,
                                         ISpaceProxy space, Transaction txn) throws RemoteException, TransactionException, UnusableEntryException, InterruptedException {

        ResponsePacket packet = new ResponsePacket();
        template.setOperationID(getOperationID());

        template.setExplainPlan(getExplainPlan());

        if (isReturnResult()) {
            // Get the max results information from the rownum
            int max = getRownumLimit();

            ArrayList<IEntryPacket> entries = template.take(space, getRouting(), getProjectionTemplate(), txn, getTimeout(), getReadModifier(), getIfExists(), max, getMinEntriesToWaitFor(), getQueryResultType());


            packet.setResultSet(entries);
            packet.setIntResult(entries.size());
        } else {
            template.setRouting(getRouting());
            int cleared = _executor.clear(template, space, txn, getReadModifier());
            packet.setIntResult(cleared);
        }


        return packet;
    }

    @Override
    public DeleteQuery clone() {
        DeleteQuery query = new DeleteQuery();

        query.tables = tables;
        query._tablesData = _tablesData;
        query.rownum = (RowNumNode) (this.rownum == null ? null : rownum.clone());
        query.isPrepared = this.isPrepared;
        query.setContainsSubQueries(this.containsSubQueries());

        query.queryColumns = this.getQueryColumns(); //this is not a clone, but there is no need.
        if (this.getExpTree() != null)
            query.setExpTree((ExpNode) this.getExpTree().clone()); //clone all the tree.
        else
            query.setExpTree(expTree);

        return query;
    }
}
