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

package com.j_spaces.jdbc.executor;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.jdbc.OrderColumn;
import com.j_spaces.jdbc.ResultEntry;
import com.j_spaces.jdbc.SelectColumn;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import com.j_spaces.jdbc.parser.AndNode;
import com.j_spaces.jdbc.parser.ExpNode;
import com.j_spaces.jdbc.parser.InNode;
import com.j_spaces.jdbc.parser.InnerQueryNode;
import com.j_spaces.jdbc.parser.NotInNode;
import com.j_spaces.jdbc.parser.OrNode;
import com.j_spaces.jdbc.query.IQueryResultSet;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.io.Externalizable;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author anna
 * @since 7.0
 */
public interface IQueryExecutor extends Externalizable {

    /**
     * Execute Or expression - execute both children and union the results
     */
    public abstract void execute(OrNode exp, ISpaceProxy space, Transaction txn,
                                 int readModifier, int max) throws SQLException;

    /**
     * Execute And expression - execute both children and union the results
     */
    public abstract void execute(AndNode exp, ISpaceProxy space, Transaction txn,
                                 int readModifier, int max) throws SQLException;


    /**
     * Execute IN expression - can be a list of simple values or an inner select query
     */
    public abstract void execute(InNode exp, ISpaceProxy space, Transaction txn,
                                 int readModifier, int max) throws SQLException;

    /**
     * execute NOT IN query
     */
    public abstract void execute(NotInNode exp, ISpaceProxy space, Transaction txn,
                                 int readModifier, int max) throws SQLException;


    /**
     * Execute inner query
     */
    public void execute(InnerQueryNode innerQueryNode, ISpaceProxy space, Transaction txn,
                        int readModifier, int max) throws SQLException;


    public abstract IQueryResultSet<IEntryPacket> execute(ISpaceProxy space, Transaction txn, int modifiers, int max)
            throws SQLException;

    /**
     * @param equalNode
     * @param space
     * @param txn
     * @param readModifier
     * @param max
     * @throws SQLException
     */
    public abstract void execute(ExpNode equalNode, ISpaceProxy space,
                                 Transaction txn, int readModifier, int max) throws SQLException;

    /**
     * Execute clear
     *
     * @return int
     */
    public int clear(QueryTemplatePacket template, ISpaceProxy space, Transaction txn, int modifier)
            throws RemoteException, TransactionException, UnusableEntryException;

    public int count(QueryTemplatePacket template, ISpaceProxy space, Transaction txn, int readModifier)
            throws SQLException;

    public IQueryResultSet<IEntryPacket> readAll(ISpaceProxy space, Transaction txn) throws SQLException;

    /**
     * Group the results according to the group by clause
     *
     * @return the results according to the group by clause
     */
    public IQueryResultSet<IEntryPacket> groupByAndKeepOrder(IQueryResultSet<IEntryPacket> entries, List<SelectColumn> groupColumns, List<OrderColumn> orderColumns, int limit) throws SQLException;
    public IQueryResultSet<IEntryPacket> groupBy(IQueryResultSet<IEntryPacket> entries, List<SelectColumn> groupColumns) throws SQLException;

    /**
     * Order the results according to the order by clause
     */
    public void orderBy(IQueryResultSet<IEntryPacket> entries, List<OrderColumn> orderColumns) throws SQLException;

    /**
     * Execute SQL function that processes the result entries and calculates one value -
     * sum,average,min,max
     *
     * @return the result entries one value - sum,average,min,max
     */
    public IEntryPacket aggregate(IQueryResultSet<IEntryPacket> entries) throws SQLException;

    /**
     * Removes duplicate entries from the result set. The rows are inserted into a TreeSet and only
     * the rows that were successfully added, are considered distinct. Note: TreeSet is used and not
     * HashSet to avoid creating hashCode for all fields.
     * @return new IQueryResultSet with distinct entries
     */
    public IQueryResultSet<IEntryPacket> filterDistinctEntries(IQueryResultSet<IEntryPacket> entries);

    /**
     * Converts the list of ExternaEntries to 2 dimensional array that contains the values of the
     * entries. Each row matches specific IEntryPacket. The conversion is necessary for entries
     * ordering.
     */
    public ResultEntry convertEntriesToResultArrays(IQueryResultSet<IEntryPacket> _entries);

    default IQueryResultSet<IEntryPacket> flattenEntryPackets(IQueryResultSet<IEntryPacket> _entries){
        throw new UnsupportedOperationException();
    }

}