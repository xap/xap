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

import com.gigaspaces.client.WriteMultipleException;
import com.gigaspaces.client.WriteMultipleException.IWriteResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;
import com.gigaspaces.security.service.SecurityContext;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceContextHelper;
import com.j_spaces.core.client.EntryVersionConflictException;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.UpdateModifiers;
import com.j_spaces.jdbc.batching.BatchResponsePacket;
import com.j_spaces.jdbc.driver.GPreparedStatement.PreparedValuesCollection;
import com.j_spaces.jdbc.driver.ObjectWithUID;
import com.j_spaces.jdbc.executor.QueryExecutor;
import com.j_spaces.jdbc.parser.ExpNode;
import com.j_spaces.jdbc.parser.LiteralNode;
import com.j_spaces.jdbc.query.IQueryResultSet;

import net.jini.core.transaction.Transaction;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class UpdateQuery extends AbstractDMLQuery {
    private List<LiteralNode> updatedValues;
    private boolean byUid = false;
    private List<UpdateColumn> _updatedColumns;
    private boolean _selfIncrementedUpdateColumn;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    public UpdateQuery() {
        super();
        _updatedColumns = new ArrayList<UpdateColumn>();
        _selfIncrementedUpdateColumn = false;
    }

    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn) throws SQLException {
        _executor = new QueryExecutor(this);
        ResponsePacket response = new ResponsePacket();
        if (_selfIncrementedUpdateColumn)
            setReadModifier(ReadModifiers.EXCLUSIVE_READ_LOCK);

        try {

            if (getSecurityInterceptor() != null) {
                SpaceContext spaceContext = getSession().getConnectionContext().getSpaceContext();
                SecurityContext securityContext = SpaceContextHelper.getSecurityContext(spaceContext);
                getSecurityInterceptor().intercept(securityContext, SpacePrivilege.WRITE, getTableName());
            }

            validateUpdateColumns();

            if (byUid && this.isPrepared()) { //if this is an update according to a specific uid.
                ObjectWithUID object = (ObjectWithUID) preparedValues[0];
                ExternalEntry template = new ExternalEntry(object.getEntryUID());
                ExternalEntry entry = (ExternalEntry) space.read(template,
                        null,
                        Integer.MAX_VALUE, getReadModifier());
                entry.m_FieldsValues[object.getObjectIndex()] = object;
                space.update(entry, txn, 0L, QueryProcessor.getDefaultConfig().getReadLease());
                //locked on update, no need to check validity
                response.setIntResult(0);
            } else {
                // If this is a prepared statement, prepare the values
                if (this.isPrepared()) {
                    int preparedValueIndex = 0;
                    for (LiteralNode literalNode : updatedValues) {
                        if (literalNode.isPreparedValue()) {
                            literalNode.setValue(preparedValues[preparedValueIndex++]);
                        }
                    }
                    if (expTree != null)
                        expTree.prepareValues(preparedValues);
                }

                build();
                //for optimistic locking, make a read with no transaction
                IQueryResultSet<IEntryPacket> entries;
                if (expTree != null) {
                    entries = _executor.execute(space, txn, getReadModifier(), Integer.MAX_VALUE);

                } else //no where clause, read everything
                {
                    entries = _executor.readAll(space, txn);
                }

                filterByRownum(entries);

                //we need to know the indices of the fields that are to be set
                //so we'll build an array of those indices.
                Iterator<IEntryPacket> iter = entries.iterator();
                if (!iter.hasNext()) {
                    response.setIntResult(0);
                    return response;
                }
                ITypeDesc info = getTypeInfo();
                int j = 0;

                //search each required column for update in the m_FieldsNames member
                int[] indices = new int[getUpdatedColumns().size()];

                // Note that this 'optimization' is actually a fix for GS-10994 so we do not
                // apply it for #executeBatch because it is not relevant in this case
                boolean[] modifiedIndices = new boolean[info.getNumOfFixedProperties()];
                boolean doPartialUpdate = true;

                Object[] convertedUpdateValues = new Object[updatedValues.size()];

                for (UpdateColumn updateColumn : _updatedColumns) {
                    for (int i = 0; i < info.getNumOfFixedProperties(); i++) {
                        if (info.getFixedProperty(i).getName().equalsIgnoreCase(updateColumn.getName())) {
                            modifiedIndices[i] = true;
                            indices[j] = i;
                            convertedUpdateValues[j] = updatedValues.get(j).getConvertedObject(info, updateColumn.getName());
                            doPartialUpdate = doPartialUpdate && convertedUpdateValues[j] != null;
                            j++;
                            break;
                        }
                    }
                }

                //now update the entries with the requested values
                //entries for update
                //take one entry for its structure
                while (iter.hasNext()) {
                    IEntryPacket entry = iter.next();
                    for (int i = 0; i < indices.length; i++) {
                        final int propertyIndex = indices[i];
                        if (_updatedColumns.get(i).isSelfIncremented()) {
                            try {
                                entry.setFieldValue(
                                        propertyIndex,
                                        NumberUtil.add(
                                                (Number) entry.getFieldValue(propertyIndex),
                                                (Number) convertedUpdateValues[i],
                                                info.getFixedProperty(propertyIndex).getTypeName()));
                            } catch (ClassCastException e) {
                                throw new SQLException("Operator '+' is only allowed for numeric column types.");
                            }
                        } else {
                            entry.setFieldValue(propertyIndex, convertedUpdateValues[i]);
                        }
                    }

                    for (int i = 0; i < modifiedIndices.length; i++) {
                        final boolean hasInvalidWrappedStringProperty = (getReadModifier() & Modifiers.RETURN_STRING_PROPERTIES) != 0 &&
                                (!doPartialUpdate || i == info.getRoutingPropertyId()) &&
                                entry.getFieldValue(i) != null &&
                                !info.getFixedProperty(i).isCommonJavaType();
                        if (hasInvalidWrappedStringProperty)
                            throw new SQLException("Cannot set a property to null on an entry that contains other non " +
                                    "primitive properties [" + info.getFixedProperty(i) + "]");

                        if (doPartialUpdate && !modifiedIndices[i] && (i != info.getRoutingPropertyId()))
                            entry.setFieldValue(i, null);
                    }

                }

                //the entries are updated, now update the space
                IEntryPacket[] originalEntries = entries.toArray(new IEntryPacket[entries.size()]);

                int modifiers = UpdateModifiers.UPDATE_ONLY;
                if (doPartialUpdate)
                    modifiers = modifiers | UpdateModifiers.PARTIAL_UPDATE;

                Object[] updatedEntries = space.writeMultiple(originalEntries, txn, 0, null, 0, modifiers);

                //now go through the array and check if all was successful;
                //if an entry wasn't successful, try and update it again until the update
                //goes through
                int notUpdated = 0;
                for (int e = 0; e < updatedEntries.length; e++) {
                    Object updateEntry = updatedEntries[e];
                    if (updateEntry == null || updateEntry instanceof EntryVersionConflictException)
                        notUpdated++; //one less to count in the return.

                }
                response.setIntResult(updatedEntries.length - notUpdated);
            }
        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.getMessage(), e);
            }

            if (e instanceof SQLException)
                throw (SQLException) e;

            SQLException se = new SQLException("Failed to update row; Cause: " + e, "GSP", -130);
            se.initCause(e);
            throw se;

        } finally {
            _executor = null;
        }
        return response;
    }

    /**
     * Validates INSERT query's update columns
     */
    private void validateUpdateColumns() throws SQLException {
        for (UpdateColumn column : _updatedColumns) {
            column.createColumnData(this);
            if (column.isSelfIncremented())
                column.validateSelfIncrementedColumnName(this);
        }
    }

    //	array of objects
    public void setUpdatedValues(List<LiteralNode> values) {
        this.updatedValues = values;
    }

    /**
     * @return Returns the byUid.
     */
    public boolean isByUid() {
        return byUid;
    }

    /**
     * @param byUid The byUid to set.
     */
    public void setByUid(boolean byUid) {
        this.byUid = byUid;
    }

    @Override
    public UpdateQuery clone() {
        UpdateQuery query = new UpdateQuery();
        query.tables = tables;
        query._tablesData = _tablesData;
        query.isPrepared = this.isPrepared;
        query.byUid = this.byUid;
        query._updatedColumns = this.getUpdatedColumns();
        query.setContainsSubQueries(this.containsSubQueries());

        if (this.getExpTree() != null && isPrepared())
            query.setExpTree((ExpNode) this.getExpTree().clone()); //clone all the tree.
        else
            query.setExpTree(expTree);

        query.updatedValues = this.updatedValues;
        return query;
    }
    /**
     * Adds a column name to the update columns list of the update query.
     */
    public void addUpdateColumn(UpdateColumn updateColumn) {
        _updatedColumns.add(updateColumn);
    }

    /**
     * Gets the update columns list of the update query.
     */
    public List<UpdateColumn> getUpdatedColumns() {
        return _updatedColumns;
    }

    /**
     * Sets whether the query contains an update column which is self incremented.
     */
    public void setSelfIncrementedUpdateColumn(
            boolean selfIncrementedUpdateColumn) {
        _selfIncrementedUpdateColumn = selfIncrementedUpdateColumn;
    }

    /**
     * Gets whether this query is forced to be executed under transaction. If the query contains a
     * column which is self incremented a TXN should be created for emulating compare & set.
     */
    public boolean isForceUnderTransaction() {
        return _selfIncrementedUpdateColumn;
    }

    @Override
    public BatchResponsePacket executePreparedValuesBatch(ISpaceProxy space,
                                                          Transaction transaction, PreparedValuesCollection preparedValuesCollection)
            throws SQLException {
        if (byUid)
            return super.executePreparedValuesBatch(space, transaction, preparedValuesCollection);
        return executeBatch(space, transaction, preparedValuesCollection);
    }

    /**
     * Batch execution optimization.
     */
    private BatchResponsePacket executeBatch(ISpaceProxy space,
                                             Transaction transaction, PreparedValuesCollection preparedValuesCollection) throws SQLException {
        _executor = new QueryExecutor(this);
        ArrayList<IEntryPacket[]> readResult = new ArrayList<IEntryPacket[]>();

        // Save the current ExpressionTree.
        // When used we'll create a clone only for the ExpressionTree.
        ExpNode tempExpTree = expTree;

        try {
            if (getSecurityInterceptor() != null) {
                SpaceContext spaceContext = getSession().getConnectionContext().getSpaceContext();
                SecurityContext securityContext = SpaceContextHelper.getSecurityContext(spaceContext);
                getSecurityInterceptor().intercept(securityContext, SpacePrivilege.WRITE, getTableName());
            }

            validateUpdateColumns();

            //we need to know the indices of the fields that are to be set
            //so we'll build an array of those indices.
            ITypeDesc info = getTypeInfo();

            //search each required column for update in the m_FieldsNames member
            int[] indices = new int[getUpdatedColumns().size()];
            int j = 0;
            for (UpdateColumn updateColumn : _updatedColumns) {
                for (int i = 0; i < info.getNumOfFixedProperties(); i++) {
                    if (info.getFixedProperty(i).getName().equalsIgnoreCase(updateColumn.getName())) {
                        indices[j] = i;
                        j++;
                        break;
                    }
                }
            }

            for (Object[] preparedValues : preparedValuesCollection.getBatchValues()) {
                // Set the prepared values
                int preparedValueIndex = 0;
                for (LiteralNode literalNode : updatedValues) {
                    if (literalNode.isPreparedValue()) {
                        literalNode.setValue(preparedValues[preparedValueIndex++]);
                    }
                }

                // If there's a WHERE clause, use the expression's tree clone
                // and set the prepared values.
                if (expTree != null) {
                    expTree = (ExpNode) tempExpTree.clone();
                    expTree.prepareValues(preparedValues);
                    setPreparedValues(preparedValues);
                    if (containsSubQueries()) {
                        executeSubQueries(space, transaction);
                    }
                }

                build();

                //for optimistic locking, make a read with no transaction
                IQueryResultSet<IEntryPacket> entries;
                if (expTree != null) {
                    entries = _executor.execute(space, transaction, getReadModifier(), Integer.MAX_VALUE);
                } else {
                    //no where clause, read everything
                    entries = _executor.readAll(space, transaction);
                }

                filterByRownum(entries);

                Object[] convertedUpdateValues = new Object[updatedValues.size()];
                j = 0;
                for (UpdateColumn updateColumn : _updatedColumns) {
                    convertedUpdateValues[j] = updatedValues.get(j).getConvertedObject(info, updateColumn.getName());
                    j++;
                }

                IEntryPacket[] readEntryPackets = entries.toArray(new IEntryPacket[entries.size()]);

                //now update the entries with the requested values
                //entries for update
                //take one entry for its structure
                for (IEntryPacket entry : readEntryPackets) {
                    for (int i = 0; i < indices.length; i++) {
                        if (_updatedColumns.get(i).isSelfIncremented()) {
                            try {
                                entry.setFieldValue(
                                        indices[i],
                                        NumberUtil.add(
                                                (Number) entry.getFieldValue(indices[i]),
                                                (Number) convertedUpdateValues[i],
                                                info.getFixedProperty(i).getTypeName()));
                            } catch (ClassCastException e) {
                                throw new SQLException("Operator '+' is only allowed for numeric column types.");
                            }
                        } else {
                            entry.setFieldValue(indices[i], convertedUpdateValues[i]);
                        }
                    }
                }

                readResult.add(readEntryPackets);
            }

        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.getMessage(), e);
            }
            SQLException se = new SQLException("Failed to update row; Cause: " + e, "GSP", -130);
            se.initCause(e);
            throw se;

        } finally {
            _executor = null;
        }

        // Gather results to a single entries array
        ArrayList<IEntryPacket> originalEntries = new ArrayList<IEntryPacket>();
        for (IEntryPacket[] updatedEntryPackets : readResult) {
            for (int i = 0; i < updatedEntryPackets.length; i++) {
                originalEntries.add(updatedEntryPackets[i]);
            }
        }

        // Write the updated entries back to the space
        try {
            if (originalEntries.size() > 0)
                space.writeMultiple(originalEntries.toArray(), transaction, 0, null, 0, UpdateModifiers.UPDATE_ONLY);
        } catch (WriteMultipleException e) {
            int[] result = new int[readResult.size()];
            int batchIndex = 0;
            int entryIndex = 0;
            for (IWriteResult writeResult : e.getResults()) {
                while (entryIndex == readResult.get(entryIndex).length) {
                    batchIndex++;
                    entryIndex = 0;
                }
                if (!writeResult.isError())
                    result[batchIndex] = Statement.EXECUTE_FAILED;
                if (result[batchIndex] != Statement.EXECUTE_FAILED)
                    result[batchIndex]++;
                entryIndex++;
            }
            throw new BatchUpdateException(e.getMessage(), result);
        } catch (Exception e) {
            int[] result = new int[readResult.size()];
            for (int i = 0; i < result.length; i++)
                result[i] = Statement.EXECUTE_FAILED;
            throw new BatchUpdateException(e.getMessage(), result);
        }

        int[] result = new int[readResult.size()];
        int resultIndex = 0;
        for (IEntryPacket[] updatedEntryPackets : readResult) {
            result[resultIndex++] = updatedEntryPackets.length;
        }
        return new BatchResponsePacket(result);
    }


}
