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
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;
import com.gigaspaces.security.service.SecurityContext;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceContextHelper;
import com.j_spaces.core.client.ClientUIDHandler;
import com.j_spaces.core.client.ExternalEntry;
import com.j_spaces.jdbc.batching.BatchResponsePacket;
import com.j_spaces.jdbc.driver.GPreparedStatement.PreparedValuesCollection;
import com.j_spaces.jdbc.driver.ObjectWithUID;
import com.j_spaces.jdbc.parser.LiteralNode;

import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * This class handles the INSERT query logic.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class InsertQuery extends AbstractDMLQuery {
    //the values to insert can either come from a VALUES clause, or
    //from an inner select
    private ArrayList<LiteralNode> values; //an array of the values to insert
    private AbstractDMLQuery innerQuery = null;

    public InsertQuery() {
        super();
    }

    public void setQuery(AbstractDMLQuery query) {
        innerQuery = query;
    }

    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn)
            throws SQLException {
        ResponsePacket packet = new ResponsePacket();
        try {
            if (getSecurityInterceptor() != null) {
                SpaceContext spaceContext = getSession().getConnectionContext().getSpaceContext();
                SecurityContext securityContext = SpaceContextHelper.getSecurityContext(spaceContext);
                getSecurityInterceptor().intercept(securityContext, SpacePrivilege.WRITE, getTableName());
            }

            ITypeDesc info = getTypeInfo();

            //if there is no inner query to take values from
            if (innerQuery == null) {
                // Handle prepared values
                if (isPrepared) {

                    // Set given prepared values for prepared nodes
                    int preparedValueIndex = 0;
                    for (LiteralNode literalNode : values) {
                        if (literalNode.isPreparedValue()) {
                            literalNode.setValue(preparedValues[preparedValueIndex++]);
                        }
                    }

                } else {
                    if (values == null || (getQueryColumns() != null && (values.size() != getQueryColumns().size()))) {
                        throw new SQLException("All values must be set ", "GSP", -126);
                    }
                }

                String[] columnNames;
                if (getQueryColumns() != null) {
                    columnNames = new String[getQueryColumns().size()];
                    getQueryColumns().toArray(columnNames);
                } else {
                    columnNames = info.getPropertiesNames();
                    if (columnNames.length != values.size())
                        throw new SQLException("All values must be set", "GSP", -126);
                }
                String myUid = null;
                String[] types = new String[values.size()];
                Object[] literalValues = new Object[values.size()];

                for (int i = 0; i < values.size(); i++) {
                    LiteralNode literalNode = values.get(i);
                    types[i] = (literalNode.getValue() == null) ? info.getFixedProperty(i).getTypeName() : literalNode.getValue().getClass().getName();
                    if (literalNode.getValue() != null &&
                            (types[i].endsWith(".Blob") || types[i].endsWith(".Clob"))) {
                        if (myUid == null)
                            myUid = ClientUIDHandler.createUIDFromName(String.valueOf(Math.random()), types[i]);

                        ((ObjectWithUID) literalNode.getValue()).setEntryUID(myUid);
                        ((ObjectWithUID) literalNode.getValue()).setObjectIndex(i);
                    }

                    if (!info.getFixedProperty(columnNames[i]).getTypeName().equals(types[i])) {
                        try {
                            // Insert the LiteralNode's value to the values array
                            literalValues[i] = literalNode.getConvertedObject(info, columnNames[i]);
                        } catch (Exception e) {
                            SQLException ex = new SQLException("Wrong type for given column " +
                                    info.getFixedProperty(i).getName() + " " +
                                    info.getFixedProperty(i).getTypeName() + " vs " + types[i], "GSP", -127);
                            ex.initCause(e);
                            throw ex;
                        }
                    } else {
                        // Insert the LiteralNode's value to the values array
                        literalValues[i] = literalNode.getValue();
                    }
                }

                ExternalEntry entry = new ExternalEntry(getTableName(), literalValues, columnNames);
                entry.m_FieldsTypes = types;
                if (myUid != null)
                    entry.setUID(myUid);
                space.write(entry, txn, QueryProcessor.getDefaultConfig().getWriteLease());
                packet.setIntResult(1); //wrote 1 entry
            }
            //else, there is an inner query
            else {
                //we need to execute the inner query, and check if the result
                //matches in type to the insert columns.
                if (innerQuery.containsSubQueries())
                    throw new IllegalArgumentException("INSERT statement with a SELECT statement containing sub queries is not supported");

                innerQuery.setSession(getSession());
                if (innerQuery.isPrepared)
                    innerQuery.setPreparedValues(preparedValues);
                innerQuery.build();
                ResponsePacket innerResponse = innerQuery.executeOnSpace(space, txn);

                ResultEntry innerResult = innerResponse.getResultEntry();
                if (innerResult == null) {
                    //nothing returned from the inner select
                    //so nothing to insert
                    packet.setIntResult(0);
                    return packet;
                }

                int[] indices;
                String[] types;

                if (getQueryColumns() != null) {
                    if (getQueryColumns().size() != innerResult.getFieldNames().length) {
                        throw new SQLException("Incorrect number of values to insert",
                                "GSP", -128);

                    }
                    indices = new int[getQueryColumns().size()];
                    types = new String[getQueryColumns().size()];
                    Iterator updateColumnIter = getQueryColumns().iterator();
                    int j = 0;
                    //search each required column for update in the m_FieldsNames member
                    while (updateColumnIter.hasNext()) {
                        String columnName = (String) updateColumnIter.next();
                        for (int i = 0; i < info.getNumOfFixedProperties(); i++) {
                            if (info.getFixedProperty(i).getName().equalsIgnoreCase(columnName)) {
                                indices[j] = i;
                                types[j] = info.getFixedProperty(i).getTypeName();
                                j++;
                                break;
                            }
                        }
                    }
                } else { //no columns, fill all values
                    if (info.getNumOfFixedProperties() != innerResult.getFieldNames().length) {
                        throw new SQLException("Incorrect number of values to insert", "GSP", -128);
                    }

                    indices = new int[info.getNumOfFixedProperties()];
                    types = new String[info.getNumOfFixedProperties()];
                    for (int i = 0; i < indices.length; i++) {
                        indices[i] = i;
                        types[i] = info.getFixedProperty(i).getTypeName();
                    }
                }

                //we need to validate the types
                for (int i = 0; i < indices.length; i++) {
                    if (!info.getFixedProperty(indices[i]).getTypeName().equals(
                            innerResult.getFieldValues(1)[i].getClass().getName())) {
                        throw new SQLException("Type mismatch in nested query", "GSP", -129);
                    }
                }

                //if we got here, we're fine. now take the full result from the
                //inner select and insert the values.
                ExternalEntry[] entriesToInsert = new ExternalEntry[innerResult.getRowNumber()];
                String[] columnNames = new String[indices.length];
                for (int i = 0; i < indices.length; i++) {
                    columnNames[i] = info.getFixedProperty(indices[i]).getName();
                }

                for (int i = 0; i < entriesToInsert.length; i++) {
                    ExternalEntry newEntry = new ExternalEntry(getTableName(),
                            innerResult.getFieldValues(i + 1),
                            columnNames);
                    newEntry.m_FieldsTypes = types;
                    entriesToInsert[i] = newEntry;
                }
                //now write them all in one action
                Lease[] leases = space.writeMultiple(entriesToInsert, txn, QueryProcessor.getDefaultConfig().getWriteLease());
                packet.setIntResult(leases.length); //wrote some entries
            }
        } catch (Exception e) {

            SQLException ex = new SQLException("Failed to insert; Cause: " + e, "GSP", -106);
            ex.initCause(e);
            throw ex;
        }
        return packet;
    }

    /**
     * The values to insert.
     */
    public void setValues(ArrayList<LiteralNode> values) {
        this.values = values;
    }

    @SuppressWarnings("unchecked")
    @Override
    public InsertQuery clone() {
        InsertQuery query = new InsertQuery();
        query.tables = tables;
        query._tablesData = _tablesData;
        query.isPrepared = this.isPrepared;
        if (innerQuery != null)
            query.innerQuery = (AbstractDMLQuery) this.innerQuery.clone();
        query.queryColumns = this.getQueryColumns(); //this is not a clone, but there is no need.

        if (preparedValues != null) {
            query.preparedValues = new Object[preparedValues.length];
            System.arraycopy(preparedValues, 0, query.preparedValues, 0, preparedValues.length);
        }
        if (values != null) {
            query.values = new ArrayList<LiteralNode>(values.size());
            for (LiteralNode value : values)
                query.values.add((LiteralNode) value.clone());
        }
        return query;
    }

    @Override
    public void validateQuery(ISpaceProxy space) throws SQLException {
        super.validateQuery(space);

        if (!isPrepared && innerQuery == null && (values == null ||
                (getQueryColumns() != null &&
                        (values.size() != getQueryColumns().size())))) {
            throw new SQLException("All values must be set", "GSP", -126);
        }

        if (innerQuery != null) {
            innerQuery.validateQuery(space);
        }
    }

    @Override
    public BatchResponsePacket executePreparedValuesBatch(ISpaceProxy space,
                                                          Transaction transaction, PreparedValuesCollection preparedValuesCollection) throws SQLException {
        if (innerQuery != null)
            return super.executePreparedValuesBatch(space, transaction, preparedValuesCollection);
        return executeBatch(space, transaction, preparedValuesCollection);
    }

    /**
     * Executes the batched query using writeMultiple operation.
     */
    private BatchResponsePacket executeBatch(IJSpace space,
                                             Transaction transaction, PreparedValuesCollection preparedValuesCollection) throws SQLException {

        int[] result = new int[preparedValuesCollection.size()];
        ExternalEntry[] entriesToWrite = new ExternalEntry[preparedValuesCollection.size()];

        try {
            if (getSecurityInterceptor() != null) {
                SpaceContext spaceContext = getSession().getConnectionContext().getSpaceContext();
                SecurityContext securityContext = SpaceContextHelper.getSecurityContext(spaceContext);
                getSecurityInterceptor().intercept(securityContext, SpacePrivilege.WRITE, getTableName());
            }

            ITypeDesc info = getTypeInfo();

            String[] columnNames;
            if (getQueryColumns() != null) {
                columnNames = new String[getQueryColumns().size()];
                getQueryColumns().toArray(columnNames);
            } else {
                columnNames = info.getPropertiesNames();
                if (columnNames.length != values.size())
                    throw new SQLException("All values must be set", "GSP", -126);
            }
            String myUid = null;
            String[] types = new String[values.size()];
            int batchIndex = 0;

            for (Object[] preparedValues : preparedValuesCollection.getBatchValues()) {

                Object[] literalValues = new Object[values.size()];
                int preparedValueIndex = 0;
                for (int i = 0; i < values.size(); i++) {
                    LiteralNode literalNode = values.get(i);
                    // Set given prepared values for prepared nodes
                    if (literalNode.isPreparedValue()) {
                        literalNode.setValue(preparedValues[preparedValueIndex++]);
                    }
                    types[i] = (literalNode.getValue() == null) ? info.getFixedProperty(i).getTypeName() : literalNode.getValue().getClass().getName();
                    if (literalNode.getValue() != null &&
                            (types[i].endsWith(".Blob") || types[i].endsWith(".Clob"))) {
                        if (myUid == null)
                            myUid = ClientUIDHandler.createUIDFromName(String.valueOf(Math.random()), types[i]);

                        ((ObjectWithUID) literalNode.getValue()).setEntryUID(myUid);
                        ((ObjectWithUID) literalNode.getValue()).setObjectIndex(i);
                    }

                    if (!info.getFixedProperty(columnNames[i]).getTypeName().equals(types[i])) {
                        try {
                            // Insert the LiteralNode's value to the values array
                            literalValues[i] = literalNode.getConvertedObject(info, columnNames[i]);
                        } catch (Exception e) {
                            SQLException ex = new SQLException("Wrong type for given column " +
                                    info.getFixedProperty(i).getName() + " " +
                                    info.getFixedProperty(i).getTypeName() + " vs " + types[i], "GSP", -127);
                            ex.initCause(e);
                            throw ex;
                        }
                    } else {
                        // Insert the LiteralNode's value to the values array
                        literalValues[i] = literalNode.getValue();
                    }
                }
                entriesToWrite[batchIndex] = new ExternalEntry(getTableName(), literalValues, columnNames);
                entriesToWrite[batchIndex].m_FieldsTypes = types;
                if (myUid != null)
                    entriesToWrite[batchIndex].setUID(myUid);
                batchIndex++;
            }
        } catch (Exception e) {
            SQLException ex = new SQLException("Failed to insert; Cause: " + e, "GSP", -106);
            ex.initCause(e);
            throw ex;
        }
        try {
            space.writeMultiple(entriesToWrite, transaction, QueryProcessor.getDefaultConfig().getWriteLease());
            for (int i = 0; i < result.length; i++) {
                result[i] = 1;
            }
        } catch (WriteMultipleException e) {
            for (int i = 0; i < result.length; i++) {
                result[i] = e.getResults()[i].isError() ? Statement.EXECUTE_FAILED : 1;
            }
            throw new BatchUpdateException(e.getMessage(), result);
        } catch (Exception e) {
            for (int i = 0; i < result.length; i++) {
                result[i] = Statement.EXECUTE_FAILED;
            }
            throw new BatchUpdateException(e.getMessage(), result);
        }
        return new BatchResponsePacket(result);
    }
}