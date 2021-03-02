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

import com.j_spaces.jdbc.driver.GPreparedStatement.PreparedValuesCollection;

import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.transaction.TransactionException;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * The RequestPacket class. Every request from the JDBC driver is wrapped by this class. it holds a
 * byte which signals the type of the request. The available types are:
 *
 * TYPE_STATEMENT - a regular statement as a string TYPE_PREPARED_STATEMENT - a regular prepared
 * statement. a string with question marks. TYPE_PREPARED_WITH_VALUES - a prepared statement,
 * accompanied with an array of values TYPE_TOGGLE_AUTO_COMMIT - toggling autocommit on/off.
 *
 * Version 8.0: PREPARED_VALUES_BATCH
 *
 * @author Michael Mitrani - 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class RequestPacket implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;


    public static enum Type {
        @Deprecated LOGIN, STATEMENT, PREPARED_STATEMENT, PREPARED_WITH_VALUES, PREPARED_VALUES_BATCH,
        GS_STATEMENT
    }


    private Type type;
    private String statement = null;
    private Object[] preparedValues = null;
    private PreparedValuesCollection _preparedValuesCollection;

    private transient Integer modifiers;

    public Integer getModifiers() {
        return modifiers;
    }

    public void setModifiers(Integer modifiers) {
        this.modifiers = modifiers;
    }

    /**
     * @return the array of prepared values that come with a prepared statement
     */
    public Object[] getPreparedValues() {
        return preparedValues;
    }

    public void setPreparedValues(Object[] preparedValues) {
        this.preparedValues = preparedValues;
    }

    /**
     * @return - The accompanying statement/prepared statement
     */
    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * @param dmlQuery
     */
    public void build(AbstractDMLQuery dmlQuery) {
        dmlQuery.setPreparedValues(getPreparedValues());

    }

    @Override
    public String toString() {
        switch (type) {
            case STATEMENT:
            case PREPARED_STATEMENT:
                return getStatement();
            case PREPARED_WITH_VALUES:
                return getStatement() + ", values=" + Arrays.toString(preparedValues);

            default:
                return super.toString();
        }
    }

    public ResponsePacket accept(QueryHandler handler, QuerySession session) throws LeaseDeniedException, RemoteException, TransactionException, SQLException {
        return handler.visit(this, session);
    }

    public void setPreparedValuesCollection(PreparedValuesCollection preparedValuesCollection) {
        _preparedValuesCollection = preparedValuesCollection;
    }

    public PreparedValuesCollection getPreparedValuesCollection() {
        return _preparedValuesCollection;
    }

}
