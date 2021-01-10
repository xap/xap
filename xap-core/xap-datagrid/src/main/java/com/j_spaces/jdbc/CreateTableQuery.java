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
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;
import com.gigaspaces.security.service.SecurityContext;
import com.gigaspaces.security.service.SecurityInterceptor;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceContextHelper;
import com.j_spaces.core.client.ExternalEntry;

import net.jini.core.transaction.Transaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class handles the CREATE TABLE logic.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class CreateTableQuery implements Query {
    private static final long serialVersionUID = 1L;

    private String tableName;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);
    private String[] _columnNames;
    private String[] _columnTypes;
    private boolean[] _indices;
    private String _routingFieldName;

    private QuerySession session;
    private SecurityInterceptor securityInterceptor;

    public CreateTableQuery() {
    }

    public CreateTableQuery(String tableName, ITypeDesc typeDesc, boolean[] extraIndices) {
        this.tableName = tableName;

        _columnNames = typeDesc.getPropertiesNames();
        _columnTypes = typeDesc.getPropertiesTypes();
        _indices = typeDesc.getPropertiesIndexTypes();
        _routingFieldName = typeDesc.getRoutingPropertyName();

        for (int i = 0; i < _indices.length; i++)
            _indices[i] = _indices[i] || extraIndices[i];
    }

    /**
     * Sets the routing field name for the created table.
     */
    public void setRoutingFieldName(String routingFieldName) {
        _routingFieldName = routingFieldName;
    }

    /**
     * Gets the routing field name for the created table.
     */
    public String getRoutingFieldName() {
        return _routingFieldName;
    }

    public void setTableName(String table) {
        tableName = table;
    }

    public void setSession(QuerySession session) {
        this.session = session;
    }

    public QuerySession getSession() {
        return session;
    }

    /**
     * The main logic method, execute the query on the space by calling snapshot
     */
    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn) throws SQLException {
        ResponsePacket response = new ResponsePacket();
        try {

            if (getSecurityInterceptor() != null) {
                SpaceContext spaceContext = getSession().getConnectionContext().getSpaceContext();
                SecurityContext securityContext = SpaceContextHelper.getSecurityContext(spaceContext);
                getSecurityInterceptor().intercept(securityContext, SpacePrivilege.ALTER, tableName);
            }

            ExternalEntry template = new ExternalEntry(tableName, null, null);
            template.setFieldsNames(_columnNames);
            template.setFieldsTypes(_columnTypes);
            template.setIndexIndicators(_indices);
            template.setRoutingFieldName(_routingFieldName);

            space.snapshot(template);
            if (_logger.isDebugEnabled()) {
                _logger.debug("Wrote a new table to space [" + tableName + "]");
            }
            response.setIntResult(0);

        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.getMessage(), e);
            }

            SQLException se = new SQLException("Failed to create table [" + tableName + "]; Cause: " + e, "GSP", -106);
            se.initCause(e);
            throw se;
        }

        return response;
    }

    @Override
    public void validateQuery(ISpaceProxy space) throws SQLException {
        ITypeDesc typeDesc = space.getDirectProxy().getTypeManager().getTypeDescIfExistsInProxy(tableName);
        if (typeDesc != null)
            throw new SQLException("Table [" + tableName + "] already exists", "GSP", -118);
    }

    public void build()
            throws SQLException {
        // TODO Auto-generated method stub

    }

    public boolean isPrepared() {
        // TODO Auto-generated method stub
        return false;
    }

    public void setColumnNames(String[] columnNames) {
        _columnNames = columnNames;
    }

    public void setColumnTypes(String[] columnTypes) {
        _columnTypes = columnTypes;
    }

    public void setIndices(boolean[] indices) {
        _indices = indices;
    }

    /*
     * @see com.j_spaces.jdbc.Query#setSecurityInterceptor(com.gigaspaces.security.service.SecurityInterceptor)
     */
    public void setSecurityInterceptor(SecurityInterceptor securityInterceptor) {
        this.securityInterceptor = securityInterceptor;
    }

    /**
     * @return the securityInterceptor
     */
    public SecurityInterceptor getSecurityInterceptor() {
        return securityInterceptor;
    }

    public boolean isForceUnderTransaction() {
        return false;
    }

    @Override
    public boolean containsSubQueries() {
        return false;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeRepetitiveString(out, tableName);
        IOUtils.writeStringArray(out, _columnNames);
        IOUtils.writeStringArray(out, _columnTypes);
        IOUtils.writeBooleanArray(out, _indices);
        IOUtils.writeRepetitiveString(out, _routingFieldName);
        IOUtils.writeObject(out, session);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tableName = IOUtils.readRepetitiveString(in);
        _columnNames = IOUtils.readStringArray(in);
        _columnTypes = IOUtils.readStringArray(in);
        _indices = IOUtils.readBooleanArray(in);
        _routingFieldName = IOUtils.readRepetitiveString(in);
        session = IOUtils.readObject(in);
    }
}
