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
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;
import com.gigaspaces.security.service.SecurityContext;
import com.gigaspaces.security.service.SecurityInterceptor;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceContextHelper;

import net.jini.core.transaction.Transaction;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ALTER TABLE implementation. Currently the only syntax supported is: ALTER TABLE table_name
 * ADD PRIMARY KEY(column_name) USING INDEX
 *
 * The command above will cause table_name to be dropped with all its data and then recreated using
 * the given columns as indices.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class AlterTableQuery implements Query {

    private String tableName;
    private String[] indices;
    private SecurityInterceptor securityInterceptor;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);
    private QuerySession session;

    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn) throws SQLException {
        try {
            if (getSecurityInterceptor() != null) {
                SpaceContext spaceContext = getSession().getConnectionContext().getSpaceContext();
                SecurityContext securityContext = SpaceContextHelper.getSecurityContext(spaceContext);
                getSecurityInterceptor().intercept(securityContext, SpacePrivilege.ALTER, tableName);
            }

            //we need to get the info of the table, drop it, and recreate it.
            ITypeDesc info = SQLUtil.checkTableExistence(tableName, space);

            int numOfProperties = info.getNumOfFixedProperties();
            boolean[] extraIndices = new boolean[numOfProperties];
            for (int i = 0; i < indices.length; i++) {
                for (int j = 0; j < numOfProperties; j++) {
                    if (info.getFixedProperty(j).getName().equals(indices[i])) {
                        extraIndices[j] = true;
                        break;
                    }
                }
            }


            DropTableQuery drop = new DropTableQuery();
            drop.setTableName(tableName);
            drop.executeOnSpace(space, txn);

            CreateTableQuery create = new CreateTableQuery(tableName, info, extraIndices);
            ResponsePacket packet = create.executeOnSpace(space, txn);

            return packet;

        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.getMessage(), e);
            }
            SQLException se = new SQLException("Can't alter table; Cause: " + e, "GSP", -101);
            se.initCause(e);
            throw se;

        }
    }

    @Override
    public void validateQuery(ISpaceProxy space) throws SQLException {
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

    public void setIndices(String[] indices) {
        this.indices = indices;
    }

    public void build()
            throws SQLException {
    }

    public boolean isPrepared() {
        return false;
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

}
