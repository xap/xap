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
 * The Drop table implementation.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class DropTableQuery implements Query {

    private String tableName;

    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_QUERY);

    private QuerySession session;

    private SecurityInterceptor securityInterceptor;

    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn) throws SQLException {
        ResponsePacket response = new ResponsePacket();
        try {
            if (getSecurityInterceptor() != null) {
                SpaceContext spaceContext = getSession().getConnectionContext().getSpaceContext();
                SecurityContext securityContext = SpaceContextHelper.getSecurityContext(spaceContext);
                getSecurityInterceptor().intercept(securityContext, SpacePrivilege.ALTER, tableName);
            }

            space.dropClass(tableName);

            response.setIntResult(0);
        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.getMessage(), e);
            }
            SQLException se = new SQLException("Failed to drop table [" + tableName + "]; Cause: " + e, "GSP", -107);
            se.initCause(e);
            throw se;
        }
        return response;
    }

    @Override
    public void validateQuery(ISpaceProxy space) throws SQLException {
        SQLUtil.checkTableExistence(tableName, space);
    }

    public void setSession(QuerySession session) {
        this.session = session;
    }

    public QuerySession getSession() {
        return session;
    }

    public void setTableName(String table) {
        tableName = table;
    }

    public void build()
            throws SQLException {
        // TODO Auto-generated method stub

    }

    public boolean isPrepared() {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * @see com.j_spaces.jdbc.Query#setSecurityInterceptor(com.gigaspaces.security.service.SecurityInterceptor)
     */
    public void setSecurityInterceptor(SecurityInterceptor securityInterceptor) {
        this.securityInterceptor = securityInterceptor;
    }

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
