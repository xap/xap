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
import com.gigaspaces.security.service.SecurityInterceptor;

import net.jini.core.transaction.Transaction;

import java.sql.SQLException;

/**
 * This class handles the COMMIT or ROLLBACK logic. basically it just calls commit or rollback on
 * the given transaction if it exists.
 *
 * @author Michael Mitrani, 2Train4, 2004
 */
@com.gigaspaces.api.InternalApi
public class CommitQuery implements Query {

    final private boolean isCommit;
    private QuerySession session = null;

    /**
     * @param isCommit - true for COMMIT, false for ROLLBACK
     */
    public CommitQuery(boolean isCommit) {
        this.isCommit = isCommit;
    }

    public boolean isCommit() {
        return isCommit;
    }

    public boolean isRollback() {
        return !isCommit;
    }

    public QuerySession getSession() {
        return session;
    }

    public void setSession(QuerySession session) {
        this.session = session;
    }

    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn) throws SQLException {
        ResponsePacket packet = new ResponsePacket();
        try {
            if (txn != null) {
                if (isCommit) {
                    if (session.isAutoCommit())
                        throw new SQLException("Cannot commit an autocommit connection", "GSP", -139);

                    txn.commit(Long.MAX_VALUE);
                } else {
                    if (session.isAutoCommit())
                        throw new SQLException("Cannot rollback an autocommit connection", "GSP", -140);

                    txn.abort(Long.MAX_VALUE);
                }
            }
            //anyways
            session.setSelectedForUpdate(null);
            session.setTransaction(null);
            packet.setIntResult(0);
        } catch (Exception e) {
            SQLException se = new SQLException((isCommit ? "Commit" : "Rollback") + " failed; Cause: " + e);
            se.initCause(e);
            throw se;

        }
        return packet;
    }

    @Override
    public void validateQuery(ISpaceProxy space) throws SQLException {
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
        //nothing to intercept
    }

    public boolean isForceUnderTransaction() {
        return false;
    }

    @Override
    public boolean containsSubQueries() {
        return false;
    }

}
