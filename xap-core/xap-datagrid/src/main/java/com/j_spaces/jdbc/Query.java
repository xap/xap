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
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.security.service.SecurityInterceptor;

import net.jini.core.transaction.Transaction;

import java.sql.SQLException;

/**
 * An interface that represents an SQL query.
 *
 * @author Michael Mitrani, 2Train4 - 2004
 */
public interface Query {
    /**
     * Will execute this query on the space and return a ResponsePacket
     */
    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn) throws SQLException;

    /**
     * Each query should have its own specific validations. this is the place to do them. if some
     * validation fail, an SQLException will be thrown.
     */
    public void validateQuery(ISpaceProxy space) throws SQLException;

    public void setSession(QuerySession session);

    public void build() throws SQLException;

    /**
     * @return isPrepared is this a PreparedStatment or not.
     */
    public boolean isPrepared();

    public void setSecurityInterceptor(SecurityInterceptor securityInterceptor);

    /**
     * Gets whether this query is forced to be executed under transaction.
     */
    public boolean isForceUnderTransaction();

    /**
     * Gets whether this query contains sub queries.
     */
    public boolean containsSubQueries();

    default void validateQuery(SpaceTypeManager typeManager) throws SQLException{

    }
}