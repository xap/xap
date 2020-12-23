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
import com.j_spaces.jdbc.parser.AndNode;
import com.j_spaces.jdbc.parser.ExpNode;
import net.jini.core.transaction.Transaction;

import java.sql.SQLException;

@com.gigaspaces.api.InternalApi
public class Join implements Query, Cloneable {

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL;

        public static JoinType parse(String s) {
            // alias support can be added here if needed.
            return valueOf(s.toUpperCase());
        }
    }

    private final JoinType joinType;
    private final Query subQuery;
    private final ExpNode onExpression;
    private final String tableName;
    private final String alias;

    /*
     * Exactly one of col, query have to be null
     */
    public Join(String joinType, Query subQuery, ExpNode onExpression, String tableName, String alias) {
        this.joinType = JoinType.parse(joinType);
        this.subQuery = subQuery;
        this.onExpression = onExpression;
        this.tableName = tableName;
        this.alias = alias;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAlias() {
        return alias;
    }

    public ExpNode getOnExpression() {
        return onExpression;
    }

    public Query getSubQuery() {
        return subQuery;
    }

    public ExpNode applyOnExpression(ExpNode expNode) {
        return expNode == null ? onExpression : new AndNode(onExpression, expNode);
    }

    @Override
    public ResponsePacket executeOnSpace(ISpaceProxy space, Transaction txn) throws SQLException {
        return null;
    }

    @Override
    public void validateQuery(ISpaceProxy space) throws SQLException {

    }

    @Override
    public void setSession(QuerySession session) {

    }

    @Override
    public void build() throws SQLException {

    }

    @Override
    public boolean isPrepared() {
        return false;
    }

    @Override
    public void setSecurityInterceptor(SecurityInterceptor securityInterceptor) {

    }

    @Override
    public boolean isForceUnderTransaction() {
        return false;
    }

    @Override
    public boolean containsSubQueries() {
        return false;
    }
}
