package com.j_spaces.jdbc;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.security.service.SecurityInterceptor;
import com.j_spaces.jdbc.parser.ExpNode;
import net.jini.core.transaction.Transaction;

import java.sql.SQLException;

@com.gigaspaces.api.InternalApi
public class Join implements Query, Cloneable {

    private String joinType;
    private Query query;
    private ExpNode onExpression;
    private String alias;

    public Join(String joinType, Query query, ExpNode onExpression, String alias) {
        this.joinType = joinType;
        this.query = query;
        this.onExpression = onExpression;
        this.alias = alias;
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
