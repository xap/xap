package com.j_spaces.jdbc;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.security.service.SecurityInterceptor;
import com.j_spaces.jdbc.parser.AndNode;
import com.j_spaces.jdbc.parser.ExpNode;
import net.jini.core.transaction.Transaction;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;

@com.gigaspaces.api.InternalApi
public class Join implements Query, Cloneable, Externalizable {
    private static final long serialVersionUID = 1L;

    public enum JoinType {
        INNER, LEFT, RIGHT, FULL;

        public static JoinType parse(String s) {
            // alias support can be added here if needed.
            return valueOf(s.toUpperCase());
        }

        public static byte toCode(JoinType joinType) {
            if (joinType == null)
                return 0;
            switch (joinType) {
                case INNER: return 1;
                case LEFT: return 2;
                case RIGHT: return 3;
                case FULL: return 4;
                default: throw new IllegalArgumentException("Unsupported join type: " + joinType);
            }
        }

        public static JoinType fromCode(byte code) {
            switch (code) {
                case 0: return null;
                case 1: return INNER;
                case 2: return LEFT;
                case 3: return RIGHT;
                case 4: return FULL;
                default: throw new IllegalArgumentException("Unsupported join code: " + code);
            }
        }
    }

    private JoinType joinType;
    private Query subQuery;
    private ExpNode onExpression;
    private String tableName;
    private String alias;

    public Join() {
    }


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


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeRepetitiveString(out, tableName);
        IOUtils.writeRepetitiveString(out, alias);
        IOUtils.writeObject(out, subQuery);
        IOUtils.writeObject(out, onExpression);
        out.writeByte(JoinType.toCode(joinType));
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        tableName = IOUtils.readRepetitiveString(in);
        alias = IOUtils.readRepetitiveString(in);
        subQuery = IOUtils.readObject(in);
        onExpression = IOUtils.readObject(in);
        joinType = JoinType.fromCode(in.readByte());
    }
}
