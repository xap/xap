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

package org.openspaces.jpa.openjpa.query;

import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Path;
import org.apache.openjpa.kernel.exps.QueryExpressions;
import org.apache.openjpa.kernel.exps.Subquery;
import org.apache.openjpa.kernel.exps.Value;
import org.apache.openjpa.meta.ClassMetaData;
import org.openspaces.jpa.openjpa.query.executor.JpaJdbcQueryExecutor;

/**
 * Represents an inner query (subquery) in OpenJPA's expression tree.
 *
 * @author idan
 * @since 8.0
 */
public class InnerQuery implements Subquery, ExpressionNode {
    //
    private static final long serialVersionUID = 1L;
    private ClassMetaData _candidate;
    private QueryExpressions _expressions;
    private Class<?> _type;
    private String _queryAlias;

    public InnerQuery(ClassMetaData classMetaData) {
        _candidate = classMetaData;
    }

    /**
     * Return the candidate alias for this subquery.
     */
    public String getCandidateAlias() {
        return "";
    }

    /**
     * Set the candidate alias for this subquery.
     */
    public void setSubqAlias(String subqAlias) {
        _queryAlias = subqAlias;
    }

    /**
     * Return the subqAlias
     */
    public String getSubqAlias() {
        return _queryAlias;
    }

    /**
     * Set the parsed subquery.
     */
    public void setQueryExpressions(QueryExpressions query) {
        _expressions = query;
    }

    public Object getSelect() {
        return null;
    }

    /**
     * Return the expected type for this value, or <code>Object</code> if the type is unknown.
     */
    @SuppressWarnings("rawtypes")
    public Class getType() {
        if (_expressions != null && _type == null) {
            if (_expressions.projections.length == 0)
                return _candidate.getDescribedType();
            if (_expressions.projections.length == 1)
                return _expressions.projections[0].getType();
        }
        return _type;
    }

    /**
     * Set the implicit type of the value, based on how it is used in the filter. This method is
     * only called on values who return <code>Object</code> from {@link #getType}.
     */
    @SuppressWarnings("rawtypes")
    public void setImplicitType(Class type) {
        if (_expressions != null && _expressions.projections.length == 1)
            _expressions.projections[0].setImplicitType(type);
        _type = type;
    }

    /**
     * Return true if this value is a variable.
     */
    public boolean isVariable() {
        return false;
    }

    /**
     * Return true if this value is an aggregate.
     */
    public boolean isAggregate() {
        return false;
    }

    /**
     * Return true if this value is an XML Path.
     */
    public boolean isXPath() {
        return false;
    }

    /**
     * Return any associated persistent type.
     */
    public ClassMetaData getMetaData() {
        return _candidate;
    }

    /**
     * Associate a persistent type with this value.
     */
    public void setMetaData(ClassMetaData meta) {
        _candidate = meta;
    }

    /**
     * Accept a visit from a tree visitor.
     */
    public void acceptVisit(ExpressionVisitor visitor) {
        visitor.enter(this);
        _expressions.filter.acceptVisit(visitor);
        visitor.exit(this);
    }

    /**
     * Return select item alias
     */
    public String getAlias() {
        return "";
    }

    /**
     * Set select item alias
     */
    public void setAlias(String alias) {
    }

    /**
     * Return 'this' concrete class if alias is set, otherwise null
     */
    public Value getSelectAs() {
        return null;
    }

    public Path getPath() {
        return null;
    }

    public String getName() {
        return "";
    }

    public void appendSql(StringBuilder sql) {
        sql.append("(");
        // In order to create the SQL string for the inner query we create
        // a JDBC query executor which creates the needed SQL string.
        JpaJdbcQueryExecutor executor = new JpaJdbcQueryExecutor(_expressions, _candidate, null);
        sql.append(executor.getSqlBuffer());
        sql.append(")");
    }

    public NodeType getNodeType() {
        return NodeType.INNER_QUERY;
    }

}
