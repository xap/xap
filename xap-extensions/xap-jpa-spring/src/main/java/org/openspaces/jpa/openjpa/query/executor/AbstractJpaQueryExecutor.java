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

package org.openspaces.jpa.openjpa.query.executor;

import org.apache.openjpa.kernel.exps.QueryExpressions;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.openspaces.jpa.StoreManager;
import org.openspaces.jpa.openjpa.query.ExpressionNode;

/**
 * A base implementation for JpaQueryExecutor interface.
 *
 * @author idan
 * @since 8.0
 */
abstract class AbstractJpaQueryExecutor implements JpaQueryExecutor {
    //
    protected QueryExpressions _expression;
    protected ClassMetaData _classMetaData;
    protected Object[] _parameters;
    protected StringBuilder _sql;

    protected AbstractJpaQueryExecutor(QueryExpressions expression, ClassMetaData cm, Object[] parameters) {
        _expression = expression;
        _classMetaData = cm;
        _parameters = parameters;
        build();
    }

    /**
     * Execute query.
     */
    public abstract ResultObjectProvider execute(StoreManager store) throws Exception;

    /**
     * Build query for execution.
     */
    protected void build() {
        _sql = new StringBuilder();
        appendWhereSql();
        appendGroupBySql();
        appendOrderBySql();
    }

    /**
     * Append WHERE clause to the SQL string builder.
     */
    protected void appendWhereSql() {
        ((ExpressionNode) _expression.filter).appendSql(_sql);
    }

    /**
     * Append ORDER BY to the SQL string builder.
     */
    protected void appendOrderBySql() {
        if (_expression.ordering.length > 0) {
            _sql.append(" ORDER BY ");
            for (int i = 0; i < _expression.ordering.length; ) {
                _sql.append(_expression.ordering[i].getName());
                _sql.append(_expression.ascending[i] ? " asc" : " desc");
                if (++i != _expression.ordering.length)
                    _sql.append(", ");
            }
        }
    }

    /**
     * Append GROUP BY to the SQL string builder.
     */
    protected void appendGroupBySql() {
        if (_expression.grouping.length > 0) {
            _sql.append(" GROUP BY ");
            for (int i = 0; i < _expression.grouping.length; ) {
                _sql.append(_expression.grouping[i].getName());
                if (++i != _expression.grouping.length)
                    _sql.append(", ");
            }
        }
    }

    /**
     * Gets the executor's generated SQL buffer.
     */
    public StringBuilder getSqlBuffer() {
        return _sql;
    }


}
