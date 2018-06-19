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

import com.j_spaces.jdbc.driver.GConnection;
import com.j_spaces.jdbc.driver.GResultSet;

import org.apache.openjpa.kernel.exps.QueryExpressions;
import org.apache.openjpa.lib.rop.ResultObjectProvider;
import org.apache.openjpa.meta.ClassMetaData;
import org.openspaces.jpa.StoreManager;
import org.openspaces.jpa.openjpa.query.ExpressionNode;
import org.openspaces.jpa.openjpa.query.ExpressionNode.NodeType;
import org.openspaces.jpa.openjpa.query.SpaceProjectionResultObjectProvider;

import java.sql.PreparedStatement;

/**
 * Executes JPA's translated expression tree as a JDBC query.
 *
 * @author idan
 * @since 8.0
 */
public class JpaJdbcQueryExecutor extends AbstractJpaQueryExecutor {

    public JpaJdbcQueryExecutor(QueryExpressions expression, ClassMetaData cm, Object[] parameters) {
        super(expression, cm, parameters);
    }

    @Override
    public ResultObjectProvider execute(StoreManager store) throws Exception {
        GConnection conn = store.getJdbcConnection();
        if (store.getCurrentTransaction() == null) {
            conn.setAutoCommit(true);
        } else {
            conn.setAutoCommit(false);
            conn.setTransaction(store.getCurrentTransaction());
        }
        PreparedStatement pstmt = conn.prepareStatement(_sql.toString());
        for (int i = 0; i < _parameters.length; i++) {
            pstmt.setObject(i + 1, _parameters[i]);
        }
        GResultSet rs = (GResultSet) pstmt.executeQuery();
        return new SpaceProjectionResultObjectProvider(rs.getResult().getFieldValues());
    }

    @Override
    protected void build() {
        _sql = new StringBuilder();
        appendSelectFromSql();
        appendWhereSql();
        appendGroupBySql();
        appendOrderBySql();
    }

    @Override
    protected void appendWhereSql() {
        ExpressionNode node = (ExpressionNode) _expression.filter;
        if (node.getNodeType() != NodeType.EMPTY_EXPRESSION) {
            _sql.append("WHERE ");
            super.appendWhereSql();
        }
    }

    /**
     * Append SELECT FROM to SQL string builder.
     */
    protected void appendSelectFromSql() {
        _sql.append("SELECT ");
        for (int i = 0; i < _expression.projections.length; i++) {
            ExpressionNode node = (ExpressionNode) _expression.projections[i];
            node.appendSql(_sql);
            if (i + 1 == _expression.projections.length)
                _sql.append(" ");
            else
                _sql.append(", ");
        }
        _sql.append("FROM ");
        _sql.append(_classMetaData.getDescribedType().getName());
        _sql.append(" ");
    }

}
