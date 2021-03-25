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
package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.exceptions.ExecutionException;
import com.gigaspaces.jdbc.handlers.QueryColumnHandler;
import com.gigaspaces.jdbc.handlers.WhereHandler;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.gigaspaces.jdbc.model.table.TempTableContainer;
import com.j_spaces.core.IJSpace;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class QueryExecutor extends SelectVisitorAdapter implements FromItemVisitor {
    private final List<TableContainer> tables = new ArrayList<>();
    private final List<QueryColumn> queryColumns = new ArrayList<>();
    private final IJSpace space;
    private final QueryExecutionConfig config;

    public QueryExecutor(IJSpace space, QueryExecutionConfig config) {
        this.space = space;
        this.config = config;
    }

    public QueryExecutor(IJSpace space) {
        this(space, new QueryExecutionConfig());
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        plainSelect.getFromItem().accept(this);
        if (plainSelect.getJoins() != null)
            plainSelect.getJoins().forEach(join -> join.getRightItem().accept(this));

        prepareQueryColumns(plainSelect);
        prepareWhereClause(plainSelect);
    }

    private void prepareQueryColumns(PlainSelect plainSelect) {
        QueryColumnHandler visitor = new QueryColumnHandler(this);
        plainSelect.getSelectItems().forEach(selectItem -> selectItem.accept(visitor));
    }

    private void prepareWhereClause(PlainSelect plainSelect) {
        if (plainSelect.getWhere() != null)
            plainSelect.getWhere().accept(new WhereHandler(this));
    }


    @Override
    public void visit(Table table) {
        tables.add(new ConcreteTableContainer(table.getFullyQualifiedName(), table.getAlias() == null ? null : table.getAlias().getName(), space));
    }

    @Override
    public void visit(SubSelect subSelect) {
        QueryExecutor subQueryExecutor = new QueryExecutor(space, config);
        try {
            tables.add(new TempTableContainer(subQueryExecutor.execute(subSelect.getSelectBody()), subSelect.getAlias() == null ? null : subSelect.getAlias().getName()));
        } catch (SQLException e) {
            throw new ExecutionException("Failed to execute subquery", e);
        }

    }

    @Override
    public void visit(SubJoin subjoin) {
        throw new UnsupportedOperationException("Unsupported yet!");
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        throw new UnsupportedOperationException("Unsupported yet!");
    }

    @Override
    public void visit(ValuesList valuesList) {
        throw new UnsupportedOperationException("Unsupported yet!");
    }

    @Override
    public void visit(TableFunction tableFunction) {
        throw new UnsupportedOperationException("Unsupported yet!");
    }

    @Override
    public void visit(ParenthesisFromItem aThis) {
        throw new UnsupportedOperationException("Unsupported yet!");
    }

    public QueryResult execute(SelectBody selectBody) throws SQLException {
        prepareForExecution(selectBody);

        if (tables.size() == 1) { //Simple Query
            return tables.get(0).executeRead(config);
        }
        JoinQueryExecutor joinE = new JoinQueryExecutor(tables, space, queryColumns, config);
        return joinE.execute();
    }

    private void prepareForExecution(SelectBody selectBody) {
        selectBody.accept(this);
    }


    public List<TableContainer> getTables() {
        return tables;
    }

    public List<QueryColumn> getQueryColumns() {
        return queryColumns;
    }
}
