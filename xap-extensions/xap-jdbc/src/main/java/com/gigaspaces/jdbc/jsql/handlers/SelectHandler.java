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
package com.gigaspaces.jdbc.jsql.handlers;

import com.gigaspaces.jdbc.JoinQueryExecutor;
import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.gigaspaces.jdbc.model.table.TempTableContainer;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class SelectHandler extends SelectVisitorAdapter implements FromItemVisitor {
    private final QueryExecutor queryExecutor;

    public SelectHandler(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }
    @Override
    public void visit(PlainSelect plainSelect) {
        plainSelect.getFromItem().accept(this);
        if (plainSelect.getJoins() != null) {
            plainSelect.getJoins().forEach(this::handleJoin);
        }
        prepareQueryColumns(plainSelect);
        prepareWhereClause(plainSelect);
        prepareOrderByClause(plainSelect);
        prepareGroupByClause(plainSelect);
        prepareDistinctClause(plainSelect);
    }

    private void handleJoin(Join join){
        Expression onExpression = join.getOnExpression();
        if(onExpression instanceof EqualsTo) {
            if (! (join.getRightItem() instanceof Table)) {
                throw new UnsupportedOperationException("Join is currently supported with concrete tables only");
            }
            Table rTable = (Table) join.getRightItem();
            List<TableContainer> tables = queryExecutor.getTables();
            tables.add(createTableContainer(rTable));
            Column rColumn = (Column) ((EqualsTo) onExpression).getRightExpression();
            Column lColumn = (Column) ((EqualsTo) onExpression).getLeftExpression();
            if(!rColumn.getTable().getName().equals(rTable.getAlias().getName())){ // in case on condition columns are reversed A JOIN B ON B.id = A.id
                Column tmp = rColumn;
                rColumn = lColumn;
                lColumn = tmp;
            }
            TableContainer rightTable = QueryColumnHandler.getTableForColumn(rColumn, tables);
            TableContainer leftTable = QueryColumnHandler.getTableForColumn(lColumn, tables);
            IQueryColumn rightColumn = rightTable.addQueryColumn(rColumn.getColumnName(), null, false, -1);
            IQueryColumn leftColumn = leftTable.addQueryColumn(lColumn.getColumnName(), null, false, -1);
            rightTable.setJoinInfo(new JoinInfo(leftColumn, rightColumn, JoinInfo.JoinType.getType(join)));
            if (leftTable.getJoinedTable() == null) { // TODO set right table every time and align it to recursive form in JoinTablesIterator
                if (!rightTable.isJoined()) {
                    leftTable.setJoinedTable(rightTable);
                    rightTable.setJoined(true);
                }
            }
        }
        else {
            throw new UnsupportedOperationException("Only simple ON clause is supported");
        }
    }

    private void prepareQueryColumns(PlainSelect plainSelect) {
        QueryColumnHandler queryColumnHandler = new QueryColumnHandler(queryExecutor);
        plainSelect.getSelectItems().forEach(selectItem -> selectItem.accept(queryColumnHandler));
    }

    private void prepareWhereClause(PlainSelect plainSelect) {
        if (plainSelect.getWhere() != null) {
            WhereHandler expressionVisitor = new WhereHandler(queryExecutor.getTables(), queryExecutor.getPreparedValues());
            plainSelect.getWhere().accept(expressionVisitor);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : expressionVisitor.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePacket(tableContainerQueryTemplatePacketEntry.getValue());
            }

            for (Map.Entry<TableContainer, Expression> tableContainerExpressionEntry : expressionVisitor.getExpTree().entrySet()) {
                tableContainerExpressionEntry.getKey().setExpTree(tableContainerExpressionEntry.getValue());
            }
        }
    }

    private void prepareOrderByClause(PlainSelect plainSelect) {
        if (plainSelect.getOrderByElements() != null) {
            OrderByHandler orderByVisitor = new OrderByHandler(queryExecutor);
            plainSelect.getOrderByElements().forEach(orderByElement -> orderByElement.accept(orderByVisitor));
        }
    }

    private void prepareGroupByClause(PlainSelect plainSelect) {
        if (plainSelect.getGroupBy() != null) {
            GroupByHandler groupByVisitor = new GroupByHandler(queryExecutor);
            plainSelect.getGroupBy().accept( groupByVisitor );
        }
    }

    private void prepareDistinctClause(PlainSelect plainSelect) {

        if (plainSelect.getDistinct() != null) {
            List<SelectItem> items = plainSelect.getSelectItems();
            if (items.get(0) instanceof AllColumns){
                for (TableContainer table : queryExecutor.getTables()){
                    if(!table.getVisibleColumns().isEmpty())
                        table.setDistinct(true);
                }
            }
            else {
                for (SelectItem item : items) {
                    SelectExpressionItem sItem = (SelectExpressionItem) item;
                    QueryColumnHandler.getTableForColumn((Column) sItem.getExpression(), queryExecutor.getTables()).setDistinct(true);
                }
            }
        }
    }

    @Override
    public void visit(Table table) {
        queryExecutor.getTables().add(createTableContainer(table));
    }

    private TableContainer createTableContainer(Table table){
        return new ConcreteTableContainer(table.getFullyQualifiedName(), table.getAlias() == null ? null : table.getAlias().getName(), queryExecutor.getSpace());
    }

    @Override
    public void visit(SubSelect subSelect) {
        QueryExecutor subQueryExecutor = new QueryExecutor(queryExecutor.getSpace(), queryExecutor.getConfig(), queryExecutor.getPreparedValues());
        SelectHandler physicalPlanHandler = new SelectHandler(subQueryExecutor);
        subQueryExecutor = physicalPlanHandler.prepareForExecution(subSelect.getSelectBody());
        try {
            queryExecutor.getTables().add(new TempTableContainer(subSelect.getAlias() == null ? null : subSelect.getAlias().getName()).init(subQueryExecutor.execute()));
        } catch (SQLException e) {
            throw new SQLExceptionWrapper(e);
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

        if (queryExecutor.getTables().size() == 1) { //Simple Query
            return queryExecutor.getTables().get(0).executeRead(queryExecutor.getConfig());
        }
        JoinQueryExecutor joinE = new JoinQueryExecutor(queryExecutor);
        return joinE.execute();
    }

    public QueryExecutor prepareForExecution(SelectBody selectBody) {
        selectBody.accept(this);
        return queryExecutor;
    }
}
