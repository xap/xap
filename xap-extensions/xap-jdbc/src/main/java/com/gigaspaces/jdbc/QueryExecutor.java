package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.exceptions.ExecutionException;
import com.gigaspaces.jdbc.handlers.QueryColumnHandler;
import com.gigaspaces.jdbc.handlers.WhereHandler;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.join.JoinType;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.gigaspaces.jdbc.model.table.TempTableContainer;
import com.j_spaces.core.IJSpace;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryExecutor extends SelectVisitorAdapter implements FromItemVisitor {
    private final List<TableContainer> tables = new ArrayList<>();
    private final List<QueryColumn> queryColumns = new ArrayList<>();
    private final IJSpace space;
    private final QueryExecutionConfig config;
    private final Object[] preparedValues;

    public QueryExecutor(IJSpace space, QueryExecutionConfig config, Object[] preparedValues) {
        this.space = space;
        this.config = config;
        this.preparedValues = preparedValues;
    }

    public QueryExecutor(IJSpace space, Object[] preparedValues) {
        this(space, new QueryExecutionConfig(), preparedValues);
    }

    @Override
    public void visit(PlainSelect plainSelect) {
        plainSelect.getFromItem().accept(this);
        if (plainSelect.getJoins() != null) {
            plainSelect.getJoins().forEach(this::handleJoin);
        }
        prepareQueryColumns(plainSelect);
        prepareWhereClause(plainSelect);
    }

    private void handleJoin(Join join){
        Expression onExpression = join.getOnExpression();
        if(onExpression instanceof EqualsTo) {
            Table rTable = (Table) join.getRightItem();
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
            QueryColumn rightColumn = rightTable.addQueryColumn(rColumn.getColumnName(), null, false);
            QueryColumn leftColumn = leftTable.addQueryColumn(lColumn.getColumnName(), null, false);
            rightTable.setJoinInfo(new JoinInfo(leftColumn, rightColumn, JoinType.getType(join)));
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
        QueryColumnHandler visitor = new QueryColumnHandler(this);
        plainSelect.getSelectItems().forEach(selectItem -> selectItem.accept(visitor));
    }

    private void prepareWhereClause(PlainSelect plainSelect) {
        if (plainSelect.getWhere() != null) {
            WhereHandler expressionVisitor = new WhereHandler(this.getTables(), preparedValues);
            plainSelect.getWhere().accept(expressionVisitor);
            for (Map.Entry<TableContainer, QueryTemplatePacket> tableContainerQueryTemplatePacketEntry : expressionVisitor.getQTPMap().entrySet()) {
                tableContainerQueryTemplatePacketEntry.getKey().setQueryTemplatePackage(tableContainerQueryTemplatePacketEntry.getValue());
            }
        }
    }


    @Override
    public void visit(Table table) {
        tables.add(createTableContainer(table));
    }

    private TableContainer createTableContainer(Table table){
        return new ConcreteTableContainer(table.getFullyQualifiedName(), table.getAlias() == null ? null : table.getAlias().getName(), space);
    }

    @Override
    public void visit(SubSelect subSelect) {
        QueryExecutor subQueryExecutor = new QueryExecutor(space, config, preparedValues);
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
