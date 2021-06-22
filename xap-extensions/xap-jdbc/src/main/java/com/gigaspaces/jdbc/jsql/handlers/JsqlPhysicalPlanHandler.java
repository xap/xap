package com.gigaspaces.jdbc.jsql.handlers;

import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.exceptions.SQLExceptionWrapper;
import com.gigaspaces.jdbc.model.join.JoinInfo;
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

public class JsqlPhysicalPlanHandler extends SelectVisitorAdapter implements FromItemVisitor {
    private final QueryExecutor queryExecutor;

    public JsqlPhysicalPlanHandler(QueryExecutor queryExecutor) {
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
            IQueryColumn rightColumn = rightTable.addQueryColumn(rColumn.getColumnName(), null, false, 0);
            IQueryColumn leftColumn = leftTable.addQueryColumn(lColumn.getColumnName(), null, false, 0);
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
        QueryColumnHandler visitor = new QueryColumnHandler(queryExecutor);
        plainSelect.getSelectItems().forEach(selectItem -> selectItem.accept(visitor));
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
        try {
            subQueryExecutor = new JsqlPhysicalPlanHandler(subQueryExecutor).prepareForExecution(subSelect.getSelectBody());
            queryExecutor.getTables().add(new TempTableContainer(subQueryExecutor.execute(), subSelect.getAlias() == null ? null : subSelect.getAlias().getName()));
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

    public QueryExecutor prepareForExecution(SelectBody selectBody) {
        selectBody.accept(this);
        return queryExecutor;
    }
}
