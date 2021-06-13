package com.gigaspaces.jdbc.jsql.handlers;


import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.model.table.ConcreteColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.GroupByVisitor;

import java.util.List;

public class GroupByHandler extends UnsupportedExpressionVisitor implements GroupByVisitor {
    //TODO: consider not to pass queryExecutor but its relevant fields, when we need to serialize this object.
    private final QueryExecutor queryExecutor;
    private Column column;
    private int columnCounter = 0;

    public GroupByHandler(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public void visit(GroupByElement groupByElement) {

        List<Expression> groupByExpressions = groupByElement.getGroupByExpressions();
        for( Expression expression : groupByExpressions ){
            expression.accept( this );
            String columnName = getColumn().getColumnName();
            TableContainer table = getTable();
            ConcreteColumn groupByColumn = new ConcreteColumn(columnName, null, null,  isVisibleColumn( columnName ), table, columnCounter++);
            table.addGroupByColumns(groupByColumn);
        }
    }

    private boolean isVisibleColumn(String columnName) {
        return this.queryExecutor.getVisibleColumns().stream().anyMatch(queryColumn -> queryColumn.getAlias().equals(columnName));
    }

    @Override
    public void visit(Column tableColumn) {
        this.column = tableColumn;
    }

    private TableContainer getTable() {
        final List<TableContainer> tables = this.queryExecutor.getTables();
        return QueryColumnHandler.getTableForColumn(column, tables);
    }

    private Column getColumn() {
        return this.column;
    }
}
