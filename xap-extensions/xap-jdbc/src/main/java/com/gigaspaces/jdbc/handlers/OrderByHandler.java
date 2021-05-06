package com.gigaspaces.jdbc.handlers;


import com.gigaspaces.jdbc.QueryExecutor;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;

import java.util.List;
import java.util.Optional;

public class OrderByHandler extends UnsupportedExpressionVisitor implements OrderByVisitor {
    private final QueryExecutor queryExecutor;
    private Column column;

    public OrderByHandler(QueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
    }

    @Override
    public void visit(OrderByElement orderByElement) {
        orderByElement.getExpression().accept(this);
        TableContainer table = getTable();
        String columnName = getColumn().getColumnName();
        OrderColumn orderColumn = new OrderColumn(columnName, null, isVisibleColumn(columnName), table);
        orderColumn.setAsc(orderByElement.isAsc());
        table.addOrderColumns(orderColumn);
    }

    private boolean isVisibleColumn(String columnName) {
        Optional<QueryColumn> fistOptional = this.queryExecutor.getQueryColumns().stream()
                .filter(e -> e.getName().equals(columnName))
                .findFirst();
        return fistOptional.isPresent();
    }

    @Override
    public void visit(Column tableColumn) {
        this.column = tableColumn;
    }

    @Override
    public void visit(LongValue longValue) {
        final List<QueryColumn> visibleColumns = this.queryExecutor.getQueryColumns();
        int colIndex = (int) longValue.getValue();
        //validate range
        if(colIndex > visibleColumns.size() || colIndex < 1) {
            String msg = "Use OrderBy with column's number [" + colIndex + "], ";
            if (visibleColumns.size() == 1) {
                msg += "but the query contain only 1 selected column";
            } else {
                msg += "but the column's numbers are within the range (1," + visibleColumns.size() +")";
            }
            throw new IllegalArgumentException(msg);
        }
        //block unsupported operation
        if(this.queryExecutor.isAllColumnsSelected()) {
            throw new UnsupportedOperationException("OrderBy column's index with 'SELECT *' not supported");
        }
        this.column = new Column().withColumnName(visibleColumns.get(colIndex - 1).getName());
    }

    private TableContainer getTable() {
        final List<TableContainer> tables = this.queryExecutor.getTables();
        return QueryColumnHandler.getTableForColumn(column, tables);
    }

    private Column getColumn() {
        return this.column;
    }
}
