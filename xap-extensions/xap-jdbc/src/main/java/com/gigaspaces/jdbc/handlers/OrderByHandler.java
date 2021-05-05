package com.gigaspaces.jdbc.handlers;


import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;

import java.util.List;

public class OrderByHandler implements OrderByVisitor {
    private final List<TableContainer> tables;
    private final Object[] preparedValues;

    public OrderByHandler(List<TableContainer> tables, Object[] preparedValues) {
        this.tables = tables;
        this.preparedValues = preparedValues;
    }

    @Override
    public void visit(OrderByElement orderByElement) {
        SingleConditionHandler handler = new SingleConditionHandler(tables, preparedValues);
        orderByElement.getExpression().accept(handler);
        TableContainer table = handler.getTable();
        String columnName = handler.getColumn().getColumnName();
        OrderColumn orderColumn = new OrderColumn(columnName, null, true, table);
        orderColumn.setDesc(!orderByElement.isAsc());
        table.addOrderColumns(orderColumn);
    }
}
