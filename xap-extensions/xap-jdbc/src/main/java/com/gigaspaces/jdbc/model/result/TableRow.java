package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;

import java.util.List;

public class TableRow {
    private QueryColumn[] columns;
    private Object[] values;
    private OrderColumn[] orderColumns;
    private Object[] orderValues;

    public TableRow(QueryColumn[] columns, Object[] values) {
        this.columns = columns;
        this.values = values;
    }

    public TableRow(IEntryPacket x, List<QueryColumn> queryColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            QueryColumn queryColumn = queryColumns.get(i);
            if (queryColumn.isUUID()) {
                values[i] = x.getUID();
            } else if (x.getTypeDescriptor().getIdPropertyName().equalsIgnoreCase(queryColumn.getName())) {
                values[i] = x.getID();
            } else {
                values[i] = x.getPropertyValue(queryColumn.getName());
            }
        }
    }

    public TableRow(List<QueryColumn> queryColumns,  List<OrderColumn> orderColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            Object value = queryColumns.get(i).getCurrentValue();
            values[i] = value;
        }
        //TODO: think of better way
        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            orderValues[i] = orderColumns.get(i).getCurrentValue();
        }
    }

    public TableRow(TableRow row, List<QueryColumn> queryColumns, List<OrderColumn> orderColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            QueryColumn queryColumn = queryColumns.get(i);
            values[i] = row.getPropertyValue(queryColumn);
        }
        //TODO: think of better way
        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            OrderColumn orderColumn = orderColumns.get(i);
            orderValues[i] = row.getPropertyValue(orderColumn.getName());
        }
    }

    public Object getPropertyValue(int index) {
        return values[index];
    }

    public Object getPropertyValue(QueryColumn column) { //TODO: hash map?
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equals(column)) {
                return values[i];
            }
        }
        return null;
    }

    public Object getPropertyValue(OrderColumn column) {
        for (int i = 0; i < orderColumns.length; i++) {
            if (orderColumns[i].equals(column)) {
                return orderValues[i];
            }
        }
        return null;
    }

    public Object getPropertyValue(String name) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getName().equals(name)) {
                return values[i];
            }
        }
        return null;
    }
}
