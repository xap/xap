package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.AggregationFunction;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;

import java.util.List;

public class TableRow implements Comparable<TableRow>{
    private final QueryColumn[] columns;
    private final Object[] values;
    private final OrderColumn[] orderColumns;
    private final Object[] orderValues;

    public TableRow(QueryColumn[] columns, Object[] values) {
        this.columns = columns;
        this.values = values;
        this.orderColumns = new OrderColumn[0];
        this.orderValues = new Object[0];
    }

    public TableRow(IEntryPacket x, List<QueryColumn> queryColumns, List<OrderColumn> orderColumns) {
        //TODO boolean field instead.
        boolean hasAggregationFunctions = queryColumns.stream().anyMatch(queryColumn -> queryColumn instanceof AggregationFunction);
        if (hasAggregationFunctions) {
            int columnsSize = queryColumns.size();
            this.columns = new QueryColumn[columnsSize];
            this.values = new Object[columnsSize];
            for (int i = 0; i < columnsSize; i++) {
                this.columns[i] = queryColumns.get(i);
                //TODO: what if we use group by, and therefore we have more values??
                this.values[i] = x.getFieldValue(i);
            }
        } else {
            this.columns = queryColumns.toArray(new QueryColumn[0]);//TODO x TypeDescriptor is null! why?
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

        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            OrderColumn orderColumn = orderColumns.get(i);
            if (orderColumn.isUUID()) {
                orderValues[i] = x.getUID();
            } else if (x.getTypeDescriptor().getIdPropertyName().equalsIgnoreCase(orderColumn.getName())) {
                orderValues[i] = x.getID();
            } else {
                orderValues[i] = x.getPropertyValue(orderColumn.getName());
            }
        }
    }

    public TableRow(List<QueryColumn> queryColumns, List<OrderColumn> orderColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            Object value = queryColumns.get(i).getCurrentValue();
            values[i] = value;
        }

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
            if(queryColumn instanceof AggregationFunction) {
                values[i] = row.getPropertyValue(((AggregationFunction) queryColumn).getColumnName());
            } else {
                values[i] = row.getPropertyValue(queryColumn);
            }
        }

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

    public Object getPropertyValue(QueryColumn column) {
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

    @Override
    public int compareTo(TableRow other) {
        int results = 0;
        for (OrderColumn orderCol : this.orderColumns) {
            Comparable first = (Comparable) this.getPropertyValue(orderCol);
            Comparable second = (Comparable) other.getPropertyValue(orderCol);

            if (first == second) {
                continue;
            }
            if (first == null) {
                return orderCol.isNullsLast() ? 1 : -1;
            }
            if (second == null) {
                return orderCol.isNullsLast() ? -1 : 1;
            }
            results = first.compareTo(second);
            if (results != 0) {
                return orderCol.isAsc() ? results : -results;
            }
        }
        return results;
    }
}
