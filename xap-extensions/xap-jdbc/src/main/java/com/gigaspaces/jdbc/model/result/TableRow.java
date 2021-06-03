package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.AggregationFunction;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.builder.QueryEntryPacket;

import java.util.*;

public class TableRow implements Comparable<TableRow> {
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

    public TableRow(QueryColumn[] columns, Object[] values, OrderColumn[] orderColumns, Object[] orderValues) {
        this.columns = columns;
        this.values = values;
        this.orderColumns = orderColumns;
        this.orderValues = orderValues;
    }

    public TableRow(IEntryPacket x, List<QueryColumn> queryColumns, TableContainer tableContainer) {
        final List<OrderColumn> orderColumns = tableContainer.getOrderColumns();
        if (tableContainer.hasAggregationFunctions()) {
            Map<String, Object> fieldNameValueMap = new HashMap<>();
            QueryEntryPacket queryEntryPacket = ((QueryEntryPacket) x);
            for(int i=0; i < x.getFieldValues().length ; i ++) {
                fieldNameValueMap.put(queryEntryPacket.getFieldNames()[i], queryEntryPacket.getFieldValues()[i]);
            }
            int columnsSize = queryColumns.size();
            this.columns = new QueryColumn[columnsSize];
            this.values = new Object[columnsSize];
            for (int i = 0; i < columnsSize; i++) {
                this.columns[i] = queryColumns.get(i);
                //TODO: what if we use group by, and therefore we have more values??
                this.values[i] = fieldNameValueMap.get(this.columns[i].getName().toLowerCase(Locale.ROOT));
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
            values[i] = row.getPropertyValue(queryColumn);
        }

        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            OrderColumn orderColumn = orderColumns.get(i);
            orderValues[i] = row.getPropertyValue(orderColumn.getName());
        }
    }

    public static TableRow aggregate(List<TableRow> tableRows, List<QueryColumn> queryColumns) {
        if (tableRows.isEmpty()) {
            return new TableRow((QueryColumn[]) null, null);
        }
        QueryColumn[] rowsColumns = queryColumns.toArray(new QueryColumn[0]);
        OrderColumn[] firstRowOrderColumns = tableRows.get(0).orderColumns; //TODO: validate!
        Object[] firstRowOrderValues = tableRows.get(0).orderValues; //TODO: validate! if from first or use aggregateValues!

        Object[] aggregateValues = new Object[rowsColumns.length];
        int index = 0;
        for (QueryColumn queryColumn : queryColumns) {
            Object value;
            Comparator<Object> valueComparator = getObjectComparator();
            if (queryColumn instanceof AggregationFunction) {
                String columnName = ((AggregationFunction) queryColumn).getColumnName();
                boolean isAllColumn = ((AggregationFunction) queryColumn).isAllColumns();
                AggregationFunction.AggregationFunctionType type = ((AggregationFunction) queryColumn).getType();
                if(tableRows.get(0).hasColumn(queryColumn)) { // if this column already exists.
                    value = tableRows.get(0).getPropertyValue(queryColumn);
                }else if (type == AggregationFunction.AggregationFunctionType.MAX) {
                    value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName)).filter(Objects::nonNull).max(valueComparator).orElse(null);
                } else if (type == AggregationFunction.AggregationFunctionType.MIN) {
                    value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName)).filter(Objects::nonNull).min(valueComparator).orElse(null);
                } else if (type == AggregationFunction.AggregationFunctionType.AVG) {
                    //TODO: supported types? need to be implement.
                    value = tableRows.stream().map(tr -> (Number) tr.getPropertyValue(columnName)).filter(Objects::nonNull).reduce(0d,
                            (a,b) -> a.doubleValue() + b.doubleValue()).doubleValue() / tableRows.size();
                } else if (type == AggregationFunction.AggregationFunctionType.SUM) {
                    //TODO: supported types? need to be implement.
                    value = tableRows.stream().map(tr -> (Number) tr.getPropertyValue(columnName)).filter(Objects::nonNull).reduce(0d,
                            (a,b) -> a.doubleValue() + b.doubleValue()).doubleValue();
                } else { // (type == AggregationFunction.AggregationFunctionType.COUNT)
                    if(isAllColumn) {
                        value = tableRows.size();
                    } else {
                        value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName)).filter(Objects::nonNull).count();
                    }
                }
            } else {
                value = tableRows.stream().map(tr -> tr.getPropertyValue(queryColumn)).filter(Objects::nonNull).findAny().orElse(null);
            }
            aggregateValues[index++] = value;
        }
        return new TableRow(rowsColumns, aggregateValues, firstRowOrderColumns, firstRowOrderValues);
    }

    private static Comparator<Object> getObjectComparator() {
        return (o1, o2) -> {
            Comparable first = (Comparable) o1; //TODO: use castToComparable from WhereHandler?
            Comparable second = (Comparable) o2;
            if (first == second) {
                return 0;
            }
            return first.compareTo(second);
        };
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
            if (columns[i].getName().equals(name)) { //TODO: if its aggregationFunction? use column name?
                return values[i];
            }
        }
        return null;
    }

    private boolean hasColumn(QueryColumn queryColumn) {
        //by reference:
        return Arrays.stream(columns).anyMatch(qc -> qc == queryColumn);   //TODO: validate!
//        for (QueryColumn column : columns) {
//            if (column.equals(queryColumn)) {
//                return true;
//            }
//        }
//        return false;
    }

    @Override
    public int compareTo(TableRow other) {
        int results = 0;
        for (OrderColumn orderCol : this.orderColumns) { //TODO: use castToComparable from WhereHandler?
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
