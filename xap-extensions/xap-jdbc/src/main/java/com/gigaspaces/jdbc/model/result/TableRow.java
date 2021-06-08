package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.jdbc.builder.QueryEntryPacket;

import java.util.*;
import java.util.stream.Collectors;

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

    public TableRow(IEntryPacket x, ConcreteTableContainer tableContainer) {
        final List<OrderColumn> orderColumns = tableContainer.getOrderColumns();
        if (tableContainer.hasAggregationFunctions() && x instanceof QueryEntryPacket) {
            final List<AggregationColumn> aggregationColumns = tableContainer.getAggregationFunctionColumns();
            Map<String, Object> fieldNameValueMap = new HashMap<>();
            QueryEntryPacket queryEntryPacket = ((QueryEntryPacket) x);
            for(int i=0; i < x.getFieldValues().length ; i ++) {
                fieldNameValueMap.put(queryEntryPacket.getFieldNames()[i], queryEntryPacket.getFieldValues()[i]);
            }
            int columnsSize = aggregationColumns.size();
            this.columns = new QueryColumn[columnsSize];
            this.values = new Object[columnsSize];
            for (int i = 0; i < columnsSize; i++) {
                this.columns[i] = aggregationColumns.get(i);
                this.values[i] = fieldNameValueMap.get(aggregationColumns.get(i).getNameWithLowerCase());
            }
        } else {
            //for the join/where contains both visible and invisible columns
            final List<QueryColumn> queryColumns = tableContainer.getAllQueryColumns();

            this.columns = queryColumns.toArray(new QueryColumn[0]);
            this.values = new Object[this.columns.length];
            for (int i = 0; i < this.columns.length; i++) {
                QueryColumn queryColumn = this.columns[i];
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

    public static TableRow aggregate(List<TableRow> tableRows, List<AggregationColumn> aggregationColumns) {
        if (tableRows.isEmpty()) {
            return new TableRow((QueryColumn[]) null, null);
        }
        QueryColumn[] rowsColumns = aggregationColumns.toArray(new QueryColumn[0]);
        //TODO: @sagiv validate! if from first or use aggregateValues
        OrderColumn[] firstRowOrderColumns = tableRows.get(0).orderColumns;
        Object[] firstRowOrderValues = tableRows.get(0).orderValues;

        Object[] aggregateValues = new Object[rowsColumns.length];
        int index = 0;
        for (AggregationColumn aggregationColumn : aggregationColumns) {
            Object value = null;
            if(tableRows.get(0).hasColumn(aggregationColumn)) { // if this column already exists by reference.
                value = tableRows.get(0).getPropertyValue(aggregationColumn);
            } else {
                AggregationFunctionType type = aggregationColumn.getType();
                String columnName = aggregationColumn.getColumnName();
                switch (type) {
                    case COUNT:
                        boolean isAllColumn = aggregationColumn.isAllColumns();
                        if (isAllColumn) {
                            value = tableRows.size();
                        } else {
                            value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName))
                                    .filter(Objects::nonNull).count();
                        }
                        break;
                    case MAX:
                        value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName))
                                .filter(Objects::nonNull).max(getObjectComparator()).orElse(null);
                        break;
                    case MIN:
                        value = tableRows.stream().map(tr -> tr.getPropertyValue(columnName))
                                .filter(Objects::nonNull).min(getObjectComparator()).orElse(null);
                        break;
                    case AVG:
                        //TODO: for now supported only Number.
                        List<Number> collect = tableRows.stream().map(tr -> (Number) tr.getPropertyValue(columnName))
                                .filter(Objects::nonNull).collect(Collectors.toList());
                        value = collect.stream()
                                .reduce(0d, (a, b) -> a.doubleValue() + b.doubleValue()).doubleValue() / collect.size();
                        break;
                    case SUM:
                        //TODO: for now supported only Number.
                        value = tableRows.stream().map(tr -> (Number) tr.getPropertyValue(columnName))
                                .filter(Objects::nonNull).reduce(0d, (a, b) -> a.doubleValue() + b.doubleValue()).doubleValue();
                        break;
                }
            }
            aggregateValues[index++] = value;
        }
        return new TableRow(rowsColumns, aggregateValues, firstRowOrderColumns, firstRowOrderValues);
    }

    private static Comparator<Object> getObjectComparator() {
        return (o1, o2) -> { //TODO: @sagiv add try catch block. like in use castToComparable from WhereHandler.
            Comparable first = (Comparable) o1;
            Comparable second = (Comparable) o2;
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
            if (columns[i].getNameOrAlias().equals(name)) {
                return values[i];
            }
        }
        return null;
    }

    private boolean hasColumn(QueryColumn queryColumn) {
        //by reference:
        return Arrays.stream(columns).anyMatch(qc -> qc == queryColumn);
    }

    @Override
    public int compareTo(TableRow other) {
        int results = 0;
        for (OrderColumn orderCol : this.orderColumns) { //TODO: @sagiv add try catch block. like in use castToComparable from WhereHandler.
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
