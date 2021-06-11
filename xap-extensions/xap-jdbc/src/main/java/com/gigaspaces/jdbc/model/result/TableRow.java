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
package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.jdbc.builder.QueryEntryPacket;

import java.util.*;
import java.util.stream.Collectors;

public class TableRow implements Comparable<TableRow>{
    private final QueryColumn[] columns;
    private final Object[] values;
    private final OrderColumn[] orderColumns;
    private final Object[] orderValues;
    private final QueryColumn[] groupByColumns;

    private final Object[] groupByValues;

    public TableRow(QueryColumn[] columns, Object[] values) {
        this.columns = columns;
        this.values = values;
        this.orderColumns = new OrderColumn[0];
        this.orderValues = new Object[0];
        this.groupByColumns = new QueryColumn[0];
        this.groupByValues = new Object[0];
    }

    public TableRow(QueryColumn[] columns, Object[] values, OrderColumn[] orderColumns, Object[] orderValues,
             QueryColumn[] groupByColumns, Object[] groupByValues) {
        this.columns = columns;
        this.values = values;
        this.orderColumns = orderColumns;
        this.orderValues = orderValues;
        this.groupByColumns = groupByColumns;
        this.groupByValues = groupByValues;
    }

    public TableRow(IEntryPacket x, ConcreteTableContainer tableContainer) {
        final List<OrderColumn> orderColumns = tableContainer.getOrderColumns();
        final List<QueryColumn> groupByColumns = tableContainer.getGroupByColumns();
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
                values[i] = getEntryPacketValue( x, queryColumns.get(i) );
            }
        }

        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            orderValues[i] = getEntryPacketValue( x, orderColumns.get(i) );
        }

        this.groupByColumns = groupByColumns.toArray(new QueryColumn[0]);
        groupByValues = new Object[this.groupByColumns.length];
        for (int i = 0; i < groupByColumns.size(); i++) {
            groupByValues[i] = getEntryPacketValue( x, groupByColumns.get(i) );
        }
    }

    public TableRow(List<QueryColumn> queryColumns, List<OrderColumn> orderColumns, List<QueryColumn> groupByColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            values[i] = queryColumns.get(i).getCurrentValue();
        }

        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            orderValues[i] = orderColumns.get(i).getCurrentValue();
        }

        this.groupByColumns = groupByColumns.toArray(new QueryColumn[0]);
        groupByValues = new Object[this.groupByColumns.length];
        for (int i = 0; i < groupByColumns.size(); i++) {
            groupByValues[i] = groupByColumns.get(i).getCurrentValue();
        }
    }

    public TableRow(TableRow row, List<QueryColumn> queryColumns, List<OrderColumn> orderColumns, List<QueryColumn> groupByColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            values[i] = row.getPropertyValue(queryColumns.get(i));
        }

        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            orderValues[i] = row.getPropertyValue(orderColumns.get(i).getName());
        }

        this.groupByColumns = groupByColumns.toArray(new QueryColumn[0]);
        groupByValues = new Object[this.groupByColumns.length];
        for (int i = 0; i < groupByColumns.size(); i++) {
            groupByValues[i] = row.getPropertyValue(groupByColumns.get(i).getName());
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
        QueryColumn[] firstRowGroupByColumns = tableRows.get(0).groupByColumns;
        Object[] firstRowGroupByValues = tableRows.get(0).groupByValues;

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
        return new TableRow(rowsColumns, aggregateValues, firstRowOrderColumns,
                firstRowOrderValues, firstRowGroupByColumns, firstRowGroupByValues);
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

    public Object[] getGroupByValues() {
        return groupByValues;
    }

    private Object getEntryPacketValue( IEntryPacket entryPacket, QueryColumn queryColumn ){

        Object value;
        if (queryColumn.isUUID()) {
            value = entryPacket.getUID();
        } else if (entryPacket.getTypeDescriptor().getIdPropertyName().equalsIgnoreCase(queryColumn.getName())) {
            value = entryPacket.getID();
        } else {
            value = entryPacket.getPropertyValue(queryColumn.getName());
        }

        return value;
    }
}