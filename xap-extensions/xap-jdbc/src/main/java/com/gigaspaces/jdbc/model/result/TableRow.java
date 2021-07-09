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

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.jdbc.builder.QueryEntryPacket;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableRow implements Comparable<TableRow> {
    private final IQueryColumn[] columns;
    private final Object[] values;
    private final OrderColumn[] orderColumns;
    private final Object[] orderValues;
    private final ConcreteColumn[] groupByColumns;
    private final Object[] groupByValues;

    public TableRow(IQueryColumn[] columns, Object... values) {
        this(columns, values, new OrderColumn[0], new Object[0], new ConcreteColumn[0], new Object[0]);
    }

    public TableRow(IQueryColumn[] columns, Object[] values, OrderColumn[] orderColumns, Object[] orderValues,
                    ConcreteColumn[] groupByColumns, Object[] groupByValues) {
        this.columns = columns;
        this.values = values;

        this.orderColumns = orderColumns;
        this.orderValues = orderValues;
        this.groupByColumns = groupByColumns;
        this.groupByValues = groupByValues;

    }

    TableRow(IEntryPacket entryPacket, ConcreteTableContainer tableContainer) {
        final List<OrderColumn> orderColumns = tableContainer.getOrderColumns();
        final List<ConcreteColumn> groupByColumns = tableContainer.getGroupByColumns();
        boolean isQueryEntryPacket = entryPacket instanceof QueryEntryPacket;
        if (tableContainer.hasAggregationFunctions() && isQueryEntryPacket) {
            QueryEntryPacket queryEntryPacket = ((QueryEntryPacket) entryPacket);
            Map<String, Object> fieldNameValueMap = new HashMap<>();
            //important since order is important and all selected columns are considered in such case
            List<IQueryColumn> selectedColumns = tableContainer.getSelectedColumns();
            int columnsSize = selectedColumns.size();
            this.columns = new IQueryColumn[columnsSize];
            this.values = new Object[columnsSize];

            for (int i = 0; i < entryPacket.getFieldValues().length; i++) {
                fieldNameValueMap.put(queryEntryPacket.getFieldNames()[i], queryEntryPacket.getFieldValues()[i]);
            }

            for (int i = 0; i < columnsSize; i++) {
                this.columns[i] = selectedColumns.get(i);
                this.values[i] = fieldNameValueMap.get(selectedColumns.get(i).getName());
            }
        } else {
            //for the join/where contains both visible and invisible columns
            final List<IQueryColumn> allQueryColumns = tableContainer.getAllQueryColumns();
            this.columns = allQueryColumns.toArray(new IQueryColumn[0]);
            this.values = new Object[this.columns.length];
            for (int i = 0; i < this.columns.length; i++) {
                values[i] = allQueryColumns.get(i).getValue(entryPacket);
            }
        }

        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            orderValues[i] = orderColumns.get(i).getValue(entryPacket);
        }

        this.groupByColumns = groupByColumns.toArray(new ConcreteColumn[0]);
        ITypeDesc typeDescriptor = entryPacket.getTypeDescriptor();
        groupByValues = new Object[ typeDescriptor != null ? this.groupByColumns.length : 0];
        if( !isQueryEntryPacket ) {
            for (int i = 0; i < groupByColumns.size(); i++) {
                groupByValues[i] = groupByColumns.get(i).getValue(entryPacket);
            }
        }
    }

    TableRow(List<IQueryColumn> columns, List<OrderColumn> orderColumns, List<ConcreteColumn> groupByColumns) {
        this.columns = columns.toArray(new IQueryColumn[0]);
        values = new Object[this.columns.length];
        for (int i = 0; i < this.columns.length; i++) {
            values[i] = this.columns[i].getCurrentValue();
        }

        this.orderColumns = orderColumns.toArray(new OrderColumn[0]);
        orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < orderColumns.size(); i++) {
            orderValues[i] = orderColumns.get(i).getCurrentValue();
        }

        this.groupByColumns = groupByColumns.toArray(new ConcreteColumn[0]);
        groupByValues = new Object[this.groupByColumns.length];
        for (int i = 0; i < groupByColumns.size(); i++) {
            groupByValues[i] = groupByColumns.get(i).getCurrentValue();
        }
    }

    TableRow(TableRow row, TempTableContainer tempTableContainer) {
        this.columns = tempTableContainer.getSelectedColumns().toArray(new IQueryColumn[0]);
        this.values = new Object[this.columns.length];
        for (int i = 0; i < this.columns.length; i++) {

            if( tempTableContainer.hasGroupByColumns() && this.columns[i] instanceof AggregationColumn ){
                AggregationColumn aggregationColumn = (AggregationColumn)this.columns[i];
                IQueryColumn column = aggregationColumn.getQueryColumn().
                                create( aggregationColumn.getQueryColumn().getName(),
                                        aggregationColumn.getQueryColumn().getAlias(),
                                        aggregationColumn.getQueryColumn().isVisible(),
                                        aggregationColumn.getQueryColumn().getColumnOrdinal() );

                this.columns[i] = column;
                this.values[i] = row.getPropertyValue(column.getAlias());
            }
            else{
                this.values[i] = row.getPropertyValue(this.columns[i]);
            }
        }

        this.orderColumns = tempTableContainer.getOrderColumns().toArray(new OrderColumn[0]);
        this.orderValues = new Object[this.orderColumns.length];
        for (int i = 0; i < this.orderColumns.length; i++) {
            this.orderValues[i] = row.getPropertyValue(this.orderColumns[i].getName());
        }

        this.groupByColumns = tempTableContainer.getGroupByColumns().toArray(new ConcreteColumn[0]);
        this.groupByValues = new Object[this.groupByColumns.length];
        for (int i = 0; i < this.groupByColumns.length; i++) {
            this.groupByValues[i] = row.getPropertyValue(this.groupByColumns[i].getName());
        }
    }

    public Object getPropertyValue(int index) {
        return values[index];
    }

    public Object getPropertyValue(IQueryColumn column) {
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

    public Object getPropertyValue(String nameOrAlias) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].getAlias().equals(nameOrAlias)) {
                return values[i];
            }
        }
        return null;
    }

    boolean hasColumn(IQueryColumn column) {
        //by reference:
        return Arrays.stream(this.columns).anyMatch(c -> c == column);
    }

    OrderColumn[] getOrderColumns() {
        return this.orderColumns;
    }

    Object[] getOrderValues() {
        return this.orderValues;
    }

    Object[] getGroupByValues() {
        return groupByValues;
    }

    public Object[] getDistinctValues() {
        Object[] distinctValues = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].isVisible()){
                distinctValues[i] = this.values[i];
            }
        }
        return distinctValues;
    }

    ConcreteColumn[] getGroupByColumns() {
        return groupByColumns;
    }

    @Override
    public int compareTo(TableRow other) {
        int results = 0;
        for (OrderColumn orderCol : this.orderColumns) {
            Comparable first = TableRowUtils.castToComparable(this.getPropertyValue(orderCol));
            Comparable second = TableRowUtils.castToComparable(other.getPropertyValue(orderCol));

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

//    public static Object getEntryPacketValue(IEntryPacket entryPacket, IQueryColumn queryColumn) {
//        Object value;
//        if (queryColumn.isUUID()) {
//            value = entryPacket.getUID();
//        } else if (entryPacket.getTypeDescriptor().getIdPropertyName().equalsIgnoreCase(queryColumn.getName())) {
//            value = entryPacket.getID();
//        } else {
//            value = entryPacket.getPropertyValue(queryColumn.getName());
//        }
//        return value;
//    }
}