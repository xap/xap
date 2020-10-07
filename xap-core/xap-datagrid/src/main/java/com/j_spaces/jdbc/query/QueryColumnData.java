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

package com.j_spaces.jdbc.query;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.SelectColumn;

import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class QueryColumnData {

    protected static final String ASTERIX_COLUMN = "*";
    protected static final String UID_COLUMN = "UID";

    // query column path - used for query navigation - Person.car.color='red'
    private final String _columnPath;
    private final String _columnName;
    private final QueryTableData _columnTable;
    private final int _columnIndex; //the index of the column in its table

    public QueryColumnData(QueryTableData tableData, String columnPath) {
        _columnPath = isUidColumn(columnPath) ? UID_COLUMN : columnPath;
        _columnName = initColumnName(_columnPath);
        _columnTable = tableData;
        if (isAsterixColumn() && tableData != null)
            tableData.setAsterixSelectColumns(true);
        _columnIndex = tableData == null || isUidColumn() || isAsterixColumn() ? -1 : initColumnIndex(tableData, _columnName);
    }

    public String getColumnName() {
        return _columnName;
    }

    public QueryTableData getColumnTableData() {
        return _columnTable;
    }

    public int getColumnIndexInTable() {
        return _columnIndex;
    }

    public String getColumnPath() {
        return _columnPath;
    }

    public boolean isNestedQueryColumn() {
        if (_columnName == null)
            return false;
        return !_columnPath.equals(_columnName);
    }

    public static QueryColumnData newColumnData(String columnPathRaw, AbstractDMLQuery query)
            throws SQLException {

        List<QueryTableData> tables = query.getTablesData();
        // Special case for asterisk (*) column:
        if (isAsterixColumn(columnPathRaw)) {
            for (QueryTableData table : tables)
                table.setAsterixSelectColumns(true);
            return new QueryColumnData(null, columnPathRaw);
        }

        // Check if the specified column path is a column alias and if so assign
        // the original column name to columnPath
        if (query.isSelectQuery()) {
            SelectColumn sc = query.getQueryColumnByAlias(columnPathRaw);
            if (sc != null) {
                columnPathRaw = sc.getName();
            }
        }
        final String columnPath = columnPathRaw;

        // Check for a table prefix in column path, and build column data if exists:
        QueryColumnData columnData = findUnique(tables, t -> tryInitWithPrefix(t, columnPath), (r1, r2) -> ambigFormatter(columnPath, r1, r2));
        if (columnData != null)
            return columnData;

        // no table prefix - find the table that has such column
        // special case - uid (reserved word, cannot be a table column)
        if (isUidColumn(columnPath)) {
            if (tables.size() > 1)
                throw new SQLException("Ambiguous UID column - query contains multiple tables: [" + query.getTablesNames() + "]");
            return new QueryColumnData(query.getTableData(), columnPath);
        }

        columnData = findUnique(tables, t -> tryInitWithoutPrefix(t, columnPath), (r1, r2) -> ambigFormatter(columnPath, r1, r2));
        if (columnData != null)
            return columnData;

        // Special case: single table which supports dynamic properties
        if (tables.size() == 1 && query.getTableData().getTypeDesc().supportsDynamicProperties()) {
            return new QueryColumnData(query.getTableData(), columnPath);
        }

        throw new SQLException("Unknown column path [" + columnPath + "] .", "GSP", -122);
    }

    public boolean isAsterixColumn() {
        return isAsterixColumn(_columnPath);
    }

    public boolean isUidColumn() {
        return UID_COLUMN.equals(_columnPath);
    }

    private static boolean isAsterixColumn(String columnName) {
        return ASTERIX_COLUMN.equals(columnName);
    }

    private static boolean isUidColumn(String columnName) {
        return UID_COLUMN.equalsIgnoreCase(columnName);
    }

    private static String initColumnName(String columnPath) {
        // Get column name by splitting the full path by '.' or "[*]" and
        // keeping the first match, for example:
        // 1. column => column
        // 2. nested.column => column
        // 3. collection[*].column => collection
        // the "2" provided to the split method is used for optimization.
        // The pattern will only process the first match and the remaining will
        // be placed in the second position of the returned array.
        return columnPath == null ? null : columnPath.split("\\.|\\[\\*\\]", 2)[0];
    }

    private static int initColumnIndex(QueryTableData tableData, String columnName) {
        ITypeDesc currentInfo = tableData.getTypeDesc();
        int colIndex = currentInfo.getFixedPropertyPositionIgnoreCase(columnName);
        if (colIndex == -1 && !currentInfo.supportsDynamicProperties())
            throw new IllegalArgumentException("Unknown column [" + columnName + "] in table [" + tableData.getTableName() + "]");
        return colIndex;
    }

    private static <T, R> R findUnique(Iterable<T> iterable, Function<T, R> mapper, BiFunction<R, R, String> errorFormatter)
            throws SQLException {
        R result = null;
        for (T item : iterable) {
            R curr = mapper.apply(item);
            if (curr != null) {
                if (result == null) {
                    result = curr;
                } else {
                    throw new SQLException(errorFormatter.apply(result, curr));
                }
            }
        }
        return result;
    }

    private static QueryColumnData tryInitWithPrefix(QueryTableData tableData, String columnPath) {
        if (columnPath != null) {
            String[] prefixes = new String[] {tableData.getTableName(), tableData.getTableAlias()};
            for (String prefix : prefixes) {
                if (prefix != null && columnPath.startsWith(prefix + "."))
                    return new QueryColumnData(tableData, columnPath.substring(prefix.length() + 1));
            }
        }
        return null;
    }

    private static QueryColumnData tryInitWithoutPrefix(QueryTableData tableData, String columnPath) {
        return tableData.getTypeDesc().getFixedPropertyPositionIgnoreCase(columnPath) == -1 ? null : new QueryColumnData(tableData, columnPath);
    }

    private static String ambigFormatter(String columnPath, QueryColumnData r1, QueryColumnData r2) {
        return "Ambiguous column [" + columnPath + "]: exists in [" +
                r1.getColumnTableData().getTableName() + "] and [" +
                r2.getColumnTableData().getTableName();
    }
}
