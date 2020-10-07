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

/**
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class QueryColumnData {

    protected static final String ASTERIX_COLUMN = "*";
    protected static final String UID_COLUMN = "UID";

    private final String _columnName;
    private QueryTableData _columnTableData;
    private int _columnIndexInTable = -1; //the index of the column in its table

    // query column path - used for query navigation - Person.car.color='red'
    private final String _columnPath;

    public QueryColumnData(QueryTableData tableData, String columnPath) {
        _columnPath = columnPath;
        // Get column name by splitting the full path by '.' or "[*]" and
        // keeping the first match, for example:
        // 1. column => column
        // 2. nested.column => column
        // 3. collection[*].column => collection
        // the "2" provided to the split method is used for optimization.
        // The pattern will only process the first match and the remaining will
        // be placed in the second position of the returned array.
        _columnName = columnPath == null ? null : columnPath.split("\\.|\\[\\*\\]", 2)[0];
        setColumnTableData(tableData);
        if (tableData != null && !UID_COLUMN.equals(_columnName) && !ASTERIX_COLUMN.equals(_columnName)) {
            ITypeDesc currentInfo = tableData.getTypeDesc();
            int pos = currentInfo.getFixedPropertyPositionIgnoreCase(_columnName);
            if (pos == -1) {
                if (!currentInfo.supportsDynamicProperties())
                    throw new IllegalArgumentException("Unknown column name '" + _columnName + "'");
            } else {
                this._columnIndexInTable = pos;
            }
        }
    }

    public String getColumnName() {
        return _columnName;
    }

    public QueryTableData getColumnTableData() {
        return _columnTableData;
    }

    public void setColumnTableData(QueryTableData columnTableData) {
        _columnTableData = columnTableData;
        if (isAsterixColumn() && columnTableData != null)
            columnTableData.setAsterixSelectColumns(true);
    }

    public int getColumnIndexInTable() {
        return _columnIndexInTable;
    }

    /**
     * Checks if given table data matches this column. If so the table data is set.
     *
     * If another table data was already assigned - column ambiguity - exception is thrown.
     *
     * @return true if the table data was set
     */
    public boolean checkAndAssignTableData(QueryTableData tableData) throws SQLException {
        if (isUidColumn()) {
            // every table has uid - so if it is already set - ambiguous expression
            if (_columnTableData != null && !_columnTableData.getTableName().equals(tableData.getTableName()))
                throw new SQLException("Ambiguous UID column: It is defined in [" + _columnTableData.getTableName() + "] and [" + tableData.getTableName() + "]");

            setColumnTableData(tableData);
            return true;
        }

        if (isAsterixColumn()) {
            // this is just an indicator of all columns - so no table data needs to be set
            if (_columnTableData != null)
                return false;
            if (tableData != null)
                tableData.setAsterixSelectColumns(true);
            return true;
        }

        ITypeDesc currentInfo = tableData.getTypeDesc();
        int pos = currentInfo.getFixedPropertyPositionIgnoreCase(getColumnName());
        if (pos == -1)
            return false;

        //found the column, check for ambiguous column
        if (_columnTableData != null && _columnTableData != tableData)
            throw new SQLException("Ambiguous column name [" + getColumnName() + "]");

        setColumnTableData(tableData);
        this._columnIndexInTable = pos;
        return true;
    }

    public String getColumnPath() {
        return _columnPath;
    }

    public boolean isNestedQueryColumn() {
        if (_columnName == null)
            return false;
        return !_columnPath.equals(_columnName);
    }

    /**
     * Create column data according to the table name
     */
    public static QueryColumnData newColumnData(String columnPath, AbstractDMLQuery query)
            throws SQLException {
        // Check if the specified column path is a column alias and if so assign
        // the original column name to columnPath
        if (query.isSelectQuery()) {
            SelectColumn sc = query.getQueryColumnByAlias(columnPath);
            if (sc != null) {
                columnPath = sc.getName();
            }
        }

        QueryColumnData columnData = null;
        // split the column path to table name an column name
        for (QueryTableData tableData : query.getTablesData()) {
            String tableName = findPrefix(columnPath, tableData.getTableName(), tableData.getTableAlias());
            if (tableName != null) {
                // check for ambiguity
                if (columnData != null) {
                    throw new SQLException("Ambiguous column path - [" + columnPath + "]");
                }
                columnData = QueryColumnData.newInstance(tableData, columnPath.substring(tableName.length() + 1));
            }
        }

        // no table data - only columnPath - find the table that has such column
        if (columnData == null) {
            columnData = QueryColumnData.newInstance(columnPath);
            //we need to know where this column is
            boolean assignedTable = false;
            for (QueryTableData tableData : query.getTablesData()) {
                if (columnData.checkAndAssignTableData(tableData))
                    assignedTable = true;
            }

            if (!assignedTable) {
                if (query.getTablesData().size() == 1) {
                    if (query.getTableData().getTypeDesc().supportsDynamicProperties()) {
                        columnData.setColumnTableData(query.getTableData());
                        return columnData;
                    }
                }
                throw new SQLException("Unknown column path [" + columnPath + "] .", "GSP", -122);
            }
        }

        return columnData;
    }

    private static QueryColumnData newInstance(String columnPath) {
        return newInstance(null, columnPath);
    }

    public static QueryColumnData newInstance(QueryTableData tableData, String columnPath) {
        if (columnPath.equalsIgnoreCase(UID_COLUMN))
            columnPath = UID_COLUMN;
        return new QueryColumnData(tableData, columnPath);
    }

    private static String findPrefix(String s, String ... prefixes) {
        if (s == null)
            return null;
        for (String prefix : prefixes) {
            if (prefix != null && s.startsWith(prefix + "."))
                return prefix;
        }
        return null;
    }

    public boolean isAsterixColumn() {
        return ASTERIX_COLUMN.equals(_columnPath);
    }

    public boolean isUidColumn() {
        return UID_COLUMN.equals(_columnPath);
    }
}
