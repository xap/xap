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

    /**
     *
     */
    protected static final String ASTERIX_COLUMN = "*";

    /**
     *
     */
    protected static final String UID_COLUMN = "UID";

    private String _columnName;
    private QueryTableData _columnTableData;
    private int _columnIndexInTable = -1; //the index of the column in its table

    // query column path - used for query navigation - Person.car.color='red'
    private final String _columnPath;


    /**
     * @param columnPath
     */
    public QueryColumnData(String columnPath) {
        if (columnPath == null) {
            _columnPath = null;

        } else {
            // Get column name by splitting the full path by '.' or "[*]" and
            // keeping the first match, for example:
            // 1. column => column
            // 2. nested.column => column
            // 3. collection[*].column => collection
            // the "2" provided to the split method is used for optimization.
            // The pattern will only process the first match and the remaining will
            // be placed in the second position of the returned array.
            _columnName = columnPath.split("\\.|\\[\\*\\]", 2)[0];
            _columnPath = columnPath;
        }
    }

    public QueryColumnData(QueryTableData tableData, String columnPath) {
        this(columnPath);
        _columnTableData = tableData;
        if (tableData != null && !UID_COLUMN.equals(_columnName) && !ASTERIX_COLUMN.equals(_columnName)) {
            ITypeDesc currentInfo = tableData.getTypeDesc();
            int pos = currentInfo.getFixedPropertyPositionIgnoreCase(_columnName);
            if (pos == -1) {
                if (!currentInfo.supportsDynamicProperties())
                    throw new IllegalArgumentException("Unknown column name '" + _columnName + "'");
            } else {
                setColumnIndexInTable(pos);
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

    }


    public int getColumnIndexInTable() {
        return _columnIndexInTable;
    }

    public void setColumnIndexInTable(int columnIndexInTable) {
        this._columnIndexInTable = columnIndexInTable;
    }

    /**
     * Checks if given table data matches this column. If so the table data is set.
     *
     * If another table data was already assigned - column ambiguity - exception is thrown.
     *
     * @return true if the table data was set
     */
    public boolean checkAndAssignTableData(QueryTableData tableData) throws SQLException {
        ITypeDesc currentInfo = tableData.getTypeDesc();
        int pos = currentInfo.getFixedPropertyPositionIgnoreCase(getColumnName());
        if (pos == -1)
            return false;

        //found the column, check for ambiguous column
        QueryTableData columnTableData = getColumnTableData();
        if (columnTableData != null && columnTableData != tableData)
            throw new SQLException("Ambiguous column name [" + getColumnName() + "]");

        setColumnTableData(tableData);
        setColumnIndexInTable(pos);
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
            String tableName = null;
            if (startsWith(columnPath, tableData.getTableName()))
                tableName = tableData.getTableName();
            else if (startsWith(columnPath, tableData.getTableAlias()))
                tableName = tableData.getTableAlias();


            if (tableName != null) {
                // check for ambiguity
                if (columnData != null) {
                    throw new SQLException("Ambiguous column path - ["
                            + columnPath + "]");

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
                throw new SQLException("Unknown column path [" + columnPath + "] .",
                        "GSP", -122);
            }
        }


        return columnData;
    }


    /**
     * @param columnPath
     * @return
     * @throws SQLException
     */
    private static QueryColumnData newInstance(String columnPath) throws SQLException {
        return newInstance(null, columnPath);
    }


    public static QueryColumnData newInstance(QueryTableData tableData, String columnPath) throws SQLException {
        if (columnPath.equalsIgnoreCase(UID_COLUMN)) {
            return new UidColumnData(tableData);
        } else if (columnPath.equals(ASTERIX_COLUMN)) {
            return new AsterixColumnData(tableData);
        } else {
            return new QueryColumnData(tableData, columnPath);
        }
    }


    /**
     * Returns true if given columnPath startsWith given tableName
     */
    public static boolean startsWith(String columnPath, String tableName) {

        if (tableName == null || columnPath == null)
            return false;

        return columnPath.startsWith(tableName + ".");


    }

    public void setColumnName(String columnName) {
        _columnName = columnName;
    }


    public boolean isAsterixColumn() {
        return false;
    }

    public boolean isUidColumn() {
        return false;
    }

}