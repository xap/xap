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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.PropertyInfo;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.jdbc.AbstractDMLQuery;
import com.j_spaces.jdbc.Query;
import com.j_spaces.jdbc.SelectColumn;
import com.j_spaces.jdbc.SelectQuery;

import java.io.*;
import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author anna
 * @since 7.0
 */
@com.gigaspaces.api.InternalApi
public class QueryColumnData implements SmartExternalizable {
    private static final long serialVersionUID = 1L;

    protected static final String ASTERIX_COLUMN = "*";
    protected static final String UID_COLUMN = "UID";

    // query column path - used for query navigation - Person.car.color='red'
    private String _columnPath;
    private String _columnName;
    private QueryTableData _columnTable;
    private int _columnIndex; //the index of the column in its table

    // Required for Externalizable
    public QueryColumnData() {
    }

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
        columnData = new QueryColumnData(null, columnPath);
        //we need to know where this column is
        boolean assignedTable = false;
        for (QueryTableData tableData : query.getTablesData()) {
            if (columnData.checkAndAssignTableData(tableData))
                assignedTable = true;
        }
        if(assignedTable)
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
        ITypeDesc typeDesc = tableData.getTypeDesc();
        if (typeDesc != null) {
            int colIndex = typeDesc.getFixedPropertyPositionIgnoreCase(columnName);
            if (colIndex == -1 && !typeDesc.supportsDynamicProperties())
                throw new IllegalArgumentException("Unknown column [" + columnName + "] in table [" + tableData.getTableName() + "]");
            return colIndex;
        }
        Query subQuery = tableData.getSubQuery();
        if (subQuery != null) {
            if (subQuery instanceof SelectQuery) {
                List<SelectColumn> subQueryColumns = ((SelectQuery) subQuery).getQueryColumns();
                int colIndex = 0;
                for (SelectColumn subQueryColumn : subQueryColumns) {
                    if (subQueryColumn.hasAlias() ? subQueryColumn.getAlias().equals(columnName) : subQueryColumn.getName().equals(columnName)) {
                        return colIndex;
                    }
                    if (subQueryColumn.isVisible()) {
                        colIndex++;
                    }
                }
                throw new IllegalArgumentException("Unknown column [" + columnName + "] in table [" + tableData.getTableName() + "]");
            } else {
                throw new IllegalStateException("Unsupported subquery class: " + subQuery.getClass());
            }
        }
        throw new IllegalStateException("QueryTableData does not have type descriptor for table [" + tableData.getTableName() + "]");
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
        ITypeDesc typeDesc = tableData.getTypeDesc();
        if (typeDesc != null) {
            return typeDesc.getFixedPropertyPositionIgnoreCase(columnPath) == -1 ? null : new QueryColumnData(tableData, columnPath);
        }
        Query subQuery = tableData.getSubQuery();
        if (subQuery != null) {
            SelectColumn col = getSubQueryColumnByName(subQuery, columnPath);
            if (col == null || !col.isVisible()) return null;
            String colName = col.hasAlias() ? col.getAlias() : col.toString();
            return new QueryColumnData(tableData, col.isAggregatedFunction() ? colName : columnPath);
        }
        throw new IllegalStateException("QueryTableData does not have type descriptor for table [" + tableData.getTableName() + "]");
    }

    private static SelectColumn getSubQueryColumnByName(Query subQuery, String columnName) {
        if (subQuery instanceof SelectQuery) {
            List<SelectColumn> subQueryColumns = ((SelectQuery) subQuery).getQueryColumns();
            for (SelectColumn subQueryColumn : subQueryColumns) {
                if (subQueryColumn.hasAlias() ? subQueryColumn.getAlias().equals(columnName): subQueryColumn.getName().equals(columnName))
                    return subQueryColumn;
            }
            return null;
        }
        throw new IllegalStateException("Unsupported subquery class: " + subQuery.getClass());
    }

    private static String ambigFormatter(String columnPath, QueryColumnData r1, QueryColumnData r2) {
        return "Ambiguous column [" + columnPath + "]: exists in [" +
                r1.getColumnTableData().getTableName() + "] and [" +
                r2.getColumnTableData().getTableName();
    }

    public boolean checkAndAssignTableData(QueryTableData tableData) throws SQLException {
        ITypeDesc currentInfo = tableData.getTypeDesc();

        for (int c = 0; c < currentInfo.getNumOfFixedProperties(); c++) {
            String columnName = getColumnName();
            PropertyInfo fixedProperty = currentInfo.getFixedProperty(c);
            if (fixedProperty.getName().equalsIgnoreCase(columnName)) {
                //found the column
                // check for ambiguous column
                QueryTableData columnTableData = getColumnTableData();
                if (columnTableData != null && columnTableData != tableData)
                    throw new SQLException("Ambiguous column name [" + columnName + "]");

                setColumnTableData(tableData);
                setColumnIndexInTable(c);

                return true;
            }
        }
        return false;
    }

    private void setColumnTableData(QueryTableData columnTable) {
        this._columnTable = columnTable;
    }

    private void setColumnIndexInTable(int columnIndex) {
        this._columnIndex = columnIndex;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _columnPath);
        IOUtils.writeString(out, _columnName);
        IOUtils.writeObject(out, _columnTable);
        out.writeInt(_columnIndex);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _columnPath = IOUtils.readString(in);
        _columnName = IOUtils.readString(in);
        _columnTable = IOUtils.readObject(in);
        _columnIndex = in.readInt();
    }
}
