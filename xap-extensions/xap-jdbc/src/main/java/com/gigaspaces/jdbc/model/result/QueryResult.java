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

import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.ResultEntry;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class QueryResult {
    private final List<IQueryColumn> selectedColumns;
    private Cursor<TableRow> cursor;
    private Map<TableRowGroupByKey,List<TableRow>> groupByRows = new HashMap<>();

    public QueryResult(List<IQueryColumn> selectedColumns) {
        this.selectedColumns = selectedColumns;
    }

    public List<IQueryColumn> getSelectedColumns() {
        return selectedColumns;
    }

    public TableContainer getTableContainer() {
        return null;
    }

    public int size() {
        return 0;
    }

    public void setRows(List<TableRow> rows) {
    }

    private void setGroupByRowsResult( Map<TableRowGroupByKey,List<TableRow>> groupByRows) {
        this.groupByRows = groupByRows;
    }

    public Map<TableRowGroupByKey,List<TableRow>> getGroupByRowsResult() {
        return groupByRows;
    }

    public void addRow(TableRow tableRow) {
    }

    public List<TableRow> getRows() {
        return null;
    }

    public void filter(Predicate<TableRow> predicate) {
        setRows(getRows().stream().filter(predicate).collect(Collectors.toList()));
    }

    public void sort() {
        Collections.sort(getRows());
    }

    public boolean next() {
        if (getTableContainer() == null || getTableContainer().getJoinedTable() == null) {
            return getCursor().next();
        }
        QueryResult joinedResult = getTableContainer().getJoinedTable().getQueryResult();
        if (joinedResult == null) {
            return getCursor().next();
        }
        while (hasNext()) {
            JoinInfo joinInfo = getTableContainer().getJoinedTable().getJoinInfo();
            if(joinInfo.getJoinType().equals(JoinInfo.JoinType.SEMI) && joinInfo.isHasMatch()) {
                if(getCursor().next()) {
                    joinedResult.reset();
                    joinInfo.resetHasMatch();
                }
                else{
                    return false;
                }
            }
            if (joinedResult.next()) {
                return true;
            }
            if (getCursor().next()) {
                joinedResult.reset();
            } else {
                return false;
            }
        }
        return false;
    }

    private boolean hasNext() {
        if (getCursor().isBeforeFirst())
            return getCursor().next();
        return true;
    }

    public TableRow getCurrent() {
        return getCursor().getCurrent();
    }

    public void reset() {
        getCursor().reset();
        if (getTableContainer() == null || getTableContainer().getJoinedTable() == null) {
            return;
        }
        QueryResult joinedResult = getTableContainer().getJoinedTable().getQueryResult();
        if (joinedResult != null) {
            joinedResult.reset();
        }
    }

    public Cursor<TableRow> getCursor() {
        if (cursor == null) {
            cursor = getCursorType().equals(Cursor.Type.SCAN) ? new RowScanCursor(getRows()) :
                    new HashedRowCursor(getTableContainer().getJoinInfo(), getRows());
        }
        return cursor;
    }

    public Cursor.Type getCursorType() {
        if (getTableContainer() != null && getTableContainer().getJoinInfo() != null) {
            return Cursor.Type.HASH;
        } else {
            return Cursor.Type.SCAN;
        }
    }

    public ResultEntry convertEntriesToResultArrays() {
        // Column (field) names and labels (aliases)
        int columns = getSelectedColumns().size();

        String[] fieldNames = getSelectedColumns().stream().map(IQueryColumn::getName).toArray(String[]::new);
        String[] columnLabels = getSelectedColumns().stream().map(qC -> qC.getAlias() == null ? qC.getName() : qC.getAlias()).toArray(String[]::new);

        //the field values for the result
        Object[][] fieldValues = new Object[size()][columns];


        int row = 0;

        while (next()) {
            TableRow entry = getCurrent();
            int column = 0;
            for (int i = 0; i < columns; i++) {
                fieldValues[row][column++] = entry.getPropertyValue(i);
            }

            row++;
        }

        return new ResultEntry(
                fieldNames,
                columnLabels,
                null, //TODO
                fieldValues);
    }

    public void groupBy(){

        Map<TableRowGroupByKey,TableRow> tableRows = new HashMap<>();
        Map<TableRowGroupByKey,List<TableRow>> groupByTableRows = new HashMap<>();

        for( TableRow tableRow : getRows() ){
            Object[] groupByValues = tableRow.getGroupByValues();
            if( groupByValues.length > 0 ){
                TableRowGroupByKey key = new TableRowGroupByKey( groupByValues );
                tableRows.putIfAbsent( key, tableRow );

                List<TableRow> tableRowsList = groupByTableRows.computeIfAbsent(key, k -> new ArrayList<>());
                tableRowsList.add( tableRow );
            }
        }
        if( !tableRows.isEmpty() ) {
            setGroupByRowsResult( groupByTableRows );
            setRows( new ArrayList<>(tableRows.values()) );
        }
    }

    public void distinct() {
        Map<TableRowGroupByKey,TableRow> tableRows = new HashMap<>();
        for( TableRow tableRow : getRows() ){
            Object[] distinctValues = tableRow.getDistinctValues();
            if( distinctValues.length > 0 ){
                tableRows.putIfAbsent( new TableRowGroupByKey( distinctValues ), tableRow );
            }
        }
        if (!tableRows.isEmpty()){
            setRows( new ArrayList<>(tableRows.values()));
        }

    }
}
