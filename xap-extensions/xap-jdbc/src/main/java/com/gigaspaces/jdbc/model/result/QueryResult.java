package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.ResultEntry;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class QueryResult {
    private final List<IQueryColumn> selectedColumns;
    private Cursor<TableRow> cursor;

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

        Map<Object,TableRow> tableRows = new HashMap<>();
        for( TableRow tableRow : getRows() ){
            Object[] groupByValues = tableRow.getGroupByValues();
            if( groupByValues.length > 0 ){
                if(groupByValues.length == 1){
                    //in the case of single value in groupByValues array use this value as a key in order to prevent list creation
                    tableRows.put(groupByValues[0], tableRow);
                }
                else {
                    //create key based on array of values
                    tableRows.put(Arrays.asList(groupByValues), tableRow);
                }
            }
        }
        if( !tableRows.isEmpty() ) {
            setRows(new ArrayList<>(tableRows.values()));
        }
    }
}
