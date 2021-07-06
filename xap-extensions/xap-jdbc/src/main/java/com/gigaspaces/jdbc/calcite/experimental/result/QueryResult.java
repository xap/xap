package com.gigaspaces.jdbc.calcite.experimental.result;


import com.gigaspaces.jdbc.calcite.experimental.ResultSupplier;
import com.gigaspaces.jdbc.calcite.experimental.model.IQueryColumn;
import com.j_spaces.jdbc.ResultEntry;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public abstract class QueryResult {
    private final List<IQueryColumn> projectedColumns;
    private Cursor<TableRow> cursor;
    private Map<TableRowGroupByKey,List<TableRow>> groupByRows = new HashMap<>();

    public QueryResult(List<IQueryColumn> projectedColumns) {
        this.projectedColumns = projectedColumns;
    }

    public List<IQueryColumn> getProjectedColumns() {
        return projectedColumns;
    }

    public ResultSupplier getSingleResultSupplier() {
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
        return getCursor().next();
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
    }

    public Cursor<TableRow> getCursor() {
        if (cursor == null) {
            cursor = getCursorType().equals(Cursor.Type.SCAN) ? new RowScanCursor(getRows()) :
                    new HashedRowCursor(getSingleResultSupplier().getJoinInfo(), getRows());
        }
        return cursor;
    }

    public Cursor.Type getCursorType() {
        if (getSingleResultSupplier() != null && getSingleResultSupplier().getJoinInfo() != null) {
            return Cursor.Type.HASH;
        } else {
            return Cursor.Type.SCAN;
        }
    }

    public ResultEntry convertEntriesToResultArrays() {
        // Column (field) names and labels (aliases)
        int columns = getProjectedColumns().size();

        String[] fieldNames = getProjectedColumns().stream().map(IQueryColumn::getName).toArray(String[]::new);
        String[] columnLabels = getProjectedColumns().stream().map(qC -> qC.getAlias() == null ? qC.getName() : qC.getAlias()).toArray(String[]::new);

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

        Map<TableRowGroupByKey, TableRow> tableRows = new HashMap<>();
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
        Map<TableRowGroupByKey, TableRow> tableRows = new HashMap<>();
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
