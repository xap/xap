package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.ResultEntry;
import com.j_spaces.jdbc.query.IQueryResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class QueryResult {
    private final List<QueryColumn> queryColumns;
    private List<TableRow> rows;
    protected final TableContainer tableContainer;
    private Cursor<TableRow> cursor;

    public QueryResult(IQueryResultSet<IEntryPacket> res, List<QueryColumn> queryColumns, TableContainer tableContainer) {
        this.queryColumns = filterNonVisibleColumns(queryColumns);
        this.tableContainer = tableContainer;
        this.rows = res.stream().map(x -> new TableRow(x, queryColumns)).collect(Collectors.toList());
    }

    public QueryResult(List<QueryColumn> queryColumns) {
        this.tableContainer = null; // TODO should be handled in subquery
        this.queryColumns = filterNonVisibleColumns(queryColumns);
        this.rows = new ArrayList<>();
    }

    public QueryResult(List<QueryColumn> visibleColumns, QueryResult tableResult) {
        this.tableContainer = null;
        this.queryColumns = visibleColumns;
        this.rows = tableResult.rows.stream().map(row -> new TableRow(row, visibleColumns)).collect(Collectors.toList());
    }

    private List<QueryColumn> filterNonVisibleColumns(List<QueryColumn> queryColumns){
        return queryColumns.stream().filter(QueryColumn::isVisible).collect(Collectors.toList());
    }

    public List<QueryColumn> getQueryColumns() {
        return queryColumns;
    }

    public int size() {
        return rows.size();
    }

    public void add(TableRow tableRow) {
        this.rows.add(tableRow);
    }

    public boolean next() {
        if(tableContainer == null || tableContainer.getJoinedTable() == null)
            return getCursor().next();
        QueryResult joinedResult = tableContainer.getJoinedTable().getQueryResult();
        if(joinedResult == null){
            return getCursor().next();
        }
        while (hasNext()){
            if(joinedResult.next()){
                return true;
            }
            if(getCursor().next()){
                joinedResult.reset();
            }else{
                return false;
            }
        }
        return false;
    }

    private boolean hasNext() {
        if(getCursor().isBeforeFirst())
            return getCursor().next();
        return true;
    }

    public TableRow getCurrent() {
        return getCursor().getCurrent();
    }

    public void reset() {
        getCursor().reset();
        if(tableContainer == null || tableContainer.getJoinedTable() == null) {
            return;
        }
        QueryResult joinedResult = tableContainer.getJoinedTable().getQueryResult();
        if(joinedResult != null) {
            joinedResult.reset();
        }
    }

    public Cursor<TableRow> getCursor() {
        if(cursor == null) {
            if(tableContainer != null && tableContainer.getJoinInfo() != null) {
                cursor = new HashedRowCursor(tableContainer.getJoinInfo(), rows);
            }
            else {
                cursor = new RowScanCursor(rows);
            }
        }
        return cursor;
    }

    public ResultEntry convertEntriesToResultArrays(QueryExecutionConfig config) {
        QueryResult queryResult = this;
        // Column (field) names and labels (aliases)
        int columns = queryResult.getQueryColumns().size();

        String[] fieldNames = queryResult.getQueryColumns().stream().map(QueryColumn::getName).toArray(String[]::new);
        String[] columnLabels = queryResult.getQueryColumns().stream().map(qC -> qC.getAlias() == null ? qC.getName() : qC.getAlias()).toArray(String[]::new);

        //the field values for the result
        Object[][] fieldValues = new Object[queryResult.size()][columns];


        int row = 0;

        while (queryResult.next()) {
            TableRow entry = queryResult.getCurrent();
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

    public List<TableRow> getRows() {
        return rows;
    }
    public void filter(Predicate<TableRow> predicate) {
        rows = rows.stream().filter(predicate).collect(Collectors.toList());
    }
}
