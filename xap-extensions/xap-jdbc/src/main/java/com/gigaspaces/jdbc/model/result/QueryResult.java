package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.table.*;
import com.j_spaces.jdbc.ResultEntry;
import com.j_spaces.jdbc.query.IQueryResultSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class QueryResult { //TODO: @sagiv make different class for each cons
    private final List<QueryColumn> queryColumns;
    protected TableContainer tableContainer;
    private List<TableRow> rows;
    private Cursor<TableRow> cursor;

    public QueryResult(IQueryResultSet<IEntryPacket> res, ConcreteTableContainer tableContainer) {
        this.tableContainer = tableContainer;
        this.queryColumns = tableContainer.getSelectedColumns();
        this.rows = res.stream().map(x -> new TableRow(x, tableContainer)).collect(Collectors.toList());
    }

    //TODO: @sagiv pass the JoinQueryExecutor
    public QueryResult(List<QueryColumn> queryColumns, List<AggregationColumn> aggregationColumns) {
        this.tableContainer = null; // TODO should be handled in subquery
        this.queryColumns = order(queryColumns, aggregationColumns);
        this.rows = new ArrayList<>();
    }


    public QueryResult(List<QueryColumn> queryColumns) {
        this.tableContainer = null; // TODO should be handled in subquery
        this.queryColumns = filterNonVisibleColumns(queryColumns);
        this.rows = new ArrayList<>();
    }

    public QueryResult(TempTableContainer tempTableContainer) {
        //TODO: @sagiv keep null? because otherwise .TempTableContainer.getJoinedTable throw
        // UnsupportedOperationException - Not supported yet!
        this.tableContainer = null;
        this.queryColumns = tempTableContainer.getSelectedColumns();
        List<TableRow> tableRows = tempTableContainer.getQueryResult().getRows();
        if (tempTableContainer.hasAggregationFunctions()) {
            List<TableRow> aggregateRows = new ArrayList<>();
            aggregateRows.add(TableRow.aggregate(tableRows, tempTableContainer.getAggregationFunctionColumns()));
            this.rows = aggregateRows;
        } else { //TODO: @sagiv pass tempTable
            this.rows = tableRows.stream().map(row -> new TableRow(row, this.queryColumns, tempTableContainer.getOrderColumns())).collect(Collectors.toList());
        }
    }

    private List<QueryColumn> filterNonVisibleColumns(List<QueryColumn> queryColumns) {
        return queryColumns.stream().filter(QueryColumn::isVisible).collect(Collectors.toList());
    }

    private List<QueryColumn> order(List<QueryColumn> queryColumns, List<AggregationColumn> aggregationColumns) {
        List<QueryColumn> result = new ArrayList<>(queryColumns);
        result.addAll(aggregationColumns);
        result.sort(null);
        return result;
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
        if (tableContainer == null || tableContainer.getJoinedTable() == null)
            return getCursor().next();
        QueryResult joinedResult = tableContainer.getJoinedTable().getQueryResult();
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
        if (tableContainer == null || tableContainer.getJoinedTable() == null) {
            return;
        }
        QueryResult joinedResult = tableContainer.getJoinedTable().getQueryResult();
        if (joinedResult != null) {
            joinedResult.reset();
        }
    }

    public Cursor<TableRow> getCursor() {
        if (cursor == null) {
            cursor = getCursorType().equals(Cursor.Type.SCAN) ? new RowScanCursor(rows) : new HashedRowCursor(tableContainer.getJoinInfo(), rows);
        }
        return cursor;
    }

    public Cursor.Type getCursorType() {
        if (tableContainer != null && tableContainer.getJoinInfo() != null) {
            return Cursor.Type.HASH;
        } else {
            return Cursor.Type.SCAN;
        }
    }

    public ResultEntry convertEntriesToResultArrays(QueryExecutionConfig config) {
        //TODO: @sagiv make static/at father pass QueryResult
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

    public void filter(Predicate<TableRow> predicate) {
        rows = rows.stream().filter(predicate).collect(Collectors.toList());
    }

    public void sort() {
        Collections.sort(rows);
    }

    public List<TableRow> getRows() {
        return rows;
    }
    public void setRows(List<TableRow> rows) {
        this.rows = rows;
    }
}
