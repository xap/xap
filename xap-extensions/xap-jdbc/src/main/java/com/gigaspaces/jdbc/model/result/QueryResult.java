package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.table.AggregationFunction;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.ResultEntry;
import com.j_spaces.jdbc.query.IQueryResultSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class QueryResult {
    private final List<QueryColumn> queryColumns;
    private List<TableRow> rows;
    protected TableContainer tableContainer;
    private Cursor<TableRow> cursor;

    public QueryResult(IQueryResultSet<IEntryPacket> res, List<QueryColumn> queryColumns, TableContainer tableContainer) {
        this.queryColumns = filterNonVisibleColumns(queryColumns);
        this.tableContainer = tableContainer;
        this.rows =
                res.stream().map(x -> new TableRow(x, this.queryColumns, tableContainer.getOrderColumns())).collect(Collectors.toList());
    }

    public QueryResult(List<QueryColumn> queryColumns) {
        this.tableContainer = null; // TODO should be handled in subquery
        this.queryColumns = filterNonVisibleColumns(queryColumns);
        this.rows = new ArrayList<>();
    }

    public QueryResult(List<QueryColumn> visibleColumns, QueryResult tableResult, List<OrderColumn> orderColumns) {
        this.tableContainer = null;
        this.queryColumns = visibleColumns;
        this.rows = tableResult.rows.stream().map(row -> new TableRow(row, visibleColumns, orderColumns)).collect(Collectors.toList());
        boolean hasAggregationFunctions =
                visibleColumns.stream().anyMatch(queryColumn -> queryColumn instanceof AggregationFunction);
        if(hasAggregationFunctions) {
            aggregate();
        }
    }

    private void aggregate() {
        final List<TableRow> aggregateRows = new ArrayList<>();

        for (QueryColumn queryColumn : queryColumns) {
            Comparator<TableRow> tableRowValueComparator = new Comparator<TableRow>() {
                @Override
                public int compare(TableRow o1, TableRow o2) {
                    Comparable first = (Comparable) o1.getPropertyValue(queryColumn.getName());
                    Comparable second = (Comparable) o2.getPropertyValue(queryColumn.getName());
                    if (first == second) {
                        return 0;
                    }
                    if (first == null) {
                        return -1;
                    }
                    if (second == null) {
                        return 1;
                    }
                    return first.compareTo(second);
                }
            };
            TableRow tableRow = null;
            if(queryColumn instanceof AggregationFunction) {
                AggregationFunction.AggregationFunctionType type = ((AggregationFunction) queryColumn).getType();
                if(type == AggregationFunction.AggregationFunctionType.MAX) {
                    tableRow = this.rows.stream().max(tableRowValueComparator).orElseGet(() -> new TableRow(new QueryColumn[]{queryColumn}, new Object[]{null}));
                } else if(type == AggregationFunction.AggregationFunctionType.MIN) {
                    tableRow = this.rows.stream().min(tableRowValueComparator).orElseGet(() -> new TableRow(new QueryColumn[]{queryColumn}, new Object[]{null}));
                } else if(type == AggregationFunction.AggregationFunctionType.AVG) {
                    //TODO: pass the column type? need to be implement.
                    double v = this.rows.stream().mapToDouble(e -> (double) e.getPropertyValue(queryColumn.getName())).average().orElse(0.0);
                    tableRow = new TableRow(new QueryColumn[] {queryColumn}, new Object[] {v});
                } else if(type == AggregationFunction.AggregationFunctionType.SUM) {
                    //TODO: pass the column type?  need to be implement.
                    Object v = this.rows.stream().mapToDouble(e -> (double) e.getPropertyValue(queryColumn.getName())).sum();
                    tableRow = new TableRow(new QueryColumn[] {queryColumn}, new Object[] {v});
                } else { // (type == AggregationFunction.AggregationFunctionType.COUNT)
                    long nonNullCount =
                            this.rows.stream().filter(tr -> tr.getPropertyValue(queryColumn.getName()) != null).count();
                    tableRow = new TableRow(new QueryColumn[]{queryColumn}, new Object[]{nonNullCount});
                }
            } else {
                tableRow = this.rows.stream().findAny().orElseGet(() -> new TableRow(new QueryColumn[]{queryColumn}, new Object[]{null}));
            }
            aggregateRows.add(tableRow);
        }
        this.rows = aggregateRows;
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
            cursor = getCursorType().equals(Cursor.Type.SCAN) ? new RowScanCursor(rows) : new HashedRowCursor(tableContainer.getJoinInfo(), rows);
        }
        return cursor;
    }

    public Cursor.Type getCursorType(){
        if(tableContainer != null && tableContainer.getJoinInfo() != null) {
            return Cursor.Type.HASH;
        }
        else {
            return Cursor.Type.SCAN;
        }
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

    public void filter(Predicate<TableRow> predicate) {
        rows = rows.stream().filter(predicate).collect(Collectors.toList());
    }

    public void sort(){
        Collections.sort(rows);
    }
}
