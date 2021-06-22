package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.TempTableContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TempQueryResult extends QueryResult{
    private List<TableRow> rows = new ArrayList<>();
    private final TempTableContainer tempTableContainer;

    public TempQueryResult(TempTableContainer tempTableContainer) {
        super(tempTableContainer.getSelectedColumns());
        this.tempTableContainer = tempTableContainer;
        List<TableRow> tableRows = tempTableContainer.getQueryResult().getRows();
        if (tempTableContainer.hasAggregationFunctions() && !tempTableContainer.hasGroupByColumns()) {
            this.rows.add( aggregate(tableRows) );
        } else {
            this.rows = tableRows.stream().map(row -> TableRowFactory.createProjectedTableRow(row, tempTableContainer)).collect(Collectors.toList());
        }
    }

    public TableRow aggregate( List<TableRow> tableRows) {
        return TableRowUtils.aggregate(tableRows, tempTableContainer.getSelectedColumns(),
                            tempTableContainer.getAggregationColumns(), tempTableContainer.getVisibleColumns() );
    }

    @Override
    public int size() {
        return this.rows.size();
    }

    @Override
    public void addRow(TableRow tableRow) {
        this.rows.add(tableRow);
    }

    @Override
    public List<TableRow> getRows() {
        return this.rows;
    }

    @Override
    public void setRows(List<TableRow> rows) {
        this.rows = rows;
    }
}
