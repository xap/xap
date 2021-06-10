package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.IQueryColumn;

import java.util.ArrayList;
import java.util.List;

public class JoinQueryResult extends QueryResult {
    private List<TableRow> rows;

    public JoinQueryResult(List<IQueryColumn> selectedColumns) {
        super(selectedColumns);
        this.rows = new ArrayList<>();
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