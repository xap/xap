package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.SchemaTableContainer;
import com.gigaspaces.jdbc.model.table.TableContainer;

import java.util.ArrayList;
import java.util.List;

public class SchemaQueryResult extends QueryResult{
    private final SchemaTableContainer schemaTableContainer;
    private List<TableRow> rows = new ArrayList<>();

    public SchemaQueryResult(SchemaTableContainer schemaTableContainer, List<IQueryColumn> selectedColumns) {
        super(selectedColumns);
        this.schemaTableContainer = schemaTableContainer;
    }

    @Override
    public TableContainer getTableContainer() {
        return schemaTableContainer;
    }

    @Override
    public int size() {
        return rows.size();
    }

    @Override
    public void setRows(List<TableRow> rows) {
        this.rows = rows;
    }

    @Override
    public void addRow(TableRow tableRow) {
        rows.add(tableRow);
    }

    @Override
    public List<TableRow> getRows() {
        return rows;
    }
}
