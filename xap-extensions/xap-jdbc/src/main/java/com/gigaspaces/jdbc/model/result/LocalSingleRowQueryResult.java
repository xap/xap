package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.IQueryColumn;

import java.util.ArrayList;
import java.util.List;

public class LocalSingleRowQueryResult extends QueryResult{
    private List<TableRow> rows = new ArrayList<>();
    public LocalSingleRowQueryResult(List<IQueryColumn> columns, TableRow row) {
        super( columns );
        rows.add(row);
    }

    @Override
    public List<TableRow> getRows() {
        return this.rows;
    }

    @Override
    public int size() {
        return this.rows.size();
    }
}
