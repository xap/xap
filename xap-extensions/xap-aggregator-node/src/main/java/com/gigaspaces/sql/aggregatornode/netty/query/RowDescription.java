package com.gigaspaces.sql.aggregatornode.netty.query;

import java.util.Collections;
import java.util.List;

public class RowDescription {
    private final List<ColumnDescription> columns;

    public RowDescription(List<ColumnDescription> columns) {
        this.columns = columns;
    }

    public int getColumnsCount() {
        return columns.size();
    }

    public List<ColumnDescription> getColumns() {
        return Collections.unmodifiableList(columns);
    }
}
