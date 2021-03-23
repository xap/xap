package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.range.Range;

import java.util.List;
import java.util.stream.Collectors;

public class TempTableContainer extends TableContainer {
    private final QueryResult tableResult;
    private final String alias;

    public TempTableContainer(QueryResult tableResult, String alias) {
        this.tableResult = tableResult;
        this.alias = alias;
    }

    @Override
    public QueryResult executeRead(QueryExecutionConfig config) {
        return tableResult;
    }

    @Override
    public QueryColumn addQueryColumn(String columnName, String alias) {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public List<QueryColumn> getVisibleColumns() {
        return tableResult.getQueryColumns();
    }

    @Override
    public List<String> getAllColumnNames() {
        return tableResult.getQueryColumns().stream().map(QueryColumn::getName).collect(Collectors.toList());
    }

    @Override
    public String getTableNameOrAlias() {
        throw new UnsupportedOperationException("Not supported yet!");
    }

    @Override
    public void addRange(Range range) {
        throw new UnsupportedOperationException("Not supported yet!");
    }
}
