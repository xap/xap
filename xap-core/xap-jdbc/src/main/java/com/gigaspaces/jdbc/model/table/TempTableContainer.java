package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.QueryResult;

public class TempTableContainer extends TableContainer {
    private final QueryResult tableResult;
    private final String alias;

    public TempTableContainer(QueryResult tableResult, String alias) {
        this.tableResult = tableResult;
        this.alias = alias;
    }

    @Override
    public QueryResult getResult() {
        return tableResult;
    }

    @Override
    public void addColumn(String columnName, String alias) {

    }
}
