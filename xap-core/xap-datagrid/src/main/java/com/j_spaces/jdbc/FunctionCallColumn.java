package com.j_spaces.jdbc;

import com.j_spaces.jdbc.query.QueryTableData;

import java.sql.SQLException;

@com.gigaspaces.api.InternalApi
public class FunctionCallColumn extends SelectColumn {
    public FunctionCallColumn() {
    }

    public FunctionCallColumn(String columnPath) {
        super(columnPath);
    }

    public FunctionCallColumn(String columnName, String columnAlias) {
        super(columnName, columnAlias);
    }

    public FunctionCallColumn(QueryTableData tableData, String columnPath) throws SQLException {
        super(tableData, columnPath);
    }

    public FunctionCallColumn(QueryTableData tableData, String columnPath, boolean isDynamic) throws SQLException {
        super(tableData, columnPath, isDynamic);
    }
}
