package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.j_spaces.jdbc.builder.range.Range;

import java.sql.SQLException;
import java.util.List;

public abstract class TableContainer {

    public abstract QueryResult executeRead(QueryExecutionConfig config) throws SQLException;

    public abstract QueryColumn addQueryColumn(String columnName, String alias);

    public abstract List<QueryColumn> getVisibleColumns();

    public abstract List<String> getAllColumnNames();

    public abstract String getTableNameOrAlias();

    public abstract void addRange(Range range);
}
