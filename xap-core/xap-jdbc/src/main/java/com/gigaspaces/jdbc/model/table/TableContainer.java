package com.gigaspaces.jdbc.model.table;

import com.gigaspaces.jdbc.QueryResult;

import java.sql.SQLException;

public abstract class TableContainer {

    public abstract QueryResult getResult() throws SQLException;

    public abstract void addColumn(String columnName, String alias);
}
