package com.gigaspaces.jdbc.exceptions;

import java.sql.SQLException;

public class SQLExceptionWrapper extends GenericJdbcException {
    private final SQLException e;

    public SQLExceptionWrapper(SQLException e) {
        super(e);
        this.e = e;
    }

    public SQLException getException() {
        return e;
    }
}
