package com.gigaspaces.jdbc.exceptions;

import java.sql.SQLException;

public class SQLExceptionWrapper extends GenericJdbcException {
    static final long serialVersionUID = -1247755779483927872L;
    private final SQLException e;

    public SQLExceptionWrapper(SQLException e) {
        super(e);
        this.e = e;
    }

    public SQLException getException() {
        return e;
    }
}
