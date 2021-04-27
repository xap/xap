package com.gigaspaces.jdbc.exceptions;


import java.sql.SQLException;

public abstract class GenericJdbcException extends RuntimeException {
    public GenericJdbcException(String message, Throwable cause) {
        super(message, cause);
    }

    public GenericJdbcException(String message) {
        super(message);
    }

    public GenericJdbcException(SQLException e) {
        super(e);
    }
}
