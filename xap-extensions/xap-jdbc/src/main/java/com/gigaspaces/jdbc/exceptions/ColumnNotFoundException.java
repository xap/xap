package com.gigaspaces.jdbc.exceptions;

public class ColumnNotFoundException extends GenericJdbcException {
    public ColumnNotFoundException(String message) {
        super(message);
    }

    public ColumnNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
