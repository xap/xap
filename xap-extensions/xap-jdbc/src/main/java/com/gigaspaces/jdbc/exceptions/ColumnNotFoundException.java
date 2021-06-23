package com.gigaspaces.jdbc.exceptions;

public class ColumnNotFoundException extends GenericJdbcException {
    static final long serialVersionUID = 11891519586470646L;
    public ColumnNotFoundException(String message) {
        super(message);
    }

    public ColumnNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
