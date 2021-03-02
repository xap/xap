package com.gigaspaces.jdbc.exceptions;


public class GenericJdbcException extends RuntimeException {
    public GenericJdbcException(String message, Throwable cause) {
        super(message, cause);
    }

    public GenericJdbcException(String message) {
        super(message);
    }
}
