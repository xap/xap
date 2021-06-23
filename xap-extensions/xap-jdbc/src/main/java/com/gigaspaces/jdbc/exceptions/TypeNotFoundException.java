package com.gigaspaces.jdbc.exceptions;

public class TypeNotFoundException extends GenericJdbcException {
    static final long serialVersionUID = -545903542424292033L;
    public TypeNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
