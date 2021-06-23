package com.gigaspaces.jdbc.exceptions;


import java.sql.SQLException;

public abstract class GenericJdbcException extends RuntimeException {
    static final long serialVersionUID = 3118109450412077316L;
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
