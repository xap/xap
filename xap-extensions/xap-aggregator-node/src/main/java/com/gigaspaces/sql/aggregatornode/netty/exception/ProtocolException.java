package com.gigaspaces.sql.aggregatornode.netty.exception;

public abstract class ProtocolException extends Exception {
    private final String code;

    public ProtocolException(String message) {
        this("XX000" /* internal error */, message);
    }

    public ProtocolException(String message, Throwable cause) {
        this("XX000" /* internal error */, message, cause);
    }

    public ProtocolException(String code, String message) {
        super(message);
        this.code = code;
    }

    public ProtocolException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public abstract boolean closeSession();
}
