package com.gigaspaces.sql.aggregatornode.netty.exception;

public final class NonBreakingException extends ProtocolException {
    public NonBreakingException(String message) {
        super(message);
    }

    public NonBreakingException(String message, Throwable cause) {
        super(message, cause);
    }

    public NonBreakingException(String code, String message) {
        super(code, message);
    }

    public NonBreakingException(String code, String message, Throwable cause) {
        super(code, message, cause);
    }

    @Override
    public boolean closeSession() {
        return false;
    }
}
