package com.gigaspaces.sql.aggregatornode.netty.exception;

import org.apache.calcite.sql.parser.SqlParserPos;

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

    public NonBreakingException(String code, SqlParserPos position, String message) {
        super(code, String.format("%s: %s", position, message));
    }

    @Override
    public boolean closeSession() {
        return false;
    }
}
