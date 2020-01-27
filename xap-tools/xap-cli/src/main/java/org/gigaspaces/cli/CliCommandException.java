package org.gigaspaces.cli;

public class CliCommandException extends Exception {
    public static final int CODE_GENERAL_ERROR = 1;
    public static final int CODE_INVALID_INPUT = 2;

    private int exitCode = CODE_GENERAL_ERROR;

    public CliCommandException(String msg) {
        super(msg);
    }

    public CliCommandException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public static CliCommandException userError(String message) {
        return new CliCommandException(message).exitCode(CODE_INVALID_INPUT);
    }

    public int getExitCode() {
        return exitCode;
    }

    public CliCommandException exitCode(int exitCode) {
        this.exitCode = exitCode;
        return this;
    }

    public boolean isUserError() {
        return exitCode == CODE_INVALID_INPUT;
    }
}
