package org.gigaspaces.cli;

public class CliCommandException extends Exception {
    private int exitCode = 1;
    private boolean userError = false;

    public CliCommandException(String msg) {
        super(msg);
    }

    public CliCommandException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public static CliCommandException userError(String message) {
        return new CliCommandException(message).userError();
    }

    public int getExitCode() {
        return exitCode;
    }

    public CliCommandException exitCode(int exitCode) {
        this.exitCode = exitCode;
        return this;
    }


    public boolean isUserError() {
        return userError;
    }

    public CliCommandException userError() {
        this.userError = true;
        return this;
    }
}
