package org.gigaspaces.cli;

public class CliCommandException extends Exception {
    private int exitCode = 1;

    public CliCommandException(String msg) {
        super(msg);
    }

    public int getExitCode() {
        return exitCode;
    }

    public CliCommandException exitCode(int exitCode) {
        this.exitCode = exitCode;
        return this;
    }
}
