package org.gigaspaces.cli.commands.utils;

import java.io.IOException;

/**
 * @since 12.3
 * @author evgeny
 */
public class ProcessBuilderWrapper {

    private final ProcessBuilder processBuilder;
    private final Object lock = new Object();
    private Process process;
    private boolean destroyed;

    public ProcessBuilderWrapper(ProcessBuilder processBuilder ){
        this.processBuilder = processBuilder;
    }

    public Process start() throws IOException {
        synchronized (lock) {
            if (destroyed)
                throw new IOException("Cannot create process - already destroyed");
            this.process = processBuilder.start();
            return process;
        }
    }

    public Process destroy() {
        synchronized (lock) {
            destroyed = true;
            if (process != null)
                process.destroy();
            return process;
        }
    }

    public boolean allowToStart(){
    return true;
  }

  public boolean isSyncCommand(){
    return true;
  }
}