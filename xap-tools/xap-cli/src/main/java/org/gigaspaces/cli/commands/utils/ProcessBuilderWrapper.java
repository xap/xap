package org.gigaspaces.cli.commands.utils;

/**
 * @since 12.3
 * @author evgeny
 */
public class ProcessBuilderWrapper {

  public ProcessBuilder getProcessBuilder() {
    return processBuilder;
  }

  private final ProcessBuilder processBuilder;

  public ProcessBuilderWrapper(ProcessBuilder processBuilder ){
    this.processBuilder = processBuilder;
  }

  public boolean allowToStart(){
    return true;
  }

  public boolean isSyncCommand(){
    return true;
  }
}