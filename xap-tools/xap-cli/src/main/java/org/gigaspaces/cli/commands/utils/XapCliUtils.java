package org.gigaspaces.cli.commands.utils;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.GSLogConfigLoader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @since 12.3
 *
 */
public class XapCliUtils {

  private static Logger LOGGER;

  static {
    GSLogConfigLoader.getLoader("cli");
    LOGGER = Logger.getLogger(Constants.LOGGER_CLI);
  }

  public static void executeProcesses(List<ProcessBuilder> processBuilders) throws InterruptedException {
    final ExecutorService executorService = Executors.newCachedThreadPool();
    final List<Future<Integer>> futures = new ArrayList<Future<Integer>>(processBuilders.size());

    for (final ProcessBuilder processBuilder : processBuilders) {
      futures.add(executorService.submit(new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          Process process = null;
          try {
            process = processBuilder.start();

            process.waitFor();
            System.exit(process.exitValue());
          }
          catch (IOException e) {
            if( LOGGER.isLoggable(Level.SEVERE ) ){
              LOGGER.log( Level.SEVERE, e.toString(), e );
            }
          }
          catch (InterruptedException e) {
            if( process != null ) {
              process.destroy();
            }
            if( LOGGER.isLoggable(Level.SEVERE ) ){
              LOGGER.log( Level.SEVERE, e.toString(), e );
            }

          }
          return process.exitValue();
        }
      }));
    }

    addShutdownHookToKillSubProcessesOnExit(futures);
    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  public static void addShutdownHookToKillSubProcessesOnExit(final List<Future<Integer>> futures) {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        for (Future<Integer> future : futures) {
          future.cancel(true);
          try {
            future.get();
          } catch (Exception e) {
          } //ignore
        }
      }
    });
  }
}
