package org.gigaspaces.cli.commands.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @since 12.3
 *
 */
public class XapCliUtils {


  public static void executeProcesses(List<ProcessBuilder> processBuilders) throws InterruptedException {
    final ExecutorService executorService = Executors.newCachedThreadPool();
    final List<Future<Integer>> futures = new ArrayList<Future<Integer>>(processBuilders.size());

    for (final ProcessBuilder processBuilder : processBuilders) {
      futures.add(executorService.submit(new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          Process process = processBuilder.start();
          try {
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = br.readLine()) != null) {
              System.out.println( "~~~ " + line );
            }

            process.waitFor();
            System.exit(process.exitValue());
          } catch (InterruptedException e) {
            process.destroy();
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
