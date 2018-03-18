/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
