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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @since 12.3
 */
public class XapCliUtils {

    private static Logger LOGGER;
    private static int processTimeoutInSeconds = 120;

    static {
        GSLogConfigLoader.getLoader("cli");
        LOGGER = Logger.getLogger(Constants.LOGGER_CLI);
    }

    public static void executeProcessesWrapper(List<ProcessBuilderWrapper> processBuilderWrappers) throws InterruptedException {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        final int TIMEOUT = 60 * 1 * 1000;

        for (final ProcessBuilderWrapper processBuilderWrapper : processBuilderWrappers) {

            executorService.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    Process process = null;
                    try {
                        long startTime = System.currentTimeMillis();
                        while (!processBuilderWrapper.allowToStart()) {
                            Thread.sleep(500);
                            //if timeout reached
                            if (System.currentTimeMillis() - startTime > TIMEOUT) {
                                break;
                            }
                        }

                        final ProcessBuilder processBuilder = processBuilderWrapper.getProcessBuilder();

                        process = processBuilder.start();
                        process.waitFor();
                        if (processBuilderWrapper.isSyncCommand()) {
                            System.exit(process.exitValue());
                        }
                    } catch (IOException e) {
                        if (LOGGER.isLoggable(Level.SEVERE)) {
                            LOGGER.log(Level.SEVERE, e.toString(), e);
                        }
                    } catch (InterruptedException e) {
                        if (process != null) {
                            if (!process.waitFor(processTimeoutInSeconds, TimeUnit.SECONDS)) {
                                LOGGER.fine("Termination timeout (" + processTimeoutInSeconds + " seconds) elapsed, one or more sub-processes might still be running");
                                process.destroyForcibly();
                            }
                        }
                        if (LOGGER.isLoggable(Level.SEVERE)) {
                            LOGGER.log(Level.SEVERE, e.toString(), e);
                        }

                    }
                    return process.exitValue();
                }
            });
        }

        addShutdownHookToKillSubProcessesOnExit(executorService);
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }


    public static void executeProcesses(List<ProcessBuilder> processBuilders) throws InterruptedException {
        executeProcessesWrapper(wrapList(processBuilders));
    }

    public static void executeProcess(ProcessBuilder processBuilder) throws InterruptedException {
        executeProcesses(Collections.singletonList(processBuilder));
    }

    private static List<ProcessBuilderWrapper> wrapList(List<ProcessBuilder> lst) {
        List<ProcessBuilderWrapper> wrappedList = new ArrayList<ProcessBuilderWrapper>(lst.size());
        for (ProcessBuilder cur : lst) {
            wrappedList.add(new ProcessBuilderWrapper(cur));
        }
        return wrappedList;
    }


    public static void addShutdownHookToKillSubProcessesOnExit(final ExecutorService executorService) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                executorService.shutdownNow();
                try {
                    if (executorService.awaitTermination(processTimeoutInSeconds + 1, TimeUnit.SECONDS)) {
                        long took = (System.currentTimeMillis() - start);
                        LOGGER.info("Termination completed successfully (duration: " + TimeUnit.MILLISECONDS.toSeconds(took) + "s)");
                    } else {
                        LOGGER.warning("Termination timeout (" + processTimeoutInSeconds + " seconds) elapsed, one or more sub-processes might still be running");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
    }
}
