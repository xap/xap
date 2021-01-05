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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 12.3
 */
public class XapCliUtils {

    private static Logger LOGGER;
    private static final String PROCESS_TERMINATION_TIMEOUT = "com.gs.cli.process-termination-timeout";
    private static final long processTimeoutInSeconds = Long.getLong(PROCESS_TERMINATION_TIMEOUT, 60);
    public static final String DEMO_SPACE_NAME = "demo";
    public static final String INSIGHTEDGE_DEMO_SPACE_NAME = "demo";

    static {
        GSLogConfigLoader.getLoader("cli");
        LOGGER = LoggerFactory.getLogger(Constants.LOGGER_CLI);
    }



    public static void executeProcessesWrapper(List<ProcessBuilderWrapper> processBuilderWrappers) throws InterruptedException {
        final ExecutorService executorService = Executors.newCachedThreadPool();
        addShutdownHookToKillSubProcessesOnExit(executorService, processBuilderWrappers);
        //wait forever
        final int TIMEOUT = Integer.MAX_VALUE;

        for (final ProcessBuilderWrapper processBuilderWrapper : processBuilderWrappers) {

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        long startTime = System.currentTimeMillis();
                        while (!processBuilderWrapper.allowToStart()) {
                            Thread.sleep(500);
                            //if timeout reached
                            if (System.currentTimeMillis() - startTime > TIMEOUT) {
                                break;
                            }
                        }

                        Process process = processBuilderWrapper.start();
                        process.waitFor();
                        if (processBuilderWrapper.isSyncCommand()) {
                            System.exit(process.exitValue());
                        }
                    } catch (IOException e) {
                        if (LOGGER.isErrorEnabled()) {
                            LOGGER.error(e.toString(), e);
                        }
                    } catch (InterruptedException e) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(e.toString(), e);
                        }
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

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


    public static void addShutdownHookToKillSubProcessesOnExit(final ExecutorService executorService, final List<ProcessBuilderWrapper> processBuilderWrappers) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                final long start = System.currentTimeMillis();
                final long deadline = start + processTimeoutInSeconds * 1000;
                int destroyed = 0;
                int zombies = 0;
                //executorService.shutdownNow();
                executorService.shutdown();

                // Destroy all processes, collect processes which don't die immediately.
                List<Process> processes = new ArrayList<Process>();
                for (ProcessBuilderWrapper processBuilderWrapper : processBuilderWrappers) {
                    Process process = processBuilderWrapper.destroy();
                    if (process != null) {
                        if (process.isAlive()) {
                            processes.add(process);
                        }
                        else {
                            destroyed++;
                        }
                    }
                }

                if (!processes.isEmpty()) {
                    System.out.println("Shutdown in progress, waiting " + processTimeoutInSeconds + " seconds for sub-processes to stop (configure timeout duration via the " + PROCESS_TERMINATION_TIMEOUT + " system property)");
                    for (Process process : processes) {
                        if (killProcess(process, deadline)) {
                            destroyed++;
                        } else {
                            zombies++;
                        }
                    }
                }

                final long duration = (System.currentTimeMillis() - start);
                if (zombies == 0) {
                    System.out.printf("Shutdown completed successfully - %d sub-processes were terminated (duration: %dms)%n", destroyed, duration);
                } else {
                    System.err.println("Shutdown did not complete before the timeout (" + processTimeoutInSeconds + " seconds) elapsed - "
                            + destroyed + " sub-processes were terminated but "
                            + zombies + " sub-processes might still be running");
                }
            }

            private boolean killProcess(Process process, long deadline) {
                long timeout = deadline - System.currentTimeMillis();
                if (timeout < 1)
                    timeout = 1;
                try {
                    if (process.waitFor(timeout, TimeUnit.MILLISECONDS)) {
                        return true;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                process.destroyForcibly();
                return !process.isAlive();
            }
        });
    }
}
