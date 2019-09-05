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

package com.j_spaces.kernel.threadpool.policy;

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.ThreadDumpUtility;
import com.gigaspaces.time.SystemTime;

import java.lang.ref.WeakReference;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A handler for rejected tasks that inserts the specified element into this queue, waiting if
 * necessary up to the specified wait time for space to become available.
 */
@com.gigaspaces.api.InternalApi
public class TimedBlockingPolicy implements RejectedExecutionHandler {
    private static final int WARN_INTERVAL_TIME = 1 * 60 * 1000;
    private static final int SAMPLE_INTERVAL_TIME = Integer.getInteger("com.gs.transport_protocol.lrmi.threshold-check-interval", 15 * 1000);

    private final long waitTime;
    private final String poolName;
    private final boolean warnOnRejection;
    private final int warnThreshold;
    private final Logger warnLogger;
    private final Object warnLock = new Object();
    private long lastWarnTime = -1;
    private long lastSampleTime = -1;
    private WeakReference<Runnable> pendingFirstTask = new WeakReference<Runnable>(null);
    private long pendingTimestamp = -1;


    /**
     * @param waitTime wait time in milliseconds for space to become available.
     */
    public TimedBlockingPolicy(long waitTime) {
        this(waitTime, null, false, -1, null);
    }

    public TimedBlockingPolicy(long waitTime, String poolName, boolean warnOnRejection, int warnThreshold, Logger warnLogger) {
        this.waitTime = waitTime;
        this.poolName = poolName + "-pool";
        this.warnOnRejection = warnOnRejection;
        this.warnThreshold = warnThreshold;
        this.warnLogger = warnLogger;
    }

    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        try {
            warnThresholdExceededIfNeeded(executor);

            boolean successful = executor.getQueue().offer(r, waitTime, TimeUnit.MILLISECONDS);
            if (!successful)
                throw new RejectedExecutionException("Rejected execution after waiting "
                        + waitTime + " ms for task [" + r.getClass() + "] to be executed.");
        } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
        }
    }

    private void warnThresholdExceededIfNeeded(ThreadPoolExecutor executor) {
        if (warnOnRejection && warnLogger.isLoggable(Level.WARNING)) {
            final long timeMillis = SystemTime.timeMillis();
            if (timeMillis - lastSampleTime < SAMPLE_INTERVAL_TIME)
                return;

            //This lock will be taken at most once every 5 seconds, has no performance impact.
            synchronized (warnLock) {
                if (timeMillis - lastSampleTime < SAMPLE_INTERVAL_TIME)
                    return;

                lastSampleTime = timeMillis;

                final Runnable firstPendingTask = executor.getQueue().peek();
                //If queue is empty, for sure we will not need to warn about queue size
                if (firstPendingTask == null)
                    return;

                final Runnable previousPendingFirstTask = pendingFirstTask.get();
                //Check if first pending task did not advance
                final boolean tasksDidNotAdvanceSinceLastCheck = (previousPendingFirstTask == firstPendingTask);
                if (!tasksDidNotAdvanceSinceLastCheck) {
                    pendingFirstTask = new WeakReference<Runnable>(firstPendingTask);
                    pendingTimestamp = timeMillis;
                }
                final int queueSize = executor.getQueue().size();
                final boolean queueSizeThresholdBreached = queueSize > warnThreshold;
                if (timeMillis - lastWarnTime >= WARN_INTERVAL_TIME && (queueSizeThresholdBreached || tasksDidNotAdvanceSinceLastCheck)) {
                    lastWarnTime = timeMillis;
                    if (queueSizeThresholdBreached) {
                        String warningMsg = poolName
                                + " resources are at critical level, executions are being queued, current queue size is "
                                + queueSize
                                + " (you can modify the threshold sample rate using 'com.gs.transport_protocol.lrmi.threshold-check-interval' system property setting the desired ms interval). "
                                + addIncreaseLogLevelMessageIfNeeded();

                        warnLogger.warning(warningMsg);
                        if (warnLogger.isLoggable(Level.FINE))
                            warnLogger.fine("Generating Thread dump upon resource critical level warning " + StringUtils.NEW_LINE + ThreadDumpUtility.generateThreadDumpIfPossible());
                    }
                    if (tasksDidNotAdvanceSinceLastCheck) {
                        String severeMsg = poolName
                                + " resources are at critical level, all threads are busy and first queued execution is pending since "
                                + StringUtils.getTimeStamp(pendingTimestamp)
                                + ". This can imply on a logical deadlock in which the current JVM stops responding to remote requests and should be restarted " +
                                "(you can modify the threshold sample rate using 'com.gs.transport_protocol.lrmi.threshold-check-interval' system property setting the desired ms interval). "
                                + addIncreaseLogLevelMessageIfNeeded();

                        warnLogger.severe(severeMsg);
                        if (warnLogger.isLoggable(Level.FINE))
                            warnLogger.fine("Generating Thread dump upon resource critical level warning " + StringUtils.NEW_LINE + ThreadDumpUtility.generateThreadDumpIfPossible());
                    }
                }
            }
        }
    }

    private String addIncreaseLogLevelMessageIfNeeded() {
        String increaseLogLevelIfNeeded = warnLogger.isLoggable(Level.FINE) ? "" : "(in order to log an automatic thread dump, increase this logger logging level to FINE)";
        return increaseLogLevelIfNeeded;
    }
}
