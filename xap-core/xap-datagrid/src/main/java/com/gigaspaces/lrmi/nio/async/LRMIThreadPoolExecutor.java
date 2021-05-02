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

package com.gigaspaces.lrmi.nio.async;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.nio.ReplyPacket;
import com.gigaspaces.start.SystemBoot;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.threadpool.DynamicExecutors;
import com.j_spaces.kernel.threadpool.DynamicThread;
import com.j_spaces.kernel.threadpool.DynamicThreadPoolExecutor;
import com.j_spaces.kernel.threadpool.policy.TimedBlockingPolicy;
import com.j_spaces.kernel.threadpool.queue.DynamicQueue;

import org.jini.rio.boot.CommonClassLoader;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a version of dynamicThreadPool that can return an IFuture from a submit call.
 *
 * @author asy ronen
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class LRMIThreadPoolExecutor extends DynamicThreadPoolExecutor {
    private static final Logger logger = LoggerFactory.getLogger(LRMIThreadPoolExecutor.class);
    private final static Logger _warnLogger = LoggerFactory.getLogger(Constants.LOGGER_LRMI + ".resources");
    private final static double _warnFactor = Double.parseDouble(System.getProperty(SystemProperties.LRMI_RESOURCE_WARN_THRESHOLD_FACTOR, "10.0"));

    public LRMIThreadPoolExecutor(int min, int max, long keepAliveTime, int capacity,
                                  long waitTime, int priority, String poolName, boolean preStart, boolean warnOnQueueUsage) {
        super(min, max, keepAliveTime, TimeUnit.MILLISECONDS, new DynamicQueue<Runnable>(capacity));
        final ThreadFactory threadFactory = DynamicExecutors.priorityThreadFactory(priority, poolName);

        //noinspection NullableProblems
        setThreadFactory(new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable runnable) {
                // since the LRMI is loaded in the common class loader, make sure each new thread
                // that is created is using the common class loader (even if it was created in the
                // context of a processing unit).
                DynamicThread t = (DynamicThread) threadFactory.newThread(runnable);
                if (SystemBoot.isRunningWithinGSC())
                    t.setAllThreadContextClassLoader(CommonClassLoader.getInstance());
                return t;
            }
        });
        //noinspection ConstantConditions
        TimedBlockingPolicy timedBlockingPolicy = warnOnQueueUsage ? new TimedBlockingPolicy(waitTime, poolName, warnOnQueueUsage, (int) (max * _warnFactor), _warnLogger) : new TimedBlockingPolicy(waitTime);
        setRejectedExecutionHandler(timedBlockingPolicy);
        ((DynamicQueue) getQueue()).setThreadPoolExecutor(this);
        if (preStart)
            prestartAllCoreThreads();
        logger.info("Created {} (min={}, max={}, capacity={}, priority={}, keepAliveTime={}, waitTime={})", poolName, min, max, capacity, priority, keepAliveTime, waitTime);
    }

    @Override
    public <T> IFuture<T> submit(Callable<T> task) {
        if (task == null)
            throw new NullPointerException("Can't execute null task.");

        //noinspection unchecked
        LRMIFuture<T> future = (LRMIFuture<T>) FutureContext.getFutureResult();
        if (future == null)
            future = new LRMIFuture<T>(Thread.currentThread().getContextClassLoader());
        else
            future.reset(Thread.currentThread().getContextClassLoader());

        execute(new FutureTask<T>(future, task));
        return future;
    }

    private static class FutureTask<T> implements Runnable {
        private final LRMIFuture<T> _future;
        private final Callable<T> _task;

        private FutureTask(LRMIFuture<T> future, Callable<T> task) {
            this._future = future;
            this._task = task;
        }

        @Override
        public void run() {
            T result = null;
            Exception exception = null;

            try {
                result = _task.call();
            } catch (Exception e) {
                exception = e;
            }

            _future.setResultPacket(new ReplyPacket<T>(result, exception));
        }
    }
}
