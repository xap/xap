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

import com.gigaspaces.lrmi.nio.ReplyPacket;
import com.gigaspaces.start.SystemBoot;
import com.j_spaces.kernel.threadpool.GsPoolExecutorService;
import org.jini.rio.boot.CommonClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LRMIForkJoinPool extends ForkJoinPool implements GsPoolExecutorService {

    private static final Logger logger = LoggerFactory.getLogger(LRMIForkJoinPool.class);

    public LRMIForkJoinPool(String poolName, int parallelism, int priority, boolean asyncMode) {
        super(parallelism, new LRMIForkJoinWorkerThreadFactory(poolName, priority), null, asyncMode);
        logger.info("Created {} (parallelism={}, priority={}, asyncMode={})", poolName, parallelism, priority, asyncMode);
    }

    @Override
    public int getActiveCount() {
        return getActiveThreadCount();
    }

    @Override
    public int getQueueSize() {
        //return getQueuedSubmissionCount();
        return (int)getQueuedTaskCount();
    }

    @Override
    public long getCompletedTaskCount() {
        return -1; // TODO: Implement efficiently.
    }

    @Override
    public <T> ForkJoinTask<T> submit(Callable<T> task) {
        return super.submit(new FutureTask<>(task));
    }

    private static class FutureTask<T> implements Callable<T> {
        private final LRMIFuture<T> future;
        private final Callable<T> task;

        private FutureTask(Callable<T> task) {
            if (task == null)
                throw new NullPointerException("Can't execute null task.");
            this.task = task;

            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            //noinspection unchecked
            LRMIFuture<T> future = (LRMIFuture<T>) FutureContext.getFutureResult();
            if (future == null)
                future = new LRMIFuture<>(contextClassLoader);
            else
                future.reset(contextClassLoader);
            this.future = future;
        }

        @Override
        public T call() throws Exception {
            T result = null;
            Exception exception = null;

            try {
                result = task.call();
            } catch (Exception e) {
                exception = e;
            }

            future.setResultPacket(new ReplyPacket<T>(result, exception));
            return result;
        }
    }

    private static class LRMIForkJoinWorkerThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
        private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;
        private final int priority;

        private LRMIForkJoinWorkerThreadFactory(String poolName, int priority) {
            this.namePrefix = poolName + "-fjp-" + POOL_NUMBER.getAndIncrement() + "-thread-";
            this.priority = priority;
        }

        @Override
        public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
            return new LRMIForkJoinWorkerThread(pool, namePrefix + threadNumber.getAndIncrement(), priority);
        }
    }

    private static class LRMIForkJoinWorkerThread extends ForkJoinWorkerThread {
        private ClassLoader dynamicContextClassLoader;

        protected LRMIForkJoinWorkerThread(ForkJoinPool pool, String name, int priority) {
            super(pool);
            setName(name);
            setPriority(priority);
            setDaemon(true);
            dynamicContextClassLoader = super.getContextClassLoader();
            if (SystemBoot.isRunningWithinGSC()) {
                ClassLoader classLoader = CommonClassLoader.getInstance();
                super.setContextClassLoader(classLoader);
                this.dynamicContextClassLoader = classLoader;
            }
        }

        @Override
        public final void setContextClassLoader(ClassLoader classLoader) {
            dynamicContextClassLoader = classLoader;
        }

        @Override
        public final ClassLoader getContextClassLoader() {
            return dynamicContextClassLoader;
        }
    }
}
