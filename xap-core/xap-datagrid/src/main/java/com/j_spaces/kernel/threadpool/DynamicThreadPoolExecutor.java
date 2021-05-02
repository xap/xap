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

package com.j_spaces.kernel.threadpool;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An {@link ExecutorService} that executes each submitted task using one of possibly several pooled
 * threads, normally configured using {@link DynamicExecutors} factory methods.
 *
 * @author moran
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class DynamicThreadPoolExecutor extends ThreadPoolExecutor implements GsPoolExecutorService {
    /**
     * number of threads that are actively executing tasks
     */
    private final AtomicInteger activeCount = new AtomicInteger();

    public DynamicThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
                                     long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    @SuppressWarnings("unused")
    public DynamicThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new LinkedBlockingQueue<Runnable>());
    }

    @Override
    public int getActiveCount() {
        return activeCount.get();
    }

    @Override
    public int getQueueSize() {
        return getQueue().size();
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        activeCount.incrementAndGet();
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        activeCount.decrementAndGet();
    }

}
