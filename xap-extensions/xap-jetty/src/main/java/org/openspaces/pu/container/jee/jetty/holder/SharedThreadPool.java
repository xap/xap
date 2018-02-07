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


package org.openspaces.pu.container.jee.jetty.holder;

import com.gigaspaces.internal.utils.SharedInstance;
import com.gigaspaces.internal.utils.Singletons;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.util.thread.ThreadPool;

/**
 * A Jetty {@link ThreadPool} that shares a single instance of a thread pool. The first thread pool
 * passed will win and be used.
 *
 * @author kimchy
 */
public class SharedThreadPool implements ThreadPool, LifeCycle {

    private static final Log logger = LogFactory.getLog(SharedThreadPool.class);
    private static final String SHARED_JETTY_KEY = "jetty.threadpool";

    private final SharedInstance<ThreadPool> threadPool;

    public SharedThreadPool(ThreadPool threadPool) {
        SharedInstance<ThreadPool> newThreadPool = new SharedInstance<ThreadPool>(threadPool);
        this.threadPool = (SharedInstance<ThreadPool>) Singletons.putIfAbsent(SHARED_JETTY_KEY, newThreadPool);

        if (this.threadPool == newThreadPool) {
            logger.debug("Using new thread pool [" + threadPool + "]");
        } else {
            logger.debug("Using existing thread pool [" + threadPool + "]");
        }
    }

    @Override
    public void execute(Runnable command) {
        threadPool.value().execute(command);
    }

    public void join() throws InterruptedException {
        threadPool.value().join();
    }

    public int getThreads() {
        return threadPool.value().getThreads();
    }

    public int getIdleThreads() {
        return threadPool.value().getIdleThreads();
    }

    public boolean isLowOnThreads() {
        return threadPool.value().isLowOnThreads();
    }

    public void start() throws Exception {
        // start the first one
        if (threadPool.increment() == 1) {
            if (logger.isDebugEnabled()) {
                logger.debug("Starting thread pool [" + threadPool + "]");
            }
            ((LifeCycle) threadPool.value()).start();
        }
    }

    public void stop() throws Exception {
        // stop the last one
        if (threadPool.decrement() == 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Stopping thread pool [" + threadPool + "]");
            }
            ((LifeCycle) threadPool.value()).stop();
        }
    }

    public boolean isRunning() {
        return ((LifeCycle) threadPool.value()).isRunning();
    }

    public boolean isStarted() {
        return ((LifeCycle) threadPool.value()).isStarted();
    }

    public boolean isStarting() {
        return ((LifeCycle) threadPool.value()).isStarting();
    }

    public boolean isStopping() {
        return ((LifeCycle) threadPool.value()).isStopping();
    }

    public boolean isStopped() {
        return ((LifeCycle) threadPool.value()).isStopped();
    }

    public boolean isFailed() {
        return ((LifeCycle) threadPool.value()).isFailed();
    }

    public void addLifeCycleListener(Listener listener) {
        ((LifeCycle) threadPool.value()).addLifeCycleListener(listener);
    }

    public void removeLifeCycleListener(Listener listener) {
        ((LifeCycle) threadPool.value()).removeLifeCycleListener(listener);
    }

    public String toString() {
        return "Shared(" + threadPool.count() + ") [" + threadPool.value() + "]";
    }
}
