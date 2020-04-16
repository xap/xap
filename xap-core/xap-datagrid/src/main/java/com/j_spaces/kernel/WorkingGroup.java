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

/*
 * Title: WorkingGroup.java Description: WorkingGroup implementation on top of
 * the concurrent ThreadPoolExecutor. As a convention, "this" calls
 * ThreadPoolExecutor's methods. Company: GigaSpaces Technologies @author Moran
 * Avigdor
 * 
 * @version 4.1 17/04/2005
 */

package com.j_spaces.kernel;

import com.gigaspaces.logger.Constants;
import com.j_spaces.kernel.threadpool.DynamicExecutors;
import com.j_spaces.kernel.threadpool.DynamicThreadPoolExecutor;
import com.j_spaces.kernel.threadpool.policy.ForceQueuePolicy;
import com.j_spaces.kernel.threadpool.policy.TimedBlockingPolicy;
import com.j_spaces.kernel.threadpool.queue.DynamicQueue;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class abstracts a group of cooperating objects that together offer asynchronous message
 * delivery to consumers. A working group is composed of 4 components: A queue of tasks, a
 * worker-thread pool, and a consumer object. When messages (TaskWrapper Objects) are inserted into
 * the WorkingGroup, they are inserted into the queue. The thread pool is responsible of deciding
 * how many worker threads read the messages from the queue, and dispatch them to the consumer
 * object. The consumer object may do anything it pleases with the messages, including forwarding
 * them to another object(s).
 */
@com.gigaspaces.api.InternalApi
public class WorkingGroup<E>
        extends DynamicThreadPoolExecutor {
    final private IConsumerObject<E> _consumerObject;
    final private String _workingGroupName;
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_KERNEL);

    /**
     * TaskWrapper for the runnable object to be run by a dispatcher. Dispatcher implements the
     * IConsumerObject interface.
     */
    private static final class TaskWrapper<T> implements Runnable {
        final private T task;
        final private IConsumerObject<T> consumerObject;

        public TaskWrapper(final T task, final IConsumerObject<T> consumerObject) {
            this.task = task;
            this.consumerObject = consumerObject;
        }

        /**
         * calls the dispatcher message of the consumer on this task
         */
        public void run() {
            consumerObject.dispatch(task);
        }

    }

    /**
     * Constructs a working-group that creates new threads as needed, but will reuse previously
     * constructed threads when they are available. If no existing thread is available, a new thread
     * will be created and added to the pool. No more than <tt>maxPoolSize</tt> threads will be
     * created. Threads that have not been used for a <tt>keepAliveTime</tt> timeout are terminated
     * and removed. Thus, a pool that remains idle for long enough will not consume any resources
     * other than the <tt>minPoolSize</tt> specified. <p> Threads are created with <tt>priority</tt>
     * and named after the working- group name <tt>wgName</tt>. <p> The working-group manages a
     * queue of tasks, which is limited in <tt>capacity</tt>. If there are no idle threads and the
     * queue has reached it's <tt>capacity</tt> the executing thread will block for
     * <tt>waitTime</tt> until the task can be queued. A {@link RejectedExecutionException} will be
     * thrown at the end of the <tt>waitTime</tt>.
     */
    public WorkingGroup(IConsumerObject<E> consumerObject, int priority,
                        String wgName, int minPoolSize, int maxPoolSize, long keepAliveTime,
                        int capacity, long waitTime) {
        super(resolvePoolSize(minPoolSize), resolvePoolSize(maxPoolSize),
                keepAliveTime, TimeUnit.MILLISECONDS, new DynamicQueue<Runnable>(capacity));

        _consumerObject = consumerObject;
        _workingGroupName = wgName;

        if (waitTime == Long.MAX_VALUE)
            setRejectedExecutionHandler(new ForceQueuePolicy());
        else
            setRejectedExecutionHandler(new TimedBlockingPolicy(waitTime));

        setThreadFactory(DynamicExecutors.priorityThreadFactory(priority, wgName));
        ((DynamicQueue<Runnable>) getQueue()).setThreadPoolExecutor(this);
    }


    /**
     * Constructs a working-group that creates new threads as needed, but will reuse previously
     * constructed threads when they are available. If no existing thread is available, a new thread
     * will be created and added to the pool. No more than <tt>maxPoolSize</tt> threads will be
     * created. Threads that have not been used for a <tt>keepAliveTime</tt> timeout are terminated
     * and removed. Thus, a pool that remains idle for long enough will not consume any resources
     * other than the <tt>minPoolSize</tt> specified. <p> Threads are created with <tt>priority</tt>
     * and named after the working- group name <tt>wgName</tt>. <p> The working-group manages an
     * <b>unbound</b> queue of tasks. The executing thread will never block and always favor queuing
     * if there is no idle thread to handle the task. Equivalent to using a <tt>capacity</tt> of
     * {@link Integer#MAX_VALUE} and a <tt>waitTime</tt> of {@link Long#MAX_VALUE}. <p>
     */
    public WorkingGroup(IConsumerObject<E> consumerObject, int priority,
                        String wgName, int minPoolSize, int maxPoolSize, long keepAliveTime) {
        this(consumerObject, priority, wgName, minPoolSize, maxPoolSize,
                keepAliveTime, Integer.MAX_VALUE /*capacity*/, Long.MAX_VALUE /*waitTime*/);
    }


    /**
     * resolve either minPoolSize or maxPoolsize to #of CPUs when zero is set.
     */
    private static int resolvePoolSize(int poolSize) {
        if (poolSize == 0)
            return Runtime.getRuntime().availableProcessors();

        return poolSize;
    }

    /**
     * Start all core threads, causing them to idly wait for work
     */
    public void start() {
        this.prestartAllCoreThreads();
    }

    /**
     * wraps the enqueueBlocked method of the threadpool
     */
    public void enqueueBlocked(E o) {
        try {
            this.execute(new TaskWrapper<E>(o, _consumerObject));
        } catch (RejectedExecutionException ree) {
            if (_logger.isInfoEnabled()) {
                _logger.info("Task cannot be accepted for execution.", ree);
            }
        }
    }

    /**
     * @return the Consumer Object of this working group.
     */
    public IConsumerObject<E> getConsumerObject() {
        return _consumerObject;
    }

    /**
     * make a graceful "shutdown" to all the worker threads in the thread pool Initiates an orderly
     * shutdown in which only executing tasks will complete, previously submitted tasks are
     * ignored,and no new tasks will be accepted. Invocation has no additional effect if already
     * shut down.
     */
    @Override
    public void shutdown() {
        if (!this.isShutdown()) {
            if (_logger.isDebugEnabled()) {
                _logger.debug(_workingGroupName
                        + " executor is commencing shutdown...");
            }

            this.getQueue().clear(); // clean previously submitted tasks
            super.shutdown(); // complete executing running tasks
            try {
                // Blocks until all tasks have completed execution after a
                // shutdown request, or the timeout occurs
                this.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn(_workingGroupName
                            + " interrupted while waiting for shutdown.", e);
                }
            }

            if (this.isTerminated()) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(_workingGroupName + " shutdown complete.");
                }
            } else {
                if (this.isTerminating())
                    if (_logger.isWarnEnabled()) {
                        _logger.warn(_workingGroupName
                                + " unsafe shutdown still in progress...");
                    } else if (_logger.isErrorEnabled()) {
                        _logger.error(_workingGroupName
                                + " shutdown failed to complete.");
                    }
            }

            _consumerObject.cleanUp(); // clean up the resources of the
            // "IConsumerObject" implementor
        }
    }

    /**
     * @return the Working Group's name.
     */
    public String getWorkingGroupName() {
        return _workingGroupName;
    }

    /**
     * Returns <tt>true</tt> if there are no more idle threads and we have reached the maximum
     * thread-growth allowed. Otherwise, <tt>false</tt> if at least one thread can (or be spawned
     * to) serve the incoming request.
     *
     * @return <code>true</code> when reached full capacity (i.e. #of active threads equals to the
     * maximum threads allowed); <code>false</code> otherwise.
     */
    public boolean hasReachedFullCapacity() {
        return (getActiveCount() == getMaximumPoolSize());
    }

}
