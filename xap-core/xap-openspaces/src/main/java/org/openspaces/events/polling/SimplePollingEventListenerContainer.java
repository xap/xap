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


package org.openspaces.events.polling;

import com.gigaspaces.admin.quiesce.QuiesceException;
import org.openspaces.core.SpaceInterruptedException;
import org.openspaces.events.AbstractEventListenerContainer;
import org.openspaces.events.SpaceDataEventListener;
import org.openspaces.events.polling.receive.ReceiveOperationHandler;
import org.openspaces.events.polling.receive.SingleTakeReceiveOperationHandler;
import org.openspaces.events.polling.trigger.TriggerOperationHandler;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceMonitors;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.scheduling.SchedulingTaskExecutor;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Event listener container variant that uses plain Space take API, specifically a loop of {@link
 * org.openspaces.core.GigaSpace#take(Object, long)} calls that also allow for transactional
 * reception of messages.
 *
 * <p> Actual event listener execution happens in asynchronous work units which are created through
 * Spring's {@link org.springframework.core.task.TaskExecutor} abstraction. By default, the
 * specified number of invoker tasks will be created on startup, according to the {@link
 * #setConcurrentConsumers "concurrentConsumers"} setting. Specify an alternative TaskExecutor to
 * integrate with an existing thread pool facility (such as a J2EE server's), for example using a
 * {@link org.springframework.scheduling.commonj.WorkManagerTaskExecutor CommonJ WorkManager}.
 *
 * <p> Event reception and listener execution can automatically be wrapped in transactions through
 * passing a Spring {@link org.springframework.transaction.PlatformTransactionManager} into the
 * {@link #setTransactionManager "transactionManager"} property. This will usually be a {@link
 * org.openspaces.core.transaction.manager.LocalJiniTransactionManager}.
 *
 * <p> Dynamic scaling of the number of concurrent invokers can be activated through specifying a
 * {@link #setMaxConcurrentConsumers "maxConcurrentConsumers"} value that is higher than the {@link
 * #setConcurrentConsumers "concurrentConsumers"} value. Since the latter's default is 1, you can
 * also simply specify a "maxConcurrentConsumers" of e.g. 5, which will lead to dynamic scaling up
 * to 5 concurrent consumers in case of increasing event load, as well as dynamic shrinking back to
 * the standard number of consumers once the load decreases. Consider adapting the {@link
 * #setIdleTaskExecutionLimit "idleTaskExecutionLimit"} setting to control the lifespan of each new
 * task, to avoid frequent scaling up and down. Note that using more than one consumer might break
 * fifo behavior if fifo is configured by the space or the specific class type, however, this is not
 * the case when using fifo grouping.
 *
 * @author kimchy
 */
public class SimplePollingEventListenerContainer extends AbstractEventListenerContainer {

    /**
     * Default thread name prefix: "DefaultPollingEventListenerContainer-".
     */
    public static final String DEFAULT_THREAD_NAME_PREFIX = ClassUtils.getShortName(SimplePollingEventListenerContainer.class)
            + "-";

    /**
     * The default recovery interval: 5000 ms = 5 seconds.
     */
    public static final long DEFAULT_RECOVERY_INTERVAL = 5000;

    /**
     * The default retry attempts: 3 times.
     */
    public static final int DEFAULT_KEEP_ALIVE_RETRIES = 3;

    /**
     * The default receive timeout: 60000 ms = 60 seconds = 1 minute.
     */
    public static final long DEFAULT_RECEIVE_TIMEOUT = 60000;

    private boolean passArrayAsIs = false;
    private long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;
    private ReceiveOperationHandler receiveOperationHandler;
    private TriggerOperationHandler triggerOperationHandler;

    private TaskExecutor taskExecutor;

    private long recoveryInterval = DEFAULT_RECOVERY_INTERVAL;

    private int concurrentConsumers = 1;

    private int maxConcurrentConsumers = 1;

    private int maxEventsPerTask = Integer.MIN_VALUE;

    private int idleTaskExecutionLimit = 1;

    private final Set<AsyncEventListenerInvoker> scheduledInvokers = new HashSet<AsyncEventListenerInvoker>();

    private AtomicInteger activeInvokerCount = new AtomicInteger(0);

    private final Object activeInvokerMonitor = new Object();

    private Object currentRecoveryMarker = new Object();

    private final Object recoveryMonitor = new Object();

    /**
     * The default keep alive retries : 3 times.
     */
    private int keepAliveRetries = DEFAULT_KEEP_ALIVE_RETRIES;

    /* (non-Javadoc)
     * @see org.openspaces.events.AbstractTransactionalEventListenerContainer#validateConfiguration()
    */
    @Override
    protected void validateConfiguration() {
        super.validateConfiguration();

        if (!disableTransactionValidation && getTransactionManager() != null && getGigaSpace().getTxProvider().isEnabled() && getTransactionDefinition() != null) {
            int timeout = getTransactionDefinition().getTimeout();
            if (timeout != TransactionDefinition.TIMEOUT_DEFAULT && (timeout * 1000) <= getReceiveTimeout())
                throw new IllegalStateException("Receive timeout [" + getReceiveTimeout() + "ms] must be lower than the transaction timeout [" + getTransactionDefinition().getTimeout() * 1000 + "ms]");
        }
    }

    /**
     * If set to <code>true</code> will pass an array value returned from a {@link
     * org.openspaces.events.polling.receive.ReceiveOperationHandler} directly to the listener
     * without "serializing" it as one array element each time. Defaults to <code>false</code>
     */
    public void setPassArrayAsIs(boolean passArrayAsIs) {
        this.passArrayAsIs = passArrayAsIs;
    }

    protected boolean isPassArrayAsIs() {
        return this.passArrayAsIs;
    }

    /**
     * Set the timeout to use for receive calls, in <b>milliseconds</b>. The default is 60000 ms,
     * that is, 1 minute.
     *
     * <p><b>NOTE:</b> This value needs to be smaller than the transaction timeout used by the
     * transaction manager (in the appropriate unit, of course).
     *
     * @see org.openspaces.core.GigaSpace#take(Object, long)
     */
    public void setReceiveTimeout(long receiveTimeout) {
        this.receiveTimeout = receiveTimeout;
    }

    /**
     * Returns the timeout used for receive calls, in <b>millisecond</b>. The default is 60000 ms,
     * that is, 1 minute.
     */
    protected long getReceiveTimeout() {
        return receiveTimeout;
    }

    /**
     * Allows to set a receive operation handler that will perform the actual receive operation.
     * Defaults to {@link org.openspaces.events.polling.receive.SingleTakeReceiveOperationHandler}.
     */
    public void setReceiveOperationHandler(ReceiveOperationHandler receiveOperationHandler) {
        this.receiveOperationHandler = receiveOperationHandler;
    }

    protected ReceiveOperationHandler getReceiveOperationHandler() {
        return this.receiveOperationHandler;
    }

    /**
     * An advance feature allows for pluggable {@link TriggerOperationHandler
     * triggerOperationHandler} which mainly makes sense when using transactions. The trigger
     * operations handler allows to perform a trigger receive outside of a transaction scope, and if
     * it returned a value, perform the take within a transaction. A useful implementation of it is
     * {@link org.openspaces.events.polling.trigger.ReadTriggerOperationHandler}. Defaults to
     * <code>null</code>.
     */
    public void setTriggerOperationHandler(TriggerOperationHandler triggerOperationHandler) {
        this.triggerOperationHandler = triggerOperationHandler;
    }

    protected TriggerOperationHandler getTriggerOperationHandler() {
        return this.triggerOperationHandler;
    }

    /**
     * Set the Spring {@link org.springframework.core.task.TaskExecutor} to use for running the
     * listener threads. Default is {@link org.springframework.core.task.SimpleAsyncTaskExecutor},
     * starting up a number of new threads, according to the specified number of concurrent
     * consumers.
     *
     * <p> Specify an alternative TaskExecutor for integration with an existing thread pool. Note
     * that this really only adds value if the threads are managed in a specific fashion, for
     * example within a J2EE environment. A plain thread pool does not add much value, as this
     * listener container will occupy a number of threads for its entire lifetime.
     *
     * @see #setConcurrentConsumers
     * @see org.springframework.core.task.SimpleAsyncTaskExecutor
     */
    public void setTaskExecutor(TaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    /**
     * Specify the interval between recovery attempts, in <b>milliseconds</b>. The default is 5000
     * ms, that is, 5 seconds.
     *
     * @see #handleListenerSetupFailure
     */
    public void setRecoveryInterval(long recoveryInterval) {
        this.recoveryInterval = recoveryInterval;
    }

    /**
     * Specify the retry attempts number of no returned events. The default is 3 times.
     *
     */
    public void setKeepAliveRetries(int keepAliveRetries) {
        this.keepAliveRetries = keepAliveRetries;
    }

    /**
     * Specify the number of concurrent consumers to create. Default is 1.
     *
     * <p> Specifying a higher value for this setting will increase the standard level of scheduled
     * concurrent consumers at runtime: This is effectively the minimum number of concurrent
     * consumers which will be scheduled at any given time. This is a static setting; for dynamic
     * scaling, consider specifying the "maxConcurrentConsumers" setting instead.
     *
     * <p> Raising the number of concurrent consumers is recommended in order to scale the
     * consumption of events. However, note that any ordering guarantees are lost once multiple
     * consumers are registered. In general, stick with 1 consumer for low-volume events.
     *
     * <p> <b>This setting can be modified at runtime, for example through JMX.</b>
     *
     * @see #setMaxConcurrentConsumers
     */
    public void setConcurrentConsumers(int concurrentConsumers) {
        Assert.isTrue(concurrentConsumers > 0, "'concurrentConsumers' value must be at least 1 (one)");
        synchronized (this.activeInvokerMonitor) {
            this.concurrentConsumers = concurrentConsumers;
            if (this.maxConcurrentConsumers < concurrentConsumers) {
                this.maxConcurrentConsumers = concurrentConsumers;
            }
        }
    }

    /**
     * Return the "concurrentConsumer" setting.
     *
     * <p> This returns the currently configured "concurrentConsumers" value; the number of
     * currently scheduled/active consumers might differ.
     *
     * @see #getScheduledConsumerCount()
     * @see #getActiveConsumerCount()
     */
    public final int getConcurrentConsumers() {
        synchronized (this.activeInvokerMonitor) {
            return this.concurrentConsumers;
        }
    }

    /**
     * Specify the maximum number of concurrent consumers to create. Default is 1.
     *
     * <p> If this setting is higher than "concurrentConsumers", the listener container will
     * dynamically schedule new consumers at runtime, provided that enough incoming messages are
     * encountered. Once the load goes down again, the number of consumers will be reduced to the
     * standard level ("concurrentConsumers") again.
     *
     * <p> Raising the number of concurrent consumers is recommended in order to scale the
     * consumption of events. However, note that any ordering guarantees are lost once multiple
     * consumers are registered. In general, stick with 1 consumer for low-volume events.
     *
     * <p> <b>This setting can be modified at runtime, for example through JMX.</b>
     *
     * @see #setConcurrentConsumers
     */
    public void setMaxConcurrentConsumers(int maxConcurrentConsumers) {
        Assert.isTrue(maxConcurrentConsumers > 0, "'maxConcurrentConsumers' value must be at least 1 (one)");
        synchronized (this.activeInvokerMonitor) {
            this.maxConcurrentConsumers = (maxConcurrentConsumers > this.concurrentConsumers ? maxConcurrentConsumers : this.concurrentConsumers);
        }
    }

    /**
     * Return the "maxConcurrentConsumer" setting.
     *
     * <p> This returns the currently configured "maxConcurrentConsumers" value; the number of
     * currently scheduled/active consumers might differ.
     *
     * @see #getScheduledConsumerCount()
     * @see #getActiveConsumerCount()
     */
    public final int getMaxConcurrentConsumers() {
        synchronized (this.activeInvokerMonitor) {
            return this.maxConcurrentConsumers;
        }
    }

    /**
     * Specify the maximum number of events to process in one task. More concretely, this limits the
     * number of event reception attempts per task, which includes receive iterations that did not
     * actually pick up a event until they hit their timeout (see "receiveTimeout" property).
     *
     * <p>Default is unlimited (-1) in case of a standard TaskExecutor, and 1 in case of a
     * SchedulingTaskExecutor that indicates a preference for short-lived tasks. Specify a number of
     * 10 to 100 messages to balance between extremely long-lived and extremely short-lived tasks
     * here.
     *
     * <p>Long-lived tasks avoid frequent thread context switches through sticking with the same
     * thread all the way through, while short-lived tasks allow thread pools to control the
     * scheduling. Hence, thread pools will usually prefer short-lived tasks.
     *
     * <p><b>This setting can be modified at runtime, for example through JMX.</b>
     *
     * @see #setTaskExecutor
     * @see #setReceiveTimeout
     * @see org.springframework.scheduling.SchedulingTaskExecutor#prefersShortLivedTasks()
     */
    public void setMaxEventsPerTask(int maxEventsPerTask) {
        Assert.isTrue(maxEventsPerTask != 0, "'maxEventsPerTask' must not be 0");
        synchronized (this.activeInvokerMonitor) {
            this.maxEventsPerTask = maxEventsPerTask;
        }
    }

    /**
     * Return the maximum number of messages to process in one task.
     */
    public int getMaxEventsPerTask() {
        synchronized (this.activeInvokerMonitor) {
            return this.maxEventsPerTask;
        }
    }

    /**
     * Specify the limit for idle executions of a receive task, not having received any event within
     * its execution. If this limit is reached, the task will shut down and leave receiving to other
     * executing tasks (in case of dynamic scheduling; see the "maxConcurrentConsumers" setting).
     * Default is 1.
     *
     * <p>Within each task execution, a number of event reception attempts (according to the
     * "maxEventsPerTask" setting) will each wait for an incoming event (according to the
     * "receiveTimeout" setting). If all of those receive attempts in a given task return without an
     * event, the task is considered idle with respect to received events. Such a task may still be
     * rescheduled; however, once it reached the specified "idleTaskExecutionLimit", it will shut
     * down (in case of dynamic scaling).
     *
     * <p>Raise this limit if you encounter too frequent scaling up and down. With this limit being
     * higher, an idle consumer will be kept around longer, avoiding the restart of a consumer once
     * a new load of messages comes in. Alternatively, specify a higher "maxMessagePerTask" and/or
     * "receiveTimeout" value, which will also lead to idle consumers being kept around for a longer
     * time (while also increasing the average execution time of each scheduled task).
     *
     * <p><b>This setting can be modified at runtime, for example through JMX.</b>
     *
     * @see #setMaxEventsPerTask
     * @see #setReceiveTimeout
     */
    public void setIdleTaskExecutionLimit(int idleTaskExecutionLimit) {
        Assert.isTrue(idleTaskExecutionLimit > 0, "'idleTaskExecutionLimit' must be 1 or higher");
        synchronized (this.activeInvokerMonitor) {
            this.idleTaskExecutionLimit = idleTaskExecutionLimit;
        }
    }

    /**
     * Return the limit for idle executions of a receive task.
     */
    public int getIdleTaskExecutionLimit() {
        synchronized (this.activeInvokerMonitor) {
            return this.idleTaskExecutionLimit;
        }
    }

    /**
     * Return the retry attempts number of no returned events. The default is 3 times.
     *
     */
    public int getKeepAliveRetries() {
        return keepAliveRetries;
    }

    @Override
    public void initialize() {
        // Prepare taskExecutor and maxEventsPerTask.
        synchronized (this.activeInvokerMonitor) {
            if (this.taskExecutor == null) {
                this.taskExecutor = createDefaultTaskExecutor();
            } else if (this.taskExecutor instanceof SchedulingTaskExecutor
                    && ((SchedulingTaskExecutor) this.taskExecutor).prefersShortLivedTasks()
                    && this.maxEventsPerTask == Integer.MIN_VALUE) {
                // TaskExecutor indicated a preference for short-lived tasks. According to
                // setMaxEventsPerTask javadoc, we'll use 1 message per task in this case
                // unless the user specified a custom value.
                this.maxEventsPerTask = 1;
            }
        }

        initReceiveOperationHandler();
        initTriggerOperationHandler();
        // Proceed with actual listener initialization.
        super.initialize();

        // now, start the scheduled threads
        synchronized (this.activeInvokerMonitor) {
            for (int i = 0; i < this.concurrentConsumers; i++) {
                scheduleNewInvoker();
            }
        }
    }

    private void initTriggerOperationHandler() {
        if (triggerOperationHandler == null && getActualEventListener() != null) {
            final AtomicReference<Method> ref = new AtomicReference<Method>();
            ReflectionUtils.doWithMethods(AopUtils.getTargetClass(getActualEventListener()), new ReflectionUtils.MethodCallback() {
                public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
                    if (method.isAnnotationPresent(TriggerHandler.class)) {
                        ref.set(method);
                    }
                }
            });
            if (ref.get() != null) {
                ref.get().setAccessible(true);
                try {
                    setTriggerOperationHandler((TriggerOperationHandler) ref.get().invoke(getActualEventListener()));
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to set ReceiveOperationHandler from method [" + ref.get().getName() + "]", e);
                }
            }
        }
    }

    private void initReceiveOperationHandler() {
        if (receiveOperationHandler == null) {
            if (getActualEventListener() != null) {
                // try and find an annotated one
                final AtomicReference<Method> ref = new AtomicReference<Method>();
                ReflectionUtils.doWithMethods(AopUtils.getTargetClass(getActualEventListener()), new ReflectionUtils.MethodCallback() {
                    public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
                        if (method.isAnnotationPresent(ReceiveHandler.class)) {
                            ref.set(method);
                        }
                    }
                });
                if (ref.get() != null) {
                    ref.get().setAccessible(true);
                    try {
                        setReceiveOperationHandler((ReceiveOperationHandler) ref.get().invoke(getActualEventListener()));
                    } catch (Exception e) {
                        throw new IllegalArgumentException("Failed to set ReceiveOperationHandler from method [" + ref.get().getName() + "]", e);
                    }
                }
            }
            if (receiveOperationHandler == null) {
                receiveOperationHandler = new SingleTakeReceiveOperationHandler();
            }
        }
    }

    @Override
    protected void doAfterStart() throws DataAccessException {
        super.doAfterStart();
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(getBeanName()).append("] ").append("Started");
            if (getTransactionManager() != null) {
                sb.append(" transactional");
            }
            sb.append(" polling event container");
            sb.append(" with receiveTimeout [").append(getReceiveTimeout()).append("]");
            if (getTemplate() != null) {
                sb.append(", template ").append(ClassUtils.getShortName(getTemplate().getClass())).append("[").append(getTemplate()).append("]");
            } else {
                sb.append(", template [null]");
            }
            sb.append(", concurrentConsumers [").append(concurrentConsumers).append("]");
            if (maxConcurrentConsumers != concurrentConsumers) {
                sb.append(", maxConcurrentConsumers [").append(maxConcurrentConsumers).append("]");
            }
            logger.debug(sb.toString());
        }
    }

    @Override
    protected void doBeforeStop() throws DataAccessException {
        super.doBeforeStop();
        if (logger.isDebugEnabled()) {
            logger.debug("Stopped polling event container");
        }
    }

    /**
     * Create a default TaskExecutor. Called if no explicit TaskExecutor has been specified.
     *
     * <p> The default implementation builds a {@link org.springframework.core.task.SimpleAsyncTaskExecutor}
     * with the specified bean name (or the class name, if no bean name specified) as thread name
     * prefix.
     *
     * @see org.springframework.core.task.SimpleAsyncTaskExecutor#SimpleAsyncTaskExecutor(String)
     */
    protected TaskExecutor createDefaultTaskExecutor() {
        String beanName = getBeanName();
        String threadNamePrefix = "GS-" + (beanName != null ? beanName + "-" : DEFAULT_THREAD_NAME_PREFIX);
        return new SimpleAsyncTaskExecutor(threadNamePrefix);
    }

    /**
     * Re-executes the given task via this listener container's TaskExecutor.
     *
     * @see #setTaskExecutor
     */
    @Override
    protected void doRescheduleTask(Object task) {
        this.taskExecutor.execute((Runnable) task);
    }

    /**
     * Template method that gets called right when a new message has been received, before
     * attempting to process it. Allows subclasses to react to the event of an actual incoming
     * message, for example adapting their consumer count.
     */
    protected void eventReceived(Object event) {
        scheduleNewInvokerIfAppropriate();
    }

    /**
     * Schedule a new invoker, increasing the total number of scheduled invokers for this listener
     * container, but only if the specified "maxConcurrentConsumers" limit has not been reached yet,
     * and only if this listener container does not currently have idle invokers that are waiting
     * for new messages already.
     *
     * <p> Called once an event has been received, to scale up while processing the event in the
     * invoker that originally received it.
     *
     * @see #setTaskExecutor
     * @see #getMaxConcurrentConsumers()
     */
    protected void scheduleNewInvokerIfAppropriate() {
        if (isRunning()) {
            synchronized (this.activeInvokerMonitor) {
                if (this.scheduledInvokers.size() < this.maxConcurrentConsumers && !hasIdleInvokers()) {
                    scheduleNewInvoker();
                    if (logger.isDebugEnabled()) {
                        logger.debug(message("Raised scheduled invoker count [" + scheduledInvokers.size() + "]"));
                    }
                }
            }
        }
    }

    /**
     * Schedule a new invoker, increasing the total number of scheduled invokers for this listener
     * container.
     */
    private void scheduleNewInvoker() {
        AsyncEventListenerInvoker invoker = new AsyncEventListenerInvoker();
        this.taskExecutor.execute(invoker);
        this.scheduledInvokers.add(invoker);
        this.activeInvokerMonitor.notifyAll();
    }

    /**
     * Determine whether this listener container currently has any idle instances among its
     * scheduled invokers.
     */
    private boolean hasIdleInvokers() {
        for (AsyncEventListenerInvoker invoker : this.scheduledInvokers) {
            if (invoker.isIdle()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determine whether the current invoker should be rescheduled, given that it might not have
     * received a message in a while.
     *
     * @param idleTaskExecutionCount the number of idle executions that this invoker task has
     *                               already accumulated (in a row)
     */
    private boolean shouldRescheduleInvoker(int idleTaskExecutionCount) {
        synchronized (this.activeInvokerMonitor) {
            boolean idle = (idleTaskExecutionCount >= this.idleTaskExecutionLimit);
            return (this.scheduledInvokers.size() <= (idle ? this.concurrentConsumers : this.maxConcurrentConsumers));
        }
    }

    /**
     * Return the number of currently scheduled consumers.
     *
     * <p> This number will always be in between "concurrentConsumers" and "maxConcurrentConsumers",
     * but might be higher than "activeConsumerCount" (in case of some consumers being scheduled but
     * not executed at the moment).
     *
     * @see #getConcurrentConsumers()
     * @see #getMaxConcurrentConsumers()
     * @see #getActiveConsumerCount()
     */
    public final int getScheduledConsumerCount() {
        synchronized (this.activeInvokerMonitor) {
            return this.scheduledInvokers.size();
        }
    }

    /**
     * Return the number of currently active consumers.
     *
     * <p> This number will always be in between "concurrentConsumers" and "maxConcurrentConsumers",
     * but might be lower than "scheduledConsumerCount". (in case of some consumers being scheduled
     * but not executed at the moment).
     *
     * @see #getConcurrentConsumers()
     * @see #getMaxConcurrentConsumers()
     * @see #getActiveConsumerCount()
     */
    public final int getActiveConsumerCount() {
        synchronized (this.activeInvokerMonitor) {
            return this.activeInvokerCount.get();
        }
    }

    /**
     * Handle the given exception that arose during setup of a listener. Called for every such
     * exception in every concurrent listener.
     *
     * <p> The default implementation logs the exception at error level if not recovered yet, and at
     * debug level if already recovered. Can be overridden in subclasses.
     *
     * @param ex               the exception to handle
     * @param alreadyRecovered whether a previously executing listener already recovered from the
     *                         present listener setup failure (this usually indicates a follow-up
     *                         failure than be ignored other than for debug log purposes)
     * @see #recoverAfterListenerSetupFailure()
     */
    protected void handleListenerSetupFailure(Throwable ex, boolean alreadyRecovered) {
        if (ex instanceof Exception) {
            invokeExceptionListener((Exception) ex);
        }
        if (alreadyRecovered) {
            logger.debug(message("Setup of event listener invoker failed - already recovered by other invoker"), ex);
        } else {
            logger.error(message("Setup of event listener invoker failed - trying to recover"), ex);
        }
    }

    /**
     * Recover this listener container after a listener failed to set itself up, for example
     * reestablishing the underlying Connection.
     *
     * <p> The default implementation delegates to <code>refreshConnectionUntilSuccessful</code>
     * which pings the space until it is available.
     *
     * @see #refreshConnectionUntilSuccessful()
     */
    protected void recoverAfterListenerSetupFailure() {
        refreshConnectionUntilSuccessful();
    }

    /**
     * Refresh the underlying Connection, not returning before an attempt has been successful.
     *
     * <p> The default implementation pings the space until a successful ping has been established.
     *
     * @see #setRecoveryInterval
     */
    protected void refreshConnectionUntilSuccessful() {
        sleepInbetweenRecoveryAttempts();
    }

    /**
     * Sleep according to the specified recovery interval. Called in between recovery attempts.
     */
    protected void sleepInbetweenRecoveryAttempts() {
        if (this.recoveryInterval > 0) {
            try {
                Thread.sleep(this.recoveryInterval);
            } catch (InterruptedException interEx) {
                // Re-interrupt current thread, to allow other threads to react.
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    protected void doInitialize() throws DataAccessException {
    }

    /**
     * Destroy the container by waiting for all the current event listeners to shutdown.
     */
    @Override
    protected void doShutdown() throws DataAccessException {
        logger.debug(message("Waiting for shutdown of event listener invokers"));
        synchronized (this.activeInvokerMonitor) {
            for (AsyncEventListenerInvoker invoker : scheduledInvokers) {
                invoker.interrupt();
            }
            while (this.activeInvokerCount.get() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug(message("Still waiting for shutdown of [" + this.activeInvokerCount + "] event listener invokers"));
                }
                try {
                    this.activeInvokerMonitor.wait();
                } catch (InterruptedException interEx) {
                    // Re-interrupt current thread, to allow other threads to react.
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public ServiceDetails[] getServicesDetails() {
        Object tempalte = getTemplate();
        if (!(tempalte instanceof Serializable)) {
            tempalte = null;
        }
        return new ServiceDetails[]{new PollingEventContainerServiceDetails(beanName, getGigaSpace().getName(), tempalte, isPerformSnapshot(), getTransactionManagerName(),
                getReceiveTimeout(), getReceiveOperationHandler().toString(), getTriggerOperationHandler() != null ? getTriggerOperationHandler().toString() : null,
                getConcurrentConsumers(), getMaxConcurrentConsumers(), isPassArrayAsIs(), isDynamicTemplate(), getKeepAliveRetries())};
    }

    public ServiceMonitors[] getServicesMonitors() {
        return new ServiceMonitors[]{new PollingEventContainerServiceMonitors(beanName, getProcessedEvents(), getFailedEvents(), getStatus(), getConcurrentConsumers())};
    }

    public String getName() {
        return beanName;
    }

    @Override
    protected String getEventListenerContainerType() {
        return "Polling Container";
    }

    protected void dump(PrintWriter writer) {
        super.dump(writer);

        writer.println("Receive Timeout       : [" + getReceiveTimeout() + "]");
        writer.println("Receive Handler       : [" + getReceiveOperationHandler().toString() + "]");
        if (getTriggerOperationHandler() != null) {
            writer.println("Trigger Handler       : [" + getTriggerOperationHandler().toString() + "]");
        }
        writer.println("Consumers             : [" + getConcurrentConsumers() + "]");
        writer.println("Max Consumers         : [" + getMaxConcurrentConsumers() + "]");
        writer.println("Pass Array            : [" + isPassArrayAsIs() + "]");
    }

    // -------------------------------------------------------------------------
    // Inner classes used as internal adapters
    // -------------------------------------------------------------------------

    /**
     * Runnable that performs looped {@link #invokeListener()}.
     */
    private class AsyncEventListenerInvoker implements SchedulingAwareRunnable {

        private Object lastRecoveryMarker;

        private boolean lastEventSucceeded;

        private int idleTaskExecutionCount = 0;

        private volatile boolean idle = true;

        private Thread invokerThread;

        private int noReceivedEventCount = 0;

        // use the getEventListener to possibly get a proptotyped listener per thread
        private SpaceDataEventListener eventListener;

        public void run() {
            synchronized (activeInvokerMonitor) {
                invokerThread = Thread.currentThread();
                activeInvokerCount.incrementAndGet();
                activeInvokerMonitor.notifyAll();
            }
            boolean eventReceived = false;
            boolean casSucceeded = false;
            try {
                if (maxEventsPerTask < 0) {
                    while (isActive() && !casSucceeded) {
                        waitWhileNotRunning();
                        if (isActive()) {
                            eventReceived = invokeListener();
                            if(activeInvokerCount.get() > concurrentConsumers) {
                                if (!eventReceived)
                                    //add log finest
                                    noReceivedEventCount++;
                                else
                                    noReceivedEventCount = 0;

                                if (noReceivedEventCount >= keepAliveRetries) {
                                    int i = activeInvokerCount.get();
                                    if (i > concurrentConsumers) {
                                        casSucceeded = activeInvokerCount.compareAndSet(i, i - 1);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    int eventCount = 0;
                    while (isRunning() && eventCount < maxEventsPerTask) {
                        eventReceived = (invokeListener() || eventReceived);
                        eventCount++;
                    }
                }
            } catch (Throwable ex) {
                clearResources();
                if (!this.lastEventSucceeded) {
                    // We failed more than once in a row - sleep for recovery interval
                    // even before first recovery attempt.
                    sleepInbetweenRecoveryAttempts();
                }
                this.lastEventSucceeded = false;
                boolean alreadyRecovered = false;
                synchronized (recoveryMonitor) {
                    if (this.lastRecoveryMarker == currentRecoveryMarker) {
                        handleListenerSetupFailure(ex, false);
                        recoverAfterListenerSetupFailure();
                        currentRecoveryMarker = new Object();
                    } else {
                        alreadyRecovered = true;
                    }
                }
                if (alreadyRecovered) {
                    handleListenerSetupFailure(ex, true);
                }
            }
            synchronized (activeInvokerMonitor) {
                if(!casSucceeded) {
                    activeInvokerCount.decrementAndGet();
                }
                activeInvokerMonitor.notifyAll();
            }

            if (!eventReceived) {
                this.idleTaskExecutionCount++;
            } else {
                this.idleTaskExecutionCount = 0;
            }
            if (!shouldRescheduleInvoker(this.idleTaskExecutionCount) || !rescheduleTaskIfNecessary(this)) {
                // We're shutting down completely.
                synchronized (activeInvokerMonitor) {
                    scheduledInvokers.remove(this);
                    if (logger.isDebugEnabled()) {
                        logger.debug(message("Lowered scheduled invoker count [" + scheduledInvokers.size() + "]"));
                    }
                    activeInvokerMonitor.notifyAll();
                }
                clearResources();
            }
        }

        private boolean invokeListener() throws Throwable {
            initResourcesIfNecessary();
            if (eventListener == null) {
                eventListener = getEventListener();
            }
            boolean eventReceived = receiveAndExecute(eventListener);
            this.lastEventSucceeded = true;
            this.idle = !eventReceived;
            return eventReceived;
        }

        private void initResourcesIfNecessary() {
            updateRecoveryMarker();
        }

        private void updateRecoveryMarker() {
            synchronized (recoveryMonitor) {
                this.lastRecoveryMarker = currentRecoveryMarker;
            }
        }

        private void clearResources() {
        }

        public boolean isLongLived() {
            return (maxEventsPerTask < 0);
        }

        public boolean isIdle() {
            return this.idle;
        }

        public void interrupt() {
            if (invokerThread != null) {
                invokerThread.interrupt();
            }
        }
    }

    /**
     * Execute the listener for a message received from the given consumer, wrapping the entire
     * operation in an external transaction if demanded.
     *
     * @see #doReceiveAndExecute
     */
    protected boolean receiveAndExecute(SpaceDataEventListener eventListener) throws Throwable, TransactionException {
        Object template = getReceiveTemplate();
        // if trigger is configure, work using trigger outside of a possible transaction
        if (triggerOperationHandler != null) {
            Object trigger;
            try {
                trigger = triggerOperationHandler.triggerReceive(template, getGigaSpace(), receiveTimeout);
            } catch (SpaceInterruptedException e) {
                return false;
            }
            if (logger.isTraceEnabled()) {
                logger.trace(message("Trigger operation handler returned [" + trigger + "]"));
            }
            if (trigger == null) {
                return false;
            }
            // if we are going to use the trigger result as a template
            if (triggerOperationHandler.isUseTriggerAsTemplate()) {
                template = trigger;
            }
        }
        if (this.getTransactionManager() != null) {
            // Execute receive within transaction.
            TransactionStatus status = this.getTransactionManager().getTransaction(this.getTransactionDefinition());
            boolean messageReceived;
            try {
                messageReceived = doReceiveAndExecute(eventListener, template, status);
            } catch (RuntimeException ex) {
                rollbackOnException(status, ex);
                throw ex;
            } catch (Error err) {
                rollbackOnException(status, err);
                throw err;
            }
            // if no message is received, rollback the transaction (for better performance).
            if (!status.isCompleted()) {
                if (!messageReceived || status.isRollbackOnly()) {
                    this.getTransactionManager().rollback(status);
                } else {
                    this.getTransactionManager().commit(status);
                }
            }
            return messageReceived;
        }

        return doReceiveAndExecute(eventListener, template, null);
    }

    protected boolean doReceiveAndExecute(SpaceDataEventListener eventListener, Object template, TransactionStatus status) {
        Object dataEvent = receiveEvent(template);
        if (dataEvent != null) {
            if (dataEvent instanceof Object[] && !passArrayAsIs) {
                Object[] dataEvents = (Object[]) dataEvent;
                for (Object dataEvent1 : dataEvents) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(message("Received event [" + dataEvent + "]"));
                    }
                    eventReceived(dataEvent1);
                    try {
                        invokeListener(eventListener, dataEvent1, status, null);
                    } catch (Throwable ex) {
                        if (status != null) {
                            // in case of an exception, we rollback the transaction and return
                            // (since we rolled back)
                            if (logger.isTraceEnabled()) {
                                logger.trace(message("Rolling back transaction because of listener exception thrown: " + ex));
                            }
                            status.setRollbackOnly();
                            rollbackOnException(status,ex);
                            handleListenerException(ex);
                            return true;
                        }
                        // in case we do not work within a transaction, just handle the
                        // exception and continue
                        handleListenerException(ex);
                    }
                }
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace(message("Received event [" + dataEvent + "]"));
                }
                if (passArrayAsIs && !(dataEvent instanceof Object[])) {
                    Object dataEventArr = Array.newInstance(dataEvent.getClass(), 1);
                    Array.set(dataEventArr, 0, dataEvent);
                    dataEvent = dataEventArr;
                }
                eventReceived(dataEvent);
                try {
                    invokeListener(eventListener, dataEvent, status, null);
                } catch (Throwable ex) {
                    if (status != null) {
                        if (logger.isTraceEnabled()) {
                            logger.trace(message("Rolling back transaction because of listener exception thrown: " + ex));
                        }
                        status.setRollbackOnly();
                    }
                    handleListenerException(ex);
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Perform a rollback, handling rollback exceptions properly.
     *
     * @param status object representing the transaction
     * @param ex     the thrown application exception or error
     */
    private void rollbackOnException(TransactionStatus status, Throwable ex) {
        logger.trace(message("Initiating transaction rollback on application exception"), ex);
        try {
            this.getTransactionManager().rollback(status);
        } catch (RuntimeException ex2) {
            logger.error(message("Application exception overridden by rollback exception"), ex);
            throw ex2;
        } catch (Error err) {
            logger.error(message("Application exception overridden by rollback error"), ex);
            throw err;
        }
    }

    /**
     * Receive an event
     */
    protected Object receiveEvent(Object template) throws DataAccessException {
        try {
            return receiveOperationHandler.receive(template, getGigaSpace(), getReceiveTimeout());
        } catch (DataAccessException e) {
            if (isInterrupted(e)) {
                // we got an interrupted exception, it means no receive operation so return null.
                if (logger.isDebugEnabled())
                    logger.debug("Polling was interrupted - ignoring exception");
                return null;
            }
            throw e;
        } catch (QuiesceException e) {
            if (logger.isDebugEnabled()) {
                logger.debug(message("receiveEvent got QuiesceException"), e);
            } else {
                logger.info(message("receiveEvent was interrupted because space is suspended"));
            }
            return null;
        }
    }

    private static boolean isInterrupted(Throwable e) {
        while (e != null) {
            if (e instanceof SpaceInterruptedException  || e instanceof InterruptedException)
                return true;
            e = e.getCause();
        }
        return false;
    }
}