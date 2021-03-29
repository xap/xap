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


package org.openspaces.events;

import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.cluster.activeelection.SpaceInitializationIndicator;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.internal.dump.InternalDump;
import com.gigaspaces.internal.dump.InternalDumpProcessor;
import com.gigaspaces.internal.dump.InternalDumpProcessorFailedException;
import com.gigaspaces.internal.server.space.quiesce.QuiesceHandler;
import com.gigaspaces.internal.server.space.suspend.SuspendTypeChangedInternalListener;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.metrics.BeanMetricManager;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.server.space.suspend.SuspendType;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin;
import com.j_spaces.core.client.EntrySnapshot;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.space.mode.AfterSpaceModeChangeEvent;
import org.openspaces.core.space.mode.BeforeSpaceModeChangeEvent;
import org.openspaces.core.transaction.manager.JiniPlatformTransactionManager;
import org.openspaces.core.util.SpaceUtils;
import org.openspaces.events.adapter.EventListenerAdapter;
import org.openspaces.events.support.AnnotationProcessorUtils;
import org.openspaces.pu.container.ProcessingUnitContainerContext;
import org.openspaces.pu.container.ProcessingUnitContainerContextAware;
import org.openspaces.pu.service.ServiceDetailsProvider;
import org.openspaces.pu.service.ServiceMonitorsProvider;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.*;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A simple based class for {@link SpaceDataEventListener} based containers. Allowing to register a
 * listener and provides several support methods like {@link #invokeListener(SpaceDataEventListener,
 * Object, org.springframework.transaction.TransactionStatus, Object)} in order to simplify event
 * listener based containers.
 *
 * @author kimchy
 */
public abstract class AbstractEventListenerContainer implements ApplicationContextAware, Lifecycle, BeanNameAware,
        InitializingBean, DisposableBean, ApplicationListener<ApplicationEvent>,
        ServiceDetailsProvider, ServiceMonitorsProvider, ProcessingUnitContainerContextAware, InternalDumpProcessor {

    protected final Log logger = LogFactory.getLog(getClass());

    private GigaSpace gigaSpace;
    protected String beanName;
    private boolean activeWhenPrimary = true;
    private boolean registerSpaceModeListener = false;
    private volatile boolean active = false;
    private volatile boolean running = false;
    private final List<Object> pausedTasks = new LinkedList<Object>();
    private final Object lifecycleMonitor = new Object();
    private PrimaryBackupListener primaryBackupListener;
    private SpaceMode currentSpaceMode;
    private volatile boolean autoStart = true;
    private volatile boolean quiesced = false;
    private volatile boolean resumeAfterSuspend = false;
    private BeanMetricManager beanMetricManager;

    private SpaceDataEventListener eventListener;
    private String eventListenerRef;
    private ApplicationContext applicationContext;
    protected EventExceptionHandler exceptionHandler;
    private final LongCounter processedEvents = new LongCounter();
    private final LongCounter failedEvents = new LongCounter();

    private Object template;
    private boolean performSnapshot = true; // enabled by default
    private Object receiveTemplate;
    private DynamicEventTemplateProvider dynamicTemplate;
    private Object dynamicTemplateRef;

    private PlatformTransactionManager transactionManager;
    private DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
    protected boolean disableTransactionValidation = false;

    protected ThreadLocal<EntrySnapshot> snapshotTemplateThreadLocal = new ThreadLocal<EntrySnapshot>();
    private SuspendTypeChangedInternalListener suspendTypeListener;

    /**
     * Sets the GigaSpace instance to be used for space event listening operations.
     */
    public void setGigaSpace(GigaSpace gigaSpace) {
        this.gigaSpace = gigaSpace;
    }

    /**
     * Returns the GigaSpace instance to be used for Space operations.
     */
    public final GigaSpace getGigaSpace() {
        return this.gigaSpace;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    /**
     * Return the bean name that this listener container has been assigned in its containing bean
     * factory, if any.
     */
    public final String getBeanName() {
        return this.beanName;
    }

    /**
     * Set whether this container will start only when it is primary (space mode).
     *
     * <p>Default is <code>true</code>. Set to <code>false</code> in order for this container to
     * always start regardless of the space mode.
     */
    public void setActiveWhenPrimary(boolean activeWhenPrimary) {
        this.activeWhenPrimary = activeWhenPrimary;
    }

    /**
     *
     */
    public void setRegisterSpaceModeListener(boolean registerSpaceModeListener) {
        this.registerSpaceModeListener = registerSpaceModeListener;
    }

    /**
     * Set whether this container will start once instantiated.
     *
     * <p>Default is <code>true</code>. Set to <code>false</code> in order for this container to be
     * started using {@link #start()}.
     */
    public void setAutoStart(boolean initOnStartup) {
        this.autoStart = initOnStartup;
    }

    /**
     * Return whether this container is currently running, that is, whether it has been started and
     * not stopped yet.
     */
    public final boolean isRunning() {
        return this.running && !this.quiesced;
    }

    /**
     * Return whether this container is currently active, that is, whether it has been set up but
     * not shut down yet.
     */
    public final boolean isActive() {
        return this.active;
    }

    protected String getStatus() {
        if (quiesced)
            return "quiesced";
        if (running)
            return "started";
        return "stopped";
    }

    public EventExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    /**
     * Sets an exception handler that will be invoked when an exception occurs on the listener
     * allowing to customize the handling of such cases.
     */
    public void setExceptionHandler(EventExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    /**
     * Sets the event listener implementation that will be used to delegate events to. Also see
     * different adapter classes provided for simpler event listeners integration.
     *
     * @param eventListener The event listener used
     */
    public void setEventListener(SpaceDataEventListener eventListener) {
        this.eventListener = eventListener;
    }

    /**
     * Sets an event listener bean reference name that will be used to lookup the actual listener
     * bean (based on its name). Mainly used when configuring a listener with specific scope setting
     * (such as prototype) allowing to scope to take affect by using <code>getBean</code> for each
     * request of the listener.
     */
    public void setEventListenerRef(String eventListenerRef) {
        this.eventListenerRef = eventListenerRef;
    }

    protected SpaceDataEventListener getEventListener() {
        if (eventListener != null) {
            return eventListener;
        }
        if (eventListenerRef == null) {
            return null;
        }
        return (SpaceDataEventListener) applicationContext.getBean(eventListenerRef);
    }

    protected Object getActualEventListener() {
        Object listener = getEventListener();
        while (listener instanceof EventListenerAdapter) {
            listener = ((EventListenerAdapter) listener).getActualEventListener();
        }
        return listener;
    }

    public void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void setProcessingUnitContainerContext(ProcessingUnitContainerContext processingUnitContainerContext) {
        this.beanMetricManager = processingUnitContainerContext.createBeanMetricManager(getBeanName());
        if (running)
            registerMetrics();
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    protected Class<?> getEventListenerClass() {
        if (eventListener != null) {
            return eventListener.getClass();
        }
        return applicationContext.getType(eventListenerRef);
    }

    /**
     * Delegates to {@link #validateConfiguration()} and {@link #initialize()}.
     */
    public void afterPropertiesSet() {
        validateConfiguration();
        initialize();
    }

    /**
     * Initialize this container. If this container is not configured with "activeWhenPrimary" flag
     * set to <code>true</code> will call {@link #doStart()} (if it is set to <code>true</code>,
     * lifecycle of the container will be controlled by the current space mode). {@link
     * #doInitialize()} will be called for additional initialization after the possible {@link
     * #doStart()} call.
     *
     * @see #onApplicationEvent(org.springframework.context.ApplicationEvent)
     */
    public void initialize() throws DataAccessException {
        initializeTransactionManager();
        initializeTemplate();
        initializeExceptionHandler();
        synchronized (this.lifecycleMonitor) {
            this.active = true;
            this.lifecycleMonitor.notifyAll();
        }

        doInitialize();

        boolean embedded = !SpaceUtils.isRemoteProtocol(gigaSpace.getSpace());

        // Register the suspendTypeListener (for reacting to suspendTypeChanges) only if this container is listening on embedded space
        if (embedded) {
            QuiesceHandler quiesceHandler = gigaSpace.getSpace().getDirectProxy().getSpaceImplIfEmbedded().getQuiesceHandler();
            quiesced = quiesceHandler.isOn();
            suspendTypeListener = new SuspendTypeInternalListener();
            quiesceHandler.addSpaceSuspendTypeListener(suspendTypeListener);
        }

        if (!activeWhenPrimary) {
            doStart();
        }

        if (registerSpaceModeListener) {
            SpaceMode currentMode = SpaceMode.PRIMARY;
            if (embedded) {
                currentMode = registerSpaceModeListener();
            }
            SpaceInitializationIndicator.setInitializer();
            try {
                onApplicationEvent(new BeforeSpaceModeChangeEvent(gigaSpace.getSpace(), currentMode));
                onApplicationEvent(new AfterSpaceModeChangeEvent(gigaSpace.getSpace(), currentMode));
            } finally {
                SpaceInitializationIndicator.unsetInitializer();
            }
        }
    }

    private SpaceMode registerSpaceModeListener() {
        primaryBackupListener = new PrimaryBackupListener();
        try {
            IJSpace clusterMemberSpace = SpaceUtils.getClusterMemberSpace(gigaSpace.getSpace());
            ISpaceModeListener remoteListener = (ISpaceModeListener) clusterMemberSpace.getDirectProxy().getStubHandler()
                    .exportObject(primaryBackupListener);
            return ((IInternalRemoteJSpaceAdmin) clusterMemberSpace.getAdmin()).addSpaceModeListener(remoteListener);
        } catch (RemoteException e) {
            throw new InvalidDataAccessResourceUsageException("Failed to register space mode listener with space [" + gigaSpace.getSpace()
                    + "]", e);
        }
    }

    /**
     * A callback to perform custom initialization steps.
     */
    protected abstract void doInitialize() throws DataAccessException;

    private void initializeTransactionManager() {
        // Use bean name as default transaction name.
        if (this.transactionDefinition.getName() == null) {
            this.transactionDefinition.setName(getBeanName());
        }
    }

    /**
     * Validate the configuration of this container.
     */
    protected void validateConfiguration() {
        Assert.notNull(gigaSpace, "gigaSpace property is required");
        if (transactionManager != null && !disableTransactionValidation) {
            if (!getGigaSpace().getTxProvider().isEnabled()) {
                throw new IllegalStateException(message("event container is configured to run under transactions (transaction manager is provided) " +
                        "but GigaSpace is not transactional. Please pass the transaction manager to the GigaSpace bean as well"));
            }
        }
    }

    private void initializeTemplate() {
        Object possibleTemplateProvider = null;

        if (template != null) {
            // check if template object is actually a template provider
            possibleTemplateProvider = template;
        } else {
            Class<?> eventListenerType = getEventListenerClass();
            if (eventListenerType != null) {
                //check if listener object is also a template provider
                possibleTemplateProvider = getActualEventListener();
            }
        }

        if (possibleTemplateProvider != null) {
            Object templateFromProvider = AnnotationProcessorUtils.findTemplateFromProvider(possibleTemplateProvider);
            if (templateFromProvider != null) {
                setTemplate(templateFromProvider);
            }
        }

        if (dynamicTemplate == null && dynamicTemplateRef != null) {
            Object dynamicTemplateProviderBean = dynamicTemplateRef;
            dynamicTemplate = AnnotationProcessorUtils.findDynamicEventTemplateProvider(dynamicTemplateProviderBean);
            if (dynamicTemplate == null) {
                throw new IllegalArgumentException("Cannot find dynamic template provider in " + dynamicTemplateRef.getClass());
            }
        }

        if (template != null && dynamicTemplate != null) {
            throw new IllegalArgumentException("dynamicTemplate and template are mutually exclusive.");
        }

        if (performSnapshot && template != null) {
            if (logger.isTraceEnabled()) {
                logger.trace(message("Performing snapshot on template [" + template + "]"));
            }
            receiveTemplate = getGigaSpace().prepareTemplate(template);
        } else {
            receiveTemplate = template;
        }
    }

    private void initializeExceptionHandler() {
        if (exceptionHandler == null && getActualEventListener() != null) {
            final AtomicReference<Method> ref = new AtomicReference<Method>();
            ReflectionUtils.doWithMethods(AopUtils.getTargetClass(getActualEventListener()), new ReflectionUtils.MethodCallback() {
                public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
                    if (method.isAnnotationPresent(ExceptionHandler.class)) {
                        ref.set(method);
                    }
                }
            });
            if (ref.get() != null) {
                ref.get().setAccessible(true);
                try {
                    setExceptionHandler((EventExceptionHandler) ref.get().invoke(getActualEventListener()));
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to set EventExceptionHandler from method [" + ref.get().getName() + "]", e);
                }
            }
        }
    }

    /**
     * Calls {@link #shutdown()} when the BeanFactory destroys the container instance.
     *
     * @see #shutdown()
     */
    public void destroy() {
        shutdown();
    }

    /**
     * Stop container, call {@link #doShutdown()}, and close this container.
     */
    public void shutdown() throws DataAccessException {
        if (logger.isDebugEnabled()) {
            logger.debug(message("Shutting down Space Event listener container"));
        }
        synchronized (this.lifecycleMonitor) {
            this.active = false;
            this.running = false;
            this.lifecycleMonitor.notifyAll();
            unregisterMetrics();
        }

        boolean embedded = !SpaceUtils.isRemoteProtocol(gigaSpace.getSpace());

        if (embedded) {
            gigaSpace.getSpace().getDirectProxy().getSpaceImplIfEmbedded().getQuiesceHandler().removeSpaceSuspendTypeListener(suspendTypeListener);
        }

        if (registerSpaceModeListener) {
            if (embedded) {
                IJSpace clusterMemberSpace = SpaceUtils.getClusterMemberSpace(gigaSpace.getSpace());
                try {
                    ISpaceModeListener remoteListener = (ISpaceModeListener) clusterMemberSpace.getDirectProxy().getStubHandler()
                            .exportObject(primaryBackupListener);
                    ((IInternalRemoteJSpaceAdmin) clusterMemberSpace.getAdmin()).removeSpaceModeListener(remoteListener);
                } catch (RemoteException e) {
                    logger.warn("Failed to unregister space mode listener with space [" + gigaSpace.getSpace() + "]", e);
                }
            }
        }

        // Shut down the invokers.
        doShutdown();
    }

    /**
     * Perform any custom shutdown operations.
     *
     * @see #shutdown()
     */
    protected abstract void doShutdown() throws DataAccessException;

    // -------------------------------------------------------------------------
    // Lifecycle methods for dynamically starting and stopping the container
    // -------------------------------------------------------------------------

    /**
     * Start this container.
     *
     * @see #doStart
     */
    public void start() throws DataAccessException {
        if (!autoStart) {
            autoStart = true;
        }
        if (!activeWhenPrimary) {
            doStart();
        } else {
            if (currentSpaceMode != null && currentSpaceMode == SpaceMode.PRIMARY) {
                doStart();
            }
        }
    }

    /**
     * Only start if we have a listener registered. If we don't, then explicit start should be
     * called.
     */
    protected void doStart() throws DataAccessException {
        if (!autoStart || running || quiesced || getEventListener() == null)
            return;

        synchronized (this.lifecycleMonitor) {
            if (running || quiesced)
                return;

            this.running = true;
            registerMetrics();
            this.lifecycleMonitor.notifyAll();
            for (Iterator<Object> it = this.pausedTasks.iterator(); it.hasNext(); ) {
                doRescheduleTask(it.next());
                it.remove();
            }
        }
        doAfterStart();
    }

    protected void doAfterStart() throws DataAccessException {

    }


    /**
     * Stop this container.
     *
     * @see #doStop
     */
    public void stop() throws DataAccessException {
        doStop();
    }

    /**
     * Notify all invoker tasks to stop
     */
    protected void doStop() throws DataAccessException {
        if (!running) {
            return;
        }
        doBeforeStop();
        synchronized (this.lifecycleMonitor) {
            this.running = false;
            this.resumeAfterSuspend = false;
            this.lifecycleMonitor.notifyAll();
            unregisterMetrics();
        }
    }

    protected void doBeforeStop() throws DataAccessException {

    }

    /**
     * If the {@link #setActiveWhenPrimary(boolean)} is set to <code>true</code> (the default), the
     * container lifecycle will be controlled by the space mode. The container will start when the
     * space is in <code>PRIMARY</code> mode, and will stop when the space is in <code>BACKUP</code>
     * mode.
     *
     * <p>Note, this might cause {@link #doStart()} or {@link #doStop()} to be called several times
     * in a row, and sub classes should take this into account.
     */
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if (activeWhenPrimary) {
            if (applicationEvent instanceof AfterSpaceModeChangeEvent) {
                AfterSpaceModeChangeEvent spEvent = (AfterSpaceModeChangeEvent) applicationEvent;
                if (spEvent.isPrimary() && SpaceUtils.isSameSpace(spEvent.getSpace(), gigaSpace.getSpace())) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(message("Space [" + getGigaSpace() + "] became primary, starting the container"));
                    }
                    doStart();
                }
            } else if (applicationEvent instanceof BeforeSpaceModeChangeEvent) {
                BeforeSpaceModeChangeEvent spEvent = (BeforeSpaceModeChangeEvent) applicationEvent;
                if (!spEvent.isPrimary() && SpaceUtils.isSameSpace(spEvent.getSpace(), gigaSpace.getSpace())) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(message("Space [" + getGigaSpace() + "] became backup, stopping the container"));
                    }
                    doStop();
                }
                currentSpaceMode = spEvent.getSpaceMode();
            }
        }
    }

    /**
     * Wait while this container is not running.
     *
     * <p>To be called by asynchronous tasks that want to block while the container is in stopped
     * state.
     */
    protected final void waitWhileNotRunning() {
        while (this.active && !this.running) {
            synchronized (this.lifecycleMonitor) {
                if (this.active && !this.running) {
                    try {
                        this.lifecycleMonitor.wait();
                    } catch (InterruptedException ex) {
                        // Re-interrupt current thread, to allow other threads to react.
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    /**
     * Take the given task object and reschedule it, either immediately if this container is
     * currently running, or later once this container has been restarted.
     *
     * <p>If this container has already been shut down, the task will not get rescheduled at all.
     *
     * @param task the task object to reschedule
     * @return whether the task has been rescheduled (either immediately or for a restart of this
     * container)
     * @see #doRescheduleTask
     */
    protected final boolean rescheduleTaskIfNecessary(Object task) {
        Assert.notNull(task, "Task object must not be null");
        if (this.running) {
            doRescheduleTask(task);
            return true;
        } else if (this.active) {
            synchronized (lifecycleMonitor) {
                if (this.running) {
                    doRescheduleTask(task);
                    return true;
                }
                this.pausedTasks.add(task);
                return true;
            }
        } else {
            return false;
        }
    }

    /**
     * Reschedule the given task object immediately.
     *
     * <p>To be implemented by subclasses if they ever call <code>rescheduleTaskIfNecessary</code>.
     * This implementation throws an UnsupportedOperationException.
     *
     * @param task the task object to reschedule
     * @see #rescheduleTaskIfNecessary
     */
    protected void doRescheduleTask(Object task) {
        throw new UnsupportedOperationException(ClassUtils.getShortName(getClass())
                + " does not support rescheduling of tasks");
    }

    // -------------------------------------------------------------------------
    // Template methods for listener execution
    // -------------------------------------------------------------------------

    /**
     * Executes the given listener if the container is running ({@link #isRunning()}.
     *
     * @param eventData The event data object
     * @param txStatus  An optional transaction status allowing to rollback a transaction
     *                  programmatically
     * @param source    An optional source (or additional event information)
     */
    protected void executeListener(SpaceDataEventListener eventListener, Object eventData, TransactionStatus txStatus, Object source) throws Throwable {
        if (!isRunning()) {
            return;
        }
        invokeListener(eventListener, eventData, txStatus, source);
    }

    /**
     * Invokes the configured {@link org.openspaces.events.SpaceDataEventListener} based on the
     * provided data. Currently simply delegates to {@link org.openspaces.events.SpaceDataEventListener#onEvent(Object,
     * org.openspaces.core.GigaSpace, org.springframework.transaction.TransactionStatus, Object)}.
     *
     * @param eventData The event data object
     * @param txStatus  An optional transaction status allowing to rollback a transaction
     *                  programmatically
     * @param source    An optional source (or additional event information)
     */
    protected void invokeListener(SpaceDataEventListener eventListener, Object eventData, TransactionStatus txStatus, Object source) throws Throwable {
        if (exceptionHandler != null) {
            try {
                eventListener.onEvent(eventData, getGigaSpace(), txStatus, source);
                exceptionHandler.onSuccess(eventData, getGigaSpace(), txStatus, source);
            } catch (Throwable e) {
                if (!(e instanceof ListenerExecutionFailedException)) {
                    e = new ListenerExecutionFailedException(e.getMessage(), e);
                }
                exceptionHandler.onException((ListenerExecutionFailedException) e, eventData, getGigaSpace(), txStatus, source);
            }
        } else {
            eventListener.onEvent(eventData, getGigaSpace(), txStatus, source);
        }
        processedEvents.inc();
    }

    /**
     * Handles exception that occurs during the event listening process. Currently simply logs it.
     *
     * @param ex the exception to handle
     */
    protected void handleListenerException(Throwable ex) {
        if (ex instanceof Exception) {
            invokeExceptionListener((Exception) ex);
        }
        if (isActive()) {
            incrementFailedEvents();
            // Regular case: failed while active. Log at error level.
            logger.error(message("Execution of event listener failed"), ex);
        } else {
            // Rare case: listener thread failed after container shutdown.
            // Log at debug level, to avoid spamming the shutdown log.
            logger.debug(message("Listener exception after container shutdown"), ex);
        }
    }

    /**
     * A callback to handle exception. Possible extension point for registered exception listeners.
     */
    protected void invokeExceptionListener(Exception e) {
    }

    public long getProcessedEvents() {
        return processedEvents.getCount();
    }

    public long getFailedEvents() {
        return failedEvents.getCount();
    }

    protected void incrementFailedEvents() {
        failedEvents.inc();
    }

    /**
     * Sets the specified template to be used with the polling space operation.
     *
     * @see org.openspaces.core.GigaSpace#take(Object, long)
     */
    public void setTemplate(Object template) {
        this.template = template;
    }

    /**
     * Returns the template that will be used. Note, in order to perform receive operations, the
     * {@link #getReceiveTemplate()} should be used.
     */
    public Object getTemplate() {
        return this.template;
    }

    /**
     * Called before each take and read polling operation to change the template Overrides any
     * template defined with {@link #setTemplate(Object)}
     *
     * @param dynamicTemplate - An object that implements {@link DynamicEventTemplateProvider} or
     *                        has a method annotated with {@link DynamicEventTemplateProvider}
     */
    public void setDynamicTemplate(Object dynamicTemplate) {
        this.dynamicTemplateRef = dynamicTemplate;
    }

    /**
     * Returns whether dynamic template is configured
     */
    public boolean isDynamicTemplate() {
        return dynamicTemplate != null;
    }

    /**
     * If set to <code>true</code> will perform prepareTemplate operation on the provided template before
     * invoking registering as an event listener.
     *
     * @see org.openspaces.core.GigaSpace#prepareTemplate(Object)
     */
    public void setPerformSnapshot(boolean performSnapshot) {
        this.performSnapshot = performSnapshot;
    }

    protected boolean isPerformSnapshot() {
        return this.performSnapshot;
    }

    /**
     * Returns the template to be used for receive operations. If {@link
     * #setPerformSnapshot(boolean)} is set to <code>true</code> (the default) will return the
     * snapshot of the provided template.
     */
    public Object getReceiveTemplate() {
        if (dynamicTemplate != null) {
            return dynamicTemplate.getDynamicTemplate();
        }

        if (isPerformSnapshot() && getTransactionManager() != null) {
            EntrySnapshot entrySnapshotTemplate = snapshotTemplateThreadLocal.get();
            //this is the first time, there is no template in thread local
            if (entrySnapshotTemplate == null) {
                ITemplatePacket cloned = ((EntrySnapshot) receiveTemplate).getTemplatePacket().clone();
                cloned.setOperationID(null);
                entrySnapshotTemplate = new EntrySnapshot(cloned);
                snapshotTemplateThreadLocal.set(entrySnapshotTemplate);
                return entrySnapshotTemplate;
            } else {
                return entrySnapshotTemplate;
            }
        }
        return receiveTemplate;
    }

    /**
     * Specify the Spring {@link org.springframework.transaction.PlatformTransactionManager} to use
     * for transactional wrapping of listener execution.
     *
     * <p> Default is none, not performing any transactional wrapping.
     */
    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    /**
     * Return the Spring PlatformTransactionManager to use for transactional wrapping of message
     * reception plus listener execution.
     */
    public final PlatformTransactionManager getTransactionManager() {
        return this.transactionManager;
    }

    /**
     * Specify the transaction name to use for transactional wrapping. Default is the bean name of
     * this listener container, if any.
     *
     * @see org.springframework.transaction.TransactionDefinition#getName()
     */
    public void setTransactionName(String transactionName) {
        this.transactionDefinition.setName(transactionName);
    }

    /**
     * Specify the transaction timeout to use for transactional wrapping, in <b>seconds</b>. Default
     * is none, using the transaction manager's default timeout.
     *
     * @see org.springframework.transaction.TransactionDefinition#getTimeout()
     */
    public void setTransactionTimeout(int transactionTimeout) {
        this.transactionDefinition.setTimeout(transactionTimeout);
    }

    /**
     * Specify the transaction isolation to use for transactional wrapping.
     *
     * @see org.springframework.transaction.support.DefaultTransactionDefinition#setIsolationLevel(int)
     */
    public void setTransactionIsolationLevel(int transactionIsolationLevel) {
        this.transactionDefinition.setIsolationLevel(transactionIsolationLevel);
    }

    /**
     * Specify the transaction isolation to use for transactional wrapping.
     *
     * @see org.springframework.transaction.support.DefaultTransactionDefinition#setIsolationLevelName(String)
     */
    public void setTransactionIsolationLevelName(String transactionIsolationLevelName) {
        this.transactionDefinition.setIsolationLevelName(transactionIsolationLevelName);
    }

    protected DefaultTransactionDefinition getTransactionDefinition() {
        return transactionDefinition;
    }

    /**
     * Should transaction validation be enabled or not (verify and fail if transaction manager is
     * provided and the GigaSpace is not transactional). Default to <code>false</code>.
     */
    public void setDisableTransactionValidation(boolean disableTransactionValidation) {
        this.disableTransactionValidation = disableTransactionValidation;
    }

    public String getTransactionManagerName() {
        if (transactionManager instanceof JiniPlatformTransactionManager) {
            return ((JiniPlatformTransactionManager) transactionManager).getBeanName();
        }
        if (transactionManager != null) {
            return "<<unknown>>";
        }
        return null;
    }

    public boolean isTransactional() {
        return transactionManager != null;
    }

    private class PrimaryBackupListener implements ISpaceModeListener {

        public void beforeSpaceModeChange(SpaceMode spaceMode) throws RemoteException {
            onApplicationEvent(new BeforeSpaceModeChangeEvent(gigaSpace.getSpace(), spaceMode));
        }

        public void afterSpaceModeChange(SpaceMode spaceMode) throws RemoteException {
            onApplicationEvent(new AfterSpaceModeChangeEvent(gigaSpace.getSpace(), spaceMode));
        }

    }

    private class SuspendTypeInternalListener implements SuspendTypeChangedInternalListener {

        @Override
        public void onSuspendTypeChanged(SuspendType suspendType) {
            logger.info(message("SuspendType was updated to " + suspendType));
            boolean suspended = suspendType != SuspendType.NONE;
            if (quiesced == suspended)
                return;
            quiesced = suspended;
            if (suspended) {
                if (logger.isDebugEnabled())
                    logger.debug(message("SuspendType was updated to " + suspendType) + ", stopping...");
                // if container was running before calling quiesce it should resume working after unquiesce
                boolean runningBeforeSuspend = running;
                //if requires stop on suspend type change
                if (requiresStopStart()) {
                    stop();
                }
                resumeAfterSuspend = runningBeforeSuspend;
            } else {
                // resume only if container was running before calling quiesce
                if (resumeAfterSuspend) {
                    if (logger.isDebugEnabled())
                        logger.debug(message("SuspendType was updated to " + suspendType) + ", starting...");
                    //if requires start on suspend type change
                    if (requiresStopStart()) {
                        start();
                    }
                } else {
                    if (logger.isDebugEnabled())
                        logger.debug(message("SuspendType was updated to " + suspendType) + " but resumeAfterSuspend was set to false, not resuming...");
                }
            }
        }
    }

    //if requires stop/start to be called on suspendTypeChange
    protected boolean requiresStopStart() { return true;}

    protected String message(String message) {
        return "[" + getBeanName() + "] " + message;
    }

    public void process(InternalDump dump) throws InternalDumpProcessorFailedException {
        dump.addPrefix("event-containers/");
        try {
            PrintWriter writer = new PrintWriter(dump.createFileWriter(beanName + ".txt"));
            dump(writer);
            writer.println();
            writer.close();
        } finally {
            dump.removePrefix();
        }
    }

    protected void dump(PrintWriter writer) {
        writer.println("===== RUNTIME =====");
        writer.println("Status: " + getStatus());
        writer.println("Processed events: " + getProcessedEvents());
        writer.println("Failed events: " + getFailedEvents());
        writer.println("===== CONFIGURATION =====");
        writer.println("Type                  : [" + getEventListenerContainerType() + "]");
        writer.println("GigaSpace             : [" + getGigaSpace().getName() + "]");
        writer.println("Template              : [" + getTemplate() + "]");
        writer.println("Transactional         : [" + getTransactionManagerName() + "]");
    }

    protected abstract String getEventListenerContainerType();

    protected void registerMetrics() {
        if (beanMetricManager != null) {
            beanMetricManager.register("processed-events", processedEvents);
            beanMetricManager.register("failed-events", failedEvents);
        }
    }

    protected void unregisterMetrics() {
        if (beanMetricManager != null)
            beanMetricManager.clear();
    }
}
