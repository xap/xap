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


package org.openspaces.events.notify;

import com.j_spaces.core.client.INotifyDelegatorFilter;

import net.jini.lease.LeaseListener;

import org.openspaces.core.GigaSpace;
import org.openspaces.events.EventExceptionHandler;
import org.openspaces.events.SpaceDataEventListener;
import org.openspaces.events.adapter.AnnotationEventListenerAdapter;
import org.openspaces.events.adapter.MethodEventListenerAdapter;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A simplified programmatic configuration that for {@link org.openspaces.events.notify.SimpleNotifyEventListenerContainer}.
 *
 * <p>Sample usage:
 * <pre>
 * UrlSpaceConfigurer urlSpaceConfigurerPrimary = new UrlSpaceConfigurer("/./space");
 * GigaSpace gigaSpace = new GigaSpaceConfigurer(urlSpaceConfigurerPrimary.space()).gigaSpace();
 * SimpleNotifyEventListenerContainer notifyEventListenerContainer = new
 * SimpleNotifyContainerConfigurer(gigaSpace)
 *              .template(new TestMessage())
 *              .eventListenerAnnotation(new Object() {
 *                  <code>@SpaceDataEvent</code> public void gotMeselfAnEvent() {
 *                      // do something
 *                  }
 *              }).notifyContainer();
 *
 * ...
 *
 * notifyEventListenerContainer.destroy();
 * urlSpaceConfigurerPrimary.destroy();
 * </pre>
 *
 * @author kimchy
 */
public class SimpleNotifyContainerConfigurer {

    final private SimpleNotifyEventListenerContainer notifyEventListenerContainer;

    private boolean initialized = false;

    public SimpleNotifyContainerConfigurer(GigaSpace gigaSpace) {
        notifyEventListenerContainer = new SimpleNotifyEventListenerContainer();
        notifyEventListenerContainer.setGigaSpace(gigaSpace);
    }

    public SimpleNotifyContainerConfigurer name(String name) {
        notifyEventListenerContainer.setBeanName(name);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setPerformTakeOnNotify(boolean)
     */
    public SimpleNotifyContainerConfigurer performTakeOnNotify(boolean performTakeOnNotify) {
        notifyEventListenerContainer.setPerformTakeOnNotify(performTakeOnNotify);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setIgnoreEventOnNullTake(boolean)
     */
    public SimpleNotifyContainerConfigurer ignoreEventOnNullTake(boolean ignoreEventOnNullTake) {
        notifyEventListenerContainer.setIgnoreEventOnNullTake(ignoreEventOnNullTake);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setFifo(boolean)
     */
    public SimpleNotifyContainerConfigurer fifo(boolean fifo) {
        notifyEventListenerContainer.setFifo(fifo);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setNotifyPreviousValueOnUpdate(boolean)
     */
    /*
    public SimpleNotifyContainerConfigurer notifyPreviousValueOnUpdate(boolean notifyPreviousValueOnUpdate) {
        notifyEventListenerContainer.setNotifyPreviousValueOnUpdate(notifyPreviousValueOnUpdate);
        return this;
    }
    */

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setPassArrayAsIs(boolean)
     */
    public SimpleNotifyContainerConfigurer passArrayAsIs(boolean passArrayAsIs) {
        notifyEventListenerContainer.setPassArrayAsIs(passArrayAsIs);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setBatchSize(Integer)
     */
    public SimpleNotifyContainerConfigurer batchSize(Integer batchSize) {
        notifyEventListenerContainer.setBatchSize(batchSize);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setBatchTime(Integer)
     */
    public SimpleNotifyContainerConfigurer batchTime(Integer batchTime) {
        notifyEventListenerContainer.setBatchTime(batchTime);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setBatchPendingThreshold(Integer)
     */
    public SimpleNotifyContainerConfigurer batchPendingThreshold(Integer batchPendingThreshold) {
        notifyEventListenerContainer.setBatchPendingThreshold(batchPendingThreshold);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setAutoRenew(boolean)
     */
    public SimpleNotifyContainerConfigurer autoRenew(boolean autoRenew) {
        notifyEventListenerContainer.setAutoRenew(autoRenew);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setRenewExpiration(long)
     */
    @Deprecated
    public SimpleNotifyContainerConfigurer renewExpiration(long renewExpiration) {
        notifyEventListenerContainer.setRenewExpiration(renewExpiration);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setRenewDuration(long)
     */
    @Deprecated
    public SimpleNotifyContainerConfigurer renewDuration(long renewDuration) {
        notifyEventListenerContainer.setRenewDuration(renewDuration);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setRenewRTT(long)
     */
    @Deprecated
    public SimpleNotifyContainerConfigurer renewRTT(long renewRTT) {
        notifyEventListenerContainer.setRenewRTT(renewRTT);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setLeaseListener(net.jini.lease.LeaseListener)
     */
    public SimpleNotifyContainerConfigurer leaseListener(LeaseListener leaseListener) {
        notifyEventListenerContainer.setLeaseListener(leaseListener);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setTemplate(Object)
     */
    public SimpleNotifyContainerConfigurer template(Object template) {
        notifyEventListenerContainer.setTemplate(template);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setPerformSnapshot(boolean)
     */
    public SimpleNotifyContainerConfigurer performSnapshot(boolean performSnapshot) {
        notifyEventListenerContainer.setPerformSnapshot(performSnapshot);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setListenerLease(long)
     * @deprecated Since 9.7 - event listener with custom lease is deprecated.
     */
    @Deprecated
    public SimpleNotifyContainerConfigurer listenerLease(long listenerLease) {
        notifyEventListenerContainer.setListenerLease(listenerLease);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setNotifyFilter(com.j_spaces.core.client.INotifyDelegatorFilter)
     */
    public SimpleNotifyContainerConfigurer notifyFilter(INotifyDelegatorFilter notifyFilter) {
        notifyEventListenerContainer.setNotifyFilter(notifyFilter);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setNotifyWrite(Boolean)
     */
    public SimpleNotifyContainerConfigurer notifyWrite(boolean notifyWrite) {
        notifyEventListenerContainer.setNotifyWrite(notifyWrite);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setNotifyUpdate(Boolean)
     */
    public SimpleNotifyContainerConfigurer notifyUpdate(boolean notifyUpdate) {
        notifyEventListenerContainer.setNotifyUpdate(notifyUpdate);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setNotifyTake(Boolean)
     */
    public SimpleNotifyContainerConfigurer notifyTake(boolean notifyTake) {
        notifyEventListenerContainer.setNotifyTake(notifyTake);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setNotifyAll(Boolean)
     */
    public SimpleNotifyContainerConfigurer notifyAll(boolean notifyAll) {
        notifyEventListenerContainer.setNotifyAll(notifyAll);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setNotifyUnmatched(Boolean)
     */
    public SimpleNotifyContainerConfigurer notifyUnmatched(boolean notifyUnmatched) {
        notifyEventListenerContainer.setNotifyUnmatched(notifyUnmatched);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setNotifyMatchedUpdate(Boolean)
     */
    public SimpleNotifyContainerConfigurer notifyMatchedUpdate(boolean notifyMatched) {
        notifyEventListenerContainer.setNotifyMatchedUpdate(notifyMatched);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setNotifyRematchedUpdate(Boolean)
     */
    public SimpleNotifyContainerConfigurer notifyRematchedUpdate(boolean notifyRematched) {
        notifyEventListenerContainer.setNotifyRematchedUpdate(notifyRematched);
        return this;
    }


    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setNotifyLeaseExpire(Boolean)
     */
    public SimpleNotifyContainerConfigurer notifyLeaseExpire(boolean notifyLeaseExpire) {
        notifyEventListenerContainer.setNotifyLeaseExpire(notifyLeaseExpire);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setGuaranteed(Boolean)
     * @deprecated Since 9.0 use {@link #durable(boolean)} instead.
     */
    @Deprecated
    public SimpleNotifyContainerConfigurer guaranteed(boolean guaranteed) {
        notifyEventListenerContainer.setGuaranteed(guaranteed);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setDurable(Boolean)
     */
    public SimpleNotifyContainerConfigurer durable(boolean durable) {
        notifyEventListenerContainer.setDurable(durable);
        return this;
    }

    /**
     * @param tag A name/id/tag to identify this <b>durable</b> notification listener endpoint.
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setTag(String)
     * @since 14.5
     */
    public SimpleNotifyContainerConfigurer tag(String tag) {
        notifyEventListenerContainer.setTag(tag);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setTriggerNotifyTemplate(boolean)
     */
    public SimpleNotifyContainerConfigurer triggerNotifyTemplate(boolean triggerNotifyTemplate) {
        notifyEventListenerContainer.setTriggerNotifyTemplate(triggerNotifyTemplate);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setReplicateNotifyTemplate(boolean)
     */
    public SimpleNotifyContainerConfigurer replicateNotifyTemplate(boolean replicateNotifyTemplate) {
        notifyEventListenerContainer.setReplicateNotifyTemplate(replicateNotifyTemplate);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setTransactionManager(org.springframework.transaction.PlatformTransactionManager)
     */
    public SimpleNotifyContainerConfigurer transactionManager(PlatformTransactionManager transactionManager) {
        notifyEventListenerContainer.setTransactionManager(transactionManager);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setTransactionName(String)
     */
    public SimpleNotifyContainerConfigurer transactionName(String transactionName) {
        notifyEventListenerContainer.setTransactionName(transactionName);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setTransactionTimeout(int)
     */
    public SimpleNotifyContainerConfigurer transactionTimeout(int transactionTimeout) {
        notifyEventListenerContainer.setTransactionTimeout(transactionTimeout);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setTransactionIsolationLevel(int)
     */
    public SimpleNotifyContainerConfigurer transactionIsolationLevel(int transactionIsolationLevel) {
        notifyEventListenerContainer.setTransactionIsolationLevel(transactionIsolationLevel);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setExceptionHandler(org.openspaces.events.EventExceptionHandler)
     */
    public SimpleNotifyContainerConfigurer exceptionHandler(EventExceptionHandler exceptionHandler) {
        notifyEventListenerContainer.setExceptionHandler(exceptionHandler);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setEventListener(org.openspaces.events.SpaceDataEventListener)
     */
    public SimpleNotifyContainerConfigurer eventListener(SpaceDataEventListener eventListener) {
        notifyEventListenerContainer.setEventListener(eventListener);
        return this;
    }

    /**
     * Sets an event listener that uses annotations
     *
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setEventListener(org.openspaces.events.SpaceDataEventListener)
     * @see org.openspaces.events.adapter.AnnotationEventListenerAdapter
     */
    public SimpleNotifyContainerConfigurer eventListenerAnnotation(Object eventListener) {
        AnnotationEventListenerAdapter annotationEventListenerAdapter = new AnnotationEventListenerAdapter();
        annotationEventListenerAdapter.setDelegate(eventListener);
        //if (notifyEventListenerContainer.isNotifyPreviousValueOnUpdate())
        //    annotationEventListenerAdapter.setNotifyPreviousValueOnUpdate(true);
        annotationEventListenerAdapter.afterPropertiesSet();
        notifyEventListenerContainer.setEventListener(annotationEventListenerAdapter);
        return this;
    }

    /**
     * Sets an event listener that uses method name as an adapter
     *
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setEventListener(org.openspaces.events.SpaceDataEventListener)
     * @see org.openspaces.events.adapter.MethodEventListenerAdapter
     */
    public SimpleNotifyContainerConfigurer eventListenerMethod(Object eventListener, String methodName) {
        MethodEventListenerAdapter methodEventListenerAdapter = new MethodEventListenerAdapter();
        methodEventListenerAdapter.setDelegate(eventListener);
        methodEventListenerAdapter.setMethodName(methodName);
        methodEventListenerAdapter.afterPropertiesSet();
        notifyEventListenerContainer.setEventListener(methodEventListenerAdapter);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setActiveWhenPrimary(boolean)
     */
    public SimpleNotifyContainerConfigurer activeWhenPrimary(boolean activeWhenPrimary) {
        notifyEventListenerContainer.setActiveWhenPrimary(activeWhenPrimary);
        return this;
    }

    /**
     * @see org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setAutoStart(boolean)
     */
    public SimpleNotifyContainerConfigurer autoStart(boolean autoStart) {
        notifyEventListenerContainer.setAutoStart(autoStart);
        return this;
    }

    /**
     * Creates a new {@link SimpleNotifyEventListenerContainer} instance.
     */
    public SimpleNotifyEventListenerContainer create() {
        if (!initialized) {
            // NO need, we always register a notify listener on embedded space, even if it is backup
//            notifyEventListenerContainer.setRegisterSpaceModeListener(true);
            notifyEventListenerContainer.afterPropertiesSet();
            initialized = true;
        }
        return notifyEventListenerContainer;
    }

    /**
     * Creates a new {@link SimpleNotifyEventListenerContainer} instance.
     *
     * @see #create()
     */
    public SimpleNotifyEventListenerContainer notifyContainer() {
        return create();
    }
}
