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

import com.gigaspaces.cluster.activeelection.SpaceInitializationIndicator;
import com.gigaspaces.events.*;
import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.internal.events.IInternalEventSessionAdmin;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;
import com.j_spaces.core.client.INotifyDelegatorFilter;
import net.jini.core.event.EventRegistration;
import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;
import net.jini.core.lease.Lease;
import net.jini.lease.LeaseListener;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.UnusableEntryException;
import org.openspaces.core.util.SpaceUtils;
import org.openspaces.events.AbstractEventListenerContainer;
import org.openspaces.pu.service.ServiceDetails;
import org.openspaces.pu.service.ServiceMonitors;
import org.springframework.core.Constants;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.util.ClassUtils;

import java.io.PrintWriter;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * A simple notification based container allowing to register a {@link
 * org.openspaces.events.SpaceDataEventListener} that will be triggered by notifications. Uses
 * {@link SimpleNotifyEventListenerContainer} for configuration of different notification
 * registration parameters and transactional semantics.
 *
 * <p>The container can automatically take the notified event data (using {@link
 * GigaSpace#take(Object)}) if the {@link #setPerformTakeOnNotify(boolean)} is set to
 * <code>true</code>. Defaults to <code>false</code>. If the flag is set to <code>true</code>,
 * {@link #setIgnoreEventOnNullTake(boolean)} can control of the event will be propagated to the
 * event listener if the take operation returned null.
 *
 * @author kimchy
 */
public class SimpleNotifyEventListenerContainer extends AbstractEventListenerContainer {

    protected static final Constants constants = new Constants(SimpleNotifyEventListenerContainer.class);

    //private boolean notifyPreviousValueOnUpdate = false;

    private boolean fifo = false;

    private Integer batchSize;

    private Integer batchTime;

    private Integer batchPendingThreshold;

    private boolean autoRenew = false;

    private long renewExpiration = EventSessionConfig.DEFAULT_RENEW_EXPIRATION;

    private long renewDuration = EventSessionConfig.DEFAULT_RENEW_DURATION;

    private long renewRTT = EventSessionConfig.DEFAULT_RENEW_RTT;

    private LeaseListener leaseListener;

    private long listenerLease = Lease.FOREVER;

    private INotifyDelegatorFilter notifyFilter;

    private Boolean notifyWrite;

    private Boolean notifyUpdate;

    private Boolean notifyTake;

    private Boolean notifyLeaseExpire;

    private Boolean notifyUnmatched;

    private Boolean notifyMatchedUpdate;

    private Boolean notifyRematchedUpdate;

    private Boolean notifyAll;

    private Boolean triggerNotifyTemplate;

    private Boolean replicateNotifyTemplate;

    private Boolean guaranteed;

    private boolean passArrayAsIs = false;

    private Boolean durable;

    private String tag;

    private boolean performTakeOnNotify = false;

    private boolean ignoreEventOnNullTake = false;

    private DataEventSession dataEventSession;

    private EventRegistration eventRegistration;

    public SimpleNotifyEventListenerContainer() {
        // we register for notifications even when the embedded space is backup
        setActiveWhenPrimary(false);
    }

    /**
     * Determines whether the previous value should be sent with the new one upon update notification.
     * @param notifyPreviousValueOnUpdate
     */
    /*
    public void setNotifyPreviousValueOnUpdate(boolean notifyPreviousValueOnUpdate) {
        this.notifyPreviousValueOnUpdate = notifyPreviousValueOnUpdate;
    }

    public boolean isNotifyPreviousValueOnUpdate() {
        return notifyPreviousValueOnUpdate;
    }
    */

    /**
     * Determines if events arrives in the same order they were triggered by the space "server".
     * Note, for a full fifo based ordering the relevant entries in the space should be configured
     * to be fifo as well.
     */
    public void setFifo(boolean fifo) {
        this.fifo = fifo;
    }

    protected boolean isFifo() {
        return this.fifo;
    }

    /**
     * If set, turns batching event notifications where the server space accumalates notifications
     * to be sent and then send them in batch. The batch size controls the number of notifications
     * are sent in each batch. If {@link #setBatchPendingThreshold(Integer)} is not specified, this
     * property will determine it as well. Note, if setting this property the {@link
     * #setBatchTime(Integer)} must be set as well.
     */
    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    protected Integer getBatchSize() {
        return this.batchSize;
    }

    /**
     * If set, turns batching event notifications where the server space accumalates notifications
     * to be sent and then send them in batch. The batch time controls the elapsed time until the
     * batch buffer is cleared and sent. The time is in <b>milliseconds</b>. Note, if setting this
     * property the {@link #setBatchSize(Integer)} must be set as well.
     */
    public void setBatchTime(Integer batchTime) {
        this.batchTime = batchTime;
    }

    protected Integer getBatchTime() {
        return this.batchTime;
    }

    /**
     * If set, turns batching event notifications where the server space accumalates notifications
     * to be sent and then send them in batch. The batch pending threshold controls the number of
     * notifications that will be accumulated before they are sent. Note, if this property not set,
     * {@link #setBatchSize(Integer)} will be used.
     */
    public void setBatchPendingThreshold(Integer batchPendingThreshold) {
        this.batchPendingThreshold = batchPendingThreshold;
    }

    protected Integer getBatchPendingThreshold() {
        return this.batchPendingThreshold;
    }

    /**
     * If {@link #setListenerLease(long)} is set, automatically performs lease renewal. Defaults to
     * <code>false</code>.
     *
     * @see #setListenerLease(long)
     */
    public void setAutoRenew(boolean autoRenew) {
        this.autoRenew = autoRenew;
    }

    protected boolean isAutoRenew() {
        return this.autoRenew;
    }

    /**
     * The period of time your notifications stop being renewed. Only applies when {@link
     * #setAutoRenew(boolean)} is <code>true</code>.
     *
     * <p>Defaults to {@link com.gigaspaces.events.EventSessionConfig#DEFAULT_RENEW_EXPIRATION}.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    protected long getRenewExpiration() {
        return renewExpiration;
    }

    /**
     * The period of time your notifications stop being renewed. Only applies when {@link
     * #setAutoRenew(boolean)} is <code>true</code>.
     *
     * <p>Defaults to {@link com.gigaspaces.events.EventSessionConfig#DEFAULT_RENEW_EXPIRATION}.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    public void setRenewExpiration(long renewExpiration) {
        this.renewExpiration = renewExpiration;
    }

    /**
     * The period of time that passes between client failure, and the time your notifications stop
     * being sent. use more than renewRTT. Only applies when {@link #setAutoRenew(boolean)} is
     * <code>true</code>.
     *
     * <p>Defaults to {@link com.gigaspaces.events.EventSessionConfig#DEFAULT_RENEW_DURATION}.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    protected long getRenewDuration() {
        return renewDuration;
    }

    /**
     * The period of time that passes between client failure, and the time your notifications stop
     * being sent. use more than renewRTT. Only applies when {@link #setAutoRenew(boolean)} is
     * <code>true</code>.
     *
     * <p>Defaults to {@link com.gigaspaces.events.EventSessionConfig#DEFAULT_RENEW_DURATION}.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    public void setRenewDuration(long renewDuration) {
        this.renewDuration = renewDuration;
    }

    /**
     * RoundTripTime - the time that takes to reach the server and return. Only applies when {@link
     * #setAutoRenew(boolean)} is <code>true</code>.
     *
     * <p>Defaults to {@link com.gigaspaces.events.EventSessionConfig#DEFAULT_RENEW_RTT}.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    protected long getRenewRTT() {
        return renewRTT;
    }

    /**
     * RoundTripTime - the time that takes to reach the server and return. Only applies when {@link
     * #setAutoRenew(boolean)} is <code>true</code>.
     *
     * <p>Defaults to {@link com.gigaspaces.events.EventSessionConfig#DEFAULT_RENEW_RTT}.
     *
     * @deprecated Since 9.7
     */
    @Deprecated
    public void setRenewRTT(long renewRTT) {
        this.renewRTT = renewRTT;
    }

    /**
     * If {@link #setAutoRenew(boolean)} is set to <code>true</code> sets the lease listener for
     * it.
     */
    public void setLeaseListener(LeaseListener leaseListener) {
        this.leaseListener = leaseListener;
    }

    /**
     * Controls the lease associated with the registered listener. Defaults to {@link
     * net.jini.core.lease.Lease#FOREVER}.
     *
     * @see #setAutoRenew(boolean)
     * @deprecated Since 9.7 - event listener with custom lease is deprecated.
     */
    @Deprecated
    public void setListenerLease(long listenerLease) {
        this.listenerLease = listenerLease;
    }

    /**
     * Allows to register a filter on the server side that can filter out or modify notifications
     * that will be sent by the space "server". Note, this filter will be passed to the space server
     * and used there.
     */
    public void setNotifyFilter(INotifyDelegatorFilter notifyFilter) {
        this.notifyFilter = notifyFilter;
    }

    /**
     * Turns on notifications for write operations. Defaults to <code>false</code>.
     */
    public void setNotifyWrite(Boolean notifyWrite) {
        this.notifyWrite = notifyWrite;
    }

    protected Boolean isNotifyWrite() {
        if (notifyWrite == null) {
            return Boolean.FALSE;
        }
        return this.notifyWrite;
    }

    /**
     * Turns on notifications for update operations. Defaults to <code>false</code>.
     */
    public void setNotifyUpdate(Boolean notifyUpdate) {
        this.notifyUpdate = notifyUpdate;
    }

    /**
     * Turns on notifications for update operations. Defaults to <code>false</code>.
     */
    protected Boolean isNotifyUpdate() {
        if (notifyUpdate == null) {
            return false;
        }
        return this.notifyUpdate;
    }

    /**
     * Turns on notifications for take operations. Defaults to <code>false</code>.
     */
    public void setNotifyTake(Boolean notifyTake) {
        this.notifyTake = notifyTake;
    }

    protected Boolean isNotifyTake() {
        if (notifyTake == null) {
            return false;
        }
        return this.notifyTake;
    }

    /**
     * Turns on notifications for all operations. This flag will override all the other notify flags
     * (if set). Defaults to <code>false</code>.
     */
    public void setNotifyAll(Boolean notifyAll) {
        this.notifyAll = notifyAll;
    }

    protected Boolean isNotifyAll() {
        if (notifyAll == null) {
            return Boolean.FALSE;
        }
        return this.notifyAll;
    }

    @Override
    protected boolean requiresStopStart() {
        return false;
    }

    /**
     * Turns on notification for least expiration. Defaults to <code>false</code>.
     */
    public void setNotifyLeaseExpire(Boolean notifyLeaseExpire) {
        this.notifyLeaseExpire = notifyLeaseExpire;
    }

    protected Boolean isNotifyLeaseExpire() {
        if (notifyLeaseExpire == null) {
            return Boolean.FALSE;
        }
        return this.notifyLeaseExpire;
    }

    /**
     * Turns on notifications for unmatched templates (a template that matched an entry but not it
     * does not). Defaults to <code>false</code>.
     */
    public void setNotifyUnmatched(Boolean notifyUnmatched) {
        this.notifyUnmatched = notifyUnmatched;
    }

    /**
     * Turns on notifications for matched templates (a template that matches an entry after the
     * entry was updated and not before). Defaults to <code>false</code>.
     *
     * @since 9.1
     */
    public void setNotifyMatchedUpdate(Boolean notifyMatchedUpdate) {
        this.notifyMatchedUpdate = notifyMatchedUpdate;
    }

    /**
     * Turns on notifications for re-matched templates (a template that matches an entry before and
     * after the entry was updated). Defaults to <code>false</code>.
     *
     * @since 9.1
     */
    public void setNotifyRematchedUpdate(Boolean notifyRematchedUpdate) {
        this.notifyRematchedUpdate = notifyRematchedUpdate;
    }


    protected Boolean isNotifyUnmatched() {
        if (notifyUnmatched == null) {
            return Boolean.FALSE;
        }
        return this.notifyUnmatched;
    }

    protected Boolean isNotifyMatchedUpdate() {
        if (notifyMatchedUpdate == null) {
            return Boolean.FALSE;
        }
        return this.notifyMatchedUpdate;
    }

    protected Boolean isNotifyRematchedUpdate() {
        if (notifyRematchedUpdate == null) {
            return Boolean.FALSE;
        }
        return this.notifyRematchedUpdate;
    }

    /**
     * If using a replicated space controls if the listener that are replicated to cluster members
     * will raise notifications.
     *
     * @see #setReplicateNotifyTemplate(boolean)
     */
    public void setTriggerNotifyTemplate(boolean triggerNotifyTemplate) {
        this.triggerNotifyTemplate = triggerNotifyTemplate;
    }

    protected Boolean isTriggerNotifyTemplate() {
        return this.triggerNotifyTemplate;
    }

    /**
     * If using a replicated space controls if the listener will be replicated between all the
     * replicated cluster members. <p/> <p>If working directly with a cluster member, the default
     * value will be <code>false</code>. Otherwise, the default value will be based on the cluster
     * schema (which is true for clusters with backups).
     *
     * @see #setTriggerNotifyTemplate(boolean)
     */
    public void setReplicateNotifyTemplate(boolean replicateNotifyTemplate) {
        this.replicateNotifyTemplate = replicateNotifyTemplate;
    }

    protected Boolean isReplicateNotifyTemplate() {
        return this.replicateNotifyTemplate;
    }

    /**
     * Controls if notifications will be guaranteed (at least once) in case of failover.
     *
     * @deprecated Since 9.0 use {@link #setDurable(Boolean)} instead.
     */
    @Deprecated
    public void setGuaranteed(Boolean guaranteed) {
        if (EventSessionConfig.USE_OLD_GUARANTEED_NOTIFICATIONS) {
            this.guaranteed = guaranteed;
        } else {
            setDurable(guaranteed);
        }
    }

    protected Boolean isGuaranteed() {
        if (EventSessionConfig.USE_OLD_GUARANTEED_NOTIFICATIONS) {
            if (guaranteed == null) {
                return Boolean.FALSE;
            }
            return guaranteed;
        } else {
            return isDurable();
        }
    }

    /**
     * Controls if notifications will sustain disconnections and failover.
     */
    public void setDurable(Boolean durable) {
        this.durable = durable;
    }

    protected Boolean isDurable() {
        if (durable == null) {
            return Boolean.FALSE;
        }
        return durable;
    }

    /**
     * A tag can be supplied to identify a <b>durable</b> notification listener endpoint for debug/auditing purposes.
     * The tag is attached to the replication statistics and outgoing channel of durable notifications.
     *
     * @param tag A name/id/tag to identify this <b>durable</b> notification listener endpoint.
     * @since 14.5
     */
    public void setTag(String tag) {
        this.tag = tag;
    }

    protected String getTag() {
        return this.tag;
    }

    /**
     * When batching is turned on, should the batch of events be passed as an <code>Object[]</code>
     * to the listener. Default to <code>false</code> which means it will be passed one event at a
     * time.
     */
    public void setPassArrayAsIs(boolean passArrayAsIs) {
        this.passArrayAsIs = passArrayAsIs;
    }

    @Override
    public void setDynamicTemplate(Object templateProvider) {
        throw new UnsupportedOperationException("Notify container does not support dynamic event templates.");
    }

    protected boolean isPassArrayAsIs() {
        return this.passArrayAsIs;
    }

    protected Boolean getNotifyWrite() {
        return notifyWrite;
    }

    protected Boolean getNotifyUpdate() {
        return notifyUpdate;
    }

    protected Boolean getNotifyTake() {
        return notifyTake;
    }

    protected Boolean getNotifyLeaseExpire() {
        return notifyLeaseExpire;
    }

    protected Boolean getNotifyUnmatched() {
        return notifyUnmatched;
    }

    protected Boolean getNotifyMatchedUpdate() {
        return notifyMatchedUpdate;
    }

    protected Boolean getNotifyRematchedUpdate() {
        return notifyRematchedUpdate;
    }

    /**
     * Returns <code>true</code> when batching is enabled.
     */
    protected boolean isBatchEnabled() {
        return (batchSize != null && batchTime != null) ||
                (durable != null && durable) ||
                (!EventSessionConfig.USE_OLD_GUARANTEED_NOTIFICATIONS && guaranteed != null && guaranteed);
    }

    /**
     * If set to <code>true</code> will remove the event from the space using take operation.
     * Default is <code>false</code>.
     */
    public void setPerformTakeOnNotify(boolean performTakeOnNotify) {
        this.performTakeOnNotify = performTakeOnNotify;
    }

    /**
     * If set to <code>true</code>, will not propagate the event if the take operation returned
     * <code>null</code>. This flag only makes sense when {@link #setPerformTakeOnNotify(boolean)}
     * is set to <code>true</code>. Defaults to <code>false</code>.
     */
    public void setIgnoreEventOnNullTake(boolean ignoreEventOnNullTake) {
        this.ignoreEventOnNullTake = ignoreEventOnNullTake;
    }

    @Override
    protected void validateConfiguration() {
        super.validateConfiguration();
        if (batchSize == null && batchTime != null) {
            throw new IllegalArgumentException("batchTime has value [" + batchTime
                    + "] which enables batching. batchSize must have a value as well");
        }
        if (batchTime == null && batchSize != null) {
            throw new IllegalArgumentException("batchSize has value [" + batchSize
                    + "] which enables batching. batchTime must have a value as well");
        }
    }

    @Override
    public void initialize() throws DataAccessException {
        if (SpaceUtils.isRemoteProtocol(getGigaSpace().getSpace())) {

        } else {
            // if we are using a Space that was started in embedded mode, no need to replicate notify template
            // by default

            if (replicateNotifyTemplate == null && !SpaceUtils.isRemoteProtocol(getGigaSpace().getSpace())) {
                if (logger.isTraceEnabled()) {
                    logger.trace(message("Setting replicateNotifyTemplate to false since working with an embedded Space"));
                }
                replicateNotifyTemplate = Boolean.FALSE;
            }
        }
        if (replicateNotifyTemplate == null && triggerNotifyTemplate != null && triggerNotifyTemplate) {
            if (logger.isTraceEnabled()) {
                logger.trace(message("triggerNotifyTemplate is set, automatically setting replicateNotifyTemplate to true"));
            }
            replicateNotifyTemplate = Boolean.TRUE;
        }

        if (getTemplate() instanceof NotifyTypeProvider) {
            NotifyTypeProvider notifyTypeProvider = (NotifyTypeProvider) getTemplate();
            if (notifyTypeProvider.isLeaseExpire() != null && notifyLeaseExpire == null) {
                notifyLeaseExpire = notifyTypeProvider.isLeaseExpire();
            }
            if (notifyTypeProvider.isTake() != null && notifyTake == null) {
                notifyTake = notifyTypeProvider.isTake();
            }
            if (notifyTypeProvider.isUpdate() != null && notifyUpdate == null) {
                notifyUpdate = notifyTypeProvider.isUpdate();
            }
            if (notifyTypeProvider.isWrite() != null && notifyWrite == null) {
                notifyWrite = notifyTypeProvider.isWrite();
            }
            if (notifyTypeProvider.isUnmatched() != null && notifyUnmatched == null) {
                notifyUnmatched = notifyTypeProvider.isUnmatched();
            }
            if (notifyTypeProvider.isMatchedUpdate() != null && notifyMatchedUpdate == null) {
                notifyMatchedUpdate = notifyTypeProvider.isMatchedUpdate();
            }
            if (notifyTypeProvider.isRematchedUpdate() != null && notifyRematchedUpdate == null) {
                notifyRematchedUpdate = notifyTypeProvider.isRematchedUpdate();
            }
        }

        if (notifyAll == null && notifyTake == null && notifyUpdate == null && notifyWrite == null
                && notifyLeaseExpire == null && notifyUnmatched == null) {
            notifyWrite = true;
            if (logger.isTraceEnabled()) {
                logger.trace(message("No notify flag is set, setting write notify to true by default"));
            }
        }

        super.initialize();
    }

    @Override
    protected void doInitialize() throws DataAccessException {
    }

    @Override
    protected void doShutdown() throws DataAccessException {
        closeSession();
    }

    @Override
    protected void doAfterStart() throws DataAccessException {
        super.doAfterStart();
        registerListener();
        if (logger.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(getBeanName()).append("] ").append("Started");
            if (getTransactionManager() != null) {
                sb.append(" transactional");
            }
            sb.append(" notify event container");
            if (getTemplate() != null) {
                sb.append(", template ").append(ClassUtils.getShortName(getTemplate().getClass())).append("[").append(getTemplate()).append("]");
            } else {
                sb.append(", template [null]");
            }
            sb.append(", notifications [");
            if (getNotifyWrite() != null && getNotifyWrite()) {
                sb.append("write,");
            }
            if (getNotifyUpdate() != null && getNotifyUpdate()) {
                sb.append("update,");
            }
            if (getNotifyUnmatched() != null && getNotifyUnmatched()) {
                sb.append("unmatched,");
            }
            if (getNotifyMatchedUpdate() != null && getNotifyMatchedUpdate()) {
                sb.append("matchedUpdate,");
            }
            if (getNotifyRematchedUpdate() != null && getNotifyRematchedUpdate()) {
                sb.append("rematchedUpdate,");
            }
            if (getNotifyTake() != null && getNotifyTake()) {
                sb.append("take,");
            }
            if (getNotifyLeaseExpire() != null && getNotifyLeaseExpire()) {
                sb.append("leaseExpire,");
            }
            sb.append("]");
            logger.debug(sb.toString());
        }
    }

    @Override
    protected void doBeforeStop() throws DataAccessException {
        super.doBeforeStop();
        closeSession();
        if (logger.isDebugEnabled()) {
            logger.debug("Stopped notify event container");
        }
    }

    protected void registerListener() throws DataAccessException {
        if (dataEventSession != null) {
            // we already registered the listener, just return.
            return;
        }
        SpaceInitializationIndicator.setInitializer();
        try {
            dataEventSession = getGigaSpace().newDataEventSession(createEventSessionConfig());
            try {
                if (isBatchEnabled()) {
                    eventRegistration = registerListener(dataEventSession, new BatchNotifyListenerDelegate());
                } else {
                    eventRegistration = registerListener(dataEventSession, new NotifyListenerDelegate());
                }
            } catch (NotifyListenerRegistrationException ex) {
                // in case of an exception, close the session
                closeSession();
                throw ex;
            }
        } finally {
            SpaceInitializationIndicator.unsetInitializer();
        }
    }

    protected void closeSession() {
        if (dataEventSession != null) {
            try {
                dataEventSession.close();
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn(message("Failed to close data event session"), e);
                }
            } finally {
                dataEventSession = null;
            }
        }
    }

    public ServiceDetails[] getServicesDetails() {
        Object tempalte = getTemplate();
        if (!(tempalte instanceof Serializable)) {
            tempalte = null;
        }
        return new ServiceDetails[]{new NotifyEventContainerServiceDetails(beanName, getGigaSpace().getName(), tempalte, isPerformSnapshot(), getTransactionManagerName(),
                1 /*multiplex*/, isFifo(), getBatchSize(), getBatchTime(), getBatchPendingThreshold(), isAutoRenew(),
                isNotifyAll(), isNotifyWrite(), isNotifyUpdate(), isNotifyTake(), isNotifyLeaseExpire(), isNotifyUnmatched(), isNotifyMatchedUpdate(), isNotifyRematchedUpdate(),
                isTriggerNotifyTemplate(), isReplicateNotifyTemplate(), performTakeOnNotify, isPassArrayAsIs(), isGuaranteed(), isDurable(), getTag())};
    }

    public ServiceMonitors[] getServicesMonitors() {
        return new ServiceMonitors[]{new NotifyEventContainerServiceMonitors(beanName, getProcessedEvents(), getFailedEvents(), getStatus())};
    }

    public String getName() {
        return beanName;
    }

    @Override
    protected String getEventListenerContainerType() {
        return "Notify Container";
    }

    @Override
    protected void dump(PrintWriter writer) {
        super.dump(writer);
        StringBuilder notifications = new StringBuilder();
        if (isNotifyAll())
            notifications.append("ALL, ");
        if (isNotifyWrite())
            notifications.append("WRITE, ");
        if (isNotifyUpdate())
            notifications.append("UPDATE, ");
        if (isNotifyTake())
            notifications.append("TAKE, ");
        if (isNotifyLeaseExpire())
            notifications.append("LEASE, ");
        if (isNotifyUnmatched())
            notifications.append("UNMATCHED, ");
        if (isNotifyMatchedUpdate())
            notifications.append("MATCHED_UPDATE, ");
        if (isNotifyRematchedUpdate())
            notifications.append("REMATCHED_UPDATE, ");

        writer.println("Fifo                  : [" + isFifo() + "]");
        writer.println("Batching              : Size [" + getBatchSize() + "], Time [" + getBatchTime() + "]");
        writer.println("Auto Renew            : [" + isAutoRenew() + "]");
        writer.println("Notifications         : [" + notifications + "]");
        writer.println("Trigger Template      : [" + isTriggerNotifyTemplate() + "]");
        writer.println("Replication Template  : [" + isReplicateNotifyTemplate() + "]");
        writer.println("Perform Snapshot      : [" + isPerformSnapshot() + "]");
        writer.println("Pass Array            : [" + isPassArrayAsIs() + "]");
        writer.println("Durable               : [" + isDurable() + "]");

        if (isDurable() &&
                eventRegistration != null &&
                dataEventSession instanceof IInternalEventSessionAdmin) {
            writer.println("===== DURABLE =====");
            if (getTag() != null) {
                writer.println("Tag : [" + getTag() + "]");
            }
            writer.println(((IInternalEventSessionAdmin) dataEventSession).dumpState(eventRegistration));
        }

    }

    /**
     * Creates a new {@link com.gigaspaces.events.EventSessionConfig} based on the different
     * parameters this container accepts.
     */
    protected EventSessionConfig createEventSessionConfig() throws IllegalArgumentException {
        EventSessionConfig eventSessionConfig = new EventSessionConfig();
        //eventSessionConfig.setNotifyPreviousValueOnUpdate(notifyPreviousValueOnUpdate);
        eventSessionConfig.setFifo(fifo);
        if (batchSize != null && batchTime != null) {
            if (batchPendingThreshold != null && batchPendingThreshold != -1) {
                eventSessionConfig.setBatch(batchSize, batchTime, batchPendingThreshold);
            } else {
                eventSessionConfig.setBatch(batchSize, batchTime);
            }
        }
        if (leaseListener == null) {
            Object possibleListener = getActualEventListener();
            if (possibleListener instanceof LeaseListener) {
                leaseListener = (LeaseListener) possibleListener;
            }
        }
        eventSessionConfig.setAutoRenew(autoRenew, leaseListener, renewExpiration, renewDuration, renewRTT);
        if (triggerNotifyTemplate != null) {
            eventSessionConfig.setTriggerNotifyTemplate(triggerNotifyTemplate);
        }
        if (replicateNotifyTemplate != null) {
            eventSessionConfig.setReplicateNotifyTemplate(replicateNotifyTemplate);
        }
        if (guaranteed != null) {
            eventSessionConfig.setGuaranteedNotifications(guaranteed);
        }
        if (durable != null) {
            eventSessionConfig.setDurableNotifications(durable);
        }
        return eventSessionConfig;
    }

    /**
     * Registers a listener using the provided {@link com.gigaspaces.events.DataEventSession} and
     * based on different parameters set on this container.
     */
    protected EventRegistration registerListener(DataEventSession dataEventSession, RemoteEventListener listener)
            throws NotifyListenerRegistrationException {

        NotifyActionType notifyType = NotifyActionType.NOTIFY_NONE;
        if (notifyWrite != null && notifyWrite) {
            notifyType = notifyType.or(NotifyActionType.NOTIFY_WRITE);
        }
        if (notifyUpdate != null && notifyUpdate) {
            notifyType = notifyType.or(NotifyActionType.NOTIFY_UPDATE);
        }
        if (notifyTake != null && notifyTake) {
            notifyType = notifyType.or(NotifyActionType.NOTIFY_TAKE);
        }
        if (notifyLeaseExpire != null && notifyLeaseExpire) {
            notifyType = notifyType.or(NotifyActionType.NOTIFY_LEASE_EXPIRATION);
        }
        if (notifyUnmatched != null && notifyUnmatched) {
            notifyType = notifyType.or(NotifyActionType.NOTIFY_UNMATCHED);
        }
        if (notifyMatchedUpdate != null && notifyMatchedUpdate) {
            notifyType = notifyType.or(NotifyActionType.NOTIFY_MATCHED_UPDATE);
        }
        if (notifyRematchedUpdate != null && notifyRematchedUpdate) {
            notifyType = notifyType.or(NotifyActionType.NOTIFY_REMATCHED_UPDATE);
        }
        if (notifyAll != null && notifyAll) {
            notifyType = notifyType.or(NotifyActionType.NOTIFY_ALL);
        }
        try {
            return ((AbstractDataEventSession) dataEventSession)
                    .addListener(getReceiveTemplate(), listenerLease,
                        new NotifyInfo(listener, notifyType, dataEventSession.getSessionConfig(), null, notifyFilter, getTag()));
        } catch (Exception e) {
            throw new NotifyListenerRegistrationException("Failed to register notify listener", e);
        }
    }

    protected void invokeListenerWithTransaction(BatchRemoteEvent batchRemoteEvent, boolean performTakeOnNotify,
                                                 boolean ignoreEventOnNullTake) throws DataAccessException {

        boolean invokeListener = true;
        TransactionStatus status = null;
        if (this.getTransactionManager() != null) {
            // Execute receive within transaction.
            status = this.getTransactionManager().getTransaction(this.getTransactionDefinition());
        }
        if (passArrayAsIs) {
            RemoteEvent[] events = batchRemoteEvent.getEvents();
            Object[] eventData = new Object[events.length];
            try {
                for (int i = 0; i < events.length; i++) {
                    try {
                        eventData[i] = ((EntryArrivedRemoteEvent) events[i]).getObject();
                    } catch (net.jini.core.entry.UnusableEntryException e) {
                        throw new UnusableEntryException("Failure to get object from event [" + events[i] + "]", e);
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace(message("Received event [" + eventData[i] + "]"));
                    }
                }
                if (performTakeOnNotify) {
                    if (ignoreEventOnNullTake) {
                        ArrayList<Object> tempEventData = new ArrayList<Object>(eventData.length);
                        for (Object data : eventData) {
                            Object takeVal = getGigaSpace().take(data, 0);
                            if (takeVal != null) {
                                tempEventData.add(data);
                            }
                        }
                        if (tempEventData.isEmpty()) {
                            invokeListener = false;
                        } else {
                            eventData = tempEventData.toArray(new Object[tempEventData.size()]);
                        }
                    } else {
                        for (Object data : eventData) {
                            getGigaSpace().take(data, 0);
                        }
                    }
                }
                try {
                    if (invokeListener) {
                        invokeListener(getEventListener(), eventData, status, batchRemoteEvent);
                    }
                } catch (Throwable t) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(message("Rolling back transaction because of listener exception thrown: " + t));
                    }
                    if (status != null) {
                        status.setRollbackOnly();
                    }
                    handleListenerException(t);
                }
            } catch (RuntimeException ex) {
                if (status != null) {
                    rollbackOnException(status, ex);
                }
                throw ex;
            } catch (Error err) {
                if (status != null) {
                    rollbackOnException(status, err);
                }
                throw err;
            }
        } else {
            for (RemoteEvent remoteEvent : batchRemoteEvent.getEvents()) {
                Object eventData;
                try {
                    try {
                        eventData = ((EntryArrivedRemoteEvent) remoteEvent).getObject();
                    } catch (net.jini.core.entry.UnusableEntryException e) {
                        throw new UnusableEntryException("Failure to get object from event [" + remoteEvent + "]", e);
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace(message("Received event [" + eventData + "]"));
                    }
                    if (performTakeOnNotify) {
                        Object takeVal = getGigaSpace().take(eventData, 0);
                        if (ignoreEventOnNullTake && takeVal == null) {
                            invokeListener = false;
                        }
                        if (logger.isTraceEnabled()) {
                            logger.trace("Performed take on notify, invoke listener is [" + invokeListener + "]");
                        }
                    }
                    try {
                        if (invokeListener) {
                            invokeListener(getEventListener(), eventData, status, remoteEvent);
                        }
                    } catch (Throwable t) {
                        if (logger.isTraceEnabled()) {
                            logger.trace(message("Rolling back transaction because of listener exception thrown: " + t));
                        }
                        if (status != null) {
                            status.setRollbackOnly();
                        }
                        handleListenerException(t);
                    }
                } catch (RuntimeException ex) {
                    if (status != null) {
                        rollbackOnException(status, ex);
                    }
                    throw ex;
                } catch (Error err) {
                    if (status != null) {
                        rollbackOnException(status, err);
                    }
                    throw err;
                }
            }
        }
        if (status != null) {
            if (!status.isCompleted()) {
                if (status.isRollbackOnly()) {
                    this.getTransactionManager().rollback(status);
                } else {
                    this.getTransactionManager().commit(status);
                }
            }
        }
    }

    /**
     * Executes the given listener. If a {@link #setTransactionManager(org.springframework.transaction.PlatformTransactionManager)}
     * is provided will perform the listener execution within a transaction, if not, the listener
     * execution is performed without a transaction. <p/> <p/> If the performTakeOnNotify flag is
     * set to <code>true</code> will also perform take operation with the given event data (i.e.
     * remove it from the space).
     *
     * @param eventData           The event data object
     * @param source              The remote notify event
     * @param performTakeOnNotify A flag indicating whether to perform take operation with the given
     *                            event data
     */
    protected void invokeListenerWithTransaction(Object eventData, Object source, boolean performTakeOnNotify,
                                                 boolean ignoreEventOnNullTake) throws DataAccessException {
        boolean invokeListener = true;
        if (this.getTransactionManager() != null) {
            // Execute receive within transaction.
            TransactionStatus status = this.getTransactionManager().getTransaction(this.getTransactionDefinition());
            try {
                if (performTakeOnNotify) {
                    Object takeVal = getGigaSpace().take(eventData, 0);
                    if (ignoreEventOnNullTake && takeVal == null) {
                        invokeListener = false;
                    }
                    if (logger.isTraceEnabled()) {
                        logger.trace("Performed take on notify, invoke listener is [" + invokeListener + "]");
                    }
                }
                try {
                    if (invokeListener) {
                        invokeListener(getEventListener(), eventData, status, source);
                    }
                } catch (Throwable t) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(message("Rolling back transaction because of listener exception thrown: " + t));
                    }
                    status.setRollbackOnly();
                    handleListenerException(t);
                }
            } catch (RuntimeException ex) {
                rollbackOnException(status, ex);
                throw ex;
            } catch (Error err) {
                rollbackOnException(status, err);
                throw err;
            }
            if (!status.isCompleted()) {
                if (status.isRollbackOnly()) {
                    this.getTransactionManager().rollback(status);
                } else {
                    this.getTransactionManager().commit(status);
                }
            }
        } else {
            if (performTakeOnNotify) {
                Object takeVal = getGigaSpace().take(eventData, 0);
                if (ignoreEventOnNullTake && takeVal == null) {
                    invokeListener = false;
                }
            }
            try {
                if (invokeListener) {
                    invokeListener(getEventListener(), eventData, null, source);
                }
            } catch (Throwable t) {
                handleListenerException(t);
            }
        }
    }

    /**
     * Perform a rollback, handling rollback exceptions properly.
     *
     * @param status object representing the transaction
     * @param ex     the thrown application exception or error
     */
    private void rollbackOnException(TransactionStatus status, Throwable ex) {
        if (logger.isDebugEnabled()) {
            logger.debug(message("Initiating transaction rollback on application exception"), ex);
        }
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
     * A simple remote listener delegate that delegates remote events to invocations of the
     * registered {@link org.openspaces.events.SpaceDataEventListener#onEvent(Object,
     * org.openspaces.core.GigaSpace, org.springframework.transaction.TransactionStatus, Object)} .
     *
     * <p>Calls {@link org.openspaces.events.notify.SimpleNotifyEventListenerContainer#invokeListenerWithTransaction(Object,
     * Object, boolean, boolean)} for a possible listener execution within a transaction and passed
     * the {@link org.openspaces.events.notify.SimpleNotifyEventListenerContainer#setPerformTakeOnNotify(boolean)}
     * flag.
     */
    private class NotifyListenerDelegate implements RemoteEventListener {

        public void notify(RemoteEvent remoteEvent) throws UnknownEventException, RemoteException {
//            if (!isRunning()) {
//                return;
//            }
            Object eventData;
            try {
                eventData = ((EntryArrivedRemoteEvent) remoteEvent).getObject();
            } catch (net.jini.core.entry.UnusableEntryException e) {
                throw new UnusableEntryException("Failure to get object from event [" + remoteEvent + "]", e);
            }
            if (logger.isTraceEnabled()) {
                logger.trace(message("Received event [" + eventData + "]"));
            }
            invokeListenerWithTransaction(eventData, remoteEvent, performTakeOnNotify, ignoreEventOnNullTake);
        }
    }

    private class BatchNotifyListenerDelegate implements BatchRemoteEventListener {

        public void notifyBatch(BatchRemoteEvent batchRemoteEvent) throws UnknownEventException, RemoteException {
//            if (!isRunning()) {
//                return;
//            }
            invokeListenerWithTransaction(batchRemoteEvent, performTakeOnNotify, ignoreEventOnNullTake);
        }

        public void notify(RemoteEvent remoteEvent) throws UnknownEventException, RemoteException {
//            if (!isRunning()) {
//                return;
//            }
            Object eventData;
            try {
                eventData = ((EntryArrivedRemoteEvent) remoteEvent).getObject();
            } catch (net.jini.core.entry.UnusableEntryException e) {
                throw new UnusableEntryException("Failure to get object from event [" + remoteEvent + "]", e);
            }
            if (logger.isTraceEnabled()) {
                logger.trace(message("Received event [" + eventData + "]"));
            }
            invokeListenerWithTransaction(eventData, remoteEvent, performTakeOnNotify, ignoreEventOnNullTake);
        }
    }
}
