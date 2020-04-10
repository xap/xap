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

package com.gigaspaces.internal.server.space.events;

import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.cluster.node.IReplicationNode;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.j_spaces.core.LeaseManager;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.filters.FilterManager;
import com.j_spaces.core.filters.FilterOperationCodes;
import com.j_spaces.kernel.WorkingGroup;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.UnknownEventException;

import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.j_spaces.core.Constants.Engine.ENGINE_NOTIFIER_RETRIES_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_NOTIFIER_TTL_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_NOTIFY_MAX_THREADS_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_NOTIFY_MAX_THREADS_PROP;
import static com.j_spaces.core.Constants.Engine.ENGINE_NOTIFY_MIN_THREADS_DEFAULT;
import static com.j_spaces.core.Constants.Engine.ENGINE_NOTIFY_MIN_THREADS_PROP;

/**
 * Control NotifyStatus status. This controller calls a given NotifyStatus for each notification
 * trigger/return.
 *
 * @author Guy korland
 * @version 1.0
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceDataEventManager implements ISpaceModeListener {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_FILTERS);

    private final IDirectSpaceProxy _spaceProxy;
    private final SpaceImpl _spaceImpl;
    private final FilterManager _filterManager;
    // stores all the notifications while space is backup
    // used to guarantee notifications delivery  on failover
    private final NotifyBackupLog _backupLog;
    private final AtomicLong _eventIdGenerator;
    private final SpaceDataEventDispatcher _dataEventDispatcher;
    private final WorkingGroup<RemoteEventBusPacket> _dataEventDispatcherWorkingGroup;
    private final BatchNotifyExecutor _batchNotifier;
    // number of notify retries
    private final int _notifyTTL;

    public SpaceDataEventManager(IDirectSpaceProxy spaceProxy, FilterManager filterManager,
                                 SpaceImpl spaceImpl, SpaceConfigReader configReader) {
        this._spaceProxy = spaceProxy;
        this._filterManager = filterManager;
        this._spaceImpl = spaceImpl;
        this._eventIdGenerator = new AtomicLong(0);

        int minThreads = configReader.getIntSpaceProperty(
                ENGINE_NOTIFY_MIN_THREADS_PROP, ENGINE_NOTIFY_MIN_THREADS_DEFAULT);
        int maxThreads = configReader.getIntSpaceProperty(
                ENGINE_NOTIFY_MAX_THREADS_PROP, ENGINE_NOTIFY_MAX_THREADS_DEFAULT);
        _notifyTTL = configReader.getIntSpaceProperty(ENGINE_NOTIFIER_TTL_PROP, ENGINE_NOTIFIER_RETRIES_DEFAULT);
        _dataEventDispatcher = new SpaceDataEventDispatcher(this);
        this._dataEventDispatcherWorkingGroup = new WorkingGroup<RemoteEventBusPacket>(
                _dataEventDispatcher, Thread.NORM_PRIORITY,
                "Notifier", minThreads, maxThreads, 60 * 1000/*timeout*/);

        this._batchNotifier = new BatchNotifyExecutor(configReader.getFullSpaceName(), this._dataEventDispatcherWorkingGroup);
        this._backupLog = new NotifyBackupLog(this._dataEventDispatcherWorkingGroup);
        this._dataEventDispatcherWorkingGroup.start();
    }

    public void init(boolean isReplicated, IReplicationNode replicationNode) {
        // check if space is backup and replication is enabled
        if (_spaceImpl.isBackup() && isReplicated) {
            _spaceImpl.addSpaceModeListener(this);
            replicationNode.setInNotificationSentHandler(_backupLog);
        }
    }

    public void setLeaseManager(LeaseManager leaseManager) {
        _dataEventDispatcher.setLeaseManager(leaseManager);
    }


    public void close() {
        _spaceImpl.removeSpaceModeListener(this);
        _batchNotifier.close();
        _dataEventDispatcherWorkingGroup.shutdown();
    }

    /**
     * Called when a notification returned
     *
     * @param notifyContext current notify status checked
     * @param template      the current template to filter
     */
    public void notifyReturned(NotifyContext notifyContext, ITemplateHolder template) {
        if (notifyContext == null)
            return;

        if (_filterManager._isFilter[FilterOperationCodes.AFTER_NOTIFY_TRIGGER]) {

            Object[] entries = new Object[2];
            entries[0] = notifyContext.getEntry();
            entries[1] = template;
            try {
                _filterManager.invokeFilters(FilterOperationCodes.AFTER_NOTIFY_TRIGGER, null, entries);
            } catch (Exception e) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Exception was thrown by filter on AFTER_NOTIFY_TRIGGER.", e);
            }
        }

        decreaseCounter(notifyContext);
    }

    /**
     * Called when the matching search is over
     *
     * @param notifyContext current notify status checked
     * @param template      the current template to filter
     */
    public void finishTemplatesSearch(NotifyContext notifyContext) {
        decreaseCounter(notifyContext);
        _backupLog.clearTriggered(notifyContext.getOperationId());
    }

    @Override
    public void afterSpaceModeChange(SpaceMode newMode) throws RemoteException {
        if (newMode == SpaceMode.PRIMARY)
            _backupLog.processOutgoingEvents();
        _backupLog.clear();
    }

    @Override
    public void beforeSpaceModeChange(SpaceMode newMode) throws RemoteException {
    }

    public long generateEventId() {
        return _eventIdGenerator.incrementAndGet();
    }

    public int getQueueSize() {
        return _dataEventDispatcherWorkingGroup.getQueue().size();
    }

    public void executePacket(RemoteEventBusPacket re, NotifyTemplateHolder th)
            throws RemoteException, UnknownEventException {
        if (th.isBatching())
            _batchNotifier.execute(re);
        else
            re.notifyListener();
    }

    /**
     * Notifies a notification template with the specified remote event Notification is done
     * asynchronously, by sending a bus packet to the Notifier.
     *
     * Assumes template is locked.
     */
    public void notifyTemplate(NotifyTemplateHolder template, IEntryHolder entry, IEntryHolder oldEntry,
                               NotifyContext notifyContext, Context ctx) {
        if (executeBeforeNotifyTriggeredFilters(notifyContext, template)) {
            RemoteEvent event = oldEntry == null ?
                    template.createRemoteEvent(entry, notifyContext.getNotifyType(), ctx.getOperationID(), _spaceProxy, ctx.isFromReplication()) :
                    template.createRemoteEvent(entry, oldEntry, notifyContext.getNotifyType(), ctx.getOperationID(), _spaceProxy, ctx.isFromReplication());

            if (event != null) {
                if (template.isGuaranteedNotification())
                    notifyContext.setGuaranteedNotifications(true);
                // create RemoteEventBusPacket packet using factory and send it on Notifier queue.
                RemoteEventBusPacket packet = new RemoteEventBusPacket(template, event, _notifyTTL, notifyContext, ctx.isFromReplication());

                // check if the event should be stored in a backlog
                if (packet.isFromReplication() && !template.isTriggerNotify() && _spaceImpl.isBackup()) {
                    if (packet.getOperationID() != null)
                        _backupLog.add(packet);
                } else {
                    _dataEventDispatcherWorkingGroup.enqueueBlocked(packet);
                }
            }
        }
    }

    private boolean executeBeforeNotifyTriggeredFilters(NotifyContext notifyContext, ITemplateHolder template) {
        /* the first notification causes the BEFORE_ALL_NOTIFY_TRIGGER
         * No synchronization is needed cause the search is done by only one thread. */
        if (notifyContext.countInc(_filterManager._isFilter[FilterOperationCodes.AFTER_ALL_NOTIFY_TRIGGER])
                && _filterManager._isFilter[FilterOperationCodes.BEFORE_ALL_NOTIFY_TRIGGER]) {
            try {
                _filterManager.invokeFilters(FilterOperationCodes.BEFORE_ALL_NOTIFY_TRIGGER, null, notifyContext.getEntry());
            } catch (RuntimeException e) // filters can only throw RuntimeException
            {
                //TODO abort all the notifications
                if (_logger.isDebugEnabled())
                    _logger.debug("Exception was thrown by filter on BEFORE_ALL_NOTIFY_TRIGGER.", e);
            }
        }

        if (_filterManager._isFilter[FilterOperationCodes.BEFORE_NOTIFY_TRIGGER]) {
            Object[] arguments = new IEntryHolder[]{notifyContext.getEntry(), template};
            try {
                _filterManager.invokeFilters(FilterOperationCodes.BEFORE_NOTIFY_TRIGGER, null, arguments);
            } catch (Exception e) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Exception was thrown by filter on BEFORE_NOTIFY_TRIGGER.", e);
                notifyContext.countDec(); // reduce the count by 1, cause this notification will never occur.
                return false; //abort notifications
            }
        }

        return true;
    }

    /**
     * Decreases status counter, and in case it returns true calls the AFTER_ALL_NOTIFY_TRIGGER.
     *
     * @param notifyContext current notify status checked
     */
    private void decreaseCounter(NotifyContext notifyContext) {
        if (notifyContext.countDec()) // true only if AFTER_ALL should be called
        {
            try {
                _filterManager.invokeFilters(FilterOperationCodes.AFTER_ALL_NOTIFY_TRIGGER, null, notifyContext);
            } catch (Exception e) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Exception was thrown by filter on AFTER_ALL_NOTIFY_TRIGGER.", e);
            }
        }
    }
}
