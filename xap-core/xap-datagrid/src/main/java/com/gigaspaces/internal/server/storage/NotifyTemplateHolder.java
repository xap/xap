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

package com.gigaspaces.internal.server.storage;

import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.FifoSearch;
import com.gigaspaces.internal.server.space.events.BatchNotifyExecutor.EventHolder;
import com.gigaspaces.internal.server.space.events.NotifyContextsHolder;
import com.gigaspaces.internal.transport.EntryPacketFactory;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.ObjectTypes;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.SpaceOperations;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;
import com.j_spaces.core.client.INotifyDelegator;
import com.j_spaces.core.client.INotifyDelegatorFilter;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.NotifyModifiers;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.transaction.server.ServerTransaction;

import java.rmi.MarshalledObject;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * this class contains all the information relevant to a notify template.
 *
 * @author asy ronen
 * @version 1.0
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class NotifyTemplateHolder extends TemplateHolder {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_NOTIFY);
    private static final String EMPTY_STRING = "";

    private final long _eventId;
    private final NotifyInfo _notifyInfo;
    private final ITemplatePacket _generationTemplate;
    private final AtomicLong _sequenceNumber;
    private final AtomicBoolean _notifyInProgress;
    private final Queue<EventHolder> _pendingBatchEvents;
    private final AtomicInteger _numPendingBatchEvents;
    private long _batchOrder;

    public NotifyTemplateHolder(IServerTypeDesc typeDesc, ITemplatePacket template,
                                String uid, long expirationTime, long eventId,
                                NotifyInfo info, boolean isFifo) {
        super(typeDesc, template, uid, expirationTime, null /*txn*/, SystemTime.timeMillis(),
                SpaceOperations.NOTIFY, null, template.isReturnOnlyUids() | info.isReturnOnlyUids(), info.getModifiers(), isFifo);

        this._eventId = eventId;
        this._notifyInfo = info;
        this._generationTemplate = template.clone();
        this._sequenceNumber = new AtomicLong(0);
        this._notifyInProgress = new AtomicBoolean(false);
        if (_notifyInfo.isBatching()) {
            _pendingBatchEvents = new ConcurrentLinkedQueue<EventHolder>();
            _numPendingBatchEvents = new AtomicInteger();
        } else {
            _pendingBatchEvents = null;
            _numPendingBatchEvents = null;
        }

        if (_logger.isDebugEnabled())
            _logger.debug("Created notify template: eventId=" + _eventId +
                    ", templateUid=" + _notifyInfo.getTemplateUID() +
                    ", type=[" + template.getTypeName() + "]" +
                    ", notifyType=" + NotifyModifiers.toString(_notifyInfo.getNotifyType()) +
                    ", fifo=" + _notifyInfo.isFifo() +
                    ", prevValue=" + _notifyInfo.isReturnPrevValue());
    }

    @Override
    public boolean isNotifyTemplate() {
        return true;
    }

    public int getSpaceItemType() {
        return isEmptyTemplate() ? ObjectTypes.NOTIFY_NULL_TEMPLATE : ObjectTypes.NOTIFY_TEMPLATE;
    }

    public long getEventId() {
        return _eventId;
    }

    public NotifyInfo getNotifyInfo() {
        return _notifyInfo;
    }

    public ITemplatePacket getGenerationTemplate() {
        return _generationTemplate;
    }

    public long getSequenceNumber() {
        return _sequenceNumber.get();
    }

    public boolean isTriggerNotify() {
        Boolean trigger = _notifyInfo.getNotifyTemplate();
        return trigger != null ? trigger : false;
    }

    public boolean isReplicateNotify() {
        Boolean replicate = _notifyInfo.getReplicateTemplate();
        return replicate != null ? replicate : false;
    }

    public boolean isGuaranteedNotification() {
        return _notifyInfo.isGuaranteedNotifications();
    }

    public boolean isBroadcast() {
        return _notifyInfo.isBroadcast();
    }

    @Override
    public int getNotifyType() {
        return _notifyInfo.getNotifyType();
    }

    public boolean containsNotifyType(NotifyActionType notifyType) {
        return Modifiers.contains(_notifyInfo.getNotifyType(), notifyType.getModifier());
    }

    @Override
    public MarshalledObject getHandback() {
        return _notifyInfo.getHandback();
    }

    public INotifyDelegatorFilter getFilter() {
        return _notifyInfo.getFilter();
    }

    public RemoteEventListener getREListener() {
        return _notifyInfo.getListener();
    }

    public void setREListener(RemoteEventListener listener) {
        _notifyInfo.setListener(listener);
    }

    public void addPendingEvent(EventHolder event) {
        _numPendingBatchEvents.incrementAndGet();
        _pendingBatchEvents.add(event);
    }

    public void clearPendingEvents() {
        _pendingBatchEvents.clear();
        _numPendingBatchEvents.set(0);
    }

    public EventHolder pollPendingEvent() {
        EventHolder res = _numPendingBatchEvents.get() > 0 ? _pendingBatchEvents.poll() : null;
        if (res != null) {
            _numPendingBatchEvents.decrementAndGet();
            return res;
        }
        return null;
    }

    public EventHolder peekPendingEvent() {
        return _numPendingBatchEvents.get() > 0 ? _pendingBatchEvents.peek() : null;
    }

    public int getPendingEventsSize() {
        return _numPendingBatchEvents.get();
    }

    public boolean isBatching() {
        return _notifyInfo.isBatching();
    }

    public int getBatchSize() {
        return _notifyInfo.getBatchSize();
    }

    public long getBatchTime() {
        return _notifyInfo.getBatchTime();
    }

    public long getBatchOrder() {
        return _batchOrder;
    }

    public void setBatchOrder(long _batchOrder) {
        this._batchOrder = _batchOrder;
    }

    public boolean trySetNotifyInProgress() {
        return _notifyInProgress.compareAndSet(false, true);
    }

    public void finnishedNotify() {
        _notifyInProgress.set(false);
    }

    @Override
    public void dump(Logger logger, String msg) {
        super.dump(logger, msg);

        logger.info("SequenceNumber : " + getSequenceNumber());
        logger.info("EventId : " + _eventId);
        logger.info("Handback : " + getHandback());
        logger.info("REListener : " + getREListener());
    }

    /**
     * Return true whether this template should match by uid only (if such provided) or with full
     * match regardless present if uid.
     */
    @Override
    public boolean isMatchByID() {
        return getUidToOperateBy() != null && NotifyModifiers.isMatchByID(getNotifyInfo().getNotifyType());
    }

    public RemoteEvent createRemoteEvent(IEntryHolder entryHolder, NotifyActionType notifyType,
                                         OperationID operationID, IJSpace spaceProxy, boolean fromReplication) {
        return createRemoteEvent(entryHolder, null, notifyType, operationID, spaceProxy, fromReplication);
    }

    public RemoteEvent createRemoteEvent(IEntryHolder entryHolder, IEntryHolder oldEntryHolder, NotifyActionType notifyType,
                                         OperationID operationID, IJSpace spaceProxy, boolean fromReplication) {
        final long seqNum = _sequenceNumber.get();

        RemoteEvent remoteEvent;
        if (getREListener() instanceof INotifyDelegator) {
            IEntryPacket entryPacket, oldEntryPacket = null;
            if (isReturnOnlyUid()) {
                entryPacket = TemplatePacketFactory.createUidPacket(entryHolder.getUID(), entryHolder.getEntryData().getVersion());
                if (notifyType.equals(NotifyActionType.NOTIFY_UNMATCHED) || (oldEntryHolder != null && (_notifyInfo.isReturnPrevValue()))) {
                    oldEntryPacket = TemplatePacketFactory.createUidPacket(oldEntryHolder.getUID(), oldEntryHolder.getEntryData().getVersion());
                }
            } else {
                entryPacket = EntryPacketFactory.createFullPacket(null, entryHolder, this);
                if (notifyType.equals(NotifyActionType.NOTIFY_UNMATCHED) || (oldEntryHolder != null && (_notifyInfo.isReturnPrevValue()))) {
                    oldEntryPacket = EntryPacketFactory.createFullPacket(null, oldEntryHolder, this);
                }
            }
            entryPacket.setOperationID(operationID);
            if (getProjectionTemplate() != null) {
                getProjectionTemplate().filterOutNonProjectionProperties(entryPacket);
                if (oldEntryPacket != null)
                    getProjectionTemplate().filterOutNonProjectionProperties(oldEntryPacket);
            }
            EntryArrivedRemoteEvent event =
                    new EntryArrivedRemoteEvent(spaceProxy, _eventId, seqNum, getHandback(), entryPacket, oldEntryPacket, notifyType, fromReplication, getUID(), getQueryResultType());

            remoteEvent = filter(event);
        } else
            remoteEvent = new RemoteEvent(EMPTY_STRING, _eventId, seqNum, getHandback());

        if (remoteEvent != null)
            _sequenceNumber.incrementAndGet();

        return remoteEvent;
    }

    private EntryArrivedRemoteEvent filter(EntryArrivedRemoteEvent event) {
        INotifyDelegatorFilter filter = this.getFilter();
        if (filter != null) {
            try {
                if (!filter.process(event))
                    return null;
            } catch (Throwable e) {
                _logger.debug("calling user filter caused an exception", e);
            }
        }

        return event;
    }

    public boolean quickReject(Context context, FifoSearch fifoSearch, ServerTransaction txn, NotifyContextsHolder notifyContextsHolder) {
        if (super.quickReject(context, fifoSearch))
            return true;

        if (txn != null && txn != this.getXidOriginatedTransaction())
            return true;

        // If the template notify type does not include the operation that triggered the notification, abort:
        boolean matchFound = false;
        for (NotifyActionType notifyType : notifyContextsHolder.getNotifyTypes()) {
            if (containsNotifyType(notifyType)) {
                matchFound = true;
                break;
            }
        }
        if (!matchFound)
            return true;


        if (context.isFromReplication() && !isTriggerNotify() && !isGuaranteedNotification())
            return true;

        if (context.getOperationVisibilityTime() != 0 && getSCN() > context.getOperationVisibilityTime())
            return true;

        return false;
    }
}
