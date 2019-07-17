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

import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.internal.backport.java.util.concurrent.FastConcurrentSkipListMap;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.kernel.WorkingGroup;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;

import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main service that notify events in batches.
 *
 * @author assafr
 * @since 6.0
 */
@com.gigaspaces.api.InternalApi
public class BatchNotifyExecutor {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_NOTIFY);
    private static final long SLEEP_PERIOD = 500;
    //granularity of the timekey in order to prevent non-stop activity in case of a large # of templates
    public static final long BATCH_NOTIFY_TIMEKEY_GRANULARITY = 20L;


    //the following are the status result of the notify op'
    private static final int STATUS_OK = 0; //notified
    private static final int STATUS_BUSY = 1; //notify template busy
    private static final int STATUS_EMPTY = 2;  //not enough entries to satisfy batch

    private final BatchNotifyThread _notifyThread;
    private final FastConcurrentSkipListMap<TimeKey, NotifyTemplateHolder> _pendingTemplates;
    private WorkingGroup<RemoteEventBusPacket> _notifyWorkingGroup;
    //for debugging
    private final AtomicInteger _estimatedNumberOfTimeKeys;


    public BatchNotifyExecutor(String fullSpaceName, WorkingGroup<RemoteEventBusPacket> notifyWorkingGroup) {
        _estimatedNumberOfTimeKeys = new AtomicInteger();
        this._pendingTemplates = new FastConcurrentSkipListMap<BatchNotifyExecutor.TimeKey, NotifyTemplateHolder>();
        this._notifyWorkingGroup = notifyWorkingGroup;
        this._notifyThread = new BatchNotifyThread(fullSpaceName, this);
        this._notifyThread.start();

    }

    public void close() {
        _notifyThread.shutdown();
    }

    public void execute(RemoteEventBusPacket re) throws RemoteException, UnknownEventException {
        if (re.isAfterBatching()) {
            re.notifyListener();
            return;
        }

        NotifyTemplateHolder template = (NotifyTemplateHolder) re.getEntryHolder();
        RemoteEvent event = re.getRemoteEvent();

        long time = SystemTime.timeMillis() + template.getBatchTime();
        time = time < 0 ? Long.MAX_VALUE : time;
        if (BATCH_NOTIFY_TIMEKEY_GRANULARITY > 0 && time != Long.MAX_VALUE) {//time cell for bathnotify thread
            long leftover = time % BATCH_NOTIFY_TIMEKEY_GRANULARITY;
            if (leftover > 0)
                time = time + (BATCH_NOTIFY_TIMEKEY_GRANULARITY - leftover);
        }
        EventHolder eventHolder = new EventHolder(event, time);
        template.addPendingEvent(eventHolder);
        //if (_logger.isLoggable(Level.FINEST))
        //{
        //    _logger.finest("execute: added to template border="  + template.getBatchOrder() + " seq=" + eventHolder.getEvent().getSequenceNumber());
        //}

        EventHolder firstEvent = null;
        if (template.getPendingEventsSize() >= template.getBatchSize() &&
                ((firstEvent = template.peekPendingEvent()) != null))
        //full pack  help out
        {
            int status = notifyEvent(template, false);
            if (status == STATUS_OK) {
                //remove the key from skip-list, if exists
                Object res = _pendingTemplates.remove(new TimeKey(firstEvent, template));
                // if events accumulated in the template during batch notification
                // than the template should be reinserted into the skiplist.
                if (_logger.isLoggable(Level.FINEST)) {
                    if (res != null)
                        _estimatedNumberOfTimeKeys.decrementAndGet();
                    _logger.finest("execute: OK from notifyEvent remained timekeys=" + _estimatedNumberOfTimeKeys.get() + " removed=" + (res != null));
                }
                firstEvent = template.peekPendingEvent();
                if (firstEvent != null) {
                    TimeKey newTime = new TimeKey(firstEvent, template);
                    _pendingTemplates.put(newTime, template);
                    _notifyThread.notifyIfNeedTo(newTime);
                    if (_logger.isLoggable(Level.FINEST)) {
                        _estimatedNumberOfTimeKeys.incrementAndGet();
                        _logger.finest("execute: added template border=" + template.getBatchOrder() + " seq=" + newTime._holder.getEvent().getSequenceNumber() + " size=" + _estimatedNumberOfTimeKeys.get());
                    }

                }

            } else {
                insertTemplateIfFirst(template, eventHolder);
            }
        } else {
            insertTemplateIfFirst(template, eventHolder);
        }
    }

    //called by the batch thread-
    private void notifyReadyEvents() throws RemoteException, UnknownEventException {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.finest("notifyReadyEvents: start scanning size=" + _estimatedNumberOfTimeKeys);
        }
        Iterator<Map.Entry<TimeKey, NotifyTemplateHolder>> iterator = _pendingTemplates.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TimeKey, NotifyTemplateHolder> entry = iterator.next();
            NotifyTemplateHolder template = entry.getValue();
            TimeKey templateKey = entry.getKey();
            long time = templateKey._holder.getTime();
            long timeToSleep = time - SystemTime.timeMillis();
            int size = template.getPendingEventsSize();
            if (timeToSleep > 0)
                return;
            // if the first event in the queue is not the one used to insert the template
            // into the skiplist then those events came after the first batch of events were fired
            // due to size constraint and the template wasn't removed from skiplist.
            if (templateKey._holder != template.peekPendingEvent() && size < template.getBatchSize()) {
                iterator.remove();
                if (_logger.isLoggable(Level.FINEST)) {
                    _estimatedNumberOfTimeKeys.decrementAndGet();
                    _logger.finest("notifyReadyEvents: timekey to just remove (first NE and size) border=" + template.getBatchOrder() + " seq=" + templateKey._holder.getEvent().getSequenceNumber() + " size=" + _estimatedNumberOfTimeKeys);
                }
                continue;
            }
            int status = notifyEvent(template, true);
            if (status == STATUS_BUSY)
                continue; //leave the key entry, the template is busy

            iterator.remove();
            if (_logger.isLoggable(Level.FINEST)) {
                _estimatedNumberOfTimeKeys.decrementAndGet();
                _logger.finest("notifyReadyEvents: timekey to remove after notify =" + status + " border=" + template.getBatchOrder() + " seq=" + templateKey._holder.getEvent().getSequenceNumber() + " size=" + _estimatedNumberOfTimeKeys);
            }
            EventHolder firstEvent = template.peekPendingEvent();
            if (firstEvent != null) {
                TimeKey key = new TimeKey(firstEvent, template);
                _pendingTemplates.put(key, template);
                if (_logger.isLoggable(Level.FINEST)) {
                    _estimatedNumberOfTimeKeys.incrementAndGet();
                    _logger.finest("notifyReadyEvents: added new firstevent after notify status=" + status + " border=" + template.getBatchOrder() + " seq=" + key._holder.getEvent().getSequenceNumber() + " size=" + _estimatedNumberOfTimeKeys);
                }
            }
        }
    }

    private void insertTemplateIfFirst(NotifyTemplateHolder template, EventHolder eventHolder) {
        EventHolder eh = template.peekPendingEvent();
        if (eh == eventHolder) {
            TimeKey newTime = new TimeKey(eh, template);
            _pendingTemplates.put(newTime, template);
            if (_logger.isLoggable(Level.FINEST)) {
                _estimatedNumberOfTimeKeys.incrementAndGet();
                _logger.finest("insertTemplateIfFirst: added new firstevent border=" + template.getBatchOrder() + " seq=" + newTime._holder.getEvent().getSequenceNumber() + " size=" + _estimatedNumberOfTimeKeys);
            }
            _notifyThread.notifyIfNeedTo(newTime);
        }
    }

    /*
     * notify  event(s), return a STATUS indicator
     */
    private int notifyEvent(NotifyTemplateHolder template, boolean fromBatchThread) throws RemoteException, UnknownEventException {
        boolean notified = false;
        while (true) {
            if (!template.trySetNotifyInProgress()) {
                // some other thread is triggering this template.
                if (_logger.isLoggable(Level.FINEST)) {
                    _logger.finest("notifyEvent: busy from batch " + fromBatchThread + " border" + template.getBatchOrder());
                }
                return notified ? STATUS_OK : STATUS_BUSY;
            }
            try {
                if (template.getPendingEventsSize() == 0) {
                    if (_logger.isLoggable(Level.FINEST)) {
                        _logger.finest("notifyEvent: empty from batch " + fromBatchThread + " border" + template.getBatchOrder());
                    }
                    return notified ? STATUS_OK : STATUS_EMPTY;
                }
                if ((!fromBatchThread) && template.getPendingEventsSize() < template.getBatchSize()) {
                    if (_logger.isLoggable(Level.FINEST)) {
                        _logger.finest("notifyEvent: half-empty from batch " + fromBatchThread + " border" + template.getBatchOrder());
                    }
                    return notified ? STATUS_OK : STATUS_EMPTY;
                }

                RemoteEventListener reListener = template.getREListener();
                //listener can be null if notification execution fails. clear
                if (reListener == null || template.isDeleted()) {
                    template.clearPendingEvents();
                    if (_logger.isLoggable(Level.FINEST)) {
                        _logger.finest("notifyEvent: INVALID from batch " + fromBatchThread + " border" + template.getBatchOrder());
                    }
                    return STATUS_EMPTY;
                }
                if (reListener instanceof BatchRemoteEventListener) {
                    BatchRemoteEventListener listener = (BatchRemoteEventListener) reListener;
                    RemoteEvent[] eventsArray = new RemoteEvent[Math.min(template.getPendingEventsSize(), template.getBatchSize())];
                    for (int i = 0; i < eventsArray.length; i++) {
                        EventHolder res = template.pollPendingEvent();
                        if (res == null) {
                            if (i == 0)
                                return notified ? STATUS_OK : STATUS_EMPTY;
                            ;
                            //create new "full" events array
                            RemoteEvent[] tempEventsArray = new RemoteEvent[i];
                            System.arraycopy(eventsArray, 0, tempEventsArray, 0, i);
                            eventsArray = tempEventsArray;
                            break;
                        }
                        if (i == 0)
                            notified = true;
                        eventsArray[i] = res.getEvent();
                    }
                    if (_logger.isLoggable(Level.FINEST)) {
                        _logger.finest("notifyEvent: sent batch " + fromBatchThread + " border" + template.getBatchOrder() + " events=" + eventsArray.length + " left=" + template.getPendingEventsSize());
                    }
                    if (fromBatchThread) {
                        RemoteEventBatchBusPacket packet = new RemoteEventBatchBusPacket(template, eventsArray);
                        packet.afterBatching();
                        _notifyWorkingGroup.enqueueBlocked(packet);
                    } else {
                        listener.notifyBatch(new BatchRemoteEvent(eventsArray));
                    }
                } else {
                    if (_logger.isLoggable(Level.FINEST)) {
                        _logger.finest("notifyEvent: sent non-batch listener" + fromBatchThread + " border" + template.getBatchOrder() + " events=" + template.getPendingEventsSize());
                    }
                    notified = true;
                    while (template.getPendingEventsSize() > 0) {
                        EventHolder res = template.pollPendingEvent();
                        if (res == null)
                            break;
                        RemoteEvent event = res.getEvent();
                        if (fromBatchThread) {
                            RemoteEventBusPacket packet = new RemoteEventBusPacket(template, event, -1, null, false);
                            packet.afterBatching();
                            _notifyWorkingGroup.enqueueBlocked(packet);
                        } else {

                            reListener.notify(event);
                        }
                    }
                }
            } finally {
                template.finnishedNotify();
            }
            //after releasing the busy indicator we must recheck the
            //batch-maybe its full again but its thread gave-up because of
            //busy status
            if (template.getPendingEventsSize() < template.getBatchSize()) {
                return notified ? STATUS_OK : STATUS_EMPTY;
            }

            if (_logger.isLoggable(Level.FINEST)) {
                _logger.finest("notifyEvent: another loop from batch " + fromBatchThread + " border=" + template.getBatchOrder() + " template events =" + template.getPendingEventsSize());
            }
            continue;
        }
    }

    // this class holds the event and the original time to fire.
    public static class EventHolder {
        private final RemoteEvent _event;
        private final long _time;

        public EventHolder(RemoteEvent event, long time) {
            _event = event;
            _time = time;
        }

        public RemoteEvent getEvent() {
            return _event;
        }

        public long getTime() {
            return _time;
        }
    }

    private static class TimeKey implements Comparable<TimeKey> {
        public final EventHolder _holder;
        private final long _templateBatchOrder;

        public TimeKey(EventHolder holder, NotifyTemplateHolder template) {
            _holder = holder;
            _templateBatchOrder = template.getBatchOrder();
        }

        @Override
        public int compareTo(TimeKey other) {
            if (_holder.getTime() != other._holder.getTime())
                return (int) (_holder.getTime() - other._holder.getTime());

            if (_templateBatchOrder != other._templateBatchOrder)
                return (int) (_templateBatchOrder - other._templateBatchOrder);

            return (int) (_holder.getEvent().getSequenceNumber() - other._holder.getEvent().getSequenceNumber());
        }
    }

    private static class BatchNotifyThread extends GSThread {

        private volatile boolean _active;
        private final BatchNotifyExecutor _notifier;
        private long _wakeUpTime;
        private volatile boolean _waiting;
        private final Object lock = new Object();

        public BatchNotifyThread(String fullSpaceName, BatchNotifyExecutor notifier) {
            super("[" + fullSpaceName + "] Batch Notifier");
            this._notifier = notifier;
            this._active = true;
        }

        public void notifyIfNeedTo(TimeKey current) {
            if (!_waiting)
                return;
            synchronized (lock) {
                if (!_waiting)
                    return;
                if (current._holder.getTime() < _wakeUpTime)
                    lock.notify();
            }

        }

        @Override
        public void run() {
            while (_active) {
                try {
                    synchronized (lock) {
                        TimeKey first = null;
                        _waiting = true;

                        try {
                            first = _notifier._pendingTemplates.firstKey();
                        } catch (NoSuchElementException ex) {
                        }
                        _wakeUpTime = first == null ? SystemTime.timeMillis() + SLEEP_PERIOD : first._holder.getTime();
                        long waitTime = _wakeUpTime - SystemTime.timeMillis();
                        if (_logger.isLoggable(Level.FINEST)) {
                            if( first != null ) {
                                _logger.finest("BatchNotifyThread examine first template border="
                                               + first._templateBatchOrder + " seq=" + first._holder
                                                   .getEvent().getSequenceNumber() + " timetosleep="
                                               + waitTime);
                            }
                            else{
                                _logger.finest("BatchNotifyThread examine TimeKey first is NULL");
                            }
                        }
                        if (waitTime > 0) {
                            lock.wait(waitTime);
                        }
                        _waiting = false;
                    }
                    _notifier.notifyReadyEvents();
                } catch (InterruptedException e) {
                    _active = false;
                    _logger.log(Level.FINE, "batching thread was interrupted");
                } catch (RemoteException e) {
                    _logger.log(Level.FINE, "got remote exception when notifying events", e);
                } catch (UnknownEventException e) {
                    _logger.log(Level.FINE, "trying to send a corrupted event", e);
                }
            }
        }

        public void shutdown() {
            _active = false;
            interrupt();
        }
    }
}
