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


package com.gigaspaces.events.fifo;

import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.events.batching.BatchRemoteEvent;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.internal.backport.java.util.concurrent.FastConcurrentSkipListMap;
import com.gigaspaces.internal.utils.concurrent.RunnableContextClassLoaderDecorator;
import com.j_spaces.core.client.EntryArrivedRemoteEvent;
import com.j_spaces.kernel.SizeConcurrentHashMap;

import net.jini.core.event.RemoteEvent;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.event.UnknownEventException;
import net.jini.id.Uuid;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BlockedOrderedQueue is a concurrent ordered queue that can deliver unordered events to a client
 * in a FIFO order (using the event sequence number) the client is notified using a thread from the
 * proxy's thread pool.
 *
 * the event dispensing / consumption algorithm is as follows:
 *
 * on ready event arrival: 1. push Msg to ready Queue 2. tryNotifyPooll()
 *
 * event onNotifyPool() // Task was added to pool 1. pop ready event from Queue 2. if event not null
 * process event 3. if Queue not empty notifyPool() and exit // flag stays marked 4. set(flag,
 * false) 5. if Queue not empty tryNotifyPool() // need to remark flag
 *
 * procedure tryNotifyPool() 1. CAS(flag,true) 2. if success notifyPool()
 *
 * procedure notifyPool() 1. Add Task(Queue) // this task can be saved with Queue and reuse
 *
 * @author asy
 * @version 1.0
 * @since 5.0
 */
@com.gigaspaces.api.InternalApi
public class BlockedOrderedQueue {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLIENT);

    private static class SingleProducer {
        public final SizeConcurrentHashMap<Long, RemoteEvent> orderedEventsMap =
                new SizeConcurrentHashMap<Long, RemoteEvent>();

        // Maps the event sequence number to the Thread that delivered the event
        public final FastConcurrentSkipListMap<Long, Thread> orderedThreadEventsMap =
                new FastConcurrentSkipListMap<Long, Thread>();

        public final AtomicLong nextInProducerOrder;
        public final AtomicLong nextInConsumerOrder;
        public final AtomicLong maxInQueue = new AtomicLong(Long.MIN_VALUE);


        public SingleProducer(long initialSeqNumber) {
            nextInProducerOrder = new AtomicLong(initialSeqNumber);
            nextInConsumerOrder = new AtomicLong(initialSeqNumber);
        }

        public RemoteEvent popNextReadyEvent() {
            return orderedEventsMap.remove(nextInConsumerOrder.getAndIncrement());
        }
    }

    private final ConcurrentHashMap<Uuid, SingleProducer> _producers = new ConcurrentHashMap<Uuid, SingleProducer>();
    private volatile ConcurrentLinkedQueue<Uuid> _readyEvents = new ConcurrentLinkedQueue<Uuid>();
    private volatile AtomicBoolean _inProgress = new AtomicBoolean(false);
    private final ExecutorService _pool;

    private final long _maxCapacity;
    private Map<Uuid, Long> _initialSeqNumbers = null;

    /**
     * an indicator that this Queue can no longer be enqueued. set after calling {@link
     * #interrupt()}
     */
    private volatile boolean _interrupted = false;

    // Stateless task used by the thread pool to retrive the next inorder event from the task.
    final private NotifyFifoTask _notifyFifoTask;

    /**
     * Constructor
     *
     * @param maxCapacity the maximum capacity of the queue
     */
    public BlockedOrderedQueue(RemoteEventListener listener, ExecutorService threadPool, long maxCapacity, NotifyInfo info) {
        _notifyFifoTask = info.getBatchSize() > 0 && (listener instanceof BatchRemoteEventListener) ?
                new BatchNotifyFifoTask((BatchRemoteEventListener) listener, info.getBatchSize()) : new NotifyFifoTask(listener);
        _maxCapacity = maxCapacity;
        _pool = threadPool;
    }

    private long getInitialSequenceNumber(Uuid uuid) {
        if (_initialSeqNumbers == null) {
            return 0;
        }

        Long initialSeqNum = _initialSeqNumbers.get(uuid);
        if (initialSeqNum == null) {
            initialSeqNum = 0L;
            _initialSeqNumbers.put(uuid, initialSeqNum);
        }

        return initialSeqNum;
    }

    /**
     * Add new event object to the queue.
     *
     * The operation will block if - the queue exceeded its maximum capacity and event sequence
     * number exceeded the maximum sequence number in queue,
     *
     * @param theEvent the event to be enqueued
     */
    public void enqueue(RemoteEvent theEvent) {
        EntryArrivedRemoteEvent event = (EntryArrivedRemoteEvent) theEvent;
        SingleProducer producer = getProducer(event);
        while (!_interrupted && !tryInsert(event)) {
            Thread currentThread = Thread.currentThread();
            // If insert failed - put the current thread into the thread map
            producer.orderedThreadEventsMap.put(theEvent.getSequenceNumber(), currentThread);

            // Another try
            if (tryInsert(event)) {
                producer.orderedThreadEventsMap.remove(theEvent.getSequenceNumber());
                break;
            }

            // Block current thread - either until unparked or interrupted
            if (!_interrupted) //unless interrupted
                LockSupport.park();
        }
    }

    private SingleProducer getProducer(EntryArrivedRemoteEvent event) {
        Uuid uuid = event.getSpaceUuid();
        return getProducer(uuid);
    }

    private SingleProducer getProducer(Uuid uuid) {
        SingleProducer producer = _producers.get(uuid);
        long initialSeqNumber = getInitialSequenceNumber(uuid);
        if (producer == null) {
            producer = new SingleProducer(initialSeqNumber);
            SingleProducer prev = _producers.putIfAbsent(uuid, producer);
            if (prev != null)
                producer = prev;
        }

        return producer;
    }

    /**
     * Try to insert the event to the queue. If the queue exceeded its maximum capacity and the
     * event sequence number exceeded the maximum  sequence number in queue, the insert will fail.
     *
     * @return true if event was inserted, false otherwise.
     */
    private boolean tryInsert(EntryArrivedRemoteEvent theEvent) {
        SingleProducer producer = getProducer(theEvent);
        long max = producer.maxInQueue.get();
        long sequenceNumber = theEvent.getSequenceNumber();

        if (sequenceNumber < max || producer.orderedEventsMap.size() < _maxCapacity) {
            boolean updateMax = false;
            while (!updateMax) {
                updateMax = producer.maxInQueue.compareAndSet(max, Math.max(max, sequenceNumber));
                if (!updateMax) {
                    max = producer.maxInQueue.get();
                    if (sequenceNumber < max) break;
                }
            }

            // MUST put the event in the queue BEFORE comparing to avoid race condition !
            producer.orderedEventsMap.put(sequenceNumber, theEvent);
            boolean isNextInOrder = producer.nextInProducerOrder.compareAndSet(sequenceNumber, sequenceNumber + 1);

            // Check if new event is the next event in queue
            if (isNextInOrder) {
                handleReadyEvent(producer, theEvent.getSpaceUuid());
                for (long i = sequenceNumber + 1; i <= producer.maxInQueue.get(); i++) {
                    if (producer.orderedEventsMap.containsKey(i)) {
                        boolean succeeded = producer.nextInProducerOrder.compareAndSet(i, i + 1);
                        if (succeeded) {
                            handleReadyEvent(producer, theEvent.getSpaceUuid());
                        }
                    } else {
                        break;
                    }
                }
            }

            return true;
        }
        return false;
    }

    /**
     * Remove and return the next event in queue.
     *
     * If the queue is empty the method will block until an event arrives
     *
     * Note: This method is not concurrent and must be called from one thread only.
     *
     * @return the removed event
     */
    public RemoteEvent dequeue() {
        Uuid uuid = _readyEvents.poll();
        if (uuid == null)
            return null;

        SingleProducer producer = getProducer(uuid);
        return producer.popNextReadyEvent();
    }

    private boolean hasReadyEvents() {
        return !_readyEvents.isEmpty();
    }

    /**
     * Stateless task!!!!!
     */
    private class NotifyFifoTask implements Runnable {
        private final RemoteEventListener _listener;

        public NotifyFifoTask(RemoteEventListener listener) {
            _listener = listener;
        }

        public void run() {
            dequeueAndTrigger();

            if (hasReadyEvents()) {
                submitNewTask();
            } else {
                _inProgress.set(false);
                // in order not to miss events that arrived after the first queue check
                // and BUT before setting off the flag
                if (hasReadyEvents()) {
                    if (_inProgress.compareAndSet(false, true)) {
                        submitNewTask();
                    }
                }
            }
        }

        protected void dequeueAndTrigger() {
            RemoteEvent theEvent = dequeue();
            if (theEvent != null) {
                try {
                    //new BatchRemoteEvent(eventsArray)
                    _listener.notify(theEvent);
                } catch (UnknownEventException e) {
                    _logger.debug("Cant deliver notification to listener", e);
                } catch (RemoteException e) {
                    _logger.debug("Cant deliver notification to listener", e);
                } catch (Throwable e) {
                    _logger.error("Notification was send but user listener throws exception", e);
                }
            }
        }
    }

    private class BatchNotifyFifoTask extends NotifyFifoTask {

        private final BatchRemoteEventListener _batchListener;
        private final int _batchMaxSize;

        BatchNotifyFifoTask(BatchRemoteEventListener listener, int batchMaxSize) {
            super(listener);
            _batchListener = listener;
            _batchMaxSize = batchMaxSize;
        }

        @Override
        protected void dequeueAndTrigger() {
            ArrayList<RemoteEvent> eventsList = new ArrayList<RemoteEvent>();
            for (int i = 0; i < _batchMaxSize; ++i) {
                RemoteEvent theEvent = dequeue();
                if (theEvent == null) {
                    break;
                }
                eventsList.add(theEvent);
            }
            if (eventsList.size() > 0) {
                try {
                    _batchListener.notifyBatch(new BatchRemoteEvent(eventsList.toArray(new RemoteEvent[eventsList.size()])));
                } catch (UnknownEventException e) {
                    _logger.debug("Cant deliver notification to listener", e);
                } catch (RemoteException e) {
                    _logger.debug("Cant deliver notification to listener", e);
                } catch (Throwable e) {
                    _logger.error("Notification was send but user listener throws exception", e);
                }
            }
        }
    }

    private void submitNewTask() {
        // the context class loader here should be the one the user registered to notification with
        _pool.execute(new RunnableContextClassLoaderDecorator(Thread.currentThread().getContextClassLoader(), _notifyFifoTask));
    }

    private void handleReadyEvent(SingleProducer producer, Uuid uuid) {
        // Remove the next event from the queue
        _readyEvents.offer(uuid);

        if (_inProgress.compareAndSet(false, true)) {
            submitNewTask();
        }

        if (_maxCapacity < Long.MAX_VALUE) //no need to check for waiting threads
        {
            // Release the next Thread that is waiting on put
            Map.Entry entry = producer.orderedThreadEventsMap.pollFirstEntry();
            if (entry != null) {
                LockSupport.unpark((Thread) entry.getValue());
            }
        }
    }

    /**
     * This method interrupts all blocked threads awaiting on enqueue. This is usually called when
     * the Queue is no longer needed and should be discarded.
     */
    public void interrupt() {
        _interrupted = true; //disallow enqueues
        for (SingleProducer producer : _producers.values()) {
            Iterator threads = producer.orderedThreadEventsMap.values().iterator();
            while (threads.hasNext()) {
                LockSupport.unpark((Thread) threads.next());
                threads.remove();
            }
        }
    }

    /**
     * @param initialSeqNumbers the initialSeqNumbers to set
     */
    public void setInitialSeqNumbers(Map<Uuid, Long> initialSeqNumbers) {
        _initialSeqNumbers = initialSeqNumbers;

    }

}
