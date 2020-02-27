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

package com.gigaspaces.internal.cluster.node.impl.processlog.globalorder;

import com.gigaspaces.internal.cluster.node.ReplicationBlobstoreBulkContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationInContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDeletedBacklogPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderReliableAsyncKeptDiscardedOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.ReplicationConsumeTimeoutException;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncTargetProcessLog;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.LongObjectIterator;
import com.gigaspaces.internal.collections.LongObjectMap;
import com.gigaspaces.internal.utils.concurrent.ExchangeCountDownLatch;
import com.j_spaces.core.exception.ClosedResourceException;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;


@com.gigaspaces.api.InternalApi
public class GlobalOrderTargetProcessLog
        extends AbstractGlobalOrderTargetProcessLog
        implements IReplicationSyncTargetProcessLog,
        IReplicationAsyncTargetProcessLog {
    private final SortedSet<IReplicationOrderedPacket> _packetsQueue;
    private final LongObjectMap<ExchangeCountDownLatch<Throwable>> _pendingPackets = CollectionsFactory.getInstance().createLongObjectMap();

    private final long _consumeTimeout;

    public GlobalOrderTargetProcessLog(
            GlobalOrderProcessLogConfig processLogConfig,
            IReplicationPacketDataConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName,
            boolean oneWayProcessing, IReplicationGroupHistory groupHistory) {
        this(processLogConfig,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                -1,
                true,
                oneWayProcessing,
                groupHistory);
    }

    public GlobalOrderTargetProcessLog(
            GlobalOrderProcessLogConfig processLogConfig,
            IReplicationPacketDataConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName, long lastProcessedKey, boolean isFirstHandshake,
            boolean oneWayProcessing, IReplicationGroupHistory groupHistory) {
        super(dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                lastProcessedKey,
                isFirstHandshake,
                groupHistory);
        _consumeTimeout = processLogConfig.getConsumeTimeout();
        _packetsQueue = new TreeSet<IReplicationOrderedPacket>(new SharedOrderedPacketComparator());
    }

    @Override
    protected boolean contentRequiredWhileProcessing() {
        return false;
    }

    public GlobalOrderProcessResult processBatch(String sourceLookupName,
                                                 List<IReplicationOrderedPacket> packets,
                                                 IReplicationInFilterCallback filterInCallback) {
        if (!isOpen())
            throwClosedException();

        boolean processedPacket = false;
        Throwable errorDuringAfterConsumption;

        ExchangeCountDownLatch<Throwable> latch = null;
        _lock.lock();
        try {
            // If closed, reject immediately
            if (isClosed())
                throwClosedException();

            filterDuplicate(packets);
            // If all were filtered, return ok process result
            if (packets.isEmpty())
                return GlobalOrderProcessResult.OK;

            _packetsQueue.addAll(packets);
            final long myLastKey = packets.get(packets.size() - 1).getKey();
            try {
                processedPacket = processPackets(sourceLookupName,
                        myLastKey,
                        filterInCallback);
            } catch (Throwable t) {
                // Exception thrown from exception handler meaning it can not
                // handle this exception
                // We must return a failed result to all pending threads
                return onErrorReleasePendingAndReturnResult(t);
            } finally {
                // call afterConsumption which triggers blobstore flush in case blobstore bulks are activated
                errorDuringAfterConsumption = afterConsumption(processedPacket, _lastProcessedKey);
            }
            // The packet of this thread was not processed
            if (!processedPacket) {
                latch = addPendingPacketLock(myLastKey);
            }
            if (errorDuringAfterConsumption != null) {
                return new GlobalOrderProcessResult(errorDuringAfterConsumption, calcLastProcessedkey());
            }
        } finally {
            _lock.unlock();
        }
        return afterProcessAttempt(processedPacket, latch);
    }


    /**
     * calculate the last processed key, in case of blobstore bulks will return the last flushed
     * entry key
     */
    private long calcLastProcessedkey() {
        ReplicationInContext replicationInContext = getReplicationInContext();
        ReplicationBlobstoreBulkContext replicationBlobstoreBulkContext = replicationInContext.getReplicationBlobstoreBulkContext();
        if (replicationBlobstoreBulkContext != null && replicationBlobstoreBulkContext.getBlobStoreReplicationBulkConsumeHelper() != null) {
            return replicationBlobstoreBulkContext.getBlobStoreReplicationBulkConsumeHelper().getLastProcessedKey();
        }
        return _lastProcessedKey;
    }

    private GlobalOrderProcessResult afterProcessAttempt(
            boolean processedPacket, ExchangeCountDownLatch<Throwable> latch) {

        if (processedPacket) {
            return GlobalOrderProcessResult.OK;
        }

        try {
            // Wait for this packet to be processed (forever)
            if (!latch.await(_consumeTimeout, TimeUnit.MILLISECONDS))
                return new GlobalOrderProcessResult(new ReplicationConsumeTimeoutException("Timeout exceeded ["
                        + _consumeTimeout
                        + "ms] while waiting for packet to be consumed"),
                        calcLastProcessedkey());

            Throwable error = latch.get();
            if (error == null) {
                return GlobalOrderProcessResult.OK;
            }
            rethrowAsClosedIfNeeded(error);
            return new GlobalOrderProcessResult(error, calcLastProcessedkey());
        } catch (InterruptedException e) {
            // Return failed result
            return new GlobalOrderProcessResult(e, calcLastProcessedkey());
        }
    }

    /**
     * This method should be called under lock!
     */
    private Throwable afterConsumption(boolean successful, long lastProcessedKey) {
        try {
            getReplicationInFacade().afterConsumption(getReplicationInContext(), successful, lastProcessedKey);
            return null;
        } catch (Throwable ex) {
            if (_specificLogger.isLoggable(Level.SEVERE)) {
                _specificLogger.log(Level.SEVERE, "failure during after consumption", ex);
            }
            final long possibleRevertedKey = calcLastProcessedkey();
            if (possibleRevertedKey != _lastProcessedKey) {
                if (_specificLogger.isLoggable(Level.SEVERE)) {
                    _specificLogger.log(Level.SEVERE, "reverting last processed redo key from: " + _lastProcessedKey + " to " + possibleRevertedKey, ex);
                }
                _lastProcessedKey = possibleRevertedKey;
            }
            return ex;
        }
    }

    private void rethrowAsClosedIfNeeded(Throwable error) {
        if (error instanceof ClosedResourceException)
            throw (ClosedResourceException) error;
    }

    public GlobalOrderProcessResult process(String sourceLookupName,
                                            IReplicationOrderedPacket packet,
                                            IReplicationInFilterCallback inFilterCallback) {
        if (!isOpen())
            throwClosedException();

        Throwable errorDuringAfterConsumption;
        ExchangeCountDownLatch<Throwable> latch = null;
        _lock.lock();
        try {
            // If closed, reject immediately
            if (isClosed())
                throwClosedException();

            // If were filtered, return ok process result
            if (filterDuplicate(packet))
                return GlobalOrderProcessResult.OK;

            final long myLastKey = packet.getKey();
            try {
                // optimize, if packet key is the last process + 1, we can
                // directly process it without
                // inserting it into the packets queue
                if (myLastKey == _lastProcessedKey + 1) {
                    processPacket(sourceLookupName,
                            inFilterCallback,
                            getReplicationInContext(),
                            packet,
                            true);
                    // No more pending packets, return ok result
                    if (!_packetsQueue.isEmpty()) {
                        processPackets(sourceLookupName,
                                myLastKey,
                                inFilterCallback);
                    }
                    return GlobalOrderProcessResult.OK;
                } else {
                    // We cannot process this packet now, add to pending queue
                    _packetsQueue.add(packet);
                    // The packet of this thread was not processed
                    latch = addPendingPacketLock(myLastKey);
                }
            } catch (Throwable t) {
                // Exception thrown from exception handler meaning it can not
                // handle this exception
                // We must return a failed result to all pending threads
                return onErrorReleasePendingAndReturnResult(t);
            } finally {
                // call afterConsumption which triggers blobstore flush in case blobstore bulks are activated
                errorDuringAfterConsumption = afterConsumption(false, _lastProcessedKey);
            }
            if (errorDuringAfterConsumption != null) {
                return new GlobalOrderProcessResult(errorDuringAfterConsumption, calcLastProcessedkey());
            }
        } finally {
            _lock.unlock();
        }
        return afterProcessAttempt(false, latch);
    }

    private ExchangeCountDownLatch<Throwable> addPendingPacketLock(long key) {
        ExchangeCountDownLatch<Throwable> latch = new ExchangeCountDownLatch<Throwable>();
        _pendingPackets.put(key, latch);
        return latch;
    }

    protected boolean processPackets(String sourceLookupName, long myLastKey,
                                     IReplicationInFilterCallback filterInCallback) throws Exception {
        for (Iterator<IReplicationOrderedPacket> iterator = _packetsQueue.iterator(); iterator.hasNext(); ) {
            IReplicationOrderedPacket packet = iterator.next();
            // We have a missing packet, break loop
            if (packet.getKey() > _lastProcessedKey + 1)
                return _lastProcessedKey >= myLastKey;

            // This is not always true due to optimization that we do not insert
            // packets
            // to the queue if we can process
            // it immediately and then we can have packets that were already
            // processed (i.e resent during
            // exception) kept in the pending queue
            if (packet.getKey() == _lastProcessedKey + 1) {
                processPacket(sourceLookupName,
                        filterInCallback,
                        getReplicationInContext(),
                        packet,
                        true);
            }

            iterator.remove();

            ExchangeCountDownLatch<Throwable> latch = _pendingPackets.remove(packet.getKey());
            // Notify pending thread
            if (latch != null)
                latch.countDown(null);

        }
        return true;
    }

    private void processPacket(String sourceLookupName,
                               IReplicationInFilterCallback filterInCallback,
                               ReplicationInContext context, IReplicationOrderedPacket packet,
                               boolean throwOnClosed) throws Exception {
        // We can process this packet, process and remove it.
        if (preprocess(packet)) {
            context.setContextPacket(packet);
            context.setLastProcessedKey(_lastProcessedKey);
            try {
                IReplicationPacketData<?> data = packet.getData();
                // If there's a replication filter and should clone, clone the
                // data
                // packet before
                // consumption
                if (filterInCallback != null && shouldCloneOnFilter())
                    data = data.clone();

                IDataConsumeResult prevResult = null;

                do {
                    // If closed, reject immediately
                    if (throwOnClosed && isClosed())
                        throw new ClosedResourceException("Process log is closed");

                    IDataConsumeResult consumeResult = getDataConsumer().consume(context,
                            data,
                            getReplicationInFacade(),
                            filterInCallback);
                    if (!consumeResult.isFailed())
                        break;

                    throwIfRepetitiveError(prevResult, consumeResult);
                    if (_specificLogger.isLoggable(Level.FINER))
                        _specificLogger.log(Level.FINER,
                                "Encountered error while consuming packet ["
                                        + packet
                                        + "], trying to resolve issue",
                                consumeResult.toException());
                    IDataConsumeFix fix = getExceptionHandler().handleException(consumeResult, packet);
                    data = getDataConsumer().applyFix(context, data, fix);
                    if (_specificLogger.isLoggable(Level.FINER))
                        _specificLogger.log(Level.FINER, "Fix applied - retrying the operation [" + fix + "]");
                    prevResult = consumeResult;
                } while (true);
                _lastProcessedKey++;
            } finally {
                //Clear packet from context
                context.setContextPacket(null);
            }
        }
        // Trigger after successful consumption
        afterSuccessfulConsumption(sourceLookupName, packet);
    }

    protected void afterSuccessfulConsumption(String sourceLookupName,
                                              IReplicationOrderedPacket packet) {
        // Default do nothing
    }

    protected boolean shouldCloneOnFilter() {
        // Default is false since this packet is not used outside of this scope
        return false;
    }

    private boolean preprocess(IReplicationOrderedPacket packet) {
        if (packet.isDataPacket())
            return true;

        if (packet instanceof GlobalOrderDeletedBacklogPacket) {
            GlobalOrderDeletedBacklogPacket deletedBacklogPacket = (GlobalOrderDeletedBacklogPacket) packet;
            logDeletion(deletedBacklogPacket);
            _lastProcessedKey = deletedBacklogPacket.getEndKey();
            return false;
        }
        // This is indication of a discarded packet either a regular path or
        // packet
        // that was discarded entirely during synchronization but was kept for
        // reliable async target
        if (packet instanceof GlobalOrderDiscardedReplicationPacket) {
            _lastProcessedKey = ((GlobalOrderDiscardedReplicationPacket) packet).getEndKey();
            return false;
        }
        if (packet instanceof GlobalOrderReliableAsyncKeptDiscardedOrderedPacket) {
            _lastProcessedKey = packet.getEndKey();
            return false;
        }

        return true;
    }

    @Override
    protected String dumpStateExtra() {
        _lock.lock();
        try {
            if (_packetsQueue.isEmpty())
                return "EMPTY";
            try {
                IReplicationOrderedPacket first = _packetsQueue.first();
                return "pending packets [" + _packetsQueue.size() + "], first packet [" + first.toString() + "]";
            } catch (NoSuchElementException e) {
                return "EMPTY";
            }
        } finally {
            _lock.unlock();
        }
    }

    @Override
    protected void onClose() {
        releasePendingWithError(new ClosedResourceException("Process log is closed"));
    }

    private GlobalOrderProcessResult onErrorReleasePendingAndReturnResult(
            Throwable error) {
        releasePendingWithError(error);

        rethrowAsClosedIfNeeded(error);

        if (_specificLogger.isLoggable(Level.FINER))
            _specificLogger.log(Level.FINER, "error while processing incoming replication", error);

        GlobalOrderProcessResult result = new GlobalOrderProcessResult(error,
                calcLastProcessedkey());

        return result;
    }

    private void releasePendingWithError(final Throwable error) {
        // Release all pending threads with the error result
        LongObjectIterator<ExchangeCountDownLatch<Throwable>> iterator = _pendingPackets.iterator();
        while (iterator.hasNext()) {
            iterator.advance();
            iterator.value().countDown(error);
        }
        _pendingPackets.clear();
    }

    public static class SharedOrderedPacketComparator
            implements Comparator<IReplicationOrderedPacket> {

        public int compare(IReplicationOrderedPacket o1,
                           IReplicationOrderedPacket o2) {
            return (o1.getKey() < o2.getKey()) ? -1
                    : o1.getKey() == o2.getKey() ? 0
                    : 1;
        }

    }

}
