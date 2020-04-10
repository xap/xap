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

package com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.ReplicationInContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.DeletedMultiBucketOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.DiscardedMultiBucketOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.DiscardedSingleBucketOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.IMultiBucketSingleFileReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.MultipleBucketOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.PacketConsumeState;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.processlog.ReplicationConsumeTimeoutException;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;



@com.gigaspaces.api.InternalApi
public class ProcessLogBucket {

    private enum ProcessResult {
        CONSUMED, MISSING_PACKET, PENDING_CONSUMPTION
    }

    ;

    // configureable?
    private final Lock _lock = new ReentrantLock();
    private final SortedSet<IMultiBucketSingleFileReplicationOrderedPacket> _packetsQueue;
    private final LongObjectMap<ExchangeCountDownLatch<ProcessExchangeObject>> _pendingPackets = CollectionsFactory.getInstance().createLongObjectMap();
    private final AbstractMultiBucketSingleFileTargetProcessLog _processLog;
    private final short _bucketIndex;
    private final ReplicationInContext _replicationInContext;

    public ProcessLogBucket(short bucketIndex,
                            AbstractMultiBucketSingleFileTargetProcessLog processLog) {
        _bucketIndex = bucketIndex;
        _processLog = processLog;
        _packetsQueue = new TreeSet<IMultiBucketSingleFileReplicationOrderedPacket>(new MultiBucketSingleFileOrderedPacketComperator(bucketIndex));
        _replicationInContext = _processLog.createReplicationInContext();
    }

    private ExchangeCountDownLatch<ProcessExchangeObject> addPendingPacketLock(
            long waitOnKey, ParallelBatchProcessingContext batchContext, int segmentIndex) {
        ExchangeCountDownLatch<ProcessExchangeObject> latch = new ExchangeCountDownLatch<ProcessExchangeObject>();
        // We are under parallel batch processing, we need to set context on
        // latch for continuation
        if (batchContext != null)
            latch.exchange(new ProcessExchangeObject(batchContext, segmentIndex));

        _pendingPackets.put(waitOnKey, latch);
        return latch;
    }

    private ExchangeCountDownLatch<ProcessExchangeObject> addPendingPacketLock(long waitOnKey) {
        return addPendingPacketLock(waitOnKey, null, -1);
    }

    public void releaseOnErrorResult(final Throwable error) {
        _lock.lock();
        try {
            LongObjectIterator<ExchangeCountDownLatch<ProcessExchangeObject>> iterator = _pendingPackets.iterator();
            // Release all pending threads with the error result
            while (iterator.hasNext()) {
                iterator.advance();
                ExchangeCountDownLatch<ProcessExchangeObject> latch = iterator.value();
                ProcessExchangeObject processExchangeObject = latch.get();
                if (processExchangeObject != null) {
                    processExchangeObject.getBatchContext().setError(error);
                } else {
                    latch.countDown(new ProcessExchangeObject(error));
                }
            }
            _pendingPackets.clear();
        } finally {
            _lock.unlock();
        }

    }

    private ProcessResult processPackets(final String sourceLookupName,
                                         final IReplicationInFilterCallback inFilterCallback, long myLastKey)
            throws Exception {
        for (Iterator<IMultiBucketSingleFileReplicationOrderedPacket> iterator = _packetsQueue.iterator(); iterator.hasNext(); ) {
            IMultiBucketSingleFileReplicationOrderedPacket packet = iterator.next();
            // We have a missing packet, break loop
            long packetBucketKey = packet.getBucketKey(_bucketIndex);
            long lastProcessedKey = _processLog.getLastProcessedKeys()[_bucketIndex];
            if (packetBucketKey > lastProcessedKey + 1) {
                return lastProcessedKey >= myLastKey ? ProcessResult.CONSUMED
                        : ProcessResult.MISSING_PACKET;
            }
            // We can process this packet, process and remove it.
            boolean processedAlready = false;
            boolean wasPreprocessed = true;
            if (preprocess(packet)) {
                wasPreprocessed = false;
                PacketConsumeState packetConsumeState = packet.getConsumeState(_bucketIndex);
                switch (packetConsumeState) {
                    // Packet can be consumed
                    case CAN_CONSUME:
                        _replicationInContext.setContextPacket(packet);
                        try {
                            IReplicationPacketData<?> data = packet.getData();
                            // If there's a replication filter and should clone,
                            // clone the
                            // data
                            // packet before
                            // consumption
                            if (inFilterCallback != null
                                    && _processLog.shouldCloneOnFilter())
                                data = data.clone();

                            IDataConsumeResult prevResult = null;
                            do {
                                _processLog.validateNotClosed();
                                IDataConsumeResult consumeResult = _processLog.getDataConsumer()
                                        .consume(_replicationInContext,
                                                data,
                                                _processLog.getReplicationInFacade(),
                                                inFilterCallback);
                                if (!consumeResult.isFailed()) {
                                    processedAlready = !packet.setConsumed();
                                    break;
                                }
                                _processLog.throwIfRepetitiveError(prevResult, consumeResult);
                                if (_replicationInContext.getContextLogger().isDebugEnabled())
                                    _replicationInContext.getContextLogger().debug(
                                            "Encountered error while consuming packet ["
                                                    + packet
                                                    + "], trying to resolve issue",
                                            consumeResult.toException());
                                IDataConsumeFix fix = _processLog.getExceptionHandler()
                                        .handleException(consumeResult, packet);
                                data = _processLog.getDataConsumer().applyFix(_replicationInContext,
                                        data,
                                        fix);
                                if (_replicationInContext.getContextLogger().isDebugEnabled())
                                    _replicationInContext.getContextLogger().debug("Fix applied - retrying the operation [" + fix + "]");
                                prevResult = consumeResult;
                            } while (true);
                        } finally {
                            _replicationInContext.setContextPacket(null);
                        }
                        break;
                    // Packet is pending, waiting for all the buckets it appears
                    // in to reach it first
                    case PENDING:
                        if (lastProcessedKey >= myLastKey)
                            return ProcessResult.CONSUMED;
                        if (packetBucketKey < myLastKey)
                            return ProcessResult.MISSING_PACKET;

                        return ProcessResult.PENDING_CONSUMPTION;
                    // Packet is consumed we can continue
                    case CONSUMED:
                        processedAlready = true;
                        break;
                }
                _processLog.getLastProcessedKeys()[_bucketIndex] = lastProcessedKey + 1;
                _processLog.getLastGlobalProcessedKeys()[_bucketIndex] = packet.getKey();
            }
            if (wasConsumedByThisIteartion(packet, wasPreprocessed, processedAlready))
                // Trigger after successful consumption
                _processLog.afterSuccessfulConsumption(sourceLookupName, packet);

            iterator.remove();

            ExchangeCountDownLatch<ProcessExchangeObject> latch = _pendingPackets.remove(_processLog.getLastProcessedKeys()[_bucketIndex]);
            // Notify pending thread
            if (latch != null) {
                final ProcessExchangeObject processExchangeObject = latch.get();
                if (processExchangeObject != null) {
                    ParallelBatchProcessingContext batchContext = processExchangeObject.getBatchContext();
                    int segmentIndex = processExchangeObject.getSegmentIndex();
                    _processLog.createBatchParallelProcessingContinuationTask(sourceLookupName, inFilterCallback, batchContext, batchContext.getSegment(segmentIndex), segmentIndex);
                } else {
                    latch.countDown(null);
                }
            }
        }
        return ProcessResult.CONSUMED;
    }

    private boolean wasConsumedByThisIteartion(IMultiBucketSingleFileReplicationOrderedPacket packet, boolean wasPreprocessed, boolean processedAlready) {
        //We need to make sure we call afterSuccessfulConsumption only once per packet, packets that were preprocessed (Discarded,Deleted)
        //did not go through the regular processing path and need to be validated differently
        if (wasPreprocessed)
            return packet.setConsumed();
        //Regular data packet that was not preprocessed has a processedAlready indication set during the regular process logic
        return !processedAlready;
    }

    public MultiBucketSingleFileProcessResult addAndProcess(
            String sourceLookupName,
            IMultiBucketSingleFileReplicationOrderedPacket packet,
            IReplicationInFilterCallback inFilterCallback,
            ParallelBatchProcessingContext batchContext, int segmentIndex) throws Throwable {
        ProcessResult processResult;
        ExchangeCountDownLatch<ProcessExchangeObject> latch = null;
        long myPacketKey = packet.getBucketKey(_bucketIndex);
        final boolean isSingleBucket = packet.bucketCount() == 1;
        _lock.lock();
        try {
            _processLog.validateNotClosed();

            if (filterDuplicate(packet))
                return MultiBucketSingleFileProcessResult.OK;

            //TODO OPT:, if packet key is the last process + 1, we can process directly without
            //inserting into the packets queue
            // Add the packet to the queue
            _packetsQueue.add(packet);

            processResult = processPackets(sourceLookupName,
                    inFilterCallback,
                    myPacketKey);
            // The packet of this thread was not consumed
            if (processResult != ProcessResult.CONSUMED) {
                // Wait for previous packet to be consumed if needed
                if (shouldWaitForConsumption(processResult, isSingleBucket))
                    latch = addPendingPacketLock(myPacketKey - 1, batchContext, segmentIndex);
            }
        } catch (Throwable t) {
            throw t;
        } finally {
            _lock.unlock();
        }
        // No latch, meaning no need to wait for some previous packet to be
        // consume
        if (latch == null)
            return MultiBucketSingleFileProcessResult.OK;
        // This is a thread which is part of batch processing, it cannot block
        // it should finish as pending
        if (batchContext != null)
            return MultiBucketSingleFileProcessResult.PENDING;

        // Wait for previous packet to be consumed
        ProcessExchangeObject prevPacketConsumeResult = waitForPacketConsumption(latch, myPacketKey - 1);
        if (prevPacketConsumeResult != null)
            return new MultiBucketSingleFileProcessResult(prevPacketConsumeResult.getError());
        _lock.lock();
        try {
            _processLog.validateNotClosed();
            // If my key is already processed I can return
            if (_processLog.getLastProcessedKeys()[_bucketIndex] >= myPacketKey)
                return MultiBucketSingleFileProcessResult.OK;
            // Else go into process cycle
            processResult = processPackets(sourceLookupName,
                    inFilterCallback,
                    myPacketKey);
            // Safety, my packet cannot have a previous missing packet at this
            // state
            if (processResult == ProcessResult.MISSING_PACKET)
                throw new IllegalStateException();
            return MultiBucketSingleFileProcessResult.OK;
        } catch (Throwable t) {
            throw t;
        } finally {
            _lock.unlock();
        }
    }

    private boolean shouldWaitForConsumption(ProcessResult processedPacket,
                                             final boolean isSingleBucket) {
        return isSingleBucket
                || processedPacket == ProcessResult.MISSING_PACKET;
    }

    public void signalDoneProcessing(String sourceLookupName, MultipleBucketOrderedPacket packet, IReplicationInFilterCallback inFilterCallback) {
        _lock.lock();
        try {
            ExchangeCountDownLatch<ProcessExchangeObject> latch = _pendingPackets.remove(packet.getBucketKey(_bucketIndex));
            // If there is someone pending for this packet and it has not been
            // removed yet
            if (latch != null) {
                final ProcessExchangeObject processExchangeObject = latch.get();
                if (processExchangeObject != null) {
                    ParallelBatchProcessingContext batchContext = processExchangeObject.getBatchContext();
                    int segmentIndex = processExchangeObject.getSegmentIndex();
                    _processLog.createBatchParallelProcessingContinuationTask(sourceLookupName, inFilterCallback, batchContext, batchContext.getSegment(segmentIndex), segmentIndex);
                } else {
                    latch.countDown(null);
                }
            }
        } finally {
            _lock.unlock();
        }
    }

    public void add(List<IMultiBucketSingleFileReplicationOrderedPacket> packets) {
        _lock.lock();
        try {
            _processLog.validateNotClosed();

            filterDuplicates(packets);

            if (packets.isEmpty())
                return;

            _packetsQueue.addAll(packets);
        } finally {
            _lock.unlock();
        }
    }

    public Throwable process(String sourceLookupName,
                             List<IMultiBucketSingleFileReplicationOrderedPacket> bucketPackets,
                             IReplicationInFilterCallback inFilterCallback) throws Throwable {
        _lock.lock();
        boolean releaseLock = true;
        try {
            _processLog.validateNotClosed();
            // get last packet in bucket key
            for (Iterator<IMultiBucketSingleFileReplicationOrderedPacket> iterator = bucketPackets.iterator(); iterator.hasNext(); ) {
                final IMultiBucketSingleFileReplicationOrderedPacket packet = iterator.next();
                final boolean isSingleBucket = packet.bucketCount() == 1;
                final long myPacketKey = packet.getBucketKey(_bucketIndex);
                // Attempt to process up to current packet
                ProcessResult processResult = processPackets(sourceLookupName,
                        inFilterCallback,
                        myPacketKey);
                if (processResult != ProcessResult.CONSUMED) {
                    // Wait for previous packet to be consumed if needed
                    if (shouldWaitForConsumption(processResult, isSingleBucket)) {
                        ExchangeCountDownLatch<ProcessExchangeObject> latch = addPendingPacketLock(myPacketKey - 1);
                        // We must release the lock here before we wait on latch
                        _lock.unlock();
                        releaseLock = false;
                        // Wait for previous packet to be consumed
                        ProcessExchangeObject prevPacketConsumeError = waitForPacketConsumption(latch, myPacketKey - 1);
                        _lock.lock();
                        releaseLock = true;
                        if (prevPacketConsumeError != null)
                            return prevPacketConsumeError.getError();
                        _processLog.validateNotClosed();
                        // If my key is already processed I can continue
                        if (_processLog.getLastProcessedKeys()[_bucketIndex] >= myPacketKey) {
                            iterator.remove();
                            continue;
                        }
                        // Else go into process cycle
                        processResult = processPackets(sourceLookupName,
                                inFilterCallback,
                                myPacketKey);
                        switch (processResult) {
                            case CONSUMED:
                                iterator.remove();
                                break;
                            case PENDING_CONSUMPTION:
                                // Cannot be pending consumption on my packet if
                                // this is a single bucket packet
                                if (isSingleBucket)
                                    throw new IllegalStateException();
                                // Move to next bucket
                                return null;
                            // Safety, my packet cannot have a previous
                            // missing packet at this state
                            case MISSING_PACKET:
                                throw new IllegalStateException();

                        }
                    }
                    // We should not wait, meaning we need to move to next
                    // bucket since we reached a multi
                    // bucket packet
                    else
                        return null;
                } else {
                    // Packet consumed, remove from list
                    iterator.remove();
                }
            }
            return null;
        } catch (Throwable t) {
            throw t;
        } finally {
            if (releaseLock)
                _lock.unlock();
        }
    }

    private ProcessExchangeObject waitForPacketConsumption(
            ExchangeCountDownLatch<ProcessExchangeObject> latch, long keyWaitingFor)
            throws InterruptedException {
        if (!latch.await(_processLog.getConsumeTimeout(),
                TimeUnit.MILLISECONDS)) {
            throw new ReplicationConsumeTimeoutException("Timeout exceeded ["
                    + _processLog.getConsumeTimeout()
                    + "ms] while waiting for packet of bucket [" + _bucketIndex
                    + "] with bucket key [" + keyWaitingFor
                    + "] to be consumed");
        }
        return latch.get();
    }

    private boolean preprocess(
            IMultiBucketSingleFileReplicationOrderedPacket packet) {
        if (packet.isDataPacket())
            return true;

        //This is indication of a discarded packet
        if (packet instanceof DiscardedSingleBucketOrderedPacket
                || packet instanceof DiscardedMultiBucketOrderedPacket) {
            _processLog.getLastProcessedKeys()[_bucketIndex] = packet.getBucketKey(_bucketIndex);
            _processLog.getLastGlobalProcessedKeys()[_bucketIndex] = packet.getKey();
        }

        if (packet instanceof DeletedMultiBucketOrderedPacket) {
            _processLog.getLastProcessedKeys()[_bucketIndex] = ((DeletedMultiBucketOrderedPacket) packet).getBucketEndKey(_bucketIndex);
            _processLog.getLastGlobalProcessedKeys()[_bucketIndex] = ((DeletedMultiBucketOrderedPacket) packet).getEndKey();
        }

        return false;
    }

    public String printPendingState() {
        _lock.lock();
        try {
            if (_packetsQueue.isEmpty())
                return "EMPTY";
            try {
                IMultiBucketSingleFileReplicationOrderedPacket first = _packetsQueue.first();
                return "pending packets [" + _packetsQueue.size() + "], first packet [" + first.toString() + "]";
            } catch (NoSuchElementException e) {
                return "EMPTY";
            }
        } finally {
            _lock.unlock();
        }
    }

    protected boolean filterDuplicate(
            IMultiBucketSingleFileReplicationOrderedPacket packet) {
        // If source is in async mode (due to disconnection, recovery or temp
        // error at target, i.e out of mem)
        // duplicate packets may arrive at the intermediate stage of the source
        // moving from async to sync
        return packet.getBucketKey(_bucketIndex) <= _processLog.getLastProcessedKeys()[_bucketIndex];
    }

    private void filterDuplicates(
            List<IMultiBucketSingleFileReplicationOrderedPacket> packets) {
        for (Iterator<IMultiBucketSingleFileReplicationOrderedPacket> iterator = packets.iterator(); iterator.hasNext(); ) {
            IMultiBucketSingleFileReplicationOrderedPacket packet = iterator.next();
            if (filterDuplicate(packet))
                iterator.remove();
        }

    }

    public boolean close(long time, TimeUnit unit) throws InterruptedException {
        if ((time > 0 && !_lock.tryLock(time, unit))
                || (time <= 0 && !_lock.tryLock())) {
            return false;
        }
        try {
            releaseOnErrorResult(new ClosedResourceException("Process log is closed"));
            return true;
        } finally {
            _lock.unlock();
        }
    }

    public static class MultiBucketSingleFileOrderedPacketComperator
            implements
            Comparator<IMultiBucketSingleFileReplicationOrderedPacket> {

        private final short _bucketIndex;

        public MultiBucketSingleFileOrderedPacketComperator(short bucketIndex) {
            _bucketIndex = bucketIndex;
        }

        public int compare(IMultiBucketSingleFileReplicationOrderedPacket o1,
                           IMultiBucketSingleFileReplicationOrderedPacket o2) {
            return (o1.getBucketKey(_bucketIndex) < o2.getBucketKey(_bucketIndex)) ? -1
                    : o1.getBucketKey(_bucketIndex) == o2.getBucketKey(_bucketIndex) ? 0
                    : 1;
        }
    }

}
