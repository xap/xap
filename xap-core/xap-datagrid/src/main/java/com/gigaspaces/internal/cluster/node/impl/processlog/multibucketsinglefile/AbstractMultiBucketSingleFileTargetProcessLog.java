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

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IIdleStateData;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.AbstractMultiBucketSingleFileGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.BucketKey;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.DeletedMultiBucketOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.DiscardedMultiBucketOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.IMultiBucketSingleFileReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.ISingleBucketReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.MultiBucketSingleFileHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.MultipleBucketOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.AbstractSingleFileTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.ProcessLogConfig;
import com.gigaspaces.internal.cluster.node.impl.processlog.ProcessLogState;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ShortObjectIterator;
import com.gigaspaces.internal.collections.ShortObjectMap;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.exception.ClosedResourceException;
import com.j_spaces.kernel.threadpool.DynamicExecutors;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractMultiBucketSingleFileTargetProcessLog extends AbstractSingleFileTargetProcessLog
        implements IMultiBucketSingleFileProcessLog {
    private final Object _lifeCycleLock = new Object();
    private final ProcessLogBucket[] _buckets;
    private final int _batchProcessingThreshold;
    private final int _batchParallelProcessingThreshold;
    private final long _consumeTimeout;

    private final MultiBucketSingleFileProcessLogConfig _typedConfig;
    // Not volatile, should be called under memory barrier when the keys updated
    // value is mandatory
    private final long[] _lastProcessedKeys;
    private final long[] _lastGlobalProcessedKeys;

    private final ExecutorService _executorService;
    private final int _parallelFactor;
    private final boolean _useCallerThreadAsParallelExecutor = true;

    private volatile boolean _firstHandshakeForTarget;
    private ProcessLogState _state = ProcessLogState.OPEN;

    public AbstractMultiBucketSingleFileTargetProcessLog(
            ProcessLogConfig config,
            IReplicationPacketDataConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName, IReplicationGroupHistory groupHistory) {
        this(config,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                new long[((MultiBucketSingleFileProcessLogConfig) config).getBucketsCount()],
                new long[((MultiBucketSingleFileProcessLogConfig) config).getBucketsCount()],
                true, groupHistory);
    }

    protected AbstractMultiBucketSingleFileTargetProcessLog(
            ProcessLogConfig config,
            IReplicationPacketDataConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName, long[] lastProcessedKeys,
            long[] lastGlobalProcessedKeys, boolean firstHandshakeForTarget, IReplicationGroupHistory groupHistory) {
        super(dataConsumer, exceptionHandler, replicationInFacade, name, groupName, sourceLookupName, groupHistory);
        _firstHandshakeForTarget = firstHandshakeForTarget;
        _typedConfig = (MultiBucketSingleFileProcessLogConfig) config;
        _consumeTimeout = _typedConfig.getConsumeTimeout();
        _buckets = createBuckets(_typedConfig.getBucketsCount());
        _lastProcessedKeys = lastProcessedKeys;
        _lastGlobalProcessedKeys = lastGlobalProcessedKeys;
        _batchProcessingThreshold = _typedConfig.getBatchProcessingThreshold();
        _batchParallelProcessingThreshold = _typedConfig.getBatchParallelProcessingThreshold();
        _parallelFactor = _typedConfig.getBatchParallelFactor();
        _executorService = DynamicExecutors.newScalingThreadPool(1,
                _parallelFactor,
                10000);
    }

    private ProcessLogBucket[] createBuckets(short numberOfBuckets) {
        ProcessLogBucket[] buckets = new ProcessLogBucket[numberOfBuckets];
        for (short i = 0; i < numberOfBuckets; ++i)
            buckets[i] = new ProcessLogBucket(i, this);
        return buckets;
    }

    public ExecutorService getExecutorService() {
        return _executorService;
    }

    public long[] getLastProcessedKeys() {
        return _lastProcessedKeys;
    }

    public long[] getLastGlobalProcessedKeys() {
        return _lastGlobalProcessedKeys;
    }

    public boolean isFirstHandshakeForTarget() {
        return _firstHandshakeForTarget;
    }

    public MultiBucketSingleFileHandshakeResponse performHandshake(
            String memberName, IBacklogHandshakeRequest handshakeRequest) throws IncomingReplicationOutOfSyncException {
        synchronized (_lifeCycleLock) {
            validateOpen();
            MultiBucketSingleFileHandshakeRequest typedHandshakeRequest = (MultiBucketSingleFileHandshakeRequest) handshakeRequest;
            // Handle first ever received handshake (target first connection,
            // can be a restart of target as well)
            if (_firstHandshakeForTarget) {
                _firstHandshakeForTarget = false;
                return overrideBucketsConfirmedKeys(typedHandshakeRequest);
            }
            // Handle first receive handshake from this source
            else if (typedHandshakeRequest.isFirstHandshake() && canResetState()) {
                // In this case we always override local key since from the
                // source
                // perspective it is the first
                // connection (source is new)
                return overrideBucketsConfirmedKeys(typedHandshakeRequest);
            } else {
                // This is a handshake probably due to disconnection of this
                // channel,
                // but the source is still the same source
                long[] handshakeBucketsConfirmedKeys = typedHandshakeRequest.getBucketsConfirmedKeys();
                long globalConfirmedKey = typedHandshakeRequest.getGlobalConfirmedKey();
                long maxGlobalLastProcessedKey = Long.MIN_VALUE;
                long[] bucketsLastProcessedkeys = new long[_buckets.length];
                for (int i = 0; i < handshakeBucketsConfirmedKeys.length; i++) {
                    long lastProcessedKey = _lastProcessedKeys[i];
                    if (lastProcessedKey < handshakeBucketsConfirmedKeys[i]) {
                        // Here we have replication sync error, the source
                        // thinks it
                        // is on a
                        // newer key then target
                        // Should not reach here!!!
                        throw new IncomingReplicationOutOfSyncException("Replication out of sync, buckets keys mismatch at index "
                                + i
                                + " confirmed="
                                + handshakeBucketsConfirmedKeys[i]
                                + " processed=" + lastProcessedKey);
                    }
                    maxGlobalLastProcessedKey = Math.max(maxGlobalLastProcessedKey,
                            _lastGlobalProcessedKeys[i]);

                    bucketsLastProcessedkeys[i] = lastProcessedKey;
                }
                if (maxGlobalLastProcessedKey < globalConfirmedKey) {
                    // Here we have replication sync error, the source
                    // thinks it
                    // is on a
                    // newer key then target
                    // Should not reach here!!!
                    throw new IncomingReplicationOutOfSyncException("Replication out of sync, global key mismatch, local max last processed key="
                            + maxGlobalLastProcessedKey
                            + " global confirmed=" + globalConfirmedKey);
                }
                //We must send back the global confirmed key of the source and filter duplicate at the target
                //since we cannot know the actual last global key processed by the target (we do not keep that)
                return new MultiBucketSingleFileHandshakeResponse(bucketsLastProcessedkeys,
                        globalConfirmedKey);
            }
        }
    }

    protected boolean canResetState() {
        return true;
    }

    public void validateOpen() {
        if (_state != ProcessLogState.OPEN)
            throwClosedException();
    }

    public void validateNotClosed() {
        if (_state == ProcessLogState.CLOSED)
            throwClosedException();
    }

    protected void throwClosedException() {
        throw new ClosedResourceException("Process log is closed");
    }

    private MultiBucketSingleFileHandshakeResponse overrideBucketsConfirmedKeys(
            MultiBucketSingleFileHandshakeRequest typedHandshakeRequest) {
        // Set confirmed keys in each bucket
        long[] handshakeBucketsConfirmedKeys = typedHandshakeRequest.getBucketsConfirmedKeys();
        long handshakeGlobalKeyConfirmed = typedHandshakeRequest.getGlobalConfirmedKey();
        for (int i = 0; i < handshakeBucketsConfirmedKeys.length; i++) {
            _lastProcessedKeys[i] = handshakeBucketsConfirmedKeys[i];
            _lastGlobalProcessedKeys[i] = handshakeGlobalKeyConfirmed;
        }
        return new MultiBucketSingleFileHandshakeResponse(handshakeBucketsConfirmedKeys,
                handshakeGlobalKeyConfirmed);
    }

    public MultiBucketSingleFileProcessResult processBatch(
            String sourceLookupName, List<IReplicationOrderedPacket> packets,
            IReplicationInFilterCallback inFilterCallback) {
        validateOpen();

        if (packets.isEmpty())
            return MultiBucketSingleFileProcessResult.OK;
        // If we are under threshold, iterate over packets and process each one
        int size = packets.size();
        MultiBucketSingleFileProcessResult result;
        if (size < _batchProcessingThreshold) {
            if (size < _batchParallelProcessingThreshold) {
                result = processIteratively(sourceLookupName,
                        packets,
                        inFilterCallback,
                        null,
                        -1);
            } else {
                result = processParallelIteratively(sourceLookupName,
                        packets,
                        inFilterCallback);
            }
        } else
            result = processInBatches(sourceLookupName,
                    packets,
                    inFilterCallback);

        // In case of closed resource exception, we need to throw it
        replaceWithExceptionIfNeeded(result);
        return result;
    }

    private MultiBucketSingleFileProcessResult processParallelIteratively(
            final String sourceLookupName,
            List<IReplicationOrderedPacket> packets,
            final IReplicationInFilterCallback inFilterCallback) {
        // Split the list into parallel lists
        List<IReplicationOrderedPacket>[] parallelLists = split(packets);
        int parallelParticipants = _useCallerThreadAsParallelExecutor ? parallelLists.length - 1
                : parallelLists.length;
        final ParallelBatchProcessingContext context = new ParallelBatchProcessingContext(parallelLists,
                parallelParticipants);
        int i = _useCallerThreadAsParallelExecutor ? 1 : 0;
        for (; i < parallelLists.length; ++i) {
            createBatchParallelProcessingContinuationTask(sourceLookupName,
                    inFilterCallback,
                    context,
                    parallelLists[i],
                    i);
        }

        // execute one in the current thread
        // and the remaining in seperate threads.
        if (_useCallerThreadAsParallelExecutor) {
            MultiBucketSingleFileProcessResult processResult = processIteratively(sourceLookupName,
                    parallelLists[0],
                    inFilterCallback,
                    null,
                    -1);
            if (!processResult.isProcessed())
                return processResult;
        }
        try {
            context.waitForCompletion(_consumeTimeout, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            // We do not release pending here since each packet process
            // invocation will release pending if it encountered an error
            return new MultiBucketSingleFileProcessResult(e);
        }
        return MultiBucketSingleFileProcessResult.OK;
    }

    private List<IReplicationOrderedPacket>[] split(
            List<IReplicationOrderedPacket> packets) {
        int packetsSize = packets.size();
        int segmentsCount = Math.min(_parallelFactor, packetsSize);
        int segmentSize = packetsSize / segmentsCount;
        List<IReplicationOrderedPacket>[] split = new List[segmentsCount];
        int index = 0;
        int nextSegmentIndex = 1;
        LinkedList<IReplicationOrderedPacket> currentSegment = new LinkedList<IReplicationOrderedPacket>();
        split[0] = currentSegment;
        for (IReplicationOrderedPacket packet : packets) {
            if (nextSegmentIndex < _parallelFactor && index++ >= segmentSize) {
                index = 1;
                currentSegment = new LinkedList<IReplicationOrderedPacket>();
                split[nextSegmentIndex++] = currentSegment;
            }
            currentSegment.add(packet);
        }
        return split;
    }

    public void createBatchParallelProcessingContinuationTask(
            final String sourceLookupName,
            final IReplicationInFilterCallback inFilterCallback,
            final ParallelBatchProcessingContext context,
            final List<IReplicationOrderedPacket> batch, final int segmentIndex) {
        try {
            _executorService.submit(new Runnable() {
                public void run() {
                    try {
                        validateOpen();

                        MultiBucketSingleFileProcessResult result = processIteratively(sourceLookupName,
                                batch,
                                inFilterCallback,
                                context,
                                segmentIndex);
                        // This packet is pending, do nothing, a different task will
                        // consume it
                        if (result.isPending())
                            return;
                        // If processed, signal the context
                        if (result.isProcessed())
                            context.signalOneProcessedOk();
                        else
                            context.setError(result.getError());
                    } catch (Throwable t) {
                        context.setError(t);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            try {
                synchronized (_lifeCycleLock) {
                    //Validate that the rejection not because the pool is being or already closed
                    validateOpen();
                }
            } catch (ClosedResourceException cre) {
                context.setError(cre);
                throw cre;
            }
            context.setError(e);
        }
    }

    private MultiBucketSingleFileProcessResult processInBatches(
            String sourceLookupName, List<IReplicationOrderedPacket> packets,
            IReplicationInFilterCallback inFilterCallback) {
        try {
            // Build mapping each bucket to which packets belong to it.
            ShortObjectMap<List<IMultiBucketSingleFileReplicationOrderedPacket>> bucketsMappedPackets = CollectionsFactory.getInstance().createShortObjectMap();
            for (IReplicationOrderedPacket packet : packets) {
                IMultiBucketSingleFileReplicationOrderedPacket typedPacket = (IMultiBucketSingleFileReplicationOrderedPacket) packet;
                typedPacket.associateToBuckets(bucketsMappedPackets);
            }
            // Add each batch to its corresponding bucket

            for (ShortObjectIterator<List<IMultiBucketSingleFileReplicationOrderedPacket>> iterator = bucketsMappedPackets.iterator(); iterator.hasNext(); ) {
                iterator.advance();
                final short bucketIndex = iterator.key();
                final List<IMultiBucketSingleFileReplicationOrderedPacket> bucketPackets = iterator.value();
                _buckets[bucketIndex].add(bucketPackets);
            }
            // While we still have packet to process
            while (!bucketsMappedPackets.isEmpty()) {
                for (ShortObjectIterator<List<IMultiBucketSingleFileReplicationOrderedPacket>> iterator = bucketsMappedPackets.iterator(); iterator.hasNext(); ) {
                    iterator.advance();
                    final short bucketIndex = iterator.key();
                    final List<IMultiBucketSingleFileReplicationOrderedPacket> bucketPackets = iterator.value();
                    Throwable error = _buckets[bucketIndex].process(sourceLookupName,
                            bucketPackets,
                            inFilterCallback);

                    // We received an error from other packet processing cycle
                    // that
                    // woke us up, return error result
                    if (error != null)
                        return new MultiBucketSingleFileProcessResult(error);

                    if (bucketPackets.isEmpty())
                        iterator.remove();
                }
            }
            return MultiBucketSingleFileProcessResult.OK;
        } catch (Throwable t) {
            releaseOnErrorResult(t);
            return new MultiBucketSingleFileProcessResult(t);
        }
    }

    private MultiBucketSingleFileProcessResult processIteratively(
            String sourceLookupName, List<IReplicationOrderedPacket> packets,
            IReplicationInFilterCallback inFilterCallback,
            ParallelBatchProcessingContext batchContext, int segmentIndex) {
        MultiBucketSingleFileProcessResult result = MultiBucketSingleFileProcessResult.OK;
        for (Iterator<IReplicationOrderedPacket> iterator = packets.iterator(); iterator.hasNext(); ) {
            IReplicationOrderedPacket packet = iterator.next();
            result = process(sourceLookupName,
                    packet,
                    inFilterCallback,
                    batchContext,
                    segmentIndex);
            if (result.isProcessed()) {
                if (batchContext != null)
                    iterator.remove();
                continue;
            }
            // error or pending
            return result;
        }
        return result;
    }

    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      IReplicationOrderedPacket packet,
                                                      IReplicationInFilterCallback inFilterCallback,
                                                      ParallelBatchProcessingContext batchContext, int segmentIndex) {
        IMultiBucketSingleFileReplicationOrderedPacket typedPacket = (IMultiBucketSingleFileReplicationOrderedPacket) packet;
        MultiBucketSingleFileProcessResult result = typedPacket.process(sourceLookupName,
                this,
                inFilterCallback,
                batchContext,
                segmentIndex);

        return result;
    }

    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      IReplicationOrderedPacket packet,
                                                      IReplicationInFilterCallback inFilterCallback) {
        MultiBucketSingleFileProcessResult processResult = process(sourceLookupName,
                packet,
                inFilterCallback,
                null,
                -1);
        replaceWithExceptionIfNeeded(processResult);
        return processResult;
    }

    private void replaceWithExceptionIfNeeded(
            MultiBucketSingleFileProcessResult processResult) {
        if (processResult.isProcessed())
            return;

        if (processResult.getError() instanceof ClosedResourceException)
            throw (ClosedResourceException) processResult.getError();
    }

    private void releaseOnErrorResult(Throwable error) {
        for (ProcessLogBucket bucket : _buckets) {
            bucket.releaseOnErrorResult(error);
        }
    }

    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      ISingleBucketReplicationOrderedPacket packet,
                                                      IReplicationInFilterCallback inFilterCallback,
                                                      ParallelBatchProcessingContext batchContext, int segmentIndex) {
        final short buckIndex = packet.getBucketIndex();
        try {
            MultiBucketSingleFileProcessResult result = _buckets[buckIndex].addAndProcess(sourceLookupName,
                    packet,
                    inFilterCallback,
                    batchContext,
                    segmentIndex);

            // We received an error from other packet processing cycle that
            // woke us up, return error result
            if (result.getError() != null)
                return new MultiBucketSingleFileProcessResult(result.getError());

            return result;
        } catch (Throwable e) {
            releaseOnErrorResult(e);
            return new MultiBucketSingleFileProcessResult(e);
        }

    }

    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      MultipleBucketOrderedPacket packet,
                                                      IReplicationInFilterCallback inFilterCallback,
                                                      ParallelBatchProcessingContext batchContext, int segmentIndex) {
        ShortObjectMap<BucketKey> bucketsKeys = packet.getBucketsKeys();
        try {

            for (ShortObjectIterator<BucketKey> iterator = bucketsKeys.iterator(); iterator.hasNext(); ) {
                iterator.advance();
                MultiBucketSingleFileProcessResult result = _buckets[iterator.key()].addAndProcess(sourceLookupName,
                        packet,
                        inFilterCallback,
                        batchContext,
                        segmentIndex);
                // We received an error from other packet processing cycle that
                // woke us up, return error result
                if (result.getError() != null)
                    return result;

                if (result.isPending())
                    return result;
            }
            // Done processing, release pending buckets
            for (ShortObjectIterator<BucketKey> iterator = bucketsKeys.iterator(); iterator.hasNext(); ) {
                iterator.advance();
                _buckets[iterator.key()].signalDoneProcessing(sourceLookupName,
                        packet,
                        inFilterCallback);
            }
            return MultiBucketSingleFileProcessResult.OK;
        } catch (Throwable e) {
            releaseOnErrorResult(e);
            return new MultiBucketSingleFileProcessResult(e);
        }
    }

    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      DiscardedMultiBucketOrderedPacket packet,
                                                      IReplicationInFilterCallback inFilterCallback,
                                                      ParallelBatchProcessingContext batchContext, int segmentIndex) {
        ShortObjectMap<BucketKey> bucketsKeys = packet.getBucketsKeys();
        try {
            for (ShortObjectIterator<BucketKey> iterator = bucketsKeys.iterator(); iterator.hasNext(); ) {
                iterator.advance();
                MultiBucketSingleFileProcessResult result = _buckets[iterator.key()].addAndProcess(sourceLookupName,
                        packet,
                        inFilterCallback,
                        batchContext,
                        segmentIndex);
                // We received an error from other packet processing cycle that
                // woke us up, return error result
                if (result.getError() != null)
                    return result;
            }
            return MultiBucketSingleFileProcessResult.OK;
        } catch (Throwable e) {
            releaseOnErrorResult(e);
            return new MultiBucketSingleFileProcessResult(e);
        }
    }

    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      DeletedMultiBucketOrderedPacket packet,
                                                      IReplicationInFilterCallback inFilterCallback,
                                                      ParallelBatchProcessingContext batchContext, int segmentIndex) {
        ShortObjectMap<BucketKey> bucketsKeys = packet.getBucketsKeys();
        try {
            for (ShortObjectIterator<BucketKey> iterator = bucketsKeys.iterator(); iterator.hasNext(); ) {
                iterator.advance();
                MultiBucketSingleFileProcessResult result = _buckets[iterator.key()].addAndProcess(sourceLookupName,
                        packet,
                        inFilterCallback,
                        batchContext,
                        segmentIndex);
                // We received an error from other packet processing cycle that
                // woke us up, return error result
                if (result.getError() != null)
                    return result;
            }
            return MultiBucketSingleFileProcessResult.OK;
        } catch (Throwable e) {
            releaseOnErrorResult(e);
            return new MultiBucketSingleFileProcessResult(e);
        }
    }

    public boolean close(long time, TimeUnit unit) throws InterruptedException {
        // Switch to closing state
        synchronized (_lifeCycleLock) {
            _state = ProcessLogState.CLOSING;
        }
        try {
            long remainingTime = unit.toMillis(time);
            for (ProcessLogBucket bucket : _buckets) {
                long startTime = SystemTime.timeMillis();
                bucket.close(remainingTime, TimeUnit.MILLISECONDS);
                long endTime = SystemTime.timeMillis();

                remainingTime -= (endTime - startTime);
                if (remainingTime <= 0)
                    return false;
            }
            return true;
        } finally {
            synchronized (_lifeCycleLock) {
                _executorService.shutdown();
                _state = ProcessLogState.CLOSED;
            }
        }
    }

    public long getConsumeTimeout() {
        return _consumeTimeout;
    }

    public void processHandshakeIteration(String sourceMemberName,
                                          IHandshakeIteration handshakeIteration) {
        throw new UnsupportedOperationException();
    }

    protected boolean shouldCloneOnFilter() {
        // Default is false since this packet is not used outside of this scope
        return false;
    }

    protected void afterSuccessfulConsumption(String sourceLookupName,
                                              IReplicationOrderedPacket packet) {
        if (!packet.isDataPacket()) {
            if (packet instanceof DeletedMultiBucketOrderedPacket) {
                DeletedMultiBucketOrderedPacket deletedBacklogPacket = (DeletedMultiBucketOrderedPacket) packet;
                String deletionMessage = "packets [" + deletedBacklogPacket.getKey() + "-" + deletedBacklogPacket.getEndKey() + "] are lost due to backlog deletion at the source";
                getGroupHistory().logEvent(getSourceLookupName(), deletionMessage);
                if (_specificLogger.isWarnEnabled())
                    _specificLogger.warn(deletionMessage);
            }
        }
    }

    public IProcessLogHandshakeResponse resync(IBacklogHandshakeRequest handshakeRequest) {
        synchronized (_lifeCycleLock) {
            validateOpen();
            MultiBucketSingleFileHandshakeRequest typedHandshakeRequest = (MultiBucketSingleFileHandshakeRequest) handshakeRequest;
            _firstHandshakeForTarget = false;
            return overrideBucketsConfirmedKeys(typedHandshakeRequest);
        }
    }

    public Object toWireForm(IProcessResult processResult) {
        //Optimization, reduce the cost of memory garbage by not creating an ok process result at the source
        //from serialization
        if (processResult == MultiBucketSingleFileProcessResult.OK)
            return null;

        return processResult;
    }

    public String dumpState() {
        return "State [" + _state + "] had any handshake ["
                + !_firstHandshakeForTarget + "] last processed bucket keys "
                + AbstractMultiBucketSingleFileGroupBacklog.printBucketsKeys(_lastProcessedKeys) + StringUtils.NEW_LINE
                + dumpStateExtra();
    }

    protected String dumpStateExtra() {
        return "last processed global key " + AbstractMultiBucketSingleFileGroupBacklog.printBucketsKeys(_lastGlobalProcessedKeys) +
                StringUtils.NEW_LINE + "buckets state " + printBucketsFirstPendingPacket();
    }

    private String printBucketsFirstPendingPacket() {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < _buckets.length; i++) {
            sb.append(i);
            sb.append(": ");
            sb.append(_buckets[i].printPendingState());
            if (i < _buckets.length - 1)
                sb.append(", ");
        }
        sb.append("]");
        return sb.toString();
    }

    public Logger getSpecificLogger() {
        return _specificLogger;
    }

    @Override
    public IProcessResult processIdleStateData(String string, IIdleStateData idleStateData,
                                               IReplicationInFilterCallback inFilterCallback) {
        throw new UnsupportedOperationException();
    }
}
