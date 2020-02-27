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
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.DeletedMultiBucketOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.DiscardedMultiBucketOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.DiscardedSingleBucketOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.IMultiBucketSingleFileReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.MultiBucketSingleFileHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataBatchConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.ProcessLogConfig;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationBatchConsumeAsyncTargetProcessLog;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ShortLongIterator;
import com.gigaspaces.internal.collections.ShortLongMap;
import com.j_spaces.core.exception.ClosedResourceException;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;


@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileBatchConsumeTargetProcessLog
        extends AbstractMultiBucketSingleFileTargetProcessLog
        implements IBatchExecutedCallback,
        IReplicationBatchConsumeAsyncTargetProcessLog {

    protected final Lock _lock = new ReentrantLock();
    private final IReplicationPacketDataBatchConsumer<?> _batchDataConsumer;
    private long _lastGlobalProcessedKey;

    public MultiBucketSingleFileBatchConsumeTargetProcessLog(
            ProcessLogConfig config,
            IReplicationPacketDataBatchConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName, IReplicationGroupHistory groupHistory) {
        super(config,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName, groupHistory);
        _lastGlobalProcessedKey = -1;
        _batchDataConsumer = dataConsumer;
    }

    protected MultiBucketSingleFileBatchConsumeTargetProcessLog(
            ProcessLogConfig config,
            IReplicationPacketDataBatchConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName, long lastGlobalProcessedKey,
            long[] lastProcessedKeys, long[] lastGlobalProcessedKeys,
            boolean firstHandshakeForTarget,
            IReplicationGroupHistory groupHistory) {
        super(config,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                lastProcessedKeys,
                lastGlobalProcessedKeys,
                firstHandshakeForTarget, groupHistory);
        _lastGlobalProcessedKey = lastGlobalProcessedKey;
        _batchDataConsumer = dataConsumer;
    }

    @Override
    protected boolean contentRequiredWhileProcessing() {
        return false;
    }

    @Override
    public MultiBucketSingleFileHandshakeResponse performHandshake(
            String memberName, IBacklogHandshakeRequest handshakeRequest) throws IncomingReplicationOutOfSyncException {
        _lock.lock();
        try {
            MultiBucketSingleFileHandshakeResponse response = super.performHandshake(memberName,
                    handshakeRequest);

            MultiBucketSingleFileHandshakeRequest typedHandshakeRequest = (MultiBucketSingleFileHandshakeRequest) handshakeRequest;
            if (typedHandshakeRequest.isFirstHandshake())
                _lastGlobalProcessedKey = response.getLastProcessedGlobalKey();
            else
                _lastGlobalProcessedKey = Math.max(response.getLastProcessedGlobalKey(), _lastGlobalProcessedKey);
            return response;
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public MultiBucketSingleFileProcessResult processBatch(
            String sourceLookupName, List<IReplicationOrderedPacket> packets,
            IReplicationInFilterCallback inFilterCallback) {
        validateOpen();

        _lock.lock();
        try {
            // If closed, reject immediately
            validateNotClosed();

            filterDuplicate(packets);
            // If all were filtered, return ok process result
            if (packets.isEmpty())
                return MultiBucketSingleFileProcessResult.OK;

            // Verify keys matching
            final long firstKeyInBatch = packets.get(0).getKey();
            if (_lastGlobalProcessedKey + 1 < firstKeyInBatch)
                return new MultiBucketSingleFileProcessResult(new ReplicationInternalSpaceException("Incompatible keys, last processed is "
                        + _lastGlobalProcessedKey
                        + " while first packets received is " + firstKeyInBatch));

            try {
                return processPackets(sourceLookupName,
                        packets,
                        inFilterCallback);
            } catch (ClosedResourceException e) {
                throw e;
            } catch (Throwable t) {
                if (_specificLogger.isLoggable(Level.FINER))
                    _specificLogger.log(Level.FINER, "error while processing incoming replication", t);

                // Exception thrown from exception handler meaning it can not
                // handle this exception
                // We must return a failed result to all pending threads
                return new MultiBucketSingleFileProcessResult(t);
            }
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      IReplicationOrderedPacket packet,
                                                      IReplicationInFilterCallback inFilterCallback) {
        throw new UnsupportedOperationException();
    }

    private MultiBucketSingleFileProcessResult processPackets(
            String sourceLookupName, List<IReplicationOrderedPacket> packets,
            IReplicationInFilterCallback inFilterCallback) throws Exception {
        MultiBucketSingleFileReplicationInBatchContext context = new MultiBucketSingleFileReplicationInBatchContext(this,
                sourceLookupName,
                getGroupName(),
                _specificLogger,
                packets.size());

        // Get the last data packet index in the batch (non discarded/deleted
        // packets)
        int lastDataPacketIndex = locateLastDataPacketIndex(packets);
        int packetIndex = 0;
        for (IReplicationOrderedPacket packet : packets) {
            // If all packets from here and onwards are not data, break loop
            if (packetIndex++ > lastDataPacketIndex)
                break;
            // We are not using for loop since the list is linked list and not
            // array list,
            // iterating over it with for loop is costly

            // Set the current key in the context in order for the context
            // to keep track of the unprocessed packet in a batch
            context.setCurrentPacket((IMultiBucketSingleFileReplicationOrderedPacket) packet);
            if (preprocess(packet)) {
                context.setContextPacket(packet);
                try {
                    IReplicationPacketData<?> data = packet.getData();
                    IDataConsumeResult prevResult = null;
                    do {
                        // If closed, reject immediately
                        validateNotClosed();

                        // Save the current context state in case we retry the operation due to an exception after applying a fix
                        context.snapshot();
                        IDataConsumeResult consumeResult = getDataConsumer().consume(context,
                                data,
                                getReplicationInFacade(),
                                inFilterCallback);
                        if (!consumeResult.isFailed())
                            break;
                        throwIfRepetitiveError(prevResult, consumeResult);
                        if (_specificLogger.isLoggable(Level.FINER))
                            _specificLogger.log(Level.FINER,
                                    "Encountered error while consuming packet ["
                                            + packet
                                            + "], trying to resolve issue",
                                    consumeResult.toException());
                        IDataConsumeFix fix = getExceptionHandler().handleException(consumeResult, null/*packet, this is batch consumption we dont know the origin packet that the data causes this error*/);
                        data = getDataConsumer().applyFix(context, data, fix);
                        if (_specificLogger.isLoggable(Level.FINER))
                            _specificLogger.log(Level.FINER, "Fix applied - retrying the operation [" + fix + "]");

                        // Rollback to the previous context snapshot state
                        context.rollback();

                        prevResult = consumeResult;
                    } while (true);
                } finally {
                    context.setContextPacket(null);
                }
            } else {
                context.updatePendingProcessedKeys();
            }
        }

        // Consume pending packets in the batch context if any exist
        consumePendingPacketsInBatch(context);

        // We have a batch of discarded packets at the end, we need to increase
        // the confirmed key
        int batchSize = packets.size();
        if (batchSize > 0 && lastDataPacketIndex < batchSize - 1) {
            updateProcessedOfRemainingPackets(lastDataPacketIndex, packets);
        }

        return MultiBucketSingleFileProcessResult.OK;
    }

    private void consumePendingPacketsInBatch(
            MultiBucketSingleFileReplicationInBatchContext context) throws Exception {
        // If closed, reject immediately
        validateNotClosed();

        IDataConsumeResult result = _batchDataConsumer.consumePendingPackets(context, getReplicationInFacade());
        if (result.isFailed())
            throw result.toException();
    }

    private int locateLastDataPacketIndex(
            List<IReplicationOrderedPacket> packets) {
        int lastDataPacketIndex = -1;
        int index = -1;
        for (IReplicationOrderedPacket packet : packets) {
            index++;
            if (packet.isDataPacket())
                lastDataPacketIndex = index;
        }
        return lastDataPacketIndex;
    }

    private boolean preprocess(IReplicationOrderedPacket packet) {
        if (packet.isDataPacket())
            return true;
        // In case of deleted/discarded do nothing
        if (packet instanceof DiscardedMultiBucketOrderedPacket)
            return false;
        if (packet instanceof DiscardedSingleBucketOrderedPacket)
            return false;
        if (packet instanceof DeletedMultiBucketOrderedPacket)
            return false;

        return true;
    }

    public void batchConsumed(ShortLongMap bucketProcessedKeys,
                              ShortLongMap bucketGlobalProcessedKeys,
                              long lastGlobalprocessedKey) {
        _lastGlobalProcessedKey = lastGlobalprocessedKey;

        for (ShortLongIterator it = bucketProcessedKeys.iterator(); it.hasNext(); ) {
            it.advance();
            short bucketIndex = it.key();
            getLastProcessedKeys()[bucketIndex] = it.value();
            getLastGlobalProcessedKeys()[bucketIndex] = bucketGlobalProcessedKeys.get(bucketIndex);
        }
    }

    private void updateProcessedOfRemainingPackets(int lastDataPacketIndex,
                                                   List<IReplicationOrderedPacket> packets) {
        ShortLongMap bucketProcessedKeys = CollectionsFactory.getInstance().createShortLongMap();
        ShortLongMap bucketGlobalProcessedKeys = CollectionsFactory.getInstance().createShortLongMap();
        long lastGlobalprocessedKey = _lastGlobalProcessedKey;
        int index = 0;
        for (IReplicationOrderedPacket packet : packets) {
            if (index++ < lastDataPacketIndex)
                continue;
            IMultiBucketSingleFileReplicationOrderedPacket typedPacket = (IMultiBucketSingleFileReplicationOrderedPacket) packet;
            for (short bucketIndex : typedPacket.getBuckets()) {
                bucketProcessedKeys.put(bucketIndex, typedPacket.getBucketKey(bucketIndex));
                bucketGlobalProcessedKeys.put(bucketIndex, packet.getKey());
            }
            lastGlobalprocessedKey = typedPacket.getKey();
        }

        batchConsumed(bucketProcessedKeys, bucketGlobalProcessedKeys, lastGlobalprocessedKey);

    }

    protected void filterDuplicate(List<IReplicationOrderedPacket> packets) {
        // If source is in async mode (due to disconnection, recovery or temp
        // error at target, i.e out of mem)
        // duplicate packets may arrive at the intermediate stage of the source
        // moving from async to sync
        for (Iterator<IReplicationOrderedPacket> iterator = packets.iterator(); iterator.hasNext(); ) {
            IReplicationOrderedPacket packet = iterator.next();
            if (filterDuplicate(packet))
                iterator.remove();
            else
                break;
        }
    }

    protected boolean filterDuplicate(IReplicationOrderedPacket packet) {
        // If source is in async mode (due to disconnection, recovery or temp
        // error at target, i.e out of mem)
        // duplicate packets may arrive at the intermediate stage of the source
        // moving from async to sync
        return packet.getKey() <= _lastGlobalProcessedKey;
    }

    public long getLastGlobalProcessedKey() {
        return _lastGlobalProcessedKey;
    }

    @Override
    protected String dumpStateExtra() {
        return "last global processed key " + _lastGlobalProcessedKey;
    }

}