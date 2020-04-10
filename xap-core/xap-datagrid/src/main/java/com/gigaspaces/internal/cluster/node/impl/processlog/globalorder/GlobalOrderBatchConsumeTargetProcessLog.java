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

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDeletedBacklogPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataBatchConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationBatchConsumeAsyncTargetProcessLog;
import com.j_spaces.core.exception.ClosedResourceException;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;

import java.util.List;



@com.gigaspaces.api.InternalApi
public class GlobalOrderBatchConsumeTargetProcessLog
        extends AbstractGlobalOrderTargetProcessLog
        implements IReplicationBatchConsumeAsyncTargetProcessLog,
        IBatchExecutedCallback {

    private final IReplicationPacketDataBatchConsumer<?> _batchDataConsumer;

    protected GlobalOrderBatchConsumeTargetProcessLog(
            IReplicationPacketDataBatchConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName, long lastProcessedKey,
            boolean firstHandshakeForTarget, IReplicationGroupHistory groupHistory) {
        super(dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                lastProcessedKey,
                firstHandshakeForTarget,
                groupHistory);
        _batchDataConsumer = dataConsumer;
    }

    public GlobalOrderBatchConsumeTargetProcessLog(
            IReplicationPacketDataBatchConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName, IReplicationGroupHistory groupHistory) {
        this(dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                -1,
                true,
                groupHistory);
    }

    @Override
    protected boolean contentRequiredWhileProcessing() {
        return false;
    }

    @Override
    protected void onClose() {
    }

    @Override
    public GlobalOrderProcessResult processBatch(String sourceLookupName,
                                                 List<IReplicationOrderedPacket> packets,
                                                 IReplicationInFilterCallback inFilterCallback) {
        if (!isOpen())
            throwClosedException();

        _lock.lock();
        try {
            // If closed, reject immediately
            if (isClosed())
                throwClosedException();

            filterDuplicate(packets);
            // If all were filtered, return ok process result
            if (packets.isEmpty())
                return GlobalOrderProcessResult.OK;

            // Verify keys matching
            final long firstKeyInBatch = packets.get(0).getKey();
            if (_lastProcessedKey + 1 < firstKeyInBatch)
                return new GlobalOrderProcessResult(new ReplicationInternalSpaceException("Incompatible keys, last processed is "
                        + _lastProcessedKey
                        + " while first packets received is "
                        + firstKeyInBatch),
                        _lastProcessedKey);

            try {
                return processPackets(sourceLookupName,
                        packets,
                        inFilterCallback);
            } catch (ClosedResourceException e) {
                throw e;
            } catch (Throwable t) {
                if (_specificLogger.isDebugEnabled())
                    _specificLogger.debug("error while processing incoming replication", t);

                // Exception thrown from exception handler meaning it can not
                // handle this exception
                // We must return a failed result to all pending threads
                return new GlobalOrderProcessResult(t, _lastProcessedKey);
            }
        } finally {
            _lock.unlock();
        }
    }

    public IProcessResult process(String sourceLookupName,
                                  IReplicationOrderedPacket packet,
                                  IReplicationInFilterCallback inFilterCallback) {
        throw new UnsupportedOperationException();
    }

    private GlobalOrderProcessResult processPackets(String sourceLookupName,
                                                    List<IReplicationOrderedPacket> packets,
                                                    IReplicationInFilterCallback inFilterCallback) throws Exception {
        final GlobalOrderReplicationInBatchContext context = new GlobalOrderReplicationInBatchContext(this, _specificLogger, packets.size(), sourceLookupName, getGroupName());

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

            if (preprocess(packet)) {
                context.setContextPacket(packet);
                try {
                    IReplicationPacketData<?> data = packet.getData();

                    // Set the current key in the context in order for the context
                    // to keep track of the unprocessed packet in a batch
                    context.setCurrentKey(packet.getKey());

                    IDataConsumeResult prevResult = null;
                    do {
                        // If closed, reject immediately
                        if (isClosed())
                            throw new ClosedResourceException("Process log is closed");

                        // Save the current context state in case we retry the operation due to an exception after applying a fix
                        context.snapshot();
                        IDataConsumeResult consumeResult = getDataConsumer().consume(context,
                                data,
                                getReplicationInFacade(),
                                inFilterCallback);
                        if (!consumeResult.isFailed())
                            break;
                        throwIfRepetitiveError(prevResult, consumeResult);
                        if (_specificLogger.isDebugEnabled())
                            _specificLogger.debug(
                                    "Encountered error while consuming packet ["
                                            + packet
                                            + "], trying to resolve issue",
                                    consumeResult.toException());
                        IDataConsumeFix fix = getExceptionHandler().handleException(consumeResult, null/*packet, this is batch consumption we dont know the origin packet that the data causes this error*/);
                        data = getDataConsumer().applyFix(context, data, fix);
                        if (_specificLogger.isDebugEnabled())
                            _specificLogger.debug("Fix applied - retrying the operation [" + fix + "]");

                        // Rollback to the previous context snapshot state
                        context.rollback();

                        prevResult = consumeResult;
                    } while (true);
                } finally {
                    context.setContextPacket(null);
                }
            }
        }

        // Consume pending packets in the batch context if any exist
        consumePendingPacketsInBatch(context);

        // We have a batch of discarded packets at the end, we need to increase
        // the confirmed key
        int batchSize = packets.size();
        if (batchSize > 0 && lastDataPacketIndex < batchSize - 1) {
            IReplicationOrderedPacket packet = packets.get(batchSize - 1);
            _lastProcessedKey = packet.getEndKey();
            if(!packet.isDiscardedPacket()){
                StringBuilder logMsg = new StringBuilder("last packet in batch, assumed as discarded batch, is not a discarded packet\n");
                logMsg.append("packets : \n");
                for (IReplicationOrderedPacket p : packets) {
                    logMsg.append(p).append("\n");
                }
                _specificLogger.warn(logMsg.toString());
            }
        }

        return GlobalOrderProcessResult.OK;
    }

    private void consumePendingPacketsInBatch(
            GlobalOrderReplicationInBatchContext context) throws Exception {
        // If closed, reject immediately
        if (isClosed())
            throw new ClosedResourceException("Process log is closed");

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
        if (packet instanceof GlobalOrderDiscardedReplicationPacket)
            return false;
        if (packet instanceof GlobalOrderDeletedBacklogPacket) {
            logDeletion((GlobalOrderDeletedBacklogPacket) packet);
            return false;
        }

        return true;
    }

    public void batchConsumed(long lastProcessKeyInBatch) {
        _lastProcessedKey = lastProcessKeyInBatch;
    }

}
