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

package com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile;

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IIdleStateData;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDeletedBacklogPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFix;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeResult;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataBatchConsumer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationParticipantsMetadata;
import com.gigaspaces.internal.cluster.node.impl.processlog.AbstractSingleFileTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.ProcessLogState;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncTargetProcessLog;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.IProcessMemoryManager;
import com.j_spaces.core.MemoryShortageException;
import com.j_spaces.core.exception.ClosedResourceException;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A reliable async replication process log which merges multiple participants operations to a
 * single operation. Packet is marked as "pending" if not all of its data arrived. Pending packets
 * are not executed until all of the participants data is arrived. In the meantime the next
 * operations are executed only if they're not interleaving with any pending packets. In such a
 * case, execution will wait until the interleaving packet is consumed.
 *
 * The different process logs synchronization is done using the {@link
 * IReplicationParticipantsMediator} component implementation.
 *
 * @author idan
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class MultiSourceSingleFileReliableAsyncTargetProcessLog extends AbstractSingleFileTargetProcessLog
        implements IReplicationReliableAsyncTargetProcessLog, IPacketConsumedCallback {
    private final Object _lifeCycleLock = new Object();
    private final Lock _lock = new ReentrantLock();
    private final IReplicationParticipantsMediator _participantsMediator;
    private final MultiSourceSingleFileProcessLogConfig _processLogConfig;
    private final IReplicationPacketDataBatchConsumer<?> _batchDataConsumer;
    private final LinkedList<Long> _batchConsumedPacketsKeys;
    private final HashMap<String, MultiSourceProcessLogPacket> _packetsByUidHelperMap;
    private final IProcessMemoryManager _processMemoryManager;

    /**
     * _lastProcesssedKey is always >= _firstUnprocessedKey - 1
     */
    private long _lastProcessedKey;
    private long _firstUnprocessedKey;
    private ProcessLogState _state = ProcessLogState.OPEN;
    private boolean _firstHandshakeForTarget;
    private LinkedList<MultiSourceProcessLogPacket> _pendingPacketsQueue;

    public MultiSourceSingleFileReliableAsyncTargetProcessLog(
            MultiSourceSingleFileProcessLogConfig processLogConfig,
            IReplicationPacketDataBatchConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName,
            boolean firstHandshakeForTarget,
            IReplicationGroupHistory groupHistory,
            IReplicationParticipantsMediator participantsMediator,
            IProcessMemoryManager processMemoryManager) {
        this(processLogConfig,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                firstHandshakeForTarget,
                groupHistory,
                participantsMediator,
                new LinkedList<MultiSourceProcessLogPacket>(),
                new HashMap<String, MultiSourceProcessLogPacket>(),
                processMemoryManager);
    }

    public MultiSourceSingleFileReliableAsyncTargetProcessLog(
            MultiSourceSingleFileProcessLogConfig processLogConfig,
            IReplicationPacketDataBatchConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade,
            String myLookupName,
            String groupName,
            String sourceLookupName,
            boolean firstHandshakeForTarget,
            IReplicationGroupHistory groupHistory,
            IReplicationParticipantsMediator participantsMediator,
            long lastProcessedKey, long firstUnprocessedKey,
            LinkedList<MultiSourceProcessLogPacket> pendingPacketsQueue,
            HashMap<String, MultiSourceProcessLogPacket> packetsByUidHelperMap,
            IProcessMemoryManager processMemoryManager) {
        this(processLogConfig,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName,
                firstHandshakeForTarget,
                groupHistory,
                participantsMediator,
                pendingPacketsQueue,
                packetsByUidHelperMap,
                processMemoryManager);
        _lastProcessedKey = lastProcessedKey;
        _firstUnprocessedKey = firstUnprocessedKey;
    }

    private MultiSourceSingleFileReliableAsyncTargetProcessLog(
            MultiSourceSingleFileProcessLogConfig processLogConfig,
            IReplicationPacketDataBatchConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName,
            boolean firstHandshakeForTarget,
            IReplicationGroupHistory groupHistory,
            IReplicationParticipantsMediator participantsMediator,
            LinkedList<MultiSourceProcessLogPacket> pendingPacketsQueue,
            HashMap<String, MultiSourceProcessLogPacket> packetsByUidHelperMap,
            IProcessMemoryManager processMemoryManager) {
        super(dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                groupHistory);
        _processLogConfig = processLogConfig;
        _firstHandshakeForTarget = firstHandshakeForTarget;
        _participantsMediator = participantsMediator;
        _pendingPacketsQueue = pendingPacketsQueue;
        _batchDataConsumer = dataConsumer;
        _packetsByUidHelperMap = packetsByUidHelperMap;
        _batchConsumedPacketsKeys = new LinkedList<Long>();
        _processMemoryManager = processMemoryManager;
    }

    @Override
    protected boolean contentRequiredWhileProcessing() {
        return false;
    }

    public GlobalOrderProcessLogHandshakeResponse performHandshake(String memberName,
                                                                   IBacklogHandshakeRequest handshakeRequest)
            throws IncomingReplicationOutOfSyncException {
        synchronized (_lifeCycleLock) {
            if (!isOpen())
                throw new ClosedResourceException("Process log is closed");
            GlobalOrderBacklogHandshakeRequest typedHandshakeRequest = (GlobalOrderBacklogHandshakeRequest) handshakeRequest;
            // Handle first ever received handshake (target first connection,
            // can be
            // a restart of target as well)
            if (_firstHandshakeForTarget || handshakeRequest.isResetTarget()) {
                _firstHandshakeForTarget = false;
                _lastProcessedKey = typedHandshakeRequest.getLastConfirmedKey();
                _firstUnprocessedKey = _lastProcessedKey + 1;
                // TODO WAN: implement MultiSourceSingleFileProcessLogHandshakeResponse
                return new GlobalOrderProcessLogHandshakeResponse(_firstUnprocessedKey - 1);
            }
            // Handle first receive handshake from this source
            else if (handshakeRequest.isFirstHandshake()) {
                // In this case we always override local key since from the
                // source
                // perspective it is the first
                // connection (source is new)
                _lastProcessedKey = typedHandshakeRequest.getLastConfirmedKey();
                _firstUnprocessedKey = _lastProcessedKey + 1;
                _pendingPacketsQueue.clear();
                return new GlobalOrderProcessLogHandshakeResponse(_firstUnprocessedKey - 1);
            }
            // This is a handshake probably due to disconnection of this
            // channel,
            // but the source is still the same source
            else if (typedHandshakeRequest.getLastConfirmedKey() <= _firstUnprocessedKey - 1) {
                return new GlobalOrderProcessLogHandshakeResponse(_firstUnprocessedKey - 1);
            } else {
                // Here we have replication sync error, the source
                // thinks it is on a newer key then target
                throw new IncomingReplicationOutOfSyncException("Replication out of sync, received last confirmed key "
                        + typedHandshakeRequest.getLastConfirmedKey()
                        + " while last processed key is "
                        + _lastProcessedKey
                        + " and first unprocessed key is "
                        + _firstUnprocessedKey);
            }
        }
    }

    public IProcessResult processBatch(String sourceLookupName,
                                       List<IReplicationOrderedPacket> packets,
                                       IReplicationInFilterCallback inFilterCallback) {
        if (!isOpen())
            throwClosedException();

        validateMemoryStatus();

        _lock.lock();
        try {
            // If closed, reject immediately
            if (isClosed())
                throwClosedException();

            filterDuplicate(packets);
            // If all were filtered, return ok process result
            if (packets.isEmpty() && _pendingPacketsQueue.isEmpty())
                return GlobalOrderProcessResult.OK;

            // All packets might be filtered but there are pending packets -> processIdleState
            if (packets.isEmpty())
                return processIdleStateData(sourceLookupName, null, inFilterCallback);

            // Verify keys matching
            final long firstKeyInBatch = packets.get(0).getKey();
            if (_lastProcessedKey + 1 < firstKeyInBatch) {
                if (_pendingPacketsQueue.isEmpty() || _pendingPacketsQueue.getLast().getKey() + 1 < firstKeyInBatch)
                    return new GlobalOrderProcessResult(new ReplicationInternalSpaceException("Incompatible keys, last processed is "
                            + _lastProcessedKey
                            + " and last key in process queue is "
                            + (_pendingPacketsQueue.isEmpty() ? "EMPTY"
                            : _pendingPacketsQueue.getLast().getKey())
                            + " while first packets received is "
                            + firstKeyInBatch),
                            _lastProcessedKey);
            }

            try {
                return processPackets(sourceLookupName,
                        packets,
                        inFilterCallback);
            } catch (ClosedResourceException e) {
                throw e;
            } catch (Throwable t) {
                if (_specificLogger.isDebugEnabled())
                    _specificLogger.debug("error while processing incoming replication", t);

                clearStateAfterException();

                // Exception thrown from exception handler meaning it can not
                // handle this exception
                // We must return a failed result to all pending threads
                return new GlobalOrderProcessResult(t, _firstUnprocessedKey - 1);
            }
        } finally {
            _lock.unlock();
        }
    }

    private void validateMemoryStatus() {
        if (!_processLogConfig.isMonitorPendingOperationsMemory())
            return;

        if (_pendingPacketsQueue.size() >= _processLogConfig.getPendingPacketsQueueSizeThreshold() && _processMemoryManager.getMemoryUsagePercentage(false) >= _processLogConfig.getHighMemoryUsagePercentage()) {
            // Perform GC 5 times (lucky number?)
            for (int i = 0; i < 5; i++)
                _processMemoryManager.performGC();

            if (_processMemoryManager.getMemoryUsagePercentage(false) >= _processLogConfig.getHighMemoryUsagePercentage()) {
                if (_specificLogger.isWarnEnabled())
                    _specificLogger.warn("Memory shortage in multi source reliable async process log [groupName="
                            + getGroupName()
                            + ", pendingPacketsQueueSize="
                            + _pendingPacketsQueue.size()
                            + ", memoryUsage="
                            + _processMemoryManager.getMemoryUsage()
                            + ", memoryUsagePercentage="
                            + _processMemoryManager.getMemoryUsagePercentage(true)
                            + ", maximumMemory="
                            + _processMemoryManager.getMaximumMemory() + "]");
                throw new MemoryShortageException(getGroupName(),
                        "",
                        SystemInfo.singleton().network().getHostId(),
                        _processMemoryManager.getMemoryUsage(),
                        _processMemoryManager.getMaximumMemory());
            }
        }
    }

    private void clearUidsDependencyHelperMap() {
        // Clear UIDs helper map
        for (Iterator<Entry<String, MultiSourceProcessLogPacket>> iterator = _packetsByUidHelperMap.entrySet().iterator(); iterator.hasNext(); ) {
            Entry<String, MultiSourceProcessLogPacket> entry = iterator.next();
            if (entry.getValue().isConsumed())
                iterator.remove();
        }
    }

    private GlobalOrderProcessResult processPackets(String sourceLookupName,
                                                    List<IReplicationOrderedPacket> packets,
                                                    IReplicationInFilterCallback inFilterCallback) throws Throwable {
        final MultiSourceSingleFileReplicationInBatchContext context = new MultiSourceSingleFileReplicationInBatchContext(this,
                _specificLogger,
                packets.size(),
                _participantsMediator,
                sourceLookupName,
                getGroupName());
        // Get the last data packet index in the batch (non discarded/deleted packets)
        int lastDataPacketIndex = locateLastDataPacketIndex(packets);
        long lastKeyInBatch = lastDataPacketIndex != -1 ? packets.get(lastDataPacketIndex).getKey() : -1;
        // If packets.size == 0 we assume there are pending packets in pendingPacketsQueue
        long firstKeyInBatch = packets.size() > 0 ? packets.get(0).getKey() : _pendingPacketsQueue.getLast().getKey() + 1;

        // Generate dependencies for new packets - packets in pendingPacketsQueue already have dependencies set.
        generatePacketsWithDependencies(packets);

        boolean packetsConsumed = true;
        // On each iteration only packets with no dependencies are executed - after each iteration
        // fully consumed packets are removed from pendingPacketsQueue so only unconsumed packets remain there for the next iteration.
        // The iterations are over when last iteration indicated no packets were consumed.
        while (packetsConsumed) {
            try {
                packetsConsumed = false;
                for (Iterator<MultiSourceProcessLogPacket> pendingPacketsIterator = _pendingPacketsQueue.iterator(); pendingPacketsIterator.hasNext(); ) {
                    final MultiSourceProcessLogPacket packet = pendingPacketsIterator.next();

                    if (packet.isMultiParticipant() && !packet.isConsolidatedByThisParticipant()) {
                        // Check if this packet is a multi participant packet consumed by another participant
                        if (packet.isConsumedByOtherParticipant()) {
                            // If this is a multi participant packet which was consumed by an other thread remove it from mediator.
                            _participantsMediator.remove(packet.getParticipantsMetadata());
                            // Packet was not consumed by this process log and therefore we need to update the lastProcessedKey
                            _lastProcessedKey = Math.max(_lastProcessedKey, packet.getKey());
                            packet.setConsumed();
                            // We can remove this packet from the pending packets queue immediately
                            pendingPacketsIterator.remove();
                            continue;
                        }
                        // else: this packet is waiting to be consumed by the consolidator packet.
                    }

                    if (packet.hasDependencies())
                        // If this packet has dependencies skip it.
                        continue;

                    if (packet.isMultiParticipant()) {
                        // Packets are usually registered in mediator when they are first scanned for dependencies but if a packet
                        // has dependencies registration in mediator is skipped until dependencies are resolved.
                        if (!packet.isRegisteredInMediator())
                            registerPacketInMediator(packet);

                        // If this packet is not a consolidator packet we check if it should be executed individually
                        // because of threshold breach.
                        if (!packet.isConsolidatedByThisParticipant()) {
                            // If threshold not breached - skip this packet.
                            if (!pendingPacketConsolidationThresholdBreached(packet, firstKeyInBatch, lastKeyInBatch))
                                continue;
                            // Threshold breached - packet will be consumed individually.
                        }
                    }
                    // Consume packet
                    consumePacket(inFilterCallback, context, packet);
                    packetsConsumed = true;
                }

                // Consume pending packets in the batch context if there are any
                if (packetsConsumed)
                    consumePendingPacketsInBatch(context);
            } finally {
                // Remove consumed packets from pendingPacketsQueue.
                // This method will be invoked after exception or after a successful iteration over pendingPacektsQueue.
                removeConsumedPacketsFromPendingPacketsQueue();
            }
        }

        // We have a batch of discarded packets at the end, we need to increase
        // the confirmed key
        int batchSize = packets.size();
        if (batchSize > 0 && lastDataPacketIndex < batchSize - 1) {
            IReplicationOrderedPacket packet = packets.get(batchSize - 1);
            _lastProcessedKey = packet.getEndKey();
        }

        GlobalOrderProcessResult result;
        // Update firstUnprocessedKey and set result
        if (_pendingPacketsQueue.isEmpty()) {
            _firstUnprocessedKey = _lastProcessedKey + 1;
            result = GlobalOrderProcessResult.OK;
        } else {
            _firstUnprocessedKey = _pendingPacketsQueue.getFirst().getKey();
            result = new GlobalOrderProcessResult(null, _firstUnprocessedKey - 1);
        }

        clearUidsDependencyHelperMap();

        return result;
    }

    /**
     * Remove consumed packets from pending packets queue according to batchConsumedPacketKeys. Both
     * lists should be 'key' ordered & each key in batchConsumedPacketKeys is expected to have a
     * corresponding packet in pendingPacketsQueue.
     */
    private void removeConsumedPacketsFromPendingPacketsQueue() {
        if (_batchConsumedPacketsKeys.isEmpty())
            return;
        final Iterator<MultiSourceProcessLogPacket> pendingPacketsIterator = _pendingPacketsQueue.iterator();
        for (Long consumedPacketKey : _batchConsumedPacketsKeys) {
            if (!pendingPacketsIterator.hasNext())
                if (_pendingPacketsQueue.size() < 100 && _batchConsumedPacketsKeys.size() < 100) {
                    throw new IllegalStateException("Packets were consumed but are missing from pending packets queue [batchConsumedPacketsKeys="
                            + _batchConsumedPacketsKeys + ", pendingPacketsQueue=" + _pendingPacketsQueue + "].");
                } else {
                    throw new IllegalStateException("Packets were consumed but are missing from pending packets queue [batchConsumedPacketsKeys size="
                            + _batchConsumedPacketsKeys.size() + ", pendingPacketsQueue size=" + _pendingPacketsQueue.size() + "].");
                }
            boolean packetFound = false;
            while (pendingPacketsIterator.hasNext()) {
                if (pendingPacketsIterator.next().getKey() == consumedPacketKey) {
                    pendingPacketsIterator.remove();
                    packetFound = true;
                    break;
                }
            }
            if (!packetFound)
                if (_pendingPacketsQueue.size() < 100 && _batchConsumedPacketsKeys.size() < 100) {
                    throw new IllegalStateException("Consumed packet [key="
                            + consumedPacketKey
                            + "] was not found in pendingPacketsQueue [batchConsumedPacketsKeys="
                            + _batchConsumedPacketsKeys + ", pendingPacketsQueue="
                            + _pendingPacketsQueue + "].");
                } else {
                    throw new IllegalStateException("Consumed packet [key="
                            + consumedPacketKey
                            + "] was not found in pendingPacketsQueue [batchConsumedPacketsKeys size="
                            + _batchConsumedPacketsKeys.size() + ", pendingPacketsQueue size="
                            + _pendingPacketsQueue.size() + "].");
                }
        }
        _batchConsumedPacketsKeys.clear();
    }

    private boolean pendingPacketConsolidationThresholdBreached(
            MultiSourceProcessLogPacket packet, long firstKeyInBatch,
            long lastKeyInBatch) {
        // Check timeout only if the packet is not from the current batch
        boolean consolidationBreached = false;
        if (firstKeyInBatch == -1 || packet.getKey() < firstKeyInBatch) {
            final long packetPendingTime = SystemTime.timeMillis() - packet.getCreationTime();
            final boolean abortTimeBased = packetPendingTime >= _processLogConfig.getConsumeTimeout();

            final long packetRetainedBacklogSize = lastKeyInBatch - packet.getKey();
            final boolean abortSizeBased = lastKeyInBatch != -1
                    && _processLogConfig.getPendingPacketPacketsIntervalBeforeConsumption() != -1
                    && packetRetainedBacklogSize >= _processLogConfig.getPendingPacketPacketsIntervalBeforeConsumption();

            if ((abortTimeBased || abortSizeBased) && _participantsMediator.abortConsolidation(packet.getParticipantsMetadata())) {
                logConsolidationAbortedWarningMessage(packet.getParticipantsMetadata(), abortTimeBased, abortTimeBased ? packetPendingTime : packetRetainedBacklogSize);
                consolidationBreached = true;
            }
        }
        return consolidationBreached;
    }

    private void consumePacket(
            final IReplicationInFilterCallback inFilterCallback,
            final MultiSourceSingleFileReplicationInBatchContext context,
            final MultiSourceProcessLogPacket packet) throws Throwable {
        if (preprocess(packet.getReplicationPacket())) {
            context.setContextPacket(packet.getReplicationPacket());
            try {
                IReplicationPacketData<?> data = packet.getData();

                // Set the current key in the context in order for the context
                // to keep track of the unprocessed packet in a batch
                context.setCurrentKey(packet.getKey());

                IDataConsumeResult prevResult = null;
                do {
                    // If closed, reject immediately
                    if (isClosed()) {
                        throw new ClosedResourceException("Process log is closed");
                    }

                    // Save the current context state in case we retry the operation due to an exception after applying a fix
                    context.snapshot();
                    packet.setBeingConsumed();
                    context.addBeingConsumedPacket(packet);
                    IDataConsumeResult consumeResult = getDataConsumer().consume(context,
                            data,
                            getReplicationInFacade(),
                            inFilterCallback);
                    if (!consumeResult.isFailed()) {
                        break;
                    }

                    // Rollback to the previous context snapshot state
                    context.rollback();
                    throwIfRepetitiveError(prevResult, consumeResult);
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug(
                                "Encountered error while consuming packet ["
                                        + packet.getReplicationPacket()
                                        + "], trying to resolve issue",
                                consumeResult.toException());
                    IDataConsumeFix fix = getExceptionHandler().handleException(consumeResult, null/*packet, this is batch consumption we dont know the origin packet that the data causes this error*/);
                    data = getDataConsumer().applyFix(context, data, fix);
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug("Fix applied - retrying the operation [" + fix + "]");

                    prevResult = consumeResult;
                } while (true);
            } finally {
                context.setContextPacket(null);
            }
        }
    }

    private void generatePacketsWithDependencies(List<IReplicationOrderedPacket> packets) {
        for (IReplicationOrderedPacket packet : packets) {
            if (preprocess(packet)) {
                MultiSourceProcessLogPacket replicationPacket;
                final IReplicationPacketData<?> data = packet.getData();
                final IReplicationParticipantsMetadata participantsMetadata = getDataConsumer().extractParticipantsMetadata(data);

                // Handle multiple participants data
                if (participantsMetadata.getTransactionParticipantsCount() > 1) {
                    replicationPacket = new MultiSourceProcessLogMultiParticipantPacket(packet, _participantsMediator, participantsMetadata);
                } else {
                    replicationPacket = new MultiSourceProcessLogPacket(packet);
                }

                if (packet.getData().isSingleEntryData()) {
                    final String uid = packet.getData().getSingleEntryData().getUid();
                    checkAndAddDependencyIfNecessary(replicationPacket, uid);
                } else {
                    for (IReplicationPacketEntryData entryData : packet.getData()) {
                        checkAndAddDependencyIfNecessary(replicationPacket, entryData.getUid());
                    }
                }

                // If this is a multi participant packet which has no dependencies - register it at mediator
                if (participantsMetadata.getTransactionParticipantsCount() > 1 && !replicationPacket.hasDependencies()) {
                    registerPacketInMediator(replicationPacket);
                }

                _pendingPacketsQueue.add(replicationPacket);
            }
        }
    }

    private void checkAndAddDependencyIfNecessary(
            MultiSourceProcessLogPacket replicationPacket, String uid) {
        final MultiSourceProcessLogPacket dependsOnPacket = _packetsByUidHelperMap.put(uid, replicationPacket);
        if (dependsOnPacket != null && !dependsOnPacket.isConsumed()
                && dependsOnPacket.getKey() < replicationPacket.getKey()) {
            replicationPacket.addDependency(dependsOnPacket);
        }
    }

    private void registerPacketInMediator(MultiSourceProcessLogPacket packet) {
        final IReplicationPacketData<?>[] allParticipantsData = _participantsMediator.getAllParticipantsData(packet);
        packet.setRegisteredInMediator();
        if (allParticipantsData != null) {
            IReplicationPacketData<?> mergedData = getDataConsumer().merge(allParticipantsData, packet.getParticipantsMetadata());
            packet.setData(mergedData);
        }
    }

    private void consumePendingPacketsInBatch(
            MultiSourceSingleFileReplicationInBatchContext context) throws Exception {
        // If closed, reject immediately
        if (isClosed())
            throw new ClosedResourceException("Process log is closed");

        IDataConsumeResult result = _batchDataConsumer.consumePendingPackets(context, getReplicationInFacade());
        if (result.isFailed())
            throw result.toException();
    }

    private void logConsolidationAbortedWarningMessage(
            IReplicationParticipantsMetadata participantsMetadata, boolean timeBased, long breachingThreshold) {
        if (_specificLogger.isWarnEnabled()) {
            if (timeBased)
                _specificLogger.warn(
                        "Timeout exceeded [" + breachingThreshold + "/" + _processLogConfig.getConsumeTimeout() + "ms] while waiting for all participants [contextId="
                                + participantsMetadata.getContextId()
                                + ", participantsCount="
                                + participantsMetadata.getTransactionParticipantsCount()
                                + "] - current participant [participantId="
                                + participantsMetadata.getParticipantId()
                                + "] will be processed independently");
            else
                _specificLogger.warn(
                        "Pending operations threshold exceeded [" + breachingThreshold + "/" + _processLogConfig.getPendingPacketPacketsIntervalBeforeConsumption() + "] while waiting for all participants [contextId="
                                + participantsMetadata.getContextId()
                                + ", participantsCount="
                                + participantsMetadata.getTransactionParticipantsCount()
                                + "] - current participant [participantId="
                                + participantsMetadata.getParticipantId()
                                + "] will be processed independently");
        }
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

    protected void logDeletion(
            GlobalOrderDeletedBacklogPacket deletedBacklogPacket) {
        String deletionMessage = "packets [" + deletedBacklogPacket.getKey() + "-" + deletedBacklogPacket.getEndKey() + "] are lost due to backlog deletion at the source";
        getGroupHistory().logEvent(getSourceLookupName(), deletionMessage);
        if (_specificLogger.isWarnEnabled())
            _specificLogger.warn(deletionMessage);
    }

    protected void filterDuplicate(List<IReplicationOrderedPacket> packets) {
        // If source is in async mode (due to disconnection, recovery or temp
        // error at target, i.e out of mem)
        // duplicate packets may arrive at the intermediate stage of the source
        // moving from async to sync
        for (Iterator<IReplicationOrderedPacket> iterator = packets.iterator(); iterator.hasNext(); ) {
            IReplicationOrderedPacket packet = iterator.next();
            if (filterDuplicate(packet)) {
                iterator.remove();
            }
        }
    }

    protected boolean filterDuplicate(IReplicationOrderedPacket packet) {
        // If source is in async mode (due to disconnection, recovery or temp
        // error at target, i.e out of mem)
        // duplicate packets may arrive at the intermediate stage of the source
        // moving from async to sync 
        return _pendingPacketsQueue.isEmpty() ? packet.getEndKey() <= _lastProcessedKey
                : packet.getEndKey() <= Math.max(_lastProcessedKey,
                _pendingPacketsQueue.getLast()
                        .getReplicationPacket()
                        .getEndKey());
    }

    protected void throwClosedException() {
        throw new ClosedResourceException("Process log is closed");
    }

    public IProcessResult process(String sourceLookupName,
                                  IReplicationOrderedPacket packet,
                                  IReplicationInFilterCallback inFilterCallback) {
        throw new UnsupportedOperationException();
    }

    public boolean close(long time, TimeUnit unit) throws InterruptedException {
        // Flush to main memory
        synchronized (_lifeCycleLock) {
            _state = ProcessLogState.CLOSING;
        }

        if (!_lock.tryLock(time, unit)) {
            // Flush to main memory
            synchronized (_lifeCycleLock) {
                _state = ProcessLogState.CLOSED;
            }
            return false;
        }
        try {
            _state = ProcessLogState.CLOSED;
            return true;
        } finally {
            getExceptionHandler().close();
            _lock.unlock();
        }
    }

    public void processHandshakeIteration(String sourceMemberName,
                                          IHandshakeIteration handshakeIteration) {
        throw new UnsupportedOperationException();
    }

    public IProcessLogHandshakeResponse resync(IBacklogHandshakeRequest handshakeRequest) {
        _lock.lock();
        try {
            if (!isOpen())
                throw new ClosedResourceException("Process log is closed");
            GlobalOrderBacklogHandshakeRequest typedHandshakeRequest = (GlobalOrderBacklogHandshakeRequest) handshakeRequest;
            _firstHandshakeForTarget = false;
            _firstUnprocessedKey = typedHandshakeRequest.getLastConfirmedKey() + 1;
            _lastProcessedKey = _firstUnprocessedKey - 1;
            _pendingPacketsQueue.clear();
            return new GlobalOrderProcessLogHandshakeResponse(_firstUnprocessedKey - 1);
        } finally {
            _lock.unlock();
        }
    }

    public Object toWireForm(IProcessResult processResult) {
        //Optimization, reduce the cost of memory garbage by not creating an ok process result at the source
        //from serialization
        if (processResult == GlobalOrderProcessResult.OK)
            return null;

        return processResult;
    }

    protected boolean isOpen() {
        return _state == ProcessLogState.OPEN;
    }

    protected boolean isClosed() {
        return _state == ProcessLogState.CLOSED;
    }

    @Override
    public void packetConsumed(long key) {
        if (key == _firstUnprocessedKey)
            _firstUnprocessedKey = key + 1;
        if (key > _lastProcessedKey)
            _lastProcessedKey = key;

        _batchConsumedPacketsKeys.add(key);
    }

    @Override
    public String dumpState() {
        return "State [" + _state + "] had any handshake ["
                + !_firstHandshakeForTarget + "] last process key ["
                + _lastProcessedKey + "] first unprocessed key [" + _firstUnprocessedKey + "] " + dumpStateExtra();
    }

    private String dumpStateExtra() {
        _lock.lock();
        try {
            StringBuilder dumpState = new StringBuilder("pending packets queue [");
            if (_pendingPacketsQueue == null || _pendingPacketsQueue.isEmpty()) {
                dumpState.append("EMPTY");
            } else {
                dumpState.append("size=").append(_pendingPacketsQueue.size());
                dumpState.append("firstPacket=").append(_pendingPacketsQueue.getFirst().toString());
                dumpState.append(", lastPacket=").append(_pendingPacketsQueue.getLast().toString());
                dumpState.append("\n]");
            }
            return dumpState.toString();
        } finally {
            _lock.unlock();
        }
    }

    public MultiSourceSingleFileProcessLogConfig getProcessLogConfig() {
        return _processLogConfig;
    }

    protected boolean isFirstHandshakeForTarget() {
        _lock.lock();
        try {
            return _firstHandshakeForTarget;
        } finally {
            _lock.unlock();
        }
    }

    public long getLastProcessedKey() {
        _lock.lock();
        try {
            return _lastProcessedKey;
        } finally {
            _lock.unlock();
        }
    }

    public long getFirstUnprocessedKey() {
        _lock.lock();
        try {
            return _firstUnprocessedKey;
        } finally {
            _lock.unlock();
        }
    }

    public LinkedList<MultiSourceProcessLogPacket> getPendingPacketsQueue() {
        _lock.lock();
        try {
            return _pendingPacketsQueue;
        } finally {
            _lock.unlock();
        }
    }

    public HashMap<String, MultiSourceProcessLogPacket> getPacketsByUidHelperMap() {
        _lock.lock();
        try {
            return _packetsByUidHelperMap;
        } finally {
            _lock.unlock();
        }
    }

    @Override
    public IProcessResult processIdleStateData(String sourceLookupName, IIdleStateData idleStateData,
                                               IReplicationInFilterCallback inFilterCallback) {
        if (!isOpen())
            throwClosedException();

        _lock.lock();
        try {
            // If closed, reject immediately
            if (isClosed())
                throwClosedException();

            try {
                return processPackets(sourceLookupName,
                        Collections.<IReplicationOrderedPacket>emptyList(),
                        inFilterCallback);
            } catch (ClosedResourceException e) {
                throw e;
            } catch (Throwable t) {
                if (_specificLogger.isDebugEnabled())
                    _specificLogger.debug("error while processing incoming idle state data replication", t);

                clearStateAfterException();

                // Exception thrown from exception handler meaning it can not
                // handle this exception
                // We must return a failed result to all pending threads
                return new GlobalOrderProcessResult(t, _firstUnprocessedKey - 1);
            }
        } finally {
            _lock.unlock();
        }
    }

    private void clearStateAfterException() {
        // Mark set 'beingConsumed' pending packets state to 'unconsumed'
        if (_pendingPacketsQueue != null && !_pendingPacketsQueue.isEmpty()) {
            for (MultiSourceProcessLogPacket packet : _pendingPacketsQueue) {
                if (packet.isBeingConsumed())
                    packet.setUnconsumed();
            }
        }

        // Clear pending packets queue & dependencies helper map - they will be resent
        clearUidsDependencyHelperMap();

        _batchConsumedPacketsKeys.clear();
    }

}
