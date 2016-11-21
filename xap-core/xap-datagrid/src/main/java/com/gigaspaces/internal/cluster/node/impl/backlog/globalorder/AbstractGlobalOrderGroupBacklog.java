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

package com.gigaspaces.internal.cluster.node.impl.backlog.globalorder;

import com.gigaspaces.cluster.replication.ReplicationException;
import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.backlog.AbstractSingleFileGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.CompletedHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.EmptyIdleStateData;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.IIdleStateData;
import com.gigaspaces.internal.cluster.node.impl.backlog.NonExistingBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataProducer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessResult;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;


public abstract class AbstractGlobalOrderGroupBacklog
        extends
        AbstractSingleFileGroupBacklog<IReplicationOrderedPacket, GlobalOrderConfirmationHolder> {

    public AbstractGlobalOrderGroupBacklog(DynamicSourceGroupConfigHolder groupConfig,
                                           String name, IReplicationPacketDataProducer<?> dataProducer) {
        super(groupConfig, name, dataProducer);
    }

    @Override
    protected Map<String, GlobalOrderConfirmationHolder> createConfirmationMap(
            SourceGroupConfig groupConfig) {
        HashMap<String, GlobalOrderConfirmationHolder> confirmationMap = new HashMap<String, GlobalOrderConfirmationHolder>();
        String[] membersLookupNames = groupConfig.getMembersLookupNames();
        for (int i = 0; i < membersLookupNames.length; i++) {
            confirmationMap.put(membersLookupNames[i],
                    new GlobalOrderConfirmationHolder());
        }
        return confirmationMap;
    }

    @Override
    protected GlobalOrderConfirmationHolder createNewConfirmationHolder() {
        GlobalOrderConfirmationHolder confirmationHolder = new GlobalOrderConfirmationHolder();
        confirmationHolder.setLastConfirmedKey(getNextKeyUnsafe() - 1);
        confirmationHolder.setWeight(0);
        return confirmationHolder;
    }

    public GlobalOrderBacklogHandshakeRequest getHandshakeRequest(
            String memberName, Object customBacklogMetadata) {
        _rwLock.writeLock().lock();
        try {
            GlobalOrderConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
            return new GlobalOrderBacklogHandshakeRequest(!confirmationHolder.hadAnyHandshake(),
                    confirmationHolder.getLastConfirmedKey());
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    @Override
    protected IReplicationOrderedPacket createBacklogOverflowPacket(
            long lastConfirmedKey, long firstKeyInBacklog, String memberName) {
        return new GlobalOrderDeletedBacklogPacket(lastConfirmedKey + 1,
                firstKeyInBacklog - 1);
    }

    public IHandshakeContext processHandshakeResponse(String memberName,
                                                      IBacklogHandshakeRequest request, IProcessLogHandshakeResponse response, PlatformLogicalVersion targetLogicalVersion, Object customBacklogMetadata) {
        GlobalOrderProcessLogHandshakeResponse typedResponse = (GlobalOrderProcessLogHandshakeResponse) response;
        _rwLock.writeLock().lock();
        try {
            long lastProcessedKey = typedResponse.getLastProcessedKey();
            updateLastConfirmedKeyUnsafe(memberName, lastProcessedKey);
            clearConfirmedPackets();
            return new CompletedHandshakeContext();
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    @Override
    protected long getMemberUnconfirmedKey(GlobalOrderConfirmationHolder value) {
        return value.hadAnyHandshake() ? value.getLastConfirmedKey() : -1;
    }

    public void processResult(String memberName, IProcessResult result,
                              List<IReplicationOrderedPacket> packets)
            throws ReplicationException {
        GlobalOrderProcessResult typedResult = (GlobalOrderProcessResult) result;

        _rwLock.writeLock().lock();
        try {
            if (typedResult.isProcessed()) {
                confirmedPackets(memberName, packets);
            } else {
                handlePendingErrorBatchPackets(memberName,
                        packets,
                        typedResult.getError(),
                        typedResult.getLastProcessedKey() + 1);
                handleErrorResult(memberName, typedResult);
            }
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public void processResult(String memberName, IProcessResult result,
                              IReplicationOrderedPacket packet) throws ReplicationException {
        GlobalOrderProcessResult typedResult = (GlobalOrderProcessResult) result;
        _rwLock.writeLock().lock();
        try {
            if (typedResult.isProcessed()) {
                confirmedPackets(memberName, packet);
            } else {
                handlePendingErrorSinglePacket(memberName, packet, typedResult.getError());
                handleErrorResult(memberName, typedResult);
            }
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    private void confirmedPackets(String memberName,
                                  List<IReplicationOrderedPacket> packets) {
        IReplicationOrderedPacket lastPacket = packets.get(packets.size() - 1);
        confirmedPackets(memberName, lastPacket);
    }

    private void confirmedPackets(String memberName,
                                  IReplicationOrderedPacket packet) {
        long packetKeykey = packet.getKey();
        if (packet.isDiscardedPacket() && packet instanceof GlobalOrderDiscardedReplicationPacket)
            packetKeykey = ((GlobalOrderDiscardedReplicationPacket) packet).getEndKey();
        confirmedKey(memberName, packetKeykey);
    }

    private void confirmedKey(String memberName, long packetKeykey) {
        GlobalOrderConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
        long lastConfirmedKey = confirmationHolder.getLastConfirmedKey();
        if (!confirmationHolder.hadAnyHandshake()
                || packetKeykey > lastConfirmedKey) {
            decreaseWeight(memberName, lastConfirmedKey, packetKeykey);
            confirmationHolder.setLastConfirmedKey(packetKeykey);
            cleanPendingErrorStateIfNeeded(memberName,
                    packetKeykey,
                    confirmationHolder);
        }
        clearConfirmedPackets();
    }

    private void handleErrorResult(String memberName,
                                   GlobalOrderProcessResult result) throws ReplicationException {
        Throwable error = result.getError();
        long lastProcessedKey = result.getLastProcessedKey();
        handleErrorResult(memberName, error, lastProcessedKey);
    }

    private void handleErrorResult(String memberName, Throwable error,
                                   long lastProcessedKey) throws ReplicationException {
        if (_logger.isLoggable(Level.FINER))
            _logger.finer(getLogPrefix()
                    + "handling error result in backlog - " + error);
        GlobalOrderConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
        if (!confirmationHolder.hadAnyHandshake() || confirmationHolder.getLastConfirmedKey() < lastProcessedKey) {
            updateLastConfirmedKeyUnsafe(memberName, lastProcessedKey);
            clearConfirmedPackets();
        }
        throw new ReplicationException(error.getMessage(), error);
    }

    public IBacklogMemberState getState(String memberName) {
        _rwLock.readLock().lock();
        try {
            GlobalOrderConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
            if (confirmationHolder == null)
                return NonExistingBacklogMemberState.INSTANCE;
            long lastConfirmedKey = confirmationHolder.getLastConfirmedKey();
            boolean hasAnyHandshake = confirmationHolder.hadAnyHandshake();
            Throwable pendingError = confirmationHolder.hasPendingError() ? confirmationHolder.getPendingError() : null;

            boolean backlogDropped = _outOfSyncDueToDeletionTargets.contains(memberName);
            return new GlobalOrderBacklogMemberState(memberName,
                    hasAnyHandshake,
                    lastConfirmedKey,
                    backlogDropped,
                    pendingError);
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    private void updateLastConfirmedKeyUnsafe(String memberName, long key) {
        GlobalOrderConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
        decreaseWeight(memberName,confirmationHolder.getLastConfirmedKey(), key);
        confirmationHolder.setLastConfirmedKey(key);

    }

    @Override
    protected void onBeginSynchronization(String memberName) {
        long key = getNextKeyUnsafe() - 1;
        updateLastConfirmedKeyUnsafe(memberName, key);
    }

    public IReplicationOrderedPacket replaceWithDiscarded(
            IReplicationOrderedPacket packet, boolean forceDiscard) {
        return new GlobalOrderDiscardedReplicationPacket(packet.getKey());
    }

    @Override
    public boolean mergeWithDiscarded(
            IReplicationOrderedPacket previousDiscardedPacket,
            IReplicationOrderedPacket mergedPacket, String memberName) {
        return false;
    }

    @Override
    public boolean supportDiscardMerge() {
        return true;
    }

    public GlobalOrderConfirmationHolder getLastConfirmationInternal(String memberName) {
        _rwLock.readLock().lock();
        try {
            return getConfirmationHolderUnsafe(memberName);
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    public String dumpState() {
        _rwLock.readLock().lock();
        try {
            StringBuilder dump = new StringBuilder("Type ["
                    + this.getClass().getName() + "]");
            dump.append(StringUtils.NEW_LINE);
            dump.append("Backlog statistics {" + getStatistics() + "}");
            appendConfirmationStateString(dump);
            return dump.toString();
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    protected IReplicationOrderedPacket addSingleOperationPacket(
            IReplicationGroupOutContext groupContext, IEntryHolder entryHolder,
            ReplicationSingleOperationType operationType) {
        ReplicationOutContext outContext = groupContext.getEntireContext();
        IReplicationPacketData<?> data = getDataProducer().createSingleOperationData(entryHolder,
                operationType,
                outContext);

        GlobalOrderOperationPacket packet = insertPacketToBacklog(data, outContext);
        return packet;
    }

    protected IReplicationOrderedPacket addTransactionOperationPacket(
            IReplicationGroupOutContext groupContext,
            ServerTransaction transaction,
            ArrayList<IEntryHolder> lockedEntries,
            ReplicationMultipleOperationType operationType) {
        ReplicationOutContext outContext = groupContext.getEntireContext();
        IReplicationPacketData<?> data = getDataProducer().createTransactionOperationData(transaction,
                lockedEntries,
                outContext,
                operationType);
        if (data.isEmpty())
            return null;

        GlobalOrderOperationPacket packet = insertPacketToBacklog(data, outContext);
        return packet;
    }

    protected IReplicationOrderedPacket addGenericOperationPacket(
            IReplicationGroupOutContext groupContext, Object operationData,
            ReplicationSingleOperationType operationType) {

        ReplicationOutContext outContext = groupContext.getEntireContext();
        IReplicationPacketData<?> data = getDataProducer().createGenericOperationData(operationData,
                operationType,
                outContext);

        GlobalOrderOperationPacket packet = insertPacketToBacklog(data, outContext);
        return packet;
    }

    private GlobalOrderOperationPacket insertPacketToBacklog(
            IReplicationPacketData<?> data, ReplicationOutContext outContext) {
        _rwLock.writeLock().lock();
        try {

            setPacketWeight(data);

            if (!shouldInsertPacket())
                return null;

            GlobalOrderOperationPacket packet = new GlobalOrderOperationPacket(takeNextKeyUnsafe(outContext),
                    data);

            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(getLogPrefix() + "inserting packet [" + packet
                        + "] to backlog");

            insertReplicationOrderedPacketToBacklog(packet, outContext);
            return packet;
        } catch (RuntimeException e) {
            if (_logger.isLoggable(Level.SEVERE))
                _logger.log(Level.SEVERE,
                        "exception while inserting a packet to the backlog file (insertPacketToBacklog), "
                                + "[" + getStatistics() + "]",
                        e);
            validateIntegrity();
            throw e;
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    @Override
    protected long getLastConfirmedKeyUnsafe(String memberLookupName) {
        GlobalOrderConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberLookupName);
        return confirmationHolder.hadAnyHandshake() ? confirmationHolder.getLastConfirmedKey() : -1;
    }

    @Override
    protected void deleteBatchFromBacklog(long deletionBatchSize) {
        getBacklogFile().deleteOldestBatch(deletionBatchSize);
    }

    public IProcessResult fromWireForm(Object wiredProcessResult) {
        if (wiredProcessResult == null)
            return GlobalOrderProcessResult.OK;

        return (IProcessResult) wiredProcessResult;
    }

    @Override
    public IIdleStateData getIdleStateData(String memberName,
                                           PlatformLogicalVersion targetMemberVersion) {
        return EmptyIdleStateData.INSTANCE;
    }

    @Override
    public void processIdleStateDataResult(String memberName, IProcessResult result,
                                           IIdleStateData idleStateData) {
    }

    @Override
    public long getConfirmed(String memberName) {
        _rwLock.readLock().lock();
        try {
            return getLastConfirmedKeyUnsafe(memberName);
        } finally {
            _rwLock.readLock().unlock();
        }

    }


}