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

package com.gigaspaces.internal.cluster.node.impl.backlog.multisourcesinglefile;

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
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderConfirmationHolder;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDeletedBacklogPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderOperationPacket;
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

public abstract class AbstractMultiSourceSingleFileGroupBacklog extends AbstractSingleFileGroupBacklog<IReplicationOrderedPacket, MultiSourceSingleFileConfirmationHolder> {

    public AbstractMultiSourceSingleFileGroupBacklog(
            DynamicSourceGroupConfigHolder groupConfig, String name,
            IReplicationPacketDataProducer<?> dataProducer) {
        super(groupConfig, name, dataProducer);
    }

    @Override
    public MultiSourceSingleFileBacklogHandshakeRequest getHandshakeRequest(String memberName, Object customBacklogMetadata) {

        _rwLock.writeLock().lock();
        try {

            GlobalOrderConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
            MultiSourceSingleFileBacklogHandshakeRequest handshakeRequest = new MultiSourceSingleFileBacklogHandshakeRequest(!confirmationHolder.hadAnyHandshake(),
                    confirmationHolder.getLastConfirmedKey());

            return getHandshakeRequestImpl(memberName, handshakeRequest);

        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    protected MultiSourceSingleFileBacklogHandshakeRequest getHandshakeRequestImpl(
            String memberName,
            MultiSourceSingleFileBacklogHandshakeRequest handshakeRequest) {
        return handshakeRequest;
    }

    @Override
    protected long getFirstRequiredKeyUnsafe(String memberName) {
        MultiSourceSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
        if (confirmationHolder.hadAnyHandshake()) {
            return Math.max(confirmationHolder.getLastConfirmedKey(), confirmationHolder.getLastReceivedKey());
        }
        return -1;
    }

    @Override
    public IHandshakeContext processHandshakeResponse(String memberName,
                                                      IBacklogHandshakeRequest request, IProcessLogHandshakeResponse response, PlatformLogicalVersion targetLogicalVersion, Object customBacklogMetadata) {
        GlobalOrderProcessLogHandshakeResponse typedResponse = (GlobalOrderProcessLogHandshakeResponse) response;
        _rwLock.writeLock().lock();
        try {
            long lastProcessedKey = typedResponse.getLastProcessedKey();

            MultiSourceSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
            decreaseWeight(memberName, confirmationHolder.getLastConfirmedKey(), lastProcessedKey);
            confirmationHolder.setLastConfirmedKey(lastProcessedKey);
            // Upon handshake the target can be new, therefore we restore the last
            // received key to be the last processed key
            confirmationHolder.setLastReceivedKey(lastProcessedKey);

            clearConfirmedPackets();

            return getHandshakeContext(memberName,
                    request,
                    targetLogicalVersion,
                    typedResponse,
                    lastProcessedKey);
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    protected IHandshakeContext getHandshakeContext(String memberName,
                                                    IBacklogHandshakeRequest request,
                                                    PlatformLogicalVersion targetLogicalVersion,
                                                    GlobalOrderProcessLogHandshakeResponse typedResponse,
                                                    long lastProcessedKey) {
        return new CompletedHandshakeContext();
    }

    private void updateLastConfirmedKeyUnsafe(String memberName, long key) {
        MultiSourceSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
        decreaseWeight(memberName, confirmationHolder.getLastConfirmedKey(), key);
        confirmationHolder.setLastConfirmedKey(key);
        confirmationHolder.setLastReceivedKey(key);
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

    public void processResult(String memberName, IProcessResult result,
                              List<IReplicationOrderedPacket> packets)
            throws ReplicationException {
        GlobalOrderProcessResult typedResult = (GlobalOrderProcessResult) result;

        _rwLock.writeLock().lock();
        try {
            // All packets were processed by process log - there are no pending packets.
            if (typedResult.isProcessed()) {
                IReplicationOrderedPacket lastPacket = packets.get(packets.size() - 1);
                confirmedKey(memberName, lastPacket.getEndKey(), true);
            }
            // Process log has pending packets to execute.
            else if (typedResult.getError() == null) {
                final MultiSourceSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
                final IReplicationOrderedPacket lastPacket = packets.get(packets.size() - 1);
                confirmedKey(memberName, typedResult.getLastProcessedKey(), lastPacket.getEndKey(), confirmationHolder, true);
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

    private void confirmedKey(String memberName, long packetKey, boolean setLastReceivedKey) {
        MultiSourceSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
        confirmedKey(memberName, packetKey, packetKey, confirmationHolder, setLastReceivedKey);
    }

    private void confirmedKey(String memberName, long lastConfirmedKey,
                              long lastReceivedKey, MultiSourceSingleFileConfirmationHolder confirmationHolder, boolean setLastReceivedKey) {
        boolean lastConfirmedKeyUpdated = false;

        if (confirmationHolder.hadAnyHandshake()) {
            if (lastConfirmedKey > confirmationHolder.getLastConfirmedKey()) {
                lastConfirmedKeyUpdated = true;
                decreaseWeight(memberName, confirmationHolder.getLastConfirmedKey(), lastConfirmedKey);
                confirmationHolder.setLastConfirmedKey(lastConfirmedKey);
            }

            if (setLastReceivedKey &&
                    lastReceivedKey > confirmationHolder.getLastReceivedKey())
                confirmationHolder.setLastReceivedKey(lastReceivedKey);
        } else {
            lastConfirmedKeyUpdated = true;
            decreaseWeight(memberName, confirmationHolder.getLastConfirmedKey(), lastConfirmedKey);
            confirmationHolder.setLastConfirmedKey(lastConfirmedKey);
            if (setLastReceivedKey)
                confirmationHolder.setLastReceivedKey(lastReceivedKey);
        }

        if (lastConfirmedKeyUpdated) {
            cleanPendingErrorStateIfNeeded(memberName,
                    lastConfirmedKey,
                    confirmationHolder);
            clearConfirmedPackets();
        }

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
        if (!confirmationHolder.hadAnyHandshake() || confirmationHolder.getLastConfirmedKey() <= lastProcessedKey) {
            updateLastConfirmedKeyUnsafe(memberName, lastProcessedKey);
            clearConfirmedPackets();
        }
        throw new ReplicationException(error.getMessage(), error);
    }

    public void processResult(String memberName, IProcessResult result,
                              IReplicationOrderedPacket packet) throws ReplicationException {
        GlobalOrderProcessResult typedResult = (GlobalOrderProcessResult) result;
        _rwLock.writeLock().lock();
        try {
            if (typedResult.isProcessed()) {
                confirmedKey(memberName, packet.getEndKey(), true);
            } else {
                handlePendingErrorSinglePacket(memberName, packet, typedResult.getError());
                handleErrorResult(memberName, typedResult);
            }
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public IReplicationOrderedPacket replaceWithDiscarded(
            IReplicationOrderedPacket packet, boolean forceDiscard) {
        if (forceDiscard || !packet.getData().isMultiParticipantData())
            return new GlobalOrderDiscardedReplicationPacket(packet.getKey());
        return packet;
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

    @Override
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

    public IProcessResult fromWireForm(Object wiredProcessResult) {
        if (wiredProcessResult == null)
            return GlobalOrderProcessResult.OK;

        return (IProcessResult) wiredProcessResult;
    }

    @Override
    protected Map<String, MultiSourceSingleFileConfirmationHolder> createConfirmationMap(
            SourceGroupConfig groupConfig) {
        HashMap<String, MultiSourceSingleFileConfirmationHolder> confirmationMap = new HashMap<String, MultiSourceSingleFileConfirmationHolder>();
        String[] membersLookupNames = groupConfig.getMembersLookupNames();
        for (int i = 0; i < membersLookupNames.length; i++) {
            confirmationMap.put(membersLookupNames[i],
                    new MultiSourceSingleFileConfirmationHolder());
        }
        return confirmationMap;
    }

    @Override
    protected MultiSourceSingleFileConfirmationHolder createNewConfirmationHolder() {
        MultiSourceSingleFileConfirmationHolder confirmationHolder = new MultiSourceSingleFileConfirmationHolder();
        long lastKeyPresentInBacklog = getNextKeyUnsafe() - 1;
        confirmationHolder.setLastConfirmedKey(lastKeyPresentInBacklog);
        confirmationHolder.setWeight(0);
        confirmationHolder.setLastReceivedKey(lastKeyPresentInBacklog);
        return confirmationHolder;
    }

    @Override
    protected void deleteBatchFromBacklog(long deletionBatchSize) {
        getBacklogFile().deleteOldestBatch(deletionBatchSize);
    }

    @Override
    protected long getLastConfirmedKeyUnsafe(String memberLookupName) {
        GlobalOrderConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberLookupName);
        return confirmationHolder.hadAnyHandshake() ? confirmationHolder.getLastConfirmedKey() : -1;
    }

    @Override
    protected void onBeginSynchronization(String memberName) {
        long key = getNextKeyUnsafe() - 1;
        updateLastConfirmedKeyUnsafe(memberName, key);
    }

    @Override
    protected IReplicationOrderedPacket createBacklogOverflowPacket(
            long globalLastConfirmedKey, long firstKeyInBacklog,
            String memberName) {
        return new GlobalOrderDeletedBacklogPacket(globalLastConfirmedKey + 1,
                firstKeyInBacklog - 1);
    }

    @Override
    protected long getMemberUnconfirmedKey(MultiSourceSingleFileConfirmationHolder value) {
        return value.hadAnyHandshake() ? value.getLastConfirmedKey() : -1;
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

    protected IReplicationOrderedPacket addTransactionOperationPacket(
            IReplicationGroupOutContext groupContext,
            ServerTransaction transaction,
            ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType) {
        ReplicationOutContext outContext = groupContext.getEntireContext();
        IReplicationPacketData<?> data = getDataProducer().createTransactionOperationData(transaction,
                lockedEntries,
                outContext,
                operationType);
        if (data.isEmpty() && !data.isMultiParticipantData())
            return null;

        GlobalOrderOperationPacket packet = insertPacketToBacklog(data, outContext);
        return packet;
    }

    @Override
    public IIdleStateData getIdleStateData(String memberName,
                                           PlatformLogicalVersion targetMemberVersion) {
        _rwLock.readLock().lock();
        try {
            final MultiSourceSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
            if (confirmationHolder.getLastReceivedKey() <= confirmationHolder.getLastConfirmedKey())
                return EmptyIdleStateData.INSTANCE;
            return new MultiSourceSingleFileIdleStateData(confirmationHolder.getLastConfirmedKey(), confirmationHolder.getLastReceivedKey());
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    @Override
    public void processIdleStateDataResult(String memberName, IProcessResult result,
                                           IIdleStateData idleStateData) throws ReplicationException {
        GlobalOrderProcessResult typedResult = (GlobalOrderProcessResult) result;
        MultiSourceSingleFileIdleStateData typedIdleStateData = (MultiSourceSingleFileIdleStateData) idleStateData;

        _rwLock.writeLock().lock();
        try {
            if (typedResult.isProcessed()) {
                confirmedKey(memberName, typedIdleStateData.getLastReceivedKey(), false);
            } else if (typedResult.getError() == null) {
                confirmedKey(memberName, typedResult.getLastProcessedKey(), false);
            } else {
                handlePendingErrorBatchPackets(memberName,
                        getSpecificPackets(typedIdleStateData.getLastConfirmedKey() + 1, typedIdleStateData.getLastReceivedKey()),
                        typedResult.getError(),
                        typedResult.getLastProcessedKey() + 1);
                handleErrorResult(memberName, typedResult);
            }
        } finally {
            _rwLock.writeLock().unlock();
        }

    }

    @Override
    public void setPendingError(String memberName, Throwable error,
                                IIdleStateData idleStateData) {
        _rwLock.writeLock().lock();
        try {
            final MultiSourceSingleFileIdleStateData typedIdleStateData = (MultiSourceSingleFileIdleStateData) idleStateData;
            final List<IReplicationOrderedPacket> packets = getSpecificPackets(typedIdleStateData.getLastConfirmedKey() + 1, typedIdleStateData.getLastReceivedKey());
            handlePendingErrorBatchPackets(memberName, packets, error, typedIdleStateData.getLastConfirmedKey() + 1);
        } finally {
            _rwLock.writeLock().unlock();
        }
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
