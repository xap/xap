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

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.backlog.CompletedHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.NoSuchReplicationMemberException;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderConfirmationHolder;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDeletedBacklogPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderReliableAsyncKeptDiscardedOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.IPacketFilteredHandler;
import com.gigaspaces.internal.cluster.node.impl.backlog.multisourcesinglefile.MultiSourceSingleFileReliableAsyncState.MultiSourceAsyncTargetState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReplicationReliableAsyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.MissingReliableAsyncTargetStateException;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.ReliableAsyncHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.ReliableAsyncHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.SynchronizeMissingPacketsHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.SynchronizeMissingPacketsHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncSourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataProducer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderReliableAsyncKeeperProcessLogHandshakeResponse;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

@com.gigaspaces.api.InternalApi
public class MultiSourceSingleFileReliableAsyncGroupBacklog extends AbstractMultiSourceSingleFileGroupBacklog
        implements IReplicationReliableAsyncGroupBacklog, IPacketFilteredHandler {

    protected final Map<String, Long> _ongoingReliableAsyncHandshakeCompletion;

    public MultiSourceSingleFileReliableAsyncGroupBacklog(
            DynamicSourceGroupConfigHolder groupConfig, String name,
            IReplicationPacketDataProducer<?> dataProducer) {
        super(groupConfig, name, dataProducer);
        _ongoingReliableAsyncHandshakeCompletion = new HashMap<String, Long>();
    }

    @Override
    public void add(ReliableAsyncReplicationGroupOutContext groupContext,
                    IEntryHolder entryHolder,
                    ReplicationSingleOperationType operationType) {
        if (!hasExistingMember())
            return;

        IReplicationOrderedPacket packet = addSingleOperationPacket(groupContext,
                entryHolder,
                operationType);
        if (packet != null)
            groupContext.addOrderedPacket(packet);
    }

    @Override
    public void addGeneric(
            ReliableAsyncReplicationGroupOutContext groupContext,
            Object operationData, ReplicationSingleOperationType operationType) {
        if (!hasExistingMember())
            return;

        IReplicationOrderedPacket packet = addGenericOperationPacket(groupContext,
                operationData,
                operationType);
        if (packet != null)
            groupContext.addOrderedPacket(packet);
    }

    @Override
    public void addTransaction(
            ReliableAsyncReplicationGroupOutContext groupContext,
            ServerTransaction transaction,
            ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType) {
        if (!hasExistingMember())
            return;

        IReplicationOrderedPacket packet = addTransactionOperationPacket(groupContext,
                transaction,
                lockedEntries,
                operationType);
        if (packet != null)
            groupContext.addOrderedPacket(packet);

    }

    @Override
    public void reliableAsyncSourceAdd(String sourceMemberName,
                                       IReplicationOrderedPacket packet) {
        insertReliableAsyncAddedPacket(sourceMemberName, packet, false);
    }

    @Override
    public void reliableAsyncSourceKeep(String sourceMemberName,
                                        IReplicationOrderedPacket packet) {
        insertReliableAsyncAddedPacket(sourceMemberName, packet, true);
    }

    private void insertReliableAsyncAddedPacket(String sourceMemberName,
                                                IReplicationOrderedPacket packet, boolean allowOlderPackets) {
        //If there are no reliable async targets, no need to keep the packet
        ReliableAsyncSourceGroupConfig groupConfig = getGroupConfigSnapshot();
        if (groupConfig.getAsyncMembersLookupNames().length == 0 && groupConfig.getSyncMembersLookupNames().length <= 1)
            return;

        // we assume that when reliableAsyncSourceAdd is called,
        // there
        // cannot be interleaving calls to regular add methods since it will
        // screw up the keys
        _rwLock.writeLock().lock();
        try {
            ensureLimit();
            // We advance the last key to be the added packets key + 1
            // This method must be called in the consecutive processing order,
            // hence the packet keys
            // order should be strict ascending
            if (packet.getEndKey() <= getLastInsertedKeyToBacklogUnsafe()) {
                //Safety check we are not somehow inserting packets with non strict ascending order                
//                if (!allowOlderPackets)
//                    throw new ReplicationInternalSpaceException("replication is out of sync, attempt to keep a reliable async packet in a keeper backlog with non strict ascending order, new packet key is ["
//                                                                + packet.getEndKey()
//                                                                + "] while the last inserted key to backlog is ["
//                                                                + getLastInsertedKeyToBacklogUnsafe() + "]");

                if (!getBacklogFile().isEmpty()) {
                    IReplicationOrderedPacket lastPacketInBacklog = getBacklogFile().readOnlyIterator(getBacklogFile().size() - 1).next();
                    if (lastPacketInBacklog.getEndKey() != packet.getKey() - 1)
                        throw new ReplicationInternalSpaceException("replication is out of sync, attempt to keep a reliable async packet in a keeper backlog with non strict ascending order, new packet key is ["
                                + packet.getEndKey()
                                + "] while the last existing packet key in the backlog is ["
                                + lastPacketInBacklog.getEndKey() + "]");
                }
            }

            if (packet instanceof GlobalOrderDeletedBacklogPacket)
                setNextKeyUnsafe(((GlobalOrderDeletedBacklogPacket) packet).getEndKey() + 1);
            else
                setNextKeyUnsafe(packet.getKey() + 1);

            getBacklogFile().add(packet);

            MultiSourceSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(sourceMemberName);
            decreaseWeight(sourceMemberName, confirmationHolder.getLastConfirmedKey(), packet.getKey());
            confirmationHolder.setLastConfirmedKey(packet.getKey());
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    @Override
    public MultiSourceSingleFileReliableAsyncState getEntireReliableAsyncState() {
        Set<Entry<String, MultiSourceSingleFileConfirmationHolder>> asyncTargetsConfirmations = getAllConfirmations();
        return buildReliableAsyncState(asyncTargetsConfirmations);
    }

    @Override
    public void updateReliableAsyncState(IReliableAsyncState reliableAsyncState, String sourceMemberName) throws NoSuchReplicationMemberException, MissingReliableAsyncTargetStateException {
        MultiSourceSingleFileReliableAsyncState sharedAsyncState = (MultiSourceSingleFileReliableAsyncState) reliableAsyncState;
        _rwLock.writeLock().lock();
        try {
            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(getLogPrefix()
                        + "incoming reliable async state update "
                        + Arrays.toString(sharedAsyncState.getAsyncTargetsState()));

            ReliableAsyncSourceGroupConfig sourceGroupConfig = getGroupConfigSnapshot();

            //This must be done before updating any member state, otherwise it can cause deletion of packets
            //that are not confirmed by the unknown member
            validateReliableAsyncUpdateTargetsMatch(reliableAsyncState, sourceMemberName);

            for (MultiSourceAsyncTargetState asyncTargetState : sharedAsyncState.getAsyncTargetsState()) {
                if (asyncTargetState.hadAnyHandshake()) {
                    long lastConfirmedKey = asyncTargetState.getLastConfirmedKey();
                    long lastReceivedKey = asyncTargetState.getLastReceivedKey();
                    final MultiSourceSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(asyncTargetState.getTargetMemberName());
                    decreaseWeight(sourceMemberName, lastReceivedKey, lastConfirmedKey);
                    confirmationHolder.setLastConfirmedKey(lastConfirmedKey);
                    confirmationHolder.setLastReceivedKey(lastReceivedKey);
                }
            }
            long minConfirmed = getNextKeyUnsafe() - 1;
            // We update all the sync keepers state to the minimum confirmed key
            // such that in case of a failover they will receive the proper
            // backlog
            if (sharedAsyncState.getAsyncTargetsState().length > 0) {
                minConfirmed = sharedAsyncState.getMinimumUnconfirmedKey() - 1;
                if (getNextKeyUnsafe() < minConfirmed + 1)
                    setNextKeyUnsafe(minConfirmed + 1);
            }

            MultiSourceSingleFileConfirmationHolder lastConfirmedBySource = getConfirmationHolderUnsafe(sourceMemberName);
            if (!lastConfirmedBySource.hadAnyHandshake()
                    || lastConfirmedBySource.getLastConfirmedKey() < minConfirmed) {
                decreaseWeight(sourceMemberName, lastConfirmedBySource.getLastConfirmedKey(), minConfirmed);
                lastConfirmedBySource.setLastConfirmedKey(minConfirmed);
            }
            clearConfirmedPackets();
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    @Override
    public List<IReplicationOrderedPacket> getReliableAsyncPackets(
            String memberName, int maxSize,
            KeeperMemberState[] keeperMembersState,
            IReplicationChannelDataFilter dataFilter, PlatformLogicalVersion targetMemberVersion, Logger logger) {
        _rwLock.readLock().lock();
        try {
            long minConfirmedKey = Long.MAX_VALUE;
            for (KeeperMemberState keeperMemberState : keeperMembersState) {
                if (keeperMemberState.cannotBypass) {
                    GlobalOrderConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(keeperMemberState.memberName);
                    if (!confirmationHolder.hadAnyHandshake()) {
                        minConfirmedKey = -1;
                        break;
                    }
                    long lastConfirmedKey = confirmationHolder.getLastConfirmedKey();
                    minConfirmedKey = Math.min(minConfirmedKey,
                            lastConfirmedKey);
                }
            }
            if (minConfirmedKey == -1)
                return new LinkedList<IReplicationOrderedPacket>();

            return getPacketsUnsafe(memberName,
                    maxSize,
                    minConfirmedKey,
                    dataFilter,
                    _defaultFilteredHandler,
                    targetMemberVersion,
                    logger);
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    @Override
    public void afterHandshake(IProcessLogHandshakeResponse handshakeResponse) {
        // We adjust the next key to be the current last processed + 1 so if
        // this will become the active source it will start from the
        // proper key.
        GlobalOrderProcessLogHandshakeResponse typedResponse = (GlobalOrderProcessLogHandshakeResponse) handshakeResponse;
        _rwLock.writeLock().lock();
        try {
            long newNextKey = typedResponse.getLastProcessedKey() + 1;

            if (getNextKeyUnsafe() <= newNextKey) {
                if (_logger.isLoggable(Level.FINER))
                    _logger.finer(getLogPrefix() + " adjusting group key ["
                            + newNextKey + "]");
                setNextKeyUnsafe(newNextKey);
            } else {
                if (_logger.isLoggable(Level.FINER))
                    _logger.finer(getLogPrefix() + " received next key ["
                            + newNextKey + "] is older than the existing one ["
                            + getNextKeyUnsafe()
                            + "], skipping local group adjustment group key");
            }

            //Also send the last key we contain in the backlog;
            if (typedResponse instanceof GlobalOrderReliableAsyncKeeperProcessLogHandshakeResponse) {
                long lastKeyInBacklog = getBacklogFile().isEmpty() ? -1 : getLastInsertedKeyToBacklogUnsafe();
                ((GlobalOrderReliableAsyncKeeperProcessLogHandshakeResponse) typedResponse).setLastKeyInBacklog(lastKeyInBacklog);
            }
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    @Override
    public String dumpState() {
        return super.dumpState() + StringUtils.NEW_LINE + "Reliable async state "
                + getEntireReliableAsyncState() + StringUtils.NEW_LINE
                + "Ongoing Reliable Async Handshake Completion "
                + _ongoingReliableAsyncHandshakeCompletion;
    }

    @Override
    protected MultiSourceSingleFileBacklogHandshakeRequest getHandshakeRequestImpl(
            String memberName,
            MultiSourceSingleFileBacklogHandshakeRequest handshakeRequest) {
        if (isAsyncKeeper(memberName)) {
            MultiSourceSingleFileReliableAsyncState reliableAsyncState = getReliableAsyncState(memberName);
            // We need to keep in the backlog all the packets from this
            // minimum during the handshake iteration process
            // otherwise the backlog can be deleted if the async targets
            // confirmed during the handshake iterations process
            _ongoingReliableAsyncHandshakeCompletion.put(memberName,
                    reliableAsyncState.getMinimumUnconfirmedKey());
            return new MultiSourceSingleFileReliableAsyncBacklogHandshakeRequest(handshakeRequest,
                    reliableAsyncState);
        }

        return handshakeRequest;
    }

    @Override
    protected IHandshakeContext getHandshakeContext(String memberName,
                                                    IBacklogHandshakeRequest request,
                                                    PlatformLogicalVersion targetLogicalVersion,
                                                    GlobalOrderProcessLogHandshakeResponse typedResponse,
                                                    long lastProcessedKey) {
        if (!isAsyncKeeper(memberName))
            return new CompletedHandshakeContext();

        MultiSourceSingleFileReliableAsyncBacklogHandshakeRequest typedRequest = (MultiSourceSingleFileReliableAsyncBacklogHandshakeRequest) request;
        MultiSourceSingleFileReliableAsyncState reliableAsyncState = typedRequest.getReliableAsyncState();

        long minimumUnconfirmedKey = -1;
        long lastInsertedPacketToBacklog = getLastInsertedKeyToBacklogUnsafe();
        //This async keeper has packets that this source never encountered, ask the target to complete this missing packets first
        if (lastInsertedPacketToBacklog < lastProcessedKey) {
            _ongoingReliableAsyncHandshakeCompletion.remove(memberName);
            return new SynchronizeMissingPacketsHandshakeContext(lastInsertedPacketToBacklog, lastProcessedKey);
        }

        minimumUnconfirmedKey = ((GlobalOrderReliableAsyncKeeperProcessLogHandshakeResponse) typedResponse).getLastKeyInBacklog() + 1;

        minimumUnconfirmedKey = Math.max(minimumUnconfirmedKey, reliableAsyncState.getMinimumUnconfirmedKey());
        if (lastProcessedKey < minimumUnconfirmedKey) {
            _ongoingReliableAsyncHandshakeCompletion.remove(memberName);
            return new CompletedHandshakeContext();
        }

        return new ReliableAsyncHandshakeContext(minimumUnconfirmedKey,
                lastProcessedKey);
    }

    private boolean isAsyncKeeper(String memberName) {
        ReliableAsyncSourceGroupConfig sourceGroupConfig = getGroupConfigSnapshot();
        for (String syncMember : sourceGroupConfig.getSyncMembersLookupNames()) {
            if (syncMember.equals(memberName))
                return true;
        }

        return false;
    }

    public MultiSourceSingleFileReliableAsyncState getReliableAsyncState(String targetMemberLookupName) {
        Set<Entry<String, MultiSourceSingleFileConfirmationHolder>> asyncTargetsConfirmations = getAllConfirmations(targetMemberLookupName);
        return buildReliableAsyncState(asyncTargetsConfirmations);
    }

    protected MultiSourceSingleFileReliableAsyncState buildReliableAsyncState(
            Set<Entry<String, MultiSourceSingleFileConfirmationHolder>> asyncTargetsConfirmations) {
        MultiSourceAsyncTargetState[] asyncTargetStates = new MultiSourceAsyncTargetState[asyncTargetsConfirmations.size()];
        int index = 0;
        for (Entry<String, MultiSourceSingleFileConfirmationHolder> entry : asyncTargetsConfirmations) {
            asyncTargetStates[index++] = new MultiSourceAsyncTargetState(entry.getKey(), entry.getValue());
        }
        return new MultiSourceSingleFileReliableAsyncState(asyncTargetStates);
    }

    @Override
    public IReplicationOrderedPacket packetFiltered(
            IReplicationOrderedPacket beforeFilter,
            IReplicationOrderedPacket afterFilter,
            IReplicationGroupBacklog groupBacklog,
            String targetMemberName) {
        long minimumAsyncTargetsUnconfirmedKey = getReliableAsyncState(targetMemberName).getMinimumUnconfirmedKey();
        if (afterFilter.getKey() < minimumAsyncTargetsUnconfirmedKey)
            return _defaultFilteredHandler.packetFiltered(beforeFilter, afterFilter, groupBacklog, targetMemberName);

        return new GlobalOrderReliableAsyncKeptDiscardedOrderedPacket(beforeFilter,
                afterFilter);
    }

    @Override
    protected IPacketFilteredHandler getFilteredHandler() {
        return this;
    }

    @Override
    public IHandshakeIteration getNextHandshakeIteration(String memberName,
                                                         IHandshakeContext handshakeContext) {
        if (handshakeContext instanceof SynchronizeMissingPacketsHandshakeContext) {
            //This iterative handshake will always have only one iteration.
            ((SynchronizeMissingPacketsHandshakeContext) handshakeContext).setDone();
            return new SynchronizeMissingPacketsHandshakeIteration();
        }

        return handleReliableAsyncBacklogCompletionIterativeHandshake(memberName, (ReliableAsyncHandshakeContext) handshakeContext);

    }

    private IHandshakeIteration handleReliableAsyncBacklogCompletionIterativeHandshake(String memberName,
                                                                                       ReliableAsyncHandshakeContext handshakeContext) {
        ReliableAsyncSourceGroupConfig sourceGroupConfig = getGroupConfigSnapshot();
        long fromKey = handshakeContext.getMinimumUnsentKey();
        long upToKey = handshakeContext.getLastProcessedKey();
        int maxSize = sourceGroupConfig.getBacklogCompletionBatchSize();
        List<IReplicationOrderedPacket> packets = getPacketsWithFullSerializedContent(fromKey,
                upToKey,
                maxSize);

        final long minimumUnsentKey = packets.isEmpty() ? upToKey + 1 : packets.get(packets.size() - 1).getKey() + 1;

        handshakeContext.setMinimumUnsentKey(minimumUnsentKey);

        if (handshakeContext.isDone()) {
            _rwLock.writeLock().lock();
            try {
                _ongoingReliableAsyncHandshakeCompletion.remove(memberName);
                clearConfirmedPackets();
            } finally {
                _rwLock.writeLock().unlock();
            }
        }

        return new ReliableAsyncHandshakeIteration(packets);
    }

    @Override
    protected long getMinimumUnconfirmedKeyUnsafe() {
        long minimumUnconfirmedKey = super.getMinimumUnconfirmedKeyUnsafe();
        if (minimumUnconfirmedKey == -1)
            return minimumUnconfirmedKey;

        for (Long minKey : _ongoingReliableAsyncHandshakeCompletion.values())
            minimumUnconfirmedKey = Math.min(minimumUnconfirmedKey, minKey);
        return minimumUnconfirmedKey;
    }

    //Must be called under readLock
    @Override
    protected long getInitialMaxAllowedDeleteUpTo() {
        long minimumUnconfirmedKey = super.getInitialMaxAllowedDeleteUpTo();

        for (Long minKey : _ongoingReliableAsyncHandshakeCompletion.values())
            minimumUnconfirmedKey = Math.min(minimumUnconfirmedKey, minKey);

        return minimumUnconfirmedKey;
    }

    @Override
    public boolean mergeWithDiscarded(
            IReplicationOrderedPacket previousDiscardedPacket,
            IReplicationOrderedPacket mergedPacket, String memberName) {
        if (previousDiscardedPacket instanceof GlobalOrderDiscardedReplicationPacket) {
            ReliableAsyncSourceGroupConfig reliableAsyncGroupconfig = getGroupConfigSnapshot();
            if (reliableAsyncGroupconfig.getChannelConfig(memberName) != null && !mergedPacket.getData().isMultiParticipantData()) {
                GlobalOrderDiscardedReplicationPacket typedDiscardedPacket = (GlobalOrderDiscardedReplicationPacket) previousDiscardedPacket;
                typedDiscardedPacket.setEndKey(mergedPacket.getKey());
                return true;
            }
        }
        return false;
    }

}
