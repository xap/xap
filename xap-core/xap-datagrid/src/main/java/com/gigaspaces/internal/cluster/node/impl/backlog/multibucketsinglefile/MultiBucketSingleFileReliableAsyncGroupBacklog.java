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

package com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.backlog.CompletedHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.NoSuchReplicationMemberException;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.IPacketFilteredHandler;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.MultiBucketSingleFileReliableAsyncState.AsyncTargetState;
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
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileHandshakeResponse;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO there's code duplication between this class and
// GlobalOrderReliableAsyncGroupBacklog
@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileReliableAsyncGroupBacklog
        extends AbstractMultiBucketSingleFileGroupBacklog
        implements IReplicationReliableAsyncGroupBacklog,
        IPacketFilteredHandler {

    private final Map<String, Long> _ongoingReliableAsyncHandshakeCompletion;
    private final SortedSet<IMultiBucketSingleFileReplicationOrderedPacket> _insertionSorter;

    public MultiBucketSingleFileReliableAsyncGroupBacklog(
            DynamicSourceGroupConfigHolder groupConfig, String name,
            IReplicationPacketDataProducer<?> dataProducer) {
        super(groupConfig, name, dataProducer);

        _ongoingReliableAsyncHandshakeCompletion = new HashMap<String, Long>();
        _insertionSorter = new TreeSet<IMultiBucketSingleFileReplicationOrderedPacket>(new PacketComperator());
    }

    public void add(ReliableAsyncReplicationGroupOutContext groupContext,
                    IEntryHolder entryHolder,
                    ReplicationSingleOperationType operationType) {
        if (!hasExistingMember())
            return;

        SingleBucketOrderedPacket packet = addSingleOperationPacket(groupContext,
                entryHolder,
                operationType);
        if (packet != null)
            groupContext.addOrderedPacket(packet);
    }

    public void addGeneric(
            ReliableAsyncReplicationGroupOutContext groupContext,
            Object operationData, ReplicationSingleOperationType operationType) {
        if (!hasExistingMember())
            return;

        SingleBucketOrderedPacket packet = addGenericOperationPacket(groupContext, operationData, operationType);
        if (packet != null)
            groupContext.addOrderedPacket(packet);
    }

    public void addTransaction(
            ReliableAsyncReplicationGroupOutContext groupContext,
            ServerTransaction transaction,
            ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType) {
        if (!hasExistingMember())
            return;

        IMultiBucketSingleFileReplicationOrderedPacket packet = addTransactionOperationPacket(groupContext,
                transaction,
                lockedEntries,
                operationType);
        if (packet != null)
            groupContext.addOrderedPacket(packet);
    }

    @Override
    public MultiBucketSingleFileHandshakeRequest getHandshakeRequest(
            String memberName, Object customBacklogMetadata) {
        _rwLock.writeLock().lock();
        try {
            MultiBucketSingleFileHandshakeRequest handshakeRequest = super.getHandshakeRequest(memberName, customBacklogMetadata);

            if (isAsyncKeeper(memberName)) {
                MultiBucketSingleFileReliableAsyncState reliableAsyncState = getReliableAsyncState(memberName);
                // We need to keep in the backlog all the packets from this
                // minimum during the handshake iteration process
                // otherwise the backlog can be deleted if the async targets
                // confirmed during the handshake iterations process
                _ongoingReliableAsyncHandshakeCompletion.put(memberName,
                        reliableAsyncState.getMinimumUnconfirmedKey());
                return new MultiBucketSingleFileReliableAsyncHandshakeRequest(handshakeRequest,
                        reliableAsyncState);
            }

            return handshakeRequest;
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    private boolean isAsyncKeeper(String memberName) {
        ReliableAsyncSourceGroupConfig sourceGroupConfig = getGroupConfigSnapshot();
        for (String syncMember : sourceGroupConfig.getSyncMembersLookupNames()) {
            if (syncMember.equals(memberName))
                return true;
        }

        return false;
    }

    public List<IReplicationOrderedPacket> getReliableAsyncPackets(
            String memberName, int maxSize,
            KeeperMemberState[] keeperMembersState,
            IReplicationChannelDataFilter dataFilter, PlatformLogicalVersion targetMemberVersion, Logger logger) {
        _rwLock.readLock().lock();
        try {
            long minConfirmedKey = Long.MAX_VALUE;
            for (KeeperMemberState keeperMemberState : keeperMembersState) {
                if (keeperMemberState.cannotBypass) {
                    MultiBucketSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(keeperMemberState.memberName);
                    if (!confirmationHolder.hadAnyHandshake()) {
                        minConfirmedKey = -1;
                        break;
                    }
                    long lastConfirmedKey = confirmationHolder.getGlobalLastConfirmedKey();
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
    public IHandshakeContext processHandshakeResponse(String memberName,
                                                      IBacklogHandshakeRequest request, IProcessLogHandshakeResponse response, PlatformLogicalVersion targetLogicalVersion, Object customBacklogMetadata) {
        super.processHandshakeResponse(memberName, request, response, targetLogicalVersion, customBacklogMetadata);
        if (!isAsyncKeeper(memberName))
            return new CompletedHandshakeContext();

        MultiBucketSingleFileHandshakeResponse typedResponse = (MultiBucketSingleFileHandshakeResponse) response;
        MultiBucketSingleFileReliableAsyncHandshakeRequest typedRequest = (MultiBucketSingleFileReliableAsyncHandshakeRequest) request;
        MultiBucketSingleFileReliableAsyncState reliableAsyncState = typedRequest.getReliableAsyncState();

        long minimumUnconfirmedKey = reliableAsyncState.getMinimumUnconfirmedKey();
        long lastProcessedKey = typedResponse.getLastProcessedGlobalKey();

        IHandshakeContext handshakeContext = new CompletedHandshakeContext();

        //False is indication that the process log created a temporary channel and this channel should not request
        //from the target to complete this target backlog if needed as this will result in logical deadlock.
        //This can only happen in concurrent replication where there is no total order between two members
        if (!Boolean.FALSE.equals(customBacklogMetadata)) {
            for (int i = 0; i < typedResponse.getLastProcessesKeysBuckets().length; i++) {
                if (_bucketLastKeys[i] <= typedResponse.getLastProcessesKeysBuckets()[i]) {
                    handshakeContext = new SynchronizeMissingPacketsHandshakeContext(_bucketLastKeys[i] - 1, typedResponse.getLastProcessesKeysBuckets()[i]);
                    break;
                }
            }
        }

        if (lastProcessedKey < minimumUnconfirmedKey) {
            _ongoingReliableAsyncHandshakeCompletion.remove(memberName);
            return handshakeContext;
        }

        if (handshakeContext.isDone()) {
            return new ReliableAsyncHandshakeContext(minimumUnconfirmedKey,
                    lastProcessedKey);
        }
        //We need to do both, synchronize missing packets and send reliable async packets to the keeper to store
        //for the async targets
        return new MultiStepHandshakeContext(handshakeContext, new ReliableAsyncHandshakeContext(minimumUnconfirmedKey,
                lastProcessedKey));
    }

    @Override
    public void reliableAsyncSourceKeep(String sourceMemberName,
                                        IReplicationOrderedPacket packet) {
        insertReliableAsyncPacket(sourceMemberName, packet, true);
    }

    public void reliableAsyncSourceAdd(String sourceMemberName,
                                       IReplicationOrderedPacket packet) {
        insertReliableAsyncPacket(sourceMemberName, packet, false);
    }

    private void insertReliableAsyncPacket(String sourceMemberName,
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
//            if (!allowOlderPackets && packet.getEndKey() <= getLastInsertedKeyToBacklogUnsafe())
//                throw new ReplicationInternalSpaceException("replication is out of sync, attempt to keep a reliable async packet in a keeper packlog which was already present in the backlog, new packet key is ["
//                                                            + packet.getEndKey()
//                                                            + "] while the last inserted key to backlog is ["
//                                                            + getLastInsertedKeyToBacklogUnsafe() + "]");
            //If we got packet that was already processes
            _insertionSorter.add((IMultiBucketSingleFileReplicationOrderedPacket) packet);
            for (Iterator<IMultiBucketSingleFileReplicationOrderedPacket> iterator = _insertionSorter.iterator(); iterator.hasNext(); ) {
                IMultiBucketSingleFileReplicationOrderedPacket nextPacket = iterator.next();
                // We check for <= because during iterative handshake completion
                // we get packets
                // with older next key because the next key is updated during
                // the handshake process
                // and the completion restore it backwards since it brings
                // packets from the past
                if (nextPacket.getKey() <= getNextKeyUnsafe()) {
                    ensureLimit(packet.getData());

                    // We advance the last key to be the added packets key + 1
                    // This method must be called in the consecutive processing order,
                    // hence the packet keys
                    // order should be strict ascending
                    if (nextPacket instanceof DeletedMultiBucketOrderedPacket)
                        setNextKeyUnsafe(((DeletedMultiBucketOrderedPacket) nextPacket).getEndKey() + 1);
                    else
                        setNextKeyUnsafe(nextPacket.getKey() + 1);

                    getBacklogFile().add(nextPacket);
                    increaseAllMembersWeight(nextPacket.getWeight(), nextPacket.getKey());

                    nextPacket.reliableAsyncKeysUpdate(_bucketLastKeys,
                            getConfirmationHolderUnsafe(sourceMemberName));
                    iterator.remove();
                } else
                    break;
            }
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public MultiBucketSingleFileReliableAsyncState getEntireReliableAsyncState() {
        Set<Entry<String, MultiBucketSingleFileConfirmationHolder>> asyncTargetsConfirmations = getAllConfirmations();
        return buildReliableAsyncState(asyncTargetsConfirmations);
    }

    @Override
    public MultiBucketSingleFileReliableAsyncState getReliableAsyncState(String targetMemberName) {
        Set<Entry<String, MultiBucketSingleFileConfirmationHolder>> asyncTargetsConfirmations = getAllConfirmations(targetMemberName);
        return buildReliableAsyncState(asyncTargetsConfirmations);
    }

    private MultiBucketSingleFileReliableAsyncState buildReliableAsyncState(
            Set<Entry<String, MultiBucketSingleFileConfirmationHolder>> asyncTargetsConfirmations) {
        AsyncTargetState[] asyncTargetStates = new AsyncTargetState[asyncTargetsConfirmations.size()];
        int index = 0;
        for (Entry<String, MultiBucketSingleFileConfirmationHolder> entry : asyncTargetsConfirmations) {
            MultiBucketSingleFileConfirmationHolder confirmationHolder = entry.getValue();
            asyncTargetStates[index++] = new AsyncTargetState(entry.getKey(),
                    confirmationHolder.hadAnyHandshake(),
                    confirmationHolder.getGlobalLastConfirmedKey(),
                    confirmationHolder.getBucketLastConfirmedKeys());
        }
        return new MultiBucketSingleFileReliableAsyncState(asyncTargetStates);
    }


    public void updateReliableAsyncState(IReliableAsyncState reliableAsyncState, String sourceMemberName) throws NoSuchReplicationMemberException, MissingReliableAsyncTargetStateException {
        MultiBucketSingleFileReliableAsyncState typedState = (MultiBucketSingleFileReliableAsyncState) reliableAsyncState;
        _rwLock.writeLock().lock();
        try {
            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(getLogPrefix()
                        + "incoming reliable async state update " + typedState);

            ReliableAsyncSourceGroupConfig sourceGroupConfig = getGroupConfigSnapshot();

            //This must be done before updating any member state, otherwise it can cause deletion of packets
            //that are not confirmed by the unknown member
            validateReliableAsyncUpdateTargetsMatch(reliableAsyncState, sourceMemberName);


            for (AsyncTargetState asyncTargetState : typedState.getAsyncTargetsState()) {
                if (asyncTargetState.hadAnyHandshake()) {
                    // TODO instead of sending all bucket keys everytime, send
                    // only changes
                    long lastConfirmedKey = asyncTargetState.getGlobalLastConfirmedKey();
                    long[] bucketLastConfirmedKeys = asyncTargetState.getBucketLastConfirmedKeys();
                    MultiBucketSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(asyncTargetState.getTargetMemberName());
                    confirmationHolder.overrideGlobalLastConfirmedKey(lastConfirmedKey);
                    for (int i = 0; i < bucketLastConfirmedKeys.length; i++)
                        confirmationHolder.getBucketLastConfirmedKeys()[i] = bucketLastConfirmedKeys[i];
                }
            }
            long minGlobalConfirmed;
            long[] minimumUnconfirmedBucketKeys;
            // We update all the sync keepers state to the minimum confirmed key
            // such that in case of a failover they will receive the proper
            // backlog
            if (typedState.getAsyncTargetsState().length > 0) {
                minGlobalConfirmed = typedState.getMinimumUnconfirmedKey() - 1;
                if (getNextKeyUnsafe() < minGlobalConfirmed + 1)
                    setNextKeyUnsafe(minGlobalConfirmed + 1);

                minimumUnconfirmedBucketKeys = typedState.getMinimumUnconfirmedBucketKeys();
            } else {
                minGlobalConfirmed = getNextKeyUnsafe() - 1;
                //if no async targets are defined - acknowledge all the packets
                minimumUnconfirmedBucketKeys = new long[_bucketLastKeys.length];

                for (int i = 0; i < minimumUnconfirmedBucketKeys.length; i++) {
                    minimumUnconfirmedBucketKeys[i] = _bucketLastKeys[i] - 1;
                }
            }

            MultiBucketSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(sourceMemberName);
            overrideConfirmationHolderAccordingToReliableAsyncState(minGlobalConfirmed,
                    minimumUnconfirmedBucketKeys,
                    confirmationHolder);
            clearConfirmedPackets();
        } finally {
            _rwLock.writeLock().unlock();
        }

    }

    private void overrideConfirmationHolderAccordingToReliableAsyncState(
            long minGlobalConfirmed, long[] minimumUnconfirmedBucketKeys,
            MultiBucketSingleFileConfirmationHolder confirmationHolder) {
        long lastConfirmedByTarget = confirmationHolder.getGlobalLastConfirmedKey();
        if (!confirmationHolder.hadAnyHandshake()
                || lastConfirmedByTarget < minGlobalConfirmed) {
            confirmationHolder.overrideGlobalLastConfirmedKey(minGlobalConfirmed);
            confirmationHolder.overrideBucketKeys(minimumUnconfirmedBucketKeys);
        }
    }

    public void afterHandshake(IProcessLogHandshakeResponse handshakeResponse) {
        // We adjust the next key to be the current last processed + 1 so if
        // this will become the active source it will start from the
        // proper key.
        MultiBucketSingleFileHandshakeResponse typedResponse = (MultiBucketSingleFileHandshakeResponse) handshakeResponse;
        _rwLock.writeLock().lock();
        try {
            long newNextKey = typedResponse.getLastProcessedGlobalKey() + 1;
            long[] lastProcessesKeysBuckets = typedResponse.getLastProcessesKeysBuckets();
            if (_logger.isLoggable(Level.FINER))
                _logger.finer(getLogPrefix() + " adjusting group key ["
                        + newNextKey
                        + "] and using last processed bucket keys "
                        + Arrays.toString(lastProcessesKeysBuckets));

            if (getNextKeyUnsafe() <= newNextKey) {
                if (_logger.isLoggable(Level.FINER))
                    _logger.finer(getLogPrefix() + " adjusting group key ["
                            + newNextKey + "]");
                setNextKeyUnsafe(newNextKey);
            }
            _logger.finer(getLogPrefix() + " adjusting group bucket keys, existing keys " + Arrays.toString(_bucketLastKeys)
                    + " using last processed bucket keys "
                    + Arrays.toString(lastProcessesKeysBuckets));

            for (int i = 0; i < lastProcessesKeysBuckets.length; i++) {
                long nextBucketKey = lastProcessesKeysBuckets[i] + 1;
                if (nextBucketKey > _bucketLastKeys[i])
                    _bucketLastKeys[i] = nextBucketKey;
            }
        } finally {
            _rwLock.writeLock().unlock();
        }

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

    // Must be called under readLock
    @Override
    protected long getInitialMaxAllowedDeleteUpTo() {
        long minimumUnconfirmedKey = super.getInitialMaxAllowedDeleteUpTo();

        for (Long minKey : _ongoingReliableAsyncHandshakeCompletion.values())
            minimumUnconfirmedKey = Math.min(minimumUnconfirmedKey, minKey);

        return minimumUnconfirmedKey;
    }

    @Override
    public IHandshakeIteration getNextHandshakeIteration(String memberName,
                                                         IHandshakeContext handshakeContext) {
        if (handshakeContext instanceof MultiStepHandshakeContext)
            return getNextHandshakeIteration(memberName, ((MultiStepHandshakeContext) handshakeContext).getCurrentStep());

        if (handshakeContext instanceof SynchronizeMissingPacketsHandshakeContext) {
            //This iterative handshake will always have only one iteration.
            ((SynchronizeMissingPacketsHandshakeContext) handshakeContext).setDone();
            return new SynchronizeMissingPacketsHandshakeIteration();
        }

        ReliableAsyncHandshakeContext typedHandshakeContext = (ReliableAsyncHandshakeContext) handshakeContext;
        ReliableAsyncSourceGroupConfig sourceGroupConfig = getGroupConfigSnapshot();
        long fromKey = typedHandshakeContext.getMinimumUnsentKey();
        long upToKey = typedHandshakeContext.getLastProcessedKey();
        int maxSize = sourceGroupConfig.getBacklogCompletionBatchSize();
        List<IReplicationOrderedPacket> packets = getPacketsWithFullSerializedContent(memberName, fromKey, upToKey, maxSize);

        final long minimumUnsentKey = packets.isEmpty() ? upToKey + 1 : packets.get(packets.size() - 1).getKey() + 1;

        typedHandshakeContext.setMinimumUnsentKey(minimumUnsentKey);

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
    protected IPacketFilteredHandler getFilteredHandler() {
        return this;
    }

    public IReplicationOrderedPacket packetFiltered(
            IReplicationOrderedPacket beforeFilter,
            IReplicationOrderedPacket afterFilter,
            IReplicationGroupBacklog groupBacklog,
            String targetMemberName) {
        long minimumAsyncTargetsUnconfirmedKey = getReliableAsyncState(targetMemberName).getMinimumUnconfirmedKey();
        if (afterFilter.getKey() < minimumAsyncTargetsUnconfirmedKey)
            return _defaultFilteredHandler.packetFiltered(beforeFilter, afterFilter, groupBacklog, targetMemberName);

        IMultiBucketSingleFileReplicationOrderedPacket typedBeforeFilter = (IMultiBucketSingleFileReplicationOrderedPacket) beforeFilter;
        IMultiBucketSingleFileReplicationOrderedPacket typedAfterFilter = (IMultiBucketSingleFileReplicationOrderedPacket) afterFilter;
        return new MultiBucketSingleFileReliableAsyncKeptDiscardedPacket(typedBeforeFilter, typedAfterFilter);
    }

    public static class PacketComperator
            implements
            Comparator<IMultiBucketSingleFileReplicationOrderedPacket> {

        public int compare(IMultiBucketSingleFileReplicationOrderedPacket o1,
                           IMultiBucketSingleFileReplicationOrderedPacket o2) {
            return (o1.getKey() < o2.getKey()) ? -1
                    : o1.getKey() == o2.getKey() ? 0
                    : 1;
        }

    }

    @Override
    public String dumpState() {
        return super.dumpState() + StringUtils.NEW_LINE
                + "Reliable async state " + getEntireReliableAsyncState()
                + StringUtils.NEW_LINE
                + "Ongoing Reliable Async Handshake Completion "
                + _ongoingReliableAsyncHandshakeCompletion + StringUtils.NEW_LINE
                + "Insertion sorter " + _insertionSorter;
    }


}
