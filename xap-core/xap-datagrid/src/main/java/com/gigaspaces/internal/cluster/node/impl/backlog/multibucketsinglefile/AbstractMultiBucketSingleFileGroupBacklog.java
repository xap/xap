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
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.ISyncReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataProducer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessResult;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ShortLongIterator;
import com.gigaspaces.internal.collections.ShortLongMap;
import com.gigaspaces.internal.collections.ShortObjectMap;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;


public abstract class AbstractMultiBucketSingleFileGroupBacklog
        extends
        AbstractSingleFileGroupBacklog<IMultiBucketSingleFileReplicationOrderedPacket, MultiBucketSingleFileConfirmationHolder> {

    protected final long[] _bucketLastKeys;
    protected final ShortLongMap _bucketsDeletionState;
    private final short _numberOfBuckets;

    public AbstractMultiBucketSingleFileGroupBacklog(
            DynamicSourceGroupConfigHolder sourceConfig, String name,
            IReplicationPacketDataProducer<?> dataProducer) {
        super(sourceConfig, name, dataProducer);
        MultiBucketSingleFileBacklogConfig backlogConfig = (MultiBucketSingleFileBacklogConfig) getGroupConfigSnapshot().getBacklogConfig();

        _numberOfBuckets = backlogConfig.getBucketsCount();
        _bucketLastKeys = new long[_numberOfBuckets];
        _bucketsDeletionState = CollectionsFactory.getInstance().createShortLongMap();
    }

    @Override
    protected Map<String, MultiBucketSingleFileConfirmationHolder> createConfirmationMap(SourceGroupConfig groupConfig) {
        HashMap<String, MultiBucketSingleFileConfirmationHolder> result = new HashMap<String, MultiBucketSingleFileConfirmationHolder>();
        short bucketsCount = ((MultiBucketSingleFileBacklogConfig) groupConfig.getBacklogConfig()).getBucketsCount();
        for (String member : groupConfig.getMembersLookupNames()) {
            long[] bucketLastConfirmedKeys = createEmptyBucketConfirmation(bucketsCount);
            result.put(member, new MultiBucketSingleFileConfirmationHolder(bucketLastConfirmedKeys));
        }
        return result;
    }

    @Override
    protected MultiBucketSingleFileConfirmationHolder createNewConfirmationHolder() {
        MultiBucketSingleFileConfirmationHolder confirmationHolder = new MultiBucketSingleFileConfirmationHolder(createEmptyBucketConfirmation(_numberOfBuckets));
        updateConfirmationHolderToCurrentLast(confirmationHolder);
        return confirmationHolder;
    }

    private long[] createEmptyBucketConfirmation(short bucketsCount) {
        long[] bucketLastConfirmedKeys = new long[bucketsCount];
        for (int i = 0; i < bucketLastConfirmedKeys.length; i++) {
            bucketLastConfirmedKeys[i] = -1;
        }
        return bucketLastConfirmedKeys;
    }

    protected MultiBucketSingleFileConfirmationHolder getConfirmationHolder(String memberName) {
        _rwLock.readLock().lock();
        try {
            return getConfirmationHolderUnsafe(memberName);
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    public MultiBucketSingleFileHandshakeRequest getHandshakeRequest(
            String memberName, Object customBacklogMetadata) {
        _rwLock.writeLock().lock();
        try {
            MultiBucketSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
            long globalLastConfirmedKey = confirmationHolder.getGlobalLastConfirmedKey();
            long[] lastConfirmedKeys = confirmationHolder.getBucketLastConfirmedKeys();
            // Is first handshake if all global last confirmed key is null
            boolean isFirstHandshake = !confirmationHolder.hadAnyHandshake();
            return new MultiBucketSingleFileHandshakeRequest(isFirstHandshake ? -1
                    : globalLastConfirmedKey,
                    lastConfirmedKeys,
                    isFirstHandshake);
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public IHandshakeContext processHandshakeResponse(String memberName,
                                                      IBacklogHandshakeRequest request, IProcessLogHandshakeResponse response, PlatformLogicalVersion targetLogicalVersion, Object customBacklogMetadata) {
        _rwLock.writeLock().lock();
        try {
            MultiBucketSingleFileHandshakeResponse typedResponse = (MultiBucketSingleFileHandshakeResponse) response;
            long[] lastProcessesKeysBuckets = typedResponse.getLastProcessesKeysBuckets();
            long lastProcessedGlobalKey = typedResponse.getLastProcessedGlobalKey();
            MultiBucketSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
            long[] lastConfirmedKeys = confirmationHolder.getBucketLastConfirmedKeys();
            confirmationHolder.overrideGlobalLastConfirmedKey(lastProcessedGlobalKey);
            for (int i = 0; i < lastConfirmedKeys.length; i++) {
                lastConfirmedKeys[i] = lastProcessesKeysBuckets[i];
            }
            return new CompletedHandshakeContext();
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public IBacklogMemberState getState(String memberName) {
        _rwLock.readLock().lock();
        try {
            MultiBucketSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
            if (confirmationHolder == null)
                return NonExistingBacklogMemberState.INSTANCE;
            long lastConfirmedKey = confirmationHolder.getGlobalLastConfirmedKey();
            boolean hadAnyHandshake = confirmationHolder.hadAnyHandshake();
            Throwable pendingError = confirmationHolder.hasPendingError() ? confirmationHolder.getPendingError() : null;

            boolean backlogDropped = _outOfSyncDueToDeletionTargets.contains(memberName);
            return new MultiBucketSingleFileBacklogMemberState(memberName,
                    hadAnyHandshake,
                    lastConfirmedKey,
                    confirmationHolder.getBucketLastConfirmedKeys(),
                    backlogDropped,
                    pendingError);
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    public void processResult(String memberName, IProcessResult result,
                              List<IReplicationOrderedPacket> packets)
            throws ReplicationException {
        _rwLock.writeLock().lock();
        try {
            MultiBucketSingleFileProcessResult typedResult = (MultiBucketSingleFileProcessResult) result;
            if (typedResult.isProcessed()) {
                MultiBucketSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
                IMultiBucketSingleFileReplicationOrderedPacket typedPacket = null;
                long lastConfirmedKey = confirmationHolder.getGlobalLastConfirmedKey();
                for (IReplicationOrderedPacket packet : packets) {
                    typedPacket = (IMultiBucketSingleFileReplicationOrderedPacket) packet;
                    lastConfirmedKey = typedPacket.processResult(memberName,
                            typedResult,
                            confirmationHolder);
                }
                cleanPendingErrorStateIfNeeded(memberName, lastConfirmedKey, confirmationHolder);
                clearConfirmedPackets();
            } else {
                IReplicationOrderedPacket lastPacket = packets.get(packets.size() - 1);
                Throwable error = typedResult.getError();
                handlePendingErrorBatchPackets(memberName, packets, error, lastPacket.getKey());
                throw new ReplicationException(error.getMessage(), error);
            }
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    public void processResult(String memberName, IProcessResult result,
                              IReplicationOrderedPacket packet) throws ReplicationException {
        _rwLock.writeLock().lock();
        try {
            MultiBucketSingleFileProcessResult typedResult = (MultiBucketSingleFileProcessResult) result;
            if (typedResult.isProcessed()) {
                IMultiBucketSingleFileReplicationOrderedPacket typedPacket = (IMultiBucketSingleFileReplicationOrderedPacket) packet;
                MultiBucketSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
                long lastConfirmedKey = typedPacket.processResult(memberName,
                        typedResult,
                        confirmationHolder);
                cleanPendingErrorStateIfNeeded(memberName, lastConfirmedKey, confirmationHolder);
                clearConfirmedPackets();
            } else {
                Throwable error = typedResult.getError();
                handlePendingErrorSinglePacket(memberName, packet, error);
                throw new ReplicationException(error.getMessage(), error);
            }
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    @Override
    protected long getLastConfirmedKeyUnsafe(String memberLookupName) {
        MultiBucketSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberLookupName);
        return confirmationHolder.hadAnyHandshake() ? confirmationHolder.getGlobalLastConfirmedKey() : -1;
    }

    @Override
    protected long getMemberUnconfirmedKey(MultiBucketSingleFileConfirmationHolder value) {
        return value.hadAnyHandshake() ? value.getGlobalLastConfirmedKey() : -1;
    }

    @Override
    protected DeletedMultiBucketOrderedPacket createBacklogOverflowPacket(
            long globalLastConfirmedKey, long firstKeyInBacklog,
            String memberName) {
        ShortObjectMap<BucketKey> bucketStartKeys = CollectionsFactory.getInstance().createShortObjectMap();
        ShortLongMap bucketEndKeys = CollectionsFactory.getInstance().createShortLongMap();

        //First we want to clear from the bucket deletion state obsolete data
        //we scan and remove deletion state of bucket below the min confirmation from
        //all targets
        Collection<MultiBucketSingleFileConfirmationHolder> allConfirmations = getAllConfirmationHoldersUnsafe();

        long[] minConfirmation = null;
        for (MultiBucketSingleFileConfirmationHolder confirmationHolder : allConfirmations) {
            if (minConfirmation == null) {
                minConfirmation = confirmationHolder.getBucketLastConfirmedKeys();
                continue;
            }
            long[] bucketLastConfirmedKeys = confirmationHolder.getBucketLastConfirmedKeys();
            for (short i = 0; i < bucketLastConfirmedKeys.length; i++) {
                if (minConfirmation[i] == -1)
                    continue;
                if (bucketLastConfirmedKeys[i] == -1 || bucketLastConfirmedKeys[i] < minConfirmation[i])
                    minConfirmation[i] = bucketLastConfirmedKeys[i];
            }
        }

        long[] bucketLastConfirmedKeys = getConfirmationHolderUnsafe(memberName).getBucketLastConfirmedKeys();

        for (ShortLongIterator it = _bucketsDeletionState.iterator(); it.hasNext(); ) {
            it.advance();
            short bucketIndex = it.key();
            long lastDeletedKey = it.value();

            //If the min confirmed key is higher than the last deleted key of this bucket, 
            //we can remove this bucket deleted state since all targets are already aware of it
            long minBucketConfirmedKey = minConfirmation[bucketIndex];
            if (minBucketConfirmedKey != -1 && minBucketConfirmedKey > lastDeletedKey) {
                it.remove();
                continue;
            }
            long bucketLastConfirmedKey = bucketLastConfirmedKeys[bucketIndex];

            if (lastDeletedKey > bucketLastConfirmedKey) {
                bucketStartKeys.put(bucketIndex, new BucketKey(bucketLastConfirmedKey + 1));
                bucketEndKeys.put(bucketIndex, lastDeletedKey);
            }
        }

        return new DeletedMultiBucketOrderedPacket(globalLastConfirmedKey + 1,
                firstKeyInBacklog - 1,
                bucketStartKeys,
                bucketEndKeys);
    }

    @Override
    protected void onBeginSynchronization(String memberName) {
        MultiBucketSingleFileConfirmationHolder confirmationHolder = getConfirmationHolderUnsafe(memberName);
        updateConfirmationHolderToCurrentLast(confirmationHolder);
    }

    protected void updateConfirmationHolderToCurrentLast(
            MultiBucketSingleFileConfirmationHolder confirmationHolder) {
        confirmationHolder.overrideGlobalLastConfirmedKey(getNextKeyUnsafe() - 1);
        confirmationHolder.overrideBucketKeys(_bucketLastKeys);
    }

    public SingleBucketOrderedPacket addSingleOperationPacket(
            ISyncReplicationGroupOutContext groupContext,
            IEntryHolder entryHolder,
            ReplicationSingleOperationType operationType) {
        // Convert operation to data
        ReplicationOutContext outContext = groupContext.getEntireContext();
        IReplicationPacketData<?> data = getDataProducer().createSingleOperationData(entryHolder,
                operationType,
                outContext);

        return createAndInsertSingleBucketPacket(data, outContext);
    }

    public SingleBucketOrderedPacket addGenericOperationPacket(
            ISyncReplicationGroupOutContext groupContext,
            Object operationData,
            ReplicationSingleOperationType operationType) {
        // Convert operation to data
        ReplicationOutContext outContext = groupContext.getEntireContext();
        IReplicationPacketData<?> data = getDataProducer().createGenericOperationData(operationData,
                operationType,
                outContext);

        return createAndInsertSingleBucketPacket(data, outContext);
    }

    private SingleBucketOrderedPacket createAndInsertSingleBucketPacket(
            IReplicationPacketData<?> data, ReplicationOutContext outContext) {
        _rwLock.writeLock().lock();
        try {

            setPacketWeight(data);

            if (!shouldInsertPacket())
                return null;

            // This is single entry operation, we can optimize
            // Extract bucket index and bitmap
            IReplicationPacketEntryData entryData = data.getSingleEntryData();
            short bucketId = extractBucketIndex(entryData);
            // Get key from bucket
            long key = _bucketLastKeys[bucketId];
            _bucketLastKeys[bucketId] = key + 1;
            // Wrap data with packet, add to backlog and context
            SingleBucketOrderedPacket packet = new SingleBucketOrderedPacket(takeNextKeyUnsafe(outContext),
                    key,
                    bucketId,
                    data);

            insertPacketToBacklog(packet, outContext);

            return packet;
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    private short extractBucketIndex(IReplicationPacketEntryData entryData) {
        return (short) Math.abs((entryData.getOrderCode() % _numberOfBuckets));
    }

    public IMultiBucketSingleFileReplicationOrderedPacket addTransactionOperationPacket(
            ISyncReplicationGroupOutContext groupContext,
            ServerTransaction transaction,
            ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType) {
        // Convert operation to data
        IReplicationPacketData<?> data = getDataProducer().createTransactionOperationData(transaction,
                lockedEntries,
                groupContext.getEntireContext(),
                operationType);
        if (data.isEmpty())
            return null;

        _rwLock.writeLock().lock();
        try {

            setPacketWeight(data);

            if (!shouldInsertPacket())
                return null;

            // Optimization if transaction only participate in one bucket, we
            // can send s single bucket operation packet
            // instead
            boolean firstEntryData = true;
            short singleBucketIndex = 0; // Avoid compilation error, will always
            // be
            // set at first iteration
            long singleBucketKey = 0; // Avoid compilation error, will always be
            // set at first iteration
            ShortObjectMap<BucketKey> bucketsKeys = null;
            for (IReplicationPacketEntryData entryData : data) {
                if (firstEntryData) {
                    singleBucketIndex = extractBucketIndex(entryData);
                    // Get key from bucket
                    singleBucketKey = _bucketLastKeys[singleBucketIndex];
                    _bucketLastKeys[singleBucketIndex] = singleBucketKey + 1;
                    firstEntryData = false;
                } else {
                    short bucketIndex = extractBucketIndex(entryData);
                    if (bucketIndex == singleBucketIndex)
                        continue;

                    // If we reached here, optimization is cancelled, we have
                    // more than one bucket
                    if (bucketsKeys == null) {
                        // Put already existing bucket in the buckets keys map
                        bucketsKeys = CollectionsFactory.getInstance().createShortObjectMap();
                        bucketsKeys.put(singleBucketIndex,
                                new BucketKey(singleBucketKey));
                    }

                    if (bucketsKeys.containsKey(bucketIndex))
                        continue;

                    // Get key from bucket
                    long key = _bucketLastKeys[bucketIndex];
                    _bucketLastKeys[bucketIndex] = key + 1;

                    bucketsKeys.put(bucketIndex, new BucketKey(key));
                }
            }

            // Protect from empty transaction
            if (firstEntryData)
                throw new IllegalArgumentException("cannot add an empty transaction to replication backlog");
            // Must create multiple bucker packet
            IMultiBucketSingleFileReplicationOrderedPacket packet;
            if (bucketsKeys != null) {
                // Wrap data with packet, add to backlog and context
                packet = new MultipleBucketOrderedPacket(takeNextKeyUnsafe(groupContext.getEntireContext()),
                        bucketsKeys,
                        data);
            }
            // Only one bucket participate, we can optimize
            else {
                packet = new SingleBucketOrderedPacket(takeNextKeyUnsafe(groupContext.getEntireContext()),
                        singleBucketKey,
                        singleBucketIndex,
                        data);
            }

            insertPacketToBacklog(packet, groupContext.getEntireContext());

            return packet;
        } finally {
            _rwLock.writeLock().unlock();
        }
    }

    private void insertPacketToBacklog(
            IMultiBucketSingleFileReplicationOrderedPacket packet, ReplicationOutContext outContext) {
        if (_logger.isLoggable(Level.FINEST))
            _logger.finest(getLogPrefix() + "inserting packet [" + packet
                    + "] to backlog");
        try {
            insertReplicationOrderedPacketToBacklog(packet, outContext);
        } catch (RuntimeException e) {
            if (_logger.isLoggable(Level.SEVERE))
                _logger.log(Level.SEVERE,
                        "exception while inserting a packet to the backlog file (insertPacketToBacklog), "
                                + "[" + getStatistics() + "]",
                        e);
            validateIntegrity();
            throw e;
        }
    }

    public IMultiBucketSingleFileReplicationOrderedPacket replaceWithDiscarded(
            IReplicationOrderedPacket packet, boolean forceDiscard) {
        IMultiBucketSingleFileReplicationOrderedPacket typedPacket = (IMultiBucketSingleFileReplicationOrderedPacket) packet;
        return typedPacket.replaceWithDiscarded();
    }

    @Override
    public boolean mergeWithDiscarded(
            IReplicationOrderedPacket previousDiscardedPacket,
            IReplicationOrderedPacket mergedPacket, String memberName) {
        //TODO LV: implement this
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportDiscardMerge() {
        return false;
    }

    @Override
    protected void deleteBatchFromBacklog(long deletionBatchSize) {
        if (deletionBatchSize == 0)
            return;

        long size = getBacklogFile().size();
        //Each packet we remove, we need
        for (int i = 0; i < Math.min(size, deletionBatchSize); ++i) {
            IMultiBucketSingleFileReplicationOrderedPacket oldestPacket = getBacklogFile().removeOldest();
            for (short bucketIndex : oldestPacket.getBuckets()) {
                _bucketsDeletionState.put(bucketIndex, oldestPacket.getBucketKey(bucketIndex));
            }
        }
    }

    public IProcessResult fromWireForm(Object wiredProcessResult) {
        if (wiredProcessResult == null)
            return MultiBucketSingleFileProcessResult.OK;

        return (IProcessResult) wiredProcessResult;
    }

    public String dumpState() {
        _rwLock.readLock().lock();
        try {
            StringBuilder dump = new StringBuilder("Type ["
                    + this.getClass().getName() + "]");
            dump.append(StringUtils.NEW_LINE);
            dump.append("Backlog statistics {" + getStatistics() + "}");
            dump.append(StringUtils.NEW_LINE);
            dump.append("Buckets " + printBucketsKeys(_bucketLastKeys));
            dump.append(StringUtils.NEW_LINE);
            dump.append("Buckets Deletion state " + toString(_bucketsDeletionState));
            dump.append(StringUtils.NEW_LINE);
            appendConfirmationStateString(dump);
            return dump.toString();
        } finally {
            _rwLock.readLock().unlock();
        }
    }

    public static String printBucketsKeys(long[] keys) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < keys.length; i++) {
            sb.append(i);
            sb.append(": ");
            sb.append(keys[i]);
            if (i < keys.length - 1)
                sb.append(", ");
        }
        sb.append("]");
        return sb.toString();
    }

    private static String toString(ShortLongMap bucketsDeletionState) {
        StringBuilder result = new StringBuilder("{");
        for (ShortLongIterator it = bucketsDeletionState.iterator(); it.hasNext(); ) {
            it.advance();
            result.append(it.key());
            result.append("=");
            result.append(it.value());
            result.append(",");
        }
        result.append("}");
        return result.toString();
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
