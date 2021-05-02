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

package com.gigaspaces.internal.cluster.node.impl.groups.reliableasync;

import com.gigaspaces.cluster.replication.ConsistencyLevelViolationException;
import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationBacklogStateListener;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReplicationReliableAsyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.MemberAddedEvent;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationSourceChannel;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationUnreliableOperation;
import com.gigaspaces.internal.cluster.node.impl.groups.consistencylevel.GroupConsistencyLevelPolicy;
import com.gigaspaces.internal.cluster.node.impl.groups.consistencylevel.SyncMembersInSyncConsistencyLevelPolicy;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.IReplicationThrottleControllerBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.SyncReplicationSourceChannel;
import com.gigaspaces.internal.cluster.node.impl.packets.ReliableAsyncStateUpdatePacket;
import com.gigaspaces.internal.cluster.node.impl.router.ConnectionState;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.utils.ConditionLatch;
import com.gigaspaces.internal.utils.ConditionLatch.Predicate;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandler;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;
import com.gigaspaces.lrmi.DynamicSmartStub;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;
import com.j_spaces.kernel.JSpaceUtilities;

import net.jini.core.transaction.server.ServerTransaction;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;



@com.gigaspaces.api.InternalApi
public class ReliableAsyncReplicationSourceGroup
        extends AbstractReplicationSourceGroup<ReliableAsyncSourceGroupConfig> implements IReplicationBacklogStateListener {

    private final IReplicationReliableAsyncGroupBacklog _groupBacklog;
    private final Map<String, SyncReplicationSourceChannel> _syncChannelsMap;
    private final SyncReplicationSourceChannel[] _syncChannels;
    private final Map<String, ReliableAsyncReplicationSourceChannel> _asyncChannelsMap;
    private final CopyOnWriteArrayList<ReliableAsyncReplicationSourceChannel> _asyncChannels;
    private final IReplicationThrottleControllerBuilder _throttleController;
    private final int _syncChannelAsyncStateBatchSize;
    private final long _syncChannelIdleDelayMilis;
    private final int _asyncChannelBatchSize;
    private final long _asyncChannelIntervalMilis;
    private final int _asyncChannelIntervalOperations;
    private final long _completionNotifierOperationThreshold;
    private final boolean _singleTarget;
    private volatile IAsyncHandler _asyncCompletionNotifier;
    private volatile boolean _active;
    private volatile boolean _passive;
    private long _unsentConfirmedClearedPacketCount;

    public ReliableAsyncReplicationSourceGroup(
            DynamicSourceGroupConfigHolder groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationReliableAsyncGroupBacklog groupBacklog,
            String myLookupName, IReplicationOutFilter outFilter,
            IReplicationThrottleControllerBuilder throttleController,
            IAsyncHandlerProvider asyncHandlerProvider,
            int syncChannelAsyncStateBatchSize, long syncChannelIdleDelayMilis,
            int asyncChannelBatchSize, long asyncChannelInterval,
            int asyncChannelIntervalOperations, IReplicationSourceGroupStateListener stateListener) {
        super(groupConfig,
                replicationRouter,
                groupBacklog,
                myLookupName,
                outFilter,
                asyncHandlerProvider,
                stateListener);
        _groupBacklog = groupBacklog;
        _syncChannelsMap = new CopyOnUpdateMap<String, SyncReplicationSourceChannel>();
        _asyncChannelsMap = new CopyOnUpdateMap<String, ReliableAsyncReplicationSourceChannel>();
        _throttleController = throttleController;
        _syncChannelAsyncStateBatchSize = syncChannelAsyncStateBatchSize;
        _syncChannelIdleDelayMilis = syncChannelIdleDelayMilis;
        _asyncChannelBatchSize = asyncChannelBatchSize;
        _asyncChannelIntervalMilis = asyncChannelInterval;
        _asyncChannelIntervalOperations = asyncChannelIntervalOperations;

        ReliableAsyncSourceGroupConfig config = (ReliableAsyncSourceGroupConfig) getConfigHolder().getConfig();
        _syncChannels = new SyncReplicationSourceChannel[config.getSyncMembersLookupNames().length];
        _asyncChannels = new CopyOnWriteArrayList<ReliableAsyncReplicationSourceChannel>();
        _completionNotifierOperationThreshold = config.getCompletionNotifierPacketsThreshold();

        _singleTarget = _syncChannels.length == 1;
        _groupBacklog.setStateListener(this);
    }

    @Override
    protected AbstractReplicationSourceChannel createChannel(
            String memberLookupName, IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationChannelDataFilter dataFilter,
            IReplicationGroupHistory groupHistory, boolean dynamicMember,
            SourceGroupConfig groupConfig, Object customBacklogMetadata, boolean resetTarget) {
        ReliableAsyncSourceGroupConfig config = (ReliableAsyncSourceGroupConfig) groupConfig;
        if (Arrays.asList(config.getSyncMembersLookupNames())
                .contains(memberLookupName))
            return createSyncChannel(memberLookupName,
                    replicationRouter,
                    connection,
                    dataFilter,
                    groupHistory,
                    dynamicMember,
                    customBacklogMetadata);
        // Assuming all sync channels are created before async channels are
        // created
        return createAsyncChannel(memberLookupName,
                replicationRouter,
                connection,
                dataFilter,
                groupHistory,
                config,
                customBacklogMetadata, resetTarget);

    }

    @Override
    protected void onCloseTemporaryChannel(String sourceMemberName,
                                           AbstractReplicationSourceChannel channel) {
        if (_syncChannelsMap.remove(sourceMemberName) != null) {
            for (int i = 0; i < _syncChannels.length; i++) {
                if (_syncChannels[i] == channel) {
                    _syncChannels[i] = null;
                    break;
                }
            }
        } else if (_asyncChannels.remove(channel))
            _asyncChannelsMap.remove(sourceMemberName);
    }

    private AbstractReplicationSourceChannel createAsyncChannel(
            String memberLookupName, IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationChannelDataFilter dataFilter,
            IReplicationGroupHistory groupHistory,
            ReliableAsyncSourceGroupConfig groupConfig, Object customBacklogMetadata, boolean resetTarget) {
        int batchSize = _asyncChannelBatchSize;
        long intervalMilis = _asyncChannelIntervalMilis;
        int intervalOperations = _asyncChannelIntervalOperations;
        String tag = null; //optional (may be null)
        ReplicationMode channelType = ReplicationMode.MIRROR;
        AsyncChannelConfig specificConfig = groupConfig.getChannelConfig(memberLookupName);
        // Use specific configuration if available
        if (specificConfig != null) {
            batchSize = specificConfig.getBatchSize();
            intervalMilis = specificConfig.getIntervalMilis();
            intervalOperations = specificConfig.getIntervalOperations();
            channelType = specificConfig.getChannelType();
            tag = specificConfig.getTag();
        }
        ReliableAsyncReplicationSourceChannel channel = new ReliableAsyncReplicationSourceChannel(
                getConfigHolder(),
                getGroupName(),
                memberLookupName,
                replicationRouter,
                connection,
                _groupBacklog,
                getOutFilter(),
                batchSize,
                intervalMilis,
                intervalOperations,
                getAsyncHandlerProvider(),
                _syncChannelsMap.values(),
                dataFilter,
                getStateListener(),
                groupHistory,
                channelType,
                customBacklogMetadata,
                tag, resetTarget);
        _asyncChannelsMap.put(memberLookupName, channel);
        _asyncChannels.add(channel);
        return channel;
    }

    private AbstractReplicationSourceChannel createSyncChannel(
            String memberLookupName, IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationChannelDataFilter dataFilter,
            IReplicationGroupHistory groupHistory, boolean dynamicMember,
            Object customBacklogMetadata) {
        // TODO LV: support dynamic addition for sync for code completion and
        // future capabilities
        if (dynamicMember)
            throw new UnsupportedOperationException("Cannot add dynamically a synchronous channel");
        SyncReplicationSourceChannel channel = new SyncReplicationSourceChannel(getConfigHolder(),
                getGroupName(),
                memberLookupName,
                replicationRouter,
                connection,
                _groupBacklog,
                getOutFilter(),
                _throttleController.createController(getGroupName(),
                        getMyLookupName(),
                        memberLookupName),
                getAsyncHandlerProvider(),
                _syncChannelAsyncStateBatchSize,
                _syncChannelIdleDelayMilis,
                dataFilter,
                getStateListener(),
                groupHistory,
                ReplicationMode.BACKUP_SPACE,
                customBacklogMetadata);
        _syncChannelsMap.put(memberLookupName, channel);
        return channel;
    }

    public void beforeExecute(ReplicationOutContext replicationContext,
                              IEntryHolder entryHolder,
                              ReplicationSingleOperationType operationType) {
        validateActive();
        // Get group specific context (create one of missing)
        ReliableAsyncReplicationGroupOutContext groupContext = getGroupContext(replicationContext);
        // Add operation to group backlog
        _groupBacklog.add(groupContext, entryHolder, operationType);
    }

    public void beforeExecuteGeneric(ReplicationOutContext replicationContext,
                                     Object operationData, ReplicationSingleOperationType operationType) {
        validateActive();
        // Get group specific context (create one of missing)
        ReliableAsyncReplicationGroupOutContext groupContext = getGroupContext(replicationContext);
        // Add operation to group backlog
        _groupBacklog.addGeneric(groupContext, operationData, operationType);
    }

    public void beforeTransactionExecute(
            ReplicationOutContext replicationContext,
            ServerTransaction transaction, ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType) {
        validateActive();
        // Get group specific context (create one of missing)
        ReliableAsyncReplicationGroupOutContext groupContext = getGroupContext(replicationContext);
        // Add operation to group backlog
        _groupBacklog.addTransaction(groupContext,
                transaction,
                lockedEntries,
                operationType);
    }

    private void validateActive() {
        // This will update memory state (_active is volatile)
        if (!_active)
            throw new IllegalStateException("Reliable async source group cannot be actively accesses when it is not in active state");
    }

    @Override
    public int executeImpl(IReplicationGroupOutContext groupContext) {
        validateActive();

        if (groupContext.isEmpty())
            return 0;
        int res = 0;
        ReliableAsyncReplicationGroupOutContext reliableAsyncGroupContext = (ReliableAsyncReplicationGroupOutContext) groupContext;
        if (isSingleTarget())
            res = _syncChannels[0].execute(reliableAsyncGroupContext);
        else {
            Future<?>[] _futures = new Future[_syncChannels.length];
            for (int i = 0; i < _syncChannels.length; i++)
                _futures[i] = _syncChannels[i].executeAsync(reliableAsyncGroupContext);

            for (int i = 0; i < _futures.length; i++) {
                try {
                    _futures[i].get();
                    res += 1;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException)
                        throw (RuntimeException) cause;
                    if (cause instanceof Error)
                        throw (Error) cause;
                }
            }

        }
        // TODO LV: opt - can be optimized to use a concrete array without
        // creating an iterator how ever
        // we will need to manage scaling up and down manually
        for (ReliableAsyncReplicationSourceChannel channel : _asyncChannels) {
            channel.execute(reliableAsyncGroupContext);
        }
        return res;
    }

    @Override
    public void execute(IReplicationUnreliableOperation operation) {
        validateActive();

        super.execute(operation);
    }

    @Override
    protected void validateGroupConsistencyLevelPolicy(GroupConsistencyLevelPolicy groupSlaPolicy,
                                                       ReliableAsyncSourceGroupConfig sourceGroupConfig) {
        if (!(groupSlaPolicy instanceof SyncMembersInSyncConsistencyLevelPolicy))
            super.validateGroupConsistencyLevelPolicy(groupSlaPolicy, sourceGroupConfig);
        else {
            SyncMembersInSyncConsistencyLevelPolicy syncMembersInSyncSlaPolicy = (SyncMembersInSyncConsistencyLevelPolicy) groupSlaPolicy;
            if (!syncMembersInSyncSlaPolicy.isAllSyncMembersNeedsToBeInSync()
                    && syncMembersInSyncSlaPolicy.getMinNumberOfSyncMembers() > sourceGroupConfig
                    .getSyncMembersLookupNames().length + 1)
                throw new IllegalArgumentException(
                        "Cannot specify minimum number of synchronous members in replication group consistency level policy which is more than the number of synchronous members in the group."
                                + "Number of members in the group is "
                                + (sourceGroupConfig.getSyncMembersLookupNames().length + 1)
                                + " while the minimal requested is "
                                + syncMembersInSyncSlaPolicy.getMinNumberOfSyncMembers());
        }

    }

    @Override
    public void monitorConsistencyLevel() throws ConsistencyLevelViolationException {
        super.monitorConsistencyLevel();

        final GroupConsistencyLevelPolicy groupConsistencyLevelPolicy = getConfigHolder().getConfig().getGroupConsistencyLevelPolicy();
        if (GroupConsistencyLevelPolicy.isEmptyPolicy(groupConsistencyLevelPolicy))
            return;

        if (groupConsistencyLevelPolicy instanceof SyncMembersInSyncConsistencyLevelPolicy) {
            if (isSingleTarget())
                ((SyncMembersInSyncConsistencyLevelPolicy) groupConsistencyLevelPolicy).checkConsistencyLevel(_syncChannels[0]);
            else
                ((SyncMembersInSyncConsistencyLevelPolicy) groupConsistencyLevelPolicy).checkConsistencyLevel(_syncChannels, getMyLookupName());
        }


    }

    private ReliableAsyncReplicationGroupOutContext getGroupContext(
            ReplicationOutContext replicationContext) {
        ReliableAsyncReplicationGroupOutContext groupContext = (ReliableAsyncReplicationGroupOutContext) replicationContext.getGroupContext(getGroupName());
        if (groupContext == null) {
            groupContext = new ReliableAsyncReplicationGroupOutContext(getGroupName());
            replicationContext.setGroupContext(groupContext);
        }
        return groupContext;
    }

    // Just for visability of tests
    @Override
    protected AbstractReplicationSourceChannel getChannel(
            String memberLookupName) {
        return super.getChannel(memberLookupName);
    }

    @Override
    protected void onMemberAdded(MemberAddedEvent memberAddedEvent,
                                 SourceGroupConfig newConfig) {
        // Assume called under channel creation lock
        if (_active) {
            super.onMemberAdded(memberAddedEvent, newConfig);

            if (_specificLogger.isDebugEnabled())
                _specificLogger.debug("Notifying reliable async keepers of member ["
                        + memberAddedEvent.getMemberName() + "] addition");

            // Notify keepers of member addition
            notifyKeepersOfGroupMembersChange(newConfig);

            // We should reset the member last confirmed key to the last in the
            // backlog since some packets
            // could have already been processed and not kept in the keepers
            // since we added the member at first place
            // and therefore they cannot be held there.
            _groupBacklog.makeMemberConfirmedOnAll(memberAddedEvent.getMemberName());

            // Notify keepers again to have their confirmation key set
            // containing the updated key because the previous key they got
            // could
            // be already processed in the keeper before the member was added
            // and no longer exists there.
            notifyKeepersOfGroupMembersChange(newConfig);
        }
    }

    protected void notifyKeepersOfGroupMembersChange(SourceGroupConfig newConfig) {
        IAsyncHandler asyncCompletionNotifier = _asyncCompletionNotifier;
        if (asyncCompletionNotifier != null) {

            ReliableAsyncSourceGroupConfig typedConfig = (ReliableAsyncSourceGroupConfig) newConfig;
            // Wake up the async notifier so it will let the target know of this
            // change and wait for it to be executed
            asyncCompletionNotifier.wakeUpAndWait(typedConfig.getCompletionNotifierInterval(),
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    protected void onMemberRemoved(String memberName,
                                   SourceGroupConfig newConfig) {
        // Assume called under channel creation lock
        if (_active) {
            _asyncChannelsMap.remove(memberName);

            super.onMemberRemoved(memberName, newConfig);

            if (_specificLogger.isDebugEnabled())
                _specificLogger.debug("Notifying reliable async keepers of member ["
                        + memberName + "] removal");

            notifyKeepersOfGroupMembersChange(newConfig);
        }
    }

    @Override
    public void setActive(MetricRegistrator metricRegistrator) {
        synchronized (_channelCreationLock) {
            super.setActive(metricRegistrator);

            if (_passive)
                logGroupEvent("Becoming active - Backlog dump: " + getGroupBacklog().dumpState());

            ReliableAsyncSourceGroupConfig sourceGroupConfig = (ReliableAsyncSourceGroupConfig) getConfigHolder().getConfig();
            if (_passive && sourceGroupConfig.getSyncMembersLookupNames().length > 1)
                createReplicationChannelsSequentially();
            else
                createReplicationChannels();
            prepareChannelsArrays();
            spawnAsyncCompletionNotifier();
            registerWith(metricRegistrator);
            // This will flush local memory (_active is volatile)
            _passive = false;
            _active = true;
        }
    }

    private void createReplicationChannelsSequentially() {
        synchronized (_channelCreationLock) {
            ReliableAsyncSourceGroupConfig config = (ReliableAsyncSourceGroupConfig) getConfigHolder().getConfig();

            //Create channels to sync member synchronously, this process will provide backlog completion of this source
            //in case the target is at a newer replication state before continuing on to the next channel and with becoming fully active.
            for (String memberLookupName : config.getSyncMembersLookupNames()) {
                createChannel(memberLookupName, false, config, true, null, false);
                final AbstractReplicationSourceChannel channel = getChannel(memberLookupName);
                ConditionLatch conditionLatch = new ConditionLatch().timeout(30, TimeUnit.SECONDS).pollingInterval(50, TimeUnit.MILLISECONDS);
                try {
                    conditionLatch.waitFor(new Predicate() {
                        @Override
                        public boolean isDone() throws InterruptedException {
                            return channel.isActive() || channel.isInconsistent() || channel.getConnection().getState() == ConnectionState.DISCONNECTED;
                        }
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (TimeoutException e) {
                    if (_specificLogger.isWarnEnabled())
                        _specificLogger.warn(getLogPrefix() + "timeout occurred while waiting for active state of newly created channel to "
                                + memberLookupName);
                }
                if (_specificLogger.isTraceEnabled())
                    _specificLogger.trace(getLogPrefix() + "created channel to "
                            + memberLookupName);
            }
            //Create channels to async members
            for (String memberLookupName : config.getAsyncMembersLookupNames()) {
                createChannel(memberLookupName, false, config, false, null, false);
                if (_specificLogger.isTraceEnabled())
                    _specificLogger.trace(getLogPrefix() + "created channel to "
                            + memberLookupName);
            }
            if (_specificLogger.isDebugEnabled())
                _specificLogger.trace(getLogPrefix() + "created all channels");
        }

    }

    private void prepareChannelsArrays() {
        int index = 0;
        Collection<SyncReplicationSourceChannel> syncValues = _syncChannelsMap.values();
        for (SyncReplicationSourceChannel sourceChannel : syncValues)
            _syncChannels[index++] = sourceChannel;

        index = 0;
        _asyncChannels.clear();
        Collection<ReliableAsyncReplicationSourceChannel> asyncValues = _asyncChannelsMap.values();
        for (ReliableAsyncReplicationSourceChannel sourceChannel : asyncValues) {
            _asyncChannels.add(sourceChannel);
        }
    }

    @Override
    public void setPassive(boolean closeProxy) {
        synchronized (_channelCreationLock) {
            try {
                super.setPassive(closeProxy);
                DynamicSmartStub.markProxyAsClosed.set(closeProxy);
                _active = false;
                closeAsyncCompletionNotifier();
                closeReplicationChannels();
                clearChannelsArrays();
                _syncChannelsMap.clear();
                _asyncChannelsMap.clear();
                _passive = true;
            } finally {
                DynamicSmartStub.markProxyAsClosed.remove();
            }
        }
    }

    private void clearChannelsArrays() {
        for (int i = 0; i < _syncChannels.length; i++)
            _syncChannels[i] = null;

        _asyncChannels.clear();
    }

    @Override
    protected void onClose() {
        synchronized (_channelCreationLock) {
            closeAsyncCompletionNotifier();
        }

        super.onClose();
    }

    private void spawnAsyncCompletionNotifier() {
        ReliableAsyncSourceGroupConfig config = (ReliableAsyncSourceGroupConfig) getConfigHolder().getConfig();
        _asyncCompletionNotifier = getAsyncHandlerProvider().start(new AsyncCompletionNotifier(),
                config.getCompletionNotifierInterval(),
                "AsyncCompletionNotifier-"
                        + getMyLookupName()
                        + "."
                        + getGroupName(),
                false);
    }

    public boolean isSingleTarget() {
        return _singleTarget;
    }

    private void closeAsyncCompletionNotifier() {
        if (_asyncCompletionNotifier != null)
            _asyncCompletionNotifier.stop(3, TimeUnit.SECONDS);
        _asyncCompletionNotifier = null;
    }

    @Override
    protected String[] getPotentialRemovedMembers(SourceGroupConfig config) {
        return ((ReliableAsyncSourceGroupConfig) config).getAsyncMembersLookupNames();
    }

    //Assume this is called under a lock from the backlog level, in a legal sequential order
    @Override
    public void onPacketsClearedAfterConfirmation(long packetsCount) {
        _unsentConfirmedClearedPacketCount += packetsCount;
        if (_unsentConfirmedClearedPacketCount >= _completionNotifierOperationThreshold) {
            _unsentConfirmedClearedPacketCount = 0;
            IAsyncHandler asyncCompletionNotifier = _asyncCompletionNotifier;
            if (asyncCompletionNotifier != null)
                asyncCompletionNotifier.wakeUp();
        }

    }

    public class AsyncCompletionNotifier
            extends AsyncCallable {
        private final ReliableAsyncStateUpdatePacket _updatePacket = new ReliableAsyncStateUpdatePacket(getGroupName());
        private Throwable _pendingException = null;

        public CycleResult call() throws Exception {
            scanAndRemoveDroppedMembers();


            for (SyncReplicationSourceChannel channel : _syncChannelsMap.values()) {
                if (!channel.isActive())
                    continue;
                IReliableAsyncState reliableAsyncState = _groupBacklog.getReliableAsyncState(channel.getMemberName());
                _updatePacket.setState(reliableAsyncState);
                IReplicationMonitoredConnection connection = channel.getConnection();

                try {
                    connection.dispatch(_updatePacket);
                    _pendingException = null;
                } catch (RemoteException re) {
                    // We ignore disconnections
                } catch (RuntimeException rt) {
                    // We ignore any exception thrown here
                    if (_specificLogger.isWarnEnabled()) {
                        if (!JSpaceUtilities.isSameException(_pendingException, rt)) {
                            _pendingException = rt;
                            _specificLogger.warn(
                                    "error while executing reliable async update",
                                    rt);
                        }
                    }

                } catch (Error er) {
                    // We ignore any exception thrown here
                    if (_specificLogger.isWarnEnabled()) {
                        if (!JSpaceUtilities.isSameException(_pendingException, er)) {
                            _pendingException = er;
                            _specificLogger.warn(
                                    "error while executing reliable async update",
                                    er);
                        }
                    }
                }
            }

            return CycleResult.IDLE_CONTINUE;
        }

    }

}
