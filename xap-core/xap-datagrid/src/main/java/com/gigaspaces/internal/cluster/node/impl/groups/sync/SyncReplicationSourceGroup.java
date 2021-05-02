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

package com.gigaspaces.internal.cluster.node.impl.groups.sync;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IReplicationSyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationSourceChannel;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


@com.gigaspaces.api.InternalApi
public class SyncReplicationSourceGroup
        extends AbstractReplicationSourceGroup<SourceGroupConfig>
        implements IReplicationSourceGroup {

    final private IReplicationSyncGroupBacklog _groupBacklog;
    // Should be readonly and never updated after constructor
    final private Map<String, SyncReplicationSourceChannel> _channelsMap;
    // Optimization to reduce garbage memory whens scanning channels
    final private SyncReplicationSourceChannel[] _channels;
    final private IReplicationThrottleControllerBuilder _throttleControllerBuilder;
    final private int _asyncStateBatchSize;
    final private long _idleDelayMilis;
    final private boolean _singleTarget;
    final private SyncReplicationSourceChannel _singleTargetChannel;
    final private ReplicationMode _channelType;

    public SyncReplicationSourceGroup(
            DynamicSourceGroupConfigHolder groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationOutFilter outFilter,
            IReplicationThrottleControllerBuilder throttleControllerBuilder,
            IAsyncHandlerProvider asyncStateHandlerProvider,
            int asyncStateBatchSize, long idleDelayMilis,
            IReplicationSyncGroupBacklog groupBacklog, String myLookupName,
            IReplicationSourceGroupStateListener stateListener,
            ReplicationMode channelType) {
        super(groupConfig,
                replicationRouter,
                groupBacklog,
                myLookupName,
                outFilter,
                asyncStateHandlerProvider,
                stateListener);
        _throttleControllerBuilder = throttleControllerBuilder;
        _asyncStateBatchSize = asyncStateBatchSize;
        _idleDelayMilis = idleDelayMilis;
        _channelType = channelType;
        _channelsMap = new HashMap<String, SyncReplicationSourceChannel>();
        _groupBacklog = groupBacklog;
        createReplicationChannels();
        _channels = prepareChannelsArray();
        // Optimize single target scenario not to work with the channels map
        _singleTarget = _channelsMap.size() == 1;
        if (_singleTarget) {
            _singleTargetChannel = _channelsMap.values().iterator().next();
        } else {
            // In multi target scenario.
            _singleTargetChannel = null;
        }
    }

    private SyncReplicationSourceChannel[] prepareChannelsArray() {
        SyncReplicationSourceChannel[] result = new SyncReplicationSourceChannel[_channelsMap.size()];
        int index = 0;
        Collection<SyncReplicationSourceChannel> values = _channelsMap.values();
        for (SyncReplicationSourceChannel sourceChannel : values)
            result[index++] = sourceChannel;

        return result;
    }

    public void beforeExecute(ReplicationOutContext replicationContext,
                              IEntryHolder entryHolder,
                              ReplicationSingleOperationType operationType) {
        // Get group specific context (create one of missing)
        ISyncReplicationGroupOutContext groupContext = getGroupContext(replicationContext);
        // Add operation to group backlog
        _groupBacklog.add(groupContext, entryHolder, operationType);
    }

    public void beforeTransactionExecute(
            ReplicationOutContext replicationContext,
            ServerTransaction transaction, ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType) {
        ISyncReplicationGroupOutContext groupContext = getGroupContext(replicationContext);
        _groupBacklog.addTransaction(groupContext, transaction, lockedEntries, operationType);
    }

    public void beforeExecuteGeneric(ReplicationOutContext replicationContext,
                                     Object operationData, ReplicationSingleOperationType operationType) {
        // Get group specific context (create one of missing)
        ISyncReplicationGroupOutContext groupContext = getGroupContext(replicationContext);
        // Add operation to group backlog
        _groupBacklog.addGeneric(groupContext, operationData, operationType);
    }

    @Override
    public int executeImpl(IReplicationGroupOutContext groupContext) {
        if (groupContext.isEmpty())
            return 0;
        ISyncReplicationGroupOutContext syncGroupContext = (ISyncReplicationGroupOutContext) groupContext;
        int res = 0;
        if (isSingleTarget())
            return _singleTargetChannel.execute(syncGroupContext);
        else {
            Future<?>[] _futures = new Future[_channels.length];
            for (int i = 0; i < _channels.length; i++)
                _futures[i] = _channels[i].executeAsync(syncGroupContext);

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
        return res;
    }

    private ISyncReplicationGroupOutContext getGroupContext(
            ReplicationOutContext replicationContext) {
        ISyncReplicationGroupOutContext groupContext = (ISyncReplicationGroupOutContext) replicationContext.getGroupContext(getGroupName());
        if (groupContext == null) {
            // TODO OPT: reuse contexts with thread local
            groupContext = new SyncReplicationGroupOutContext(getGroupName());
            replicationContext.setGroupContext(groupContext);
        }
        return groupContext;
    }

    @Override
    protected AbstractReplicationSourceChannel createChannel(
            String memberLookupName, IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationChannelDataFilter dataFilter,
            IReplicationGroupHistory groupHistory, boolean dynamicMember,
            SourceGroupConfig groupConfig, Object customBacklogMetadata, boolean resetTarget) {
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
                _throttleControllerBuilder.createController(getGroupName(),
                        getMyLookupName(),
                        memberLookupName),
                getAsyncHandlerProvider(),
                _asyncStateBatchSize,
                _idleDelayMilis,
                dataFilter,
                getStateListener(),
                groupHistory,
                _channelType,
                customBacklogMetadata);
        _channelsMap.put(memberLookupName, channel);
        return channel;
    }

    @Override
    protected void onCloseTemporaryChannel(String sourceMemberName,
                                           AbstractReplicationSourceChannel channel) {
        _channelsMap.remove(sourceMemberName);
        for (int i = 0; i < _channels.length; i++) {
            if (_channels[i] == channel)
                _channels[i] = null;
        }
    }

    // Just for visability of tests
    @Override
    protected AbstractReplicationSourceChannel getChannel(
            String memberLookupName) {
        return super.getChannel(memberLookupName);
    }

    public boolean isSingleTarget() {
        return _singleTarget;
    }

}
