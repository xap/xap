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

package com.gigaspaces.internal.cluster.node.impl.groups.async;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationSingleOperationType;
import com.gigaspaces.internal.cluster.node.impl.backlog.async.IReplicationAsyncGroupBacklog;
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
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.AsyncChannelConfig;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandler;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;

import net.jini.core.transaction.server.ServerTransaction;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;


@com.gigaspaces.api.InternalApi
public class AsyncReplicationSourceGroup
        extends AbstractReplicationSourceGroup<SourceGroupConfig>
        implements IReplicationSourceGroup {

    private final IReplicationAsyncGroupBacklog _groupBacklog;
    private final Map<String, AsyncReplicationSourceChannel> _channelsMap;
    // TODO LV: reoptimize this to use array and instead of iterator to save
    // garbage
    private final CopyOnWriteArrayList<AsyncReplicationSourceChannel> _channels;
    private final int _batchSize;
    private final long _interval;
    private final int _intervalOperations;
    private final IAsyncHandler _droppedTargetScanner;

    public AsyncReplicationSourceGroup(
            DynamicSourceGroupConfigHolder groupConfig,
            IReplicationRouter replicationRouter,
            IReplicationAsyncGroupBacklog groupBacklog, String myLookupName,
            IReplicationOutFilter outFilter, int batchSize, long intervalMilis,
            int intervalOperations, IAsyncHandlerProvider asyncProvider,
            IReplicationSourceGroupStateListener stateListener) {
        super(groupConfig,
                replicationRouter,
                groupBacklog,
                myLookupName,
                outFilter,
                asyncProvider,
                stateListener);
        _batchSize = batchSize;
        _interval = intervalMilis;
        _intervalOperations = intervalOperations;
        _groupBacklog = groupBacklog;
        _channelsMap = new CopyOnUpdateMap<String, AsyncReplicationSourceChannel>();
        _channels = new CopyOnWriteArrayList<AsyncReplicationSourceChannel>();

        createReplicationChannels();

        _droppedTargetScanner = getAsyncHandlerProvider().start(new DroppedTargetScanner(), _interval, "DroppedTargetScanner-" + groupConfig.getConfig().getName(), false);
    }

    @Override
    protected AbstractReplicationSourceChannel createChannel(
            String memberLookupName, IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationChannelDataFilter dataFilter,
            IReplicationGroupHistory groupHistory, boolean dynamicMember,
            SourceGroupConfig groupConfig,
            Object customBacklogMetadata, boolean resetTarget) {
        AsyncSourceGroupConfig asyncGroupConfig = (AsyncSourceGroupConfig) groupConfig;
        int batchSize = _batchSize;
        long intervalMilis = _interval;
        int intervalOperations = _intervalOperations;
        String tag = null; //optional (may be null)
        ReplicationMode channelType = ReplicationMode.ACTIVE_SPACE;
        AsyncChannelConfig specificConfig = asyncGroupConfig.getChannelConfig(memberLookupName);
        // Use specific configuration if available
        if (specificConfig != null) {
            batchSize = specificConfig.getBatchSize();
            intervalMilis = specificConfig.getIntervalMilis();
            intervalOperations = specificConfig.getIntervalOperations();
            channelType = specificConfig.getChannelType();
            tag = specificConfig.getTag();
        }
        AsyncReplicationSourceChannel channel = new AsyncReplicationSourceChannel(getConfigHolder(),
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
                dataFilter,
                getStateListener(),
                groupHistory,
                channelType,
                customBacklogMetadata,
                tag, resetTarget);
        _channelsMap.put(memberLookupName, channel);
        _channels.add(channel);
        return channel;
    }

    @Override
    protected void onCloseTemporaryChannel(String sourceMemberName,
                                           AbstractReplicationSourceChannel channel) {
        _channels.remove(channel);
        _channelsMap.remove(sourceMemberName);
    }

    public void beforeExecute(ReplicationOutContext replicationContext,
                              IEntryHolder entryHolder,
                              ReplicationSingleOperationType operationType) {
        IAsyncReplicationGroupOutContext groupContext = getGroupContext(replicationContext);
        _groupBacklog.add(groupContext, entryHolder, operationType);
    }

    public void beforeTransactionExecute(
            ReplicationOutContext replicationContext,
            ServerTransaction transaction,
            ArrayList<IEntryHolder> lockedEntries, ReplicationMultipleOperationType operationType) {
        IAsyncReplicationGroupOutContext groupContext = getGroupContext(replicationContext);
        _groupBacklog.addTransaction(groupContext,
                transaction,
                lockedEntries,
                operationType);
    }

    public void beforeExecuteGeneric(ReplicationOutContext replicationContext,
                                     Object operationData, ReplicationSingleOperationType operationType) {
        IAsyncReplicationGroupOutContext groupContext = getGroupContext(replicationContext);
        _groupBacklog.addGeneric(groupContext, operationData, operationType);
    }

    @Override
    public int executeImpl(IReplicationGroupOutContext groupContext) {
        if (groupContext.isEmpty())
            return 0;
        IAsyncReplicationGroupOutContext asyncGroupContext = (IAsyncReplicationGroupOutContext) groupContext;
        for (AsyncReplicationSourceChannel channel : _channels)
            channel.execute(asyncGroupContext);
        return 0;
    }

    private IAsyncReplicationGroupOutContext getGroupContext(
            ReplicationOutContext replicationContext) {
        IAsyncReplicationGroupOutContext groupContext = (IAsyncReplicationGroupOutContext) replicationContext.getGroupContext(getGroupName());
        if (groupContext == null) {
            // TODO OPT: reuse contexts with thread local
            groupContext = new AsyncReplicationGroupOutContext(getGroupName());
            replicationContext.setGroupContext(groupContext);
        }
        return groupContext;
    }

    @Override
    protected AbstractReplicationSourceChannel getChannel(
            String memberLookupName) {
        return super.getChannel(memberLookupName);
    }

    @Override
    protected String[] getPotentialRemovedMembers(SourceGroupConfig config) {
        return config.getMembersLookupNames();
    }

    @Override
    protected void onClose() {
        _droppedTargetScanner.stop(3, TimeUnit.SECONDS);
        super.onClose();
    }

    public class DroppedTargetScanner
            extends AsyncCallable {

        @Override
        public CycleResult call() throws Exception {
            scanAndRemoveDroppedMembers();
            return CycleResult.IDLE_CONTINUE;
        }

    }

}
