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

import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReplicationReliableAsyncGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReplicationReliableAsyncGroupBacklog.KeeperMemberState;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationSourceChannel;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.async.AsyncReplicationSourceChannel;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.SyncReplicationSourceChannel;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationOperatingMode;

import java.util.Collection;
import java.util.List;


@com.gigaspaces.api.InternalApi
public class ReliableAsyncReplicationSourceChannel
        extends AsyncReplicationSourceChannel {

    private final IReplicationReliableAsyncGroupBacklog _groupBacklog;
    private final AbstractReplicationSourceChannel[] _keeperSyncChannels;
    private final KeeperMemberState[] _keeperMembersState;

    public ReliableAsyncReplicationSourceChannel(
            DynamicSourceGroupConfigHolder groupConfig,
            String groupName,
            String memberName,
            IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationReliableAsyncGroupBacklog groupBacklog,
            IReplicationOutFilter outFilter,
            int batchSize,
            long intervalMilis,
            int intervalOperations,
            IAsyncHandlerProvider asyncHandlerProvider,
            Collection<SyncReplicationSourceChannel> keeperSyncChannelsCollection,
            IReplicationChannelDataFilter dataFilter,
            IReplicationSourceGroupStateListener stateListener,
            IReplicationGroupHistory groupHistory, ReplicationMode channelType,
            Object customBacklogMetadata,
            String tag, boolean resetTarget) {
        super(groupConfig,
                groupName,
                memberName,
                replicationRouter,
                connection,
                groupBacklog,
                outFilter,
                batchSize,
                intervalMilis,
                intervalOperations,
                asyncHandlerProvider,
                false,
                dataFilter,
                stateListener,
                groupHistory,
                channelType,
                customBacklogMetadata,
                tag, resetTarget);

        _groupBacklog = groupBacklog;
        _keeperSyncChannels = keeperSyncChannelsCollection.toArray(new AbstractReplicationSourceChannel[keeperSyncChannelsCollection.size()]);
        // Build keeper member state array
        _keeperMembersState = new KeeperMemberState[_keeperSyncChannels.length];
        for (int i = 0; i < _keeperSyncChannels.length; i++)
            _keeperMembersState[i] = new KeeperMemberState(_keeperSyncChannels[i].getMemberName());

        start();
    }

    @Override
    protected List<IReplicationOrderedPacket> getPendingPackets() {
        return _groupBacklog.getReliableAsyncPackets(getMemberName(),
                getBatchSize(),
                getKeeperMembersState(),
                getDataFilter(),
                getTargetLogicalVersion(),
                _specificLogger);
    }

    private KeeperMemberState[] getKeeperMembersState() {
        // Calculate connected state of all the sync keeper members
        for (int i = 0; i < _keeperSyncChannels.length; i++) {
            AbstractReplicationSourceChannel keeperChannel = _keeperSyncChannels[i];
            _keeperMembersState[i].cannotBypass = keeperChannel.isActive() &&
                    !keeperChannel.isInconsistent() &&
                    !keeperChannel.isSynchronizing();
        }
        return _keeperMembersState;
    }

    @Override
    public ReplicationOperatingMode getChannelOpertingMode() {
        for (int i = 0; i < _keeperSyncChannels.length; i++) {
            AbstractReplicationSourceChannel channel = _keeperSyncChannels[i];
            if (channel.isActive() && !channel.isInconsistent() && channel.getChannelOpertingMode() == ReplicationOperatingMode.SYNC)
                return ReplicationOperatingMode.RELIABLE_ASYNC;
        }
        return ReplicationOperatingMode.ASYNC;
    }
}
