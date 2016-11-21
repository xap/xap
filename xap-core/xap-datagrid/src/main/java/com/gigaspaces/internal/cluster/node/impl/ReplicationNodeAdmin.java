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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.cluster.replication.ConsistencyLevelViolationException;
import com.gigaspaces.cluster.replication.IRedoLogStatistics;
import com.gigaspaces.cluster.replication.RedoLogCapacityExceededException;
import com.gigaspaces.internal.cluster.node.IReplicationNodeAdmin;
import com.gigaspaces.internal.cluster.node.IReplicationNodeStateListener;
import com.gigaspaces.internal.cluster.node.impl.backlog.OperationWeightInfo;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeMode;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceChannelStatistics;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupStatistics;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.router.ConnectionState;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouterAdmin;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.filters.ReplicationStatistics;
import com.j_spaces.core.filters.ReplicationStatistics.ChannelState;
import com.j_spaces.core.filters.ReplicationStatistics.OutgoingChannel;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;
import com.j_spaces.kernel.JSpaceUtilities;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Administrator of a {@link ReplicationNode}
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationNodeAdmin
        implements IReplicationNodeAdmin {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION_NODE);

    private final ReplicationNode _replicationNode;

    public ReplicationNodeAdmin(ReplicationNode replicationNode) {
        _replicationNode = replicationNode;
    }

    public boolean flushPendingReplication(long timeout, TimeUnit units) {
        return _replicationNode.flushPendingReplication(timeout, units);
    }

    public ReplicationStatistics getStatistics() {
        // get backlog statistics
        IRedoLogStatistics backLogStatistics = _replicationNode.getBackLogStatistics();

        // get per channel statistics
        LinkedList<OutgoingChannel> outChannelsStat = new LinkedList<ReplicationStatistics.OutgoingChannel>();
        for (IReplicationSourceGroup sourceGroup : _replicationNode.getReplicationSourceGroups()) {
            IReplicationSourceGroupStatistics groupStat = sourceGroup.getStatistics();
            for (IReplicationSourceChannelStatistics channelStat : groupStat.getChannelStatistics()) {
                String name = channelStat.getName();
                ReplicationMode replicationMode = channelStat.getChannelType();
                ChannelState state;
                if (channelStat.getConnectionState() == ConnectionState.DISCONNECTED)
                    state = ChannelState.DISCONNECTED;
                else if (channelStat.isActive())
                    state = ChannelState.ACTIVE;
                else
                    state = ChannelState.CONNECTED;

                long lastConfirmedKeyFromTarget = channelStat.getLastConfirmedKey();

                Throwable inconsistencyReason = channelStat.getInconsistencyReason();
                OutgoingChannel outChannelStat = new OutgoingChannel(name,
                        replicationMode,
                        state,
                        backLogStatistics.getLastKeyInRedoLog(),
                        lastConfirmedKeyFromTarget,
                        channelStat.getPacketsTP(),
                        channelStat.getTotalNumberOfReplicatedPackets(),
                        inconsistencyReason != null ? JSpaceUtilities.getStackTrace(inconsistencyReason) : null,
                        channelStat.getGeneratedTraffic(),
                        channelStat.getReceivedTraffic(),
                        channelStat.getGeneratedTrafficTP(),
                        channelStat.getReceivedTrafficTP(),
                        channelStat.getGeneratedTrafficPerPacket(),
                        channelStat.getBacklogRetainedSize(),
                        channelStat.getOperatingType(),
                        channelStat.getTargetDetails(),
                        channelStat.getDelegatorDetails());
                outChannelsStat.add(outChannelStat);
            }
        }

        ReplicationStatistics.OutgoingReplication outStat = new ReplicationStatistics.OutgoingReplication(backLogStatistics,
                outChannelsStat);
        ReplicationStatistics replicationStatistics = new ReplicationStatistics(outStat);
        return replicationStatistics;
    }

    public Object[] getStatus() {
        // Adapt new result to old format in the best way it can
        LinkedList<String> allChannelsNames = new LinkedList<String>();
        LinkedList<Integer> allChannelsStatus = new LinkedList<Integer>();
        for (IReplicationSourceGroup sourceGroup : _replicationNode.getReplicationSourceGroups()) {
            Map<String, Boolean> channelsStatus = sourceGroup.getChannelsStatus();
            for (Map.Entry<String, Boolean> entry : channelsStatus.entrySet()) {
                allChannelsNames.add(entry.getKey());
                boolean value = entry.getValue();
                int status = value ? IRemoteJSpaceAdmin.REPLICATION_STATUS_ACTIVE
                        : IRemoteJSpaceAdmin.REPLICATION_STATUS_DISCONNECTED;
                allChannelsStatus.add(status);
            }
        }
        int[] allChannelsStatusPrimitive = new int[allChannelsStatus.size()];
        for (int i = 0; i < allChannelsStatus.size(); i++)
            allChannelsStatusPrimitive[i] = allChannelsStatus.get(i);

        return new Object[]{allChannelsNames.toArray(new String[allChannelsNames.size()]),
                allChannelsStatusPrimitive};
    }

    public void monitorState(OperationWeightInfo info) throws RedoLogCapacityExceededException, ConsistencyLevelViolationException {
        _replicationNode.monitorState(info);
    }

    private String nodeModeDisplayString() {
        return (_replicationNode.getNodeMode() == null ? "NONE" : _replicationNode.getNodeMode().toString());
    }

    public synchronized void setActive() {
        if (_replicationNode.getNodeMode() == ReplicationNodeMode.ACTIVE)
            return;
        if (_logger.isLoggable(Level.FINER))
            _logger.finer("Replication node [" + _replicationNode.getName()
                    + "] moving from " + nodeModeDisplayString() + " mode to "
                    + ReplicationNodeMode.ACTIVE + " mode");
        _replicationNode.createSourceGroups(ReplicationNodeMode.ACTIVE);
        _replicationNode.createTargetGroups(ReplicationNodeMode.ACTIVE);

        if (_logger.isLoggable(Level.FINE))
            _logger.fine("Replication node [" + _replicationNode.getName()
                    + "] moved from " + nodeModeDisplayString() + " mode to "
                    + ReplicationNodeMode.ACTIVE + " mode");

        for (IReplicationSourceGroup sourceGroup : _replicationNode.getReplicationSourceGroups())
            sourceGroup.setActive();
        for (IReplicationTargetGroup targetGroup : _replicationNode.getReplicationTargetGroups())
            targetGroup.setActive();

        if (_replicationNode.getNodeMode() == ReplicationNodeMode.PASSIVE) {
            _replicationNode.closeSourceGroups(ReplicationNodeMode.PASSIVE);
            _replicationNode.closeTargetGroups(ReplicationNodeMode.PASSIVE);
        }

        _replicationNode.setNodeMode(ReplicationNodeMode.ACTIVE);
    }

    public synchronized void setPassive() {
        if (_replicationNode.getNodeMode() == ReplicationNodeMode.PASSIVE)
            return;
        if (_logger.isLoggable(Level.FINER))
            _logger.finer("Replication node [" + _replicationNode.getName()
                    + "] moving from " + nodeModeDisplayString() + " mode to "
                    + ReplicationNodeMode.PASSIVE + " mode");

        _replicationNode.createSourceGroups(ReplicationNodeMode.PASSIVE);
        _replicationNode.createTargetGroups(ReplicationNodeMode.PASSIVE);

        if (_logger.isLoggable(Level.FINE))
            _logger.fine("Replication node [" + _replicationNode.getName()
                    + "] moved from " + nodeModeDisplayString() + " mode to "
                    + ReplicationNodeMode.PASSIVE + " mode");

        for (IReplicationSourceGroup sourceGroup : _replicationNode.getReplicationSourceGroups())
            sourceGroup.setPassive();
        for (IReplicationTargetGroup targetGroup : _replicationNode.getReplicationTargetGroups())
            targetGroup.setPassive();

        if (_replicationNode.getNodeMode() == ReplicationNodeMode.ACTIVE) {
            _replicationNode.closeSourceGroups(ReplicationNodeMode.ACTIVE);
            _replicationNode.closeTargetGroups(ReplicationNodeMode.ACTIVE);
        }

        _replicationNode.setNodeMode(ReplicationNodeMode.PASSIVE);
    }

    public void setNodeStateListener(IReplicationNodeStateListener listener) {
        _replicationNode.setNodeStateListener(listener);
    }

    public String dumpState() {
        return "ReplicationNode [" + _replicationNode.getName() + ", " + _replicationNode.getUniqueId() + "] mode: "
                + nodeModeDisplayString() + StringUtils.NEW_LINE
                + _replicationNode.dumpState();
    }

    public void clearStaleReplicas(long expirationTime) {
        _replicationNode.clearStaleReplicas(expirationTime);
    }

    @Override
    public IReplicationRouterAdmin getRouterAdmin() {
        return _replicationNode.getRouterAdmin();
    }

    @Override
    public DynamicSourceGroupConfigHolder getSourceGroupConfigHolder(
            String groupName) {
        IReplicationSourceGroup replicationSourceGroup = _replicationNode.getReplicationSourceGroup(groupName);
        return replicationSourceGroup.getConfigHolder();
    }


}
