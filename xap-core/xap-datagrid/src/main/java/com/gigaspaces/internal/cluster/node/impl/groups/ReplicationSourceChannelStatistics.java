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

package com.gigaspaces.internal.cluster.node.impl.groups;

import com.gigaspaces.internal.cluster.node.impl.router.ConnectionState;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationOperatingMode;


@com.gigaspaces.api.InternalApi
public class ReplicationSourceChannelStatistics
        implements IReplicationSourceChannelStatistics {

    private final String _name;
    private final ReplicationMode _channelType;
    private final ConnectionState _connectionState;
    private final long _lastConfirmedKey;
    private final int _packetsTP;
    private final long _totalNumberOfReplicatedPackets;
    private final Throwable _inconsistencyReason;
    private final boolean _active;
    private final long _generatedTraffic;
    private final long _receivedTraffic;
    private final long _generatedTrafficTP;
    private final long _receivedTrafficTP;
    private final long _generatedTrafficPerPacket;
    private final long _backlogRetainedSize;
    private final ReplicationOperatingMode _operatingMode;
    private final ReplicationEndpointDetails _targetDetails;
    private final ConnectionEndpointDetails _delegatorDetails;
    private final String _tag;

    public ReplicationSourceChannelStatistics(String name,
                                              ReplicationMode mode, ConnectionState state, boolean active,
                                              long lastConfirmedKey, int packetsTP,
                                              long totalNumberOfReplicatedPackets, Throwable inconsistencyReason,
                                              long generatedTraffic, long receivedTraffic,
                                              long generatedTrafficTP, long receivedTrafficTP,
                                              long generatedTrafficPerPacket, long backlogRetainedSize,
                                              ReplicationOperatingMode operatingMode,
                                              ReplicationEndpointDetails targetDetails,
                                              ConnectionEndpointDetails delegatorDetails,
                                              String tag) {
        _name = name;
        _channelType = mode;
        _connectionState = state;
        _active = active;
        _lastConfirmedKey = lastConfirmedKey;
        _packetsTP = packetsTP;
        _totalNumberOfReplicatedPackets = totalNumberOfReplicatedPackets;
        _inconsistencyReason = inconsistencyReason;
        _generatedTraffic = generatedTraffic;
        _receivedTraffic = receivedTraffic;
        _generatedTrafficTP = generatedTrafficTP;
        _receivedTrafficTP = receivedTrafficTP;
        _generatedTrafficPerPacket = generatedTrafficPerPacket;
        _backlogRetainedSize = backlogRetainedSize;
        _operatingMode = operatingMode;
        _targetDetails = targetDetails;
        _delegatorDetails = delegatorDetails;
        _tag = tag;
    }

    public String getName() {
        return _name;
    }

    public ReplicationMode getChannelType() {
        return _channelType;
    }

    public ConnectionState getConnectionState() {
        return _connectionState;
    }

    public long getLastConfirmedKey() {
        return _lastConfirmedKey;
    }

    public int getPacketsTP() {
        return _packetsTP;
    }

    @Override
    public long getTotalNumberOfReplicatedPackets() {
        return _totalNumberOfReplicatedPackets;
    }

    public boolean isInconsistent() {
        return _inconsistencyReason != null;
    }

    public Throwable getInconsistencyReason() {
        return _inconsistencyReason;
    }

    public boolean isActive() {
        return _active;
    }

    public long getGeneratedTraffic() {
        return _generatedTraffic;
    }

    public long getReceivedTraffic() {
        return _receivedTraffic;
    }

    public long getGeneratedTrafficTP() {
        return _generatedTrafficTP;
    }

    public long getReceivedTrafficTP() {
        return _receivedTrafficTP;
    }

    public long getGeneratedTrafficPerPacket() {
        return _generatedTrafficPerPacket;
    }

    public long getBacklogRetainedSize() {
        return _backlogRetainedSize;
    }

    public ReplicationOperatingMode getOperatingType() {
        return _operatingMode;
    }

    @Override
    public ReplicationEndpointDetails getTargetDetails() {
        return _targetDetails;
    }

    @Override
    public ConnectionEndpointDetails getDelegatorDetails() {
        return _delegatorDetails;
    }

    @Override
    public String getTag() { return _tag; }
}
