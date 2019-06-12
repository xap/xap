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


public interface IReplicationSourceChannelStatistics {
    String getName();

    ReplicationMode getChannelType();

    ConnectionState getConnectionState();

    boolean isActive();

    long getLastConfirmedKey();

    int getPacketsTP();

    long getTotalNumberOfReplicatedPackets();

    boolean isInconsistent();

    Throwable getInconsistencyReason();

    long getGeneratedTraffic();

    long getReceivedTraffic();

    long getGeneratedTrafficTP();

    long getReceivedTrafficTP();

    long getGeneratedTrafficPerPacket();

    long getBacklogRetainedSize();

    ReplicationOperatingMode getOperatingType();

    ReplicationEndpointDetails getTargetDetails();

    ConnectionEndpointDetails getDelegatorDetails();

    /**
     * @return a name/id/tag identifying the endpoint remote event listener
     * @since 14.5
     */
    String getTag();
}
