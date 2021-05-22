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

package com.gigaspaces.internal.cluster.node.impl.packets.data;

import com.gigaspaces.internal.cluster.node.impl.ReplicationMultipleOperationType;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.serialization.SmartExternalizable;

import java.util.Collection;


/**
 * The replication packet data that is kept in side an {@link IReplicationOrderedPacket} which
 * contains the data needed for the processing of the packet
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationPacketData<T extends IReplicationPacketEntryData>
        extends Collection<T>, SmartExternalizable, Cloneable, ISwapExternalizable {
    IReplicationPacketData<T> clone();

    /**
     * If the replication of this packet is delayed due to being asynchronous by nature or the
     * target was disconnected when the replication packet was generated, this method is called
     * before the actual replication and the implementor can modify the packet accordingly, mostly
     * relevant for time to live update. If the method returns false the packet is no longer
     * relevant and it is discarded
     */
    boolean beforeDelayedReplication();

    boolean isSingleEntryData();

    IReplicationPacketEntryData getSingleEntryData();

    boolean supportsReplicationFilter();

    /**
     * Gets whether this data is part of multiple participants data
     */
    boolean isMultiParticipantData();

    ReplicationMultipleOperationType getMultipleOperationType();

    boolean requiresRecoveryFiltering();

    Object getRecoveryFilteringId();

    /**
     * @return Gets whether this packet was received from gateway replication
     */
    boolean isFromGateway();

    void setWeight(int weight);

    int getWeight();

}
