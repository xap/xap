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

package com.gigaspaces.internal.cluster.node.impl.packets;

import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ISwapExternalizable;
import com.gigaspaces.serialization.SmartExternalizable;


/**
 * A replication packet that has an order that specify a certain order of these packets that needs
 * to be kept when processed at the target
 *
 * @author eitany
 * @since 8.0
 */
public interface IReplicationOrderedPacket extends Cloneable, SmartExternalizable, ISwapExternalizable {
    /**
     * @return the actual packet data that is needed for the processing
     */
    IReplicationPacketData<?> getData();

    long getKey();

    /**
     * return the last key for given packets - relevant to range packets - merged discarded/deleted
     * packets
     */
    long getEndKey();

    boolean isDataPacket();

    boolean isDiscardedPacket();

    IReplicationOrderedPacket clone();

    /**
     * Clones the packet replacing its data with the new data
     */
    IReplicationOrderedPacket cloneWithNewData(IReplicationPacketData<?> newData);

    int getWeight();

}
