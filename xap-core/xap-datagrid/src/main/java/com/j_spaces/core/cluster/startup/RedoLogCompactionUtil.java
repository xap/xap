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
package com.j_spaces.core.cluster.startup;

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.j_spaces.core.cluster.ReplicationPolicy;

import java.util.ListIterator;

/**
 * @author Yael Nahon
 * @since 12.3
 */
public class RedoLogCompactionUtil {

    public static long calculateWeight(long weight, long discardedCount){
        return (long) (weight + discardedCount * ReplicationPolicy.DEFAULT_DISCARDED_PACKET_WEIGHT_LOAD_FACTOR);
    }

    public static long compact(long from, long to, ListIterator iterator) {
        //        boolean rangeDiscarded = false;
        long discardedCount = 0;

        while (iterator.hasNext()) {
            IReplicationOrderedPacket current = (IReplicationOrderedPacket) iterator.next();
            if (current.getKey() < from) {
                continue;
            }
            if (current.getKey() > to) {
                break;
            }
            IReplicationOrderedPacket discarded;

            if (isTransient(current)) {
//                if (!rangeDiscarded) {
                discarded =  new GlobalOrderDiscardedReplicationPacket(current.getKey());
//                    rangeDiscarded = true;
                iterator.set(discarded);
                discardedCount++;
//                } else {
//                    iterator.remove();
//                    weightToRemove += current.getWeight();
//                    discarded = iterator.previous();
//                    ((GlobalOrderDiscardedReplicationPacket) discarded).setEndKey(current.getKey());
//                    iterator.next();
//                }
//            } else {
//                rangeDiscarded = false;
            }
        }
        return discardedCount;
    }

    public static boolean isTransient(IReplicationOrderedPacket packet) {
        IReplicationPacketData<?> data = packet.getData();
        return data != null && data.isSingleEntryData() && data.getSingleEntryData() != null && data.getSingleEntryData().isTransient();
    }
}
