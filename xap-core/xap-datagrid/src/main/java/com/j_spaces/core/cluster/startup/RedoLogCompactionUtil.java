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
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationTransactionalPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractTransactionReplicationPacketData;
import com.j_spaces.core.cluster.ReplicationPolicy;

import java.util.ListIterator;

/**
 * @author Yael Nahon
 * @since 12.3
 */
public class RedoLogCompactionUtil {

    public static long calculateWeight(long weight, long discardedCount) {
        return (long) (weight + discardedCount * ReplicationPolicy.DEFAULT_DISCARDED_PACKET_WEIGHT_LOAD_FACTOR);
    }

    public static CompactionResult compact(long from, long to, ListIterator iterator) {
        long discardedCount = 0;
        int deletedFromTxns = 0;

        while (iterator.hasNext()) {
            IReplicationOrderedPacket current = (IReplicationOrderedPacket) iterator.next();
            if (current.getKey() < from) {
                continue;
            }
            if (current.getKey() > to) {
                break;
            }
            IReplicationOrderedPacket discardedPacket;

            if (isCompactable(current)) {
                if (current.getData().isSingleEntryData()) {
                    discardedPacket = new GlobalOrderDiscardedReplicationPacket(current.getKey());
                    iterator.set(discardedPacket);
                    discardedCount++;
                } else /*is txn packet*/ {
                    AbstractTransactionReplicationPacketData txnPacketData = (AbstractTransactionReplicationPacketData) current.getData();
                    if (!txnPacketData.hasPersistentMembers()) {
                        discardedPacket = new GlobalOrderDiscardedReplicationPacket(current.getKey());
                        iterator.set(discardedPacket);
                        discardedCount++;
                    } else {
                        int deleted = compactTxn(txnPacketData.listIterator());
                        txnPacketData.unsetHasTransientMembersFlag();
                        txnPacketData.setWeight(txnPacketData.getWeight() - deleted);
                        deletedFromTxns += deleted;
                    }
                }
            }

        }
        return new CompactionResult(discardedCount, deletedFromTxns);
    }

    public static int compactTxn(ListIterator<IReplicationTransactionalPacketEntryData> iterator) {
        int result = 0;
        while (iterator.hasNext()) {
            IReplicationTransactionalPacketEntryData next = iterator.next();
            if (next.isTransient()) {
                iterator.remove();
                result++;
            }
        }
        return result;
    }

    public static boolean isCompactable(IReplicationOrderedPacket packet) {
        IReplicationPacketData<?> data = packet.getData();
        if (data == null) {
            return false;
        }
        if (!data.isSingleEntryData() /*is txn packet*/) {
            return ((AbstractTransactionReplicationPacketData) data).hasTransientMembers();
        }
        return data.getSingleEntryData() != null && data.getSingleEntryData().isTransient();
    }
}