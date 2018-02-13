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
        boolean rangeDiscarded = false;

        while (iterator.hasNext()) {
            IReplicationOrderedPacket current = (IReplicationOrderedPacket) iterator.next();

            if (current.getKey() < from) {
                continue;
            }
            if (current.getKey() > to) {
                break;
            }
            if (isCompactable(current)) {
                if (!current.getData().isSingleEntryData()) {   //txn packet
                    AbstractTransactionReplicationPacketData txnPacketData = (AbstractTransactionReplicationPacketData) current.getData();
                    int deleted = compactTxn(txnPacketData.listIterator());
                    if (!txnPacketData.isEmpty()) {
                        txnPacketData.setWeight(txnPacketData.getWeight() - deleted);
                        deletedFromTxns += deleted;
                        continue;
                    }
                }
                if (discardPacket(iterator, current, rangeDiscarded)) {
                    discardedCount++;
                    rangeDiscarded = true;
                }

            } else {
                rangeDiscarded = false;
            }
        }
        return new CompactionResult(discardedCount, deletedFromTxns);
    }

    private static boolean discardPacket(ListIterator iterator, IReplicationOrderedPacket current, boolean rangeDiscarded) {
        IReplicationOrderedPacket discardedPacket = new GlobalOrderDiscardedReplicationPacket(current.getKey());
        if (!rangeDiscarded) {
            iterator.set(discardedPacket);
            return true;
        } else {
            iterator.remove();
            discardedPacket = (IReplicationOrderedPacket) iterator.previous();
            ((GlobalOrderDiscardedReplicationPacket) discardedPacket).setEndKey(current.getKey());
            iterator.next();
            return false;
        }
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