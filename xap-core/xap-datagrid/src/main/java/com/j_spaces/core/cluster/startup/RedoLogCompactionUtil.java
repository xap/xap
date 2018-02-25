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

    public static final byte HAS_TRANSIENT_MEMBERS = 1 << 0;

    public static final byte HAS_PERSISTENT_MEMBERS = 1 << 1;

    public static long calculateWeight(long weight, long discardedCount) {
        return (long) (weight + discardedCount * ReplicationPolicy.DEFAULT_DISCARDED_PACKET_WEIGHT_LOAD_FACTOR);
    }

    public static CompactionResult compact(long from, long to, ListIterator iterator) {
        long discardedCount = 0;
        long weightRemoved = 0;
        boolean rangeDiscarded = false;

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
                if (!current.getData().isSingleEntryData()) {   //txn packet
                    AbstractTransactionReplicationPacketData txnPacketData = (AbstractTransactionReplicationPacketData) current.getData();
                    if ((txnPacketData.getMembersPersistentStateFlag() & HAS_PERSISTENT_MEMBERS) != 0) {
                        int deleted = compactTxn(txnPacketData.listIterator());
                        txnPacketData.setWeight(txnPacketData.getWeight() - deleted);
                        weightRemoved += deleted;
                        txnPacketData.setMembersPersistentStateFlag(RedoLogCompactionUtil.HAS_PERSISTENT_MEMBERS);
                        continue;
                    } else {
                        weightRemoved += txnPacketData.getWeight();
                    }
                } else {
                    weightRemoved++;
                }
                if (discardPacket(iterator, current, rangeDiscarded)) {
                    discardedCount++;
                    rangeDiscarded = true;
                }

            } else {
                rangeDiscarded = false;
            }
        }
        return new CompactionResult(discardedCount, weightRemoved);
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
            byte persistentStateFlag = ((AbstractTransactionReplicationPacketData) data).getMembersPersistentStateFlag();
            return (persistentStateFlag & HAS_TRANSIENT_MEMBERS) != 0;
        }
        return data.getSingleEntryData() != null && data.getSingleEntryData().isTransient();
    }

    public static byte getPersistentStateFlag(boolean hasTransientMembers, boolean hasPersistentMembers) {
        byte stateFlag = 0;
        if(hasTransientMembers && hasPersistentMembers){
            stateFlag = (byte) (RedoLogCompactionUtil.HAS_TRANSIENT_MEMBERS + RedoLogCompactionUtil.HAS_PERSISTENT_MEMBERS);
        } else if (hasTransientMembers){
            stateFlag = RedoLogCompactionUtil.HAS_TRANSIENT_MEMBERS;
        } else if (hasPersistentMembers){
            stateFlag = RedoLogCompactionUtil.HAS_PERSISTENT_MEMBERS;
        }
        return stateFlag;
    }

    public static <T extends IReplicationOrderedPacket> void skipToKey(ListIterator<T> iterator, long fromKey) {
        while (iterator.hasNext()) {
            T next = iterator.next();
            if (next.isDiscardedPacket()) {
                if (fromKey > next.getKey() && fromKey < next.getEndKey()) {
                    iterator.previous();
                    break;
                }
            } else if (next.getKey() == fromKey) {
                iterator.previous();
                break;
            }
        }
    }
}