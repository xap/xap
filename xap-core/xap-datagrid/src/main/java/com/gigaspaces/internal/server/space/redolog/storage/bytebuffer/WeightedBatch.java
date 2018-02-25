package com.gigaspaces.internal.server.space.redolog.storage.bytebuffer;

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractTransactionReplicationPacketData;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.cluster.startup.RedoLogCompactionUtil;

import java.util.ArrayList;
import java.util.List;

import static com.j_spaces.core.cluster.startup.RedoLogCompactionUtil.HAS_PERSISTENT_MEMBERS;
import static com.j_spaces.core.cluster.startup.RedoLogCompactionUtil.compactTxn;

/**
 * @author yael nahon
 * @since 12.1
 */
public class WeightedBatch<T extends IReplicationOrderedPacket> {

    private List<T> batch;
    private long weight;
    private boolean limitReached = false;
    private CompactionResult compactionResult = new CompactionResult();
    private final long lastCompactionRangeKey;
    private T rangeDiscarded = null;

    public WeightedBatch(long lastCompactionRangeEndKey) {
        batch = new ArrayList<T>();
        this.lastCompactionRangeKey = lastCompactionRangeEndKey;
    }

    public List<T> getBatch() {
        return batch;
    }

    public void setBatch(List<T> batch) {
        this.batch = batch;
    }

    public long getWeight() {
        return RedoLogCompactionUtil.calculateWeight(weight, compactionResult.getDiscardedCount());
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }

    public boolean isLimitReached() {
        return limitReached;
    }

    public void setLimitReached(boolean limitReached) {
        this.limitReached = limitReached;
    }

    public CompactionResult getCompactionResult() {
        return compactionResult;
    }

    public void addToBatch(T packet) {
        if (packet.getKey() <= lastCompactionRangeKey && RedoLogCompactionUtil.isCompactable(packet)) {
            if (!packet.getData().isSingleEntryData()) {   //txn packet
                AbstractTransactionReplicationPacketData txnPacketData = (AbstractTransactionReplicationPacketData) packet.getData();
                if ((txnPacketData.getMembersPersistentStateFlag() & HAS_PERSISTENT_MEMBERS) != 0) {
                    int deleted = compactTxn(txnPacketData.listIterator());
                    txnPacketData.setWeight(txnPacketData.getWeight() - deleted);
                    txnPacketData.setMembersPersistentStateFlag(HAS_PERSISTENT_MEMBERS);
                    compactionResult.increaseWeightRemoved(deleted);
                    batch.add(packet);
                    weight += packet.getWeight();
                    return;
                }
            } else {
                compactionResult.increaseWeightRemoved(1);
            }
            if (discardPacket(packet)) {
                compactionResult.increaseDiscardedCount(1);
            }
        } else {
            batch.add(packet);
            weight += packet.getWeight();
            rangeDiscarded = null;
        }
    }

    private boolean discardPacket(IReplicationOrderedPacket current) {
        IReplicationOrderedPacket discardedPacket = new GlobalOrderDiscardedReplicationPacket(current.getKey());
        if (rangeDiscarded == null) {
            rangeDiscarded = (T) discardedPacket;
            return true;
        } else {
            ((GlobalOrderDiscardedReplicationPacket) rangeDiscarded).setEndKey(current.getKey());
            return false;
        }
    }

    public int size() {
        return batch.size();
    }
}