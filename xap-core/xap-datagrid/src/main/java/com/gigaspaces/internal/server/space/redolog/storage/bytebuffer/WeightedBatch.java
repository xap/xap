package com.gigaspaces.internal.server.space.redolog.storage.bytebuffer;

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.j_spaces.core.cluster.startup.RedoLogCompactionUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.1
 */
public class WeightedBatch<T> {

    private List<T> batch;
    private long weight;
    private boolean limitReached = false;
    private long discardedPacketCount = 0;
    private final long lastCompactionRangeKey;

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
        return RedoLogCompactionUtil.calculateWeight(weight,discardedPacketCount);
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

    public long getDiscardedPacketCount() {
        return discardedPacketCount;
    }

    public void addToBatch(T packet) {
        long packetWeight = ((IReplicationOrderedPacket) packet).getWeight();
        if(((IReplicationOrderedPacket) packet).getKey() <= lastCompactionRangeKey && RedoLogCompactionUtil.isTransient((IReplicationOrderedPacket) packet)){
            T discarded = (T) new GlobalOrderDiscardedReplicationPacket(((IReplicationOrderedPacket) packet).getKey());
            batch.add(discarded);
            discardedPacketCount++;
            this.weight -= packetWeight;
        }else{
            batch.add(packet);
            this.weight += packetWeight;
        }
    }

    public int size() {
        return batch.size();
    }
}
