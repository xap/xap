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
package com.gigaspaces.internal.server.space.redolog.storage.bytebuffer;

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderDiscardedReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractTransactionReplicationPacketData;
import com.j_spaces.core.cluster.startup.CompactionResult;
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
    private CompactionResult compactionResult = new CompactionResult();
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
        long packetWeight = ((IReplicationOrderedPacket) packet).getWeight();
        if (((IReplicationOrderedPacket) packet).getKey() <= lastCompactionRangeKey && RedoLogCompactionUtil.isCompactable((IReplicationOrderedPacket) packet)) {
            if (((IReplicationOrderedPacket) packet).getData().isSingleEntryData()) {
                T discarded = (T) new GlobalOrderDiscardedReplicationPacket(((IReplicationOrderedPacket) packet).getKey());
                batch.add(discarded);
                compactionResult.increaseDiscardedCount(1);
                this.weight -= packetWeight;
            } else {
                AbstractTransactionReplicationPacketData txnPacketData = (AbstractTransactionReplicationPacketData) ((IReplicationOrderedPacket) packet).getData();
                int deletedFromTxn = RedoLogCompactionUtil.compactTxn(txnPacketData.listIterator());
                compactionResult.increaseDeletedFromTxnCount(deletedFromTxn);
                txnPacketData.setWeight(txnPacketData.getWeight() - compactionResult.getDeletedFromTxn());
                batch.add(packet);
                this.weight += packetWeight;
            }
        } else {
            batch.add(packet);
            this.weight += packetWeight;
        }
    }

    public int size() {
        return batch.size();
    }
}
