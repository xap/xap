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

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yael nahon
 * @since 12.1
 */
public class WeightedBatch<T> {

    private  List<T> batch;
    private long weight;
    private boolean limitReached = false;

    public WeightedBatch() {
        batch = new ArrayList<T>();
    }

    public WeightedBatch(List<T> batch) {
        this.batch = batch;
    }

    public WeightedBatch(List<T> batch, long weight) {
        this.batch = batch;
        this.weight = weight;
    }

    public List<T> getBatch() {
        return batch;
    }

    public void setBatch(List<T> batch) {
        this.batch = batch;
    }

    public long getWeight() {
        return weight;
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

    public void addToBatch(T packet){
       batch.add(packet);
        int packetWeight = ((IReplicationOrderedPacket) packet).getWeight();
        this.weight += packetWeight;
    }

    public int size() {
        return batch.size();
    }
}
