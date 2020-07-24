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
package com.gigaspaces.internal.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ChunksRoutingManager implements Externalizable {
    private static final long serialVersionUID = 1L;

    private Map<Integer, PartitionToChunksMap> maps;
    private Map <Integer,Integer> partitionGenerationMap;
    private int currentGeneration;

    public ChunksRoutingManager() {
    }

    public ChunksRoutingManager(PartitionToChunksMap chunksMap) {
        this.currentGeneration = chunksMap.getGeneration();
        maps = new HashMap<>();
        partitionGenerationMap = new HashMap<>();
        addMap(chunksMap, 1);
    }

    public ChunksRoutingManager(ChunksRoutingManager other) {
        this.maps = new HashMap<>(other.maps);
        this.partitionGenerationMap = new HashMap<>(other.partitionGenerationMap);
        this.currentGeneration = other.currentGeneration;
    }

    public PartitionToChunksMap getLatestMap() {
        return maps.get(currentGeneration);
    }

    public PartitionToChunksMap getMapForLatestGeneration() {
        return maps.get(getLatestGeneration());
    }


    public PartitionToChunksMap getMapForPartition(int partitionId) {
        Integer partitionGeneration = this.partitionGenerationMap.get(partitionId);
        return partitionGeneration == null ? null : maps.get(partitionGeneration);
    }

    public void addMap(PartitionToChunksMap chunksMap, int basePartitionToUpdate) {
        maps.put(chunksMap.getGeneration(), chunksMap);
        updatePartitionsGeneration(basePartitionToUpdate, chunksMap.getNumOfPartitions(), chunksMap.getGeneration());
    }

    public void updatePartitionsGeneration(int from, int to, int generation) {
        for (int partitionId = from ; partitionId <= to; partitionId++) {
            partitionGenerationMap.put(partitionId, generation);
        }
    }

    public void deletePartitionGeneration(int partitionId){
        this.partitionGenerationMap.remove(partitionId);
    }

    public int getLatestGeneration() {
        return Collections.max(maps.keySet());
    }
    public void updateGeneration(){
        currentGeneration = getLatestGeneration();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(currentGeneration);
        out.writeShort(maps.size());
        for (PartitionToChunksMap map : maps.values()) {
            out.writeObject(map);
        }
        out.writeShort(partitionGenerationMap.size());
        for (Map.Entry<Integer, Integer> entry : partitionGenerationMap.entrySet()) {
            out.writeShort(entry.getKey());
            out.writeShort(entry.getValue());
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.currentGeneration = in.readShort();
        int count = in.readShort();
        maps = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            PartitionToChunksMap map = (PartitionToChunksMap) in.readObject();
            maps.put(map.getGeneration(),map);
        }
        int size = in.readShort();
        partitionGenerationMap = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            partitionGenerationMap.put((int) in.readShort(), (int)in.readShort());
        }
    }

    @Override
    public String toString() {
        return "ChunksRoutingManager{" +
                "maps=" + maps +
                ", partitionGenerationMap=" + partitionGenerationMap +
                ", currentGeneration=" + currentGeneration +
                '}';
    }
}
