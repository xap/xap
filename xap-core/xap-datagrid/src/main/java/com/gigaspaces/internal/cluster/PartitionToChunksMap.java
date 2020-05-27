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

import com.gigaspaces.internal.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


public class PartitionToChunksMap implements Externalizable {
    public static final int CHUNKS_COUNT = 4096;
    private static final long serialVersionUID = 1L;
    private int generation;
    private Map<Integer, Set<Integer>> partitionsToChunksMap;
    private Map<Integer, Integer> chunksToPartitionMap;
    private int numOfPartitions;

    public PartitionToChunksMap() {
    }

    public PartitionToChunksMap(int partitions, int generation) {
        this.generation = generation;
        this.numOfPartitions = partitions == 0 ? 1 : partitions;
        this.partitionsToChunksMap = new HashMap<>(this.numOfPartitions);
        this.chunksToPartitionMap = new HashMap<>(CHUNKS_COUNT);
        for (int i = 1; i <= this.numOfPartitions; i++) {
            this.partitionsToChunksMap.put(i, new TreeSet<>());
        }
    }

    public void init() {
        for (int i = 0; i < CHUNKS_COUNT; i++) {
            int partitionId = (i % numOfPartitions) + 1;
            addChunk(partitionId, i);
            chunksToPartitionMap.put(i, partitionId);
        }
    }

    private void addChunk(int partitionId, int chunk) {
        partitionsToChunksMap.get(partitionId).add(chunk);
    }

    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    public void setNumOfPartitions(int numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
    }

    public int getPartitionId(int routingValue) {
        int chunk = routingValue % CHUNKS_COUNT;
        return getPartitionIdImpl(chunk);
    }

    public int getPartitionId(long routingValue) {
        int chunk = (int) (routingValue % CHUNKS_COUNT);
        return getPartitionIdImpl(chunk);
    }

    private int getPartitionIdImpl(int chunk) {
        return chunksToPartitionMap.get(chunk) - 1;
    }

    public int getGeneration() {
        return generation;
    }

    public void setGeneration(int generation) {
        this.generation = generation;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("Cluster Map\n");
        for (Map.Entry<Integer, Set<Integer>> entry : partitionsToChunksMap.entrySet()) {
            stringBuilder.append("[").append(entry.getKey()).append("] ---> ");
            if (!entry.getValue().isEmpty()) {
                stringBuilder.append(entry.getValue().size()).append(" chunks: ").append(entry.getValue());
            }
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeShort(out, (short) generation);
        IOUtils.writeShort(out, (short) numOfPartitions);
        for (Map.Entry<Integer, Set<Integer>> entry : partitionsToChunksMap.entrySet()) {
            IOUtils.writeShort(out, entry.getKey().shortValue());
            Set<Integer> set = entry.getValue();
            if (set == null)
                IOUtils.writeShort(out, (short) -1);
            else {
                int length = set.size();
                IOUtils.writeShort(out, (short) length);
                for (Integer integer : set) {
                    IOUtils.writeShort(out, integer.shortValue());
                }
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        generation = IOUtils.readShort(in);
        numOfPartitions = IOUtils.readShort(in);
        partitionsToChunksMap = new HashMap<>(numOfPartitions);
        chunksToPartitionMap = new HashMap<>(CHUNKS_COUNT);
        for (int i = 0; i < numOfPartitions; i++) {
            int key = IOUtils.readShort(in);
            Set<Integer> set = null;
            int length = IOUtils.readShort(in);
            if (length >= 0) {
                set = new TreeSet<>();
                for (int j = 0; j < length; j++) {
                    int chunk = IOUtils.readShort(in);
                    set.add(chunk);
                    chunksToPartitionMap.put(chunk, key);
                }
            }
            partitionsToChunksMap.put(key, set);
        }
    }

    public Map<Integer, Set<Integer>> getPartitionsToChunksMap() {
        return partitionsToChunksMap;
    }

    public Map<Integer, Integer> getChunksToPartitionMap() {
        return chunksToPartitionMap;
    }

    public static ScalePlan scaleOutMap(PartitionToChunksMap currentMap, int factor) {
        ScalePlan scalePlan = new ScalePlan();
        scalePlan.initScaleOutPlan(currentMap, factor);
        PartitionToChunksMap newMap = new PartitionToChunksMap(currentMap.getNumOfPartitions() + factor, currentMap.getGeneration() + 1);
        int newChunksPerPartition = PartitionToChunksMap.CHUNKS_COUNT / newMap.getNumOfPartitions();
        int newPartitionsAssignments = 0;
        int newPartitionsBeginningIndex = currentMap.getNumOfPartitions() + 1;
        for (Map.Entry<Integer, Set<Integer>> partitionMapping : currentMap.getPartitionsToChunksMap().entrySet()) {
            Integer partition = partitionMapping.getKey();
            int oldPartitionAssignments = 0;
            for (Integer chunk : partitionMapping.getValue()) {
                if (oldPartitionAssignments < newChunksPerPartition) {
                    newMap.getPartitionsToChunksMap().get(partition).add(chunk);
                    newMap.getChunksToPartitionMap().put(chunk, partition);
                    oldPartitionAssignments++;
                } else {
                    int newPartitionIndex = newPartitionsBeginningIndex + (newPartitionsAssignments % factor);
                    newMap.getPartitionsToChunksMap().get(newPartitionIndex).add(chunk);
                    newMap.getChunksToPartitionMap().put(chunk, newPartitionIndex);
                    newPartitionsAssignments++;
                    scalePlan.getPlans().get(partition).get(newPartitionIndex).add(chunk);
                }
            }
        }
        scalePlan.setNewMap(newMap);
        return scalePlan;
    }

    public static ScalePlan scaleInMap(PartitionToChunksMap currentMap, int factor) {
        ScalePlan scalePlan = new ScalePlan();
        scalePlan.initScaleInPlan(currentMap, factor);
        int newPartitionCount = currentMap.getNumOfPartitions() - factor;
        PartitionToChunksMap newMap = new PartitionToChunksMap(newPartitionCount, currentMap.getGeneration() + 1);
        int remainingPartitionsAssignments = 0;
        for (Map.Entry<Integer, Set<Integer>> partitionMapping : currentMap.getPartitionsToChunksMap().entrySet()) {
            Integer partition = partitionMapping.getKey();
            if (partition <= newMap.getNumOfPartitions()) {
                newMap.getPartitionsToChunksMap().get(partition).addAll(partitionMapping.getValue());
                for (Integer chunk : partitionMapping.getValue()) {
                    newMap.getChunksToPartitionMap().put(chunk, partition);
                }
            } else {
                for (Integer chunk : partitionMapping.getValue()) {
                    int newPartitionIndex = 1 + (remainingPartitionsAssignments % newPartitionCount);
                    newMap.getPartitionsToChunksMap().get(newPartitionIndex).add(chunk);
                    newMap.getChunksToPartitionMap().put(chunk, newPartitionIndex);
                    remainingPartitionsAssignments++;
                    scalePlan.getPlans().get(partition).get(newPartitionIndex).add(chunk);
                }
            }

        }
        scalePlan.setNewMap(newMap);
        return scalePlan;
    }
}
