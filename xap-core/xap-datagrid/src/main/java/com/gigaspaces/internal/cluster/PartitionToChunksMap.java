package com.gigaspaces.internal.cluster;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.GsEnv;
import com.j_spaces.core.Constants;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;


public class PartitionToChunksMap implements Externalizable {
    public static final int CHUNKS_COUNT = GsEnv.propertyInt(Constants.ChunksRouting.CHUNKS_SPACE_ROUTING_COUNT).get(Constants.ChunksRouting.CHUNKS_SPACE_ROUTING_COUNT_DEFAULT);
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

    public static ScalePlan scaleOutMap(PartitionToChunksMap currentMap, int factor) {
        ScalePlan scalePlan = new ScalePlan();
        scalePlan.initScaleOutPlan(currentMap, factor);
        PartitionToChunksMap newMap = new PartitionToChunksMap(currentMap.getNumOfPartitions() + factor, currentMap.getGeneration() + 1);
        HashMap<Integer, Integer> newChunksCountPerPartition = getChunksCountPerPartitionMap(newMap);
        int newPartitionId = currentMap.getNumOfPartitions() + 1;
        for (Map.Entry<Integer, Set<Integer>> partitionMapping : currentMap.getPartitionsToChunksMap().entrySet()) {
            int currentPartitionId = partitionMapping.getKey();
            int oldPartitionAssignments = 0;
            for (Integer chunk : partitionMapping.getValue()) {
                if (oldPartitionAssignments < newChunksCountPerPartition.get(currentPartitionId)) {
                    newMap.getPartitionsToChunksMap().get(currentPartitionId).add(chunk);
                    newMap.getChunksToPartitionMap().put(chunk, currentPartitionId);
                    oldPartitionAssignments++;
                } else {
                    if (newMap.getPartitionsToChunksMap().get(newPartitionId).size() == newChunksCountPerPartition.get(newPartitionId) && newPartitionId < newMap.getNumOfPartitions()) {
                        newPartitionId++;
                    }
                    newMap.getPartitionsToChunksMap().get(newPartitionId).add(chunk);
                    newMap.getChunksToPartitionMap().put(chunk, newPartitionId);
                    Map<Integer, Set<Integer>> partitionPlan = scalePlan.getPlans().get(currentPartitionId);
                    if (!partitionPlan.containsKey(newPartitionId)) {
                        partitionPlan.put(newPartitionId, new HashSet<>());
                    }
                    partitionPlan.get(newPartitionId).add(chunk);
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
        HashMap<Integer, Integer> newChunksCountPerPartition = getChunksCountPerPartitionMap(newMap);
        int remainingPartitionId = 1;
        for (Map.Entry<Integer, Set<Integer>> partitionMapping : currentMap.getPartitionsToChunksMap().entrySet()) {
            int currentPartitionId = partitionMapping.getKey();
            if (currentPartitionId <= newMap.getNumOfPartitions()) {
                newMap.getPartitionsToChunksMap().get(currentPartitionId).addAll(partitionMapping.getValue());
                for (Integer chunk : partitionMapping.getValue()) {
                    newMap.getChunksToPartitionMap().put(chunk, currentPartitionId);
                }
            } else {
                for (Integer chunk : partitionMapping.getValue()) {
                    if (newMap.getPartitionsToChunksMap().get(remainingPartitionId).size() == newChunksCountPerPartition.get(remainingPartitionId) && remainingPartitionId < newMap.getNumOfPartitions()) {
                        remainingPartitionId++;
                    }
                    newMap.getPartitionsToChunksMap().get(remainingPartitionId).add(chunk);
                    newMap.getChunksToPartitionMap().put(chunk, remainingPartitionId);
                    Map<Integer, Set<Integer>> partitionPlan = scalePlan.getPlans().get(currentPartitionId);
                    if (!partitionPlan.containsKey(remainingPartitionId)) {
                        partitionPlan.put(remainingPartitionId, new HashSet<>());
                    }
                    partitionPlan.get(remainingPartitionId).add(chunk);
                }
            }
        }


        scalePlan.setNewMap(newMap);
        return scalePlan;
    }

    private static HashMap<Integer, Integer> getChunksCountPerPartitionMap(PartitionToChunksMap newMap) {
        HashMap<Integer, Integer> newChunksCountPerPartition = new HashMap<>(newMap.getNumOfPartitions());
        int newChunksPerPartition = PartitionToChunksMap.CHUNKS_COUNT / newMap.getNumOfPartitions();
        int newLeftover = PartitionToChunksMap.CHUNKS_COUNT % newMap.getNumOfPartitions();
        for (int i = 1; i <= newMap.getNumOfPartitions(); i++) {
            newChunksCountPerPartition.put(i, newChunksPerPartition);
        }
        for (int i = 0, index = 1; i < newLeftover; i++, index++) {
            newChunksCountPerPartition.put(index, newChunksCountPerPartition.get(index) + 1);
        }
        return newChunksCountPerPartition;
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
}
