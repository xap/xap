package com.gigaspaces.internal.cluster;

import com.gigaspaces.internal.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

public class ScalePlan implements Externalizable {

    private PartitionToChunksMap currentMap;
    private PartitionToChunksMap newMap;
    private Map<Integer, Map<Integer, Set<Integer>>> plans;

    public ScalePlan() {
    }

    public void initScaleOutPlan(PartitionToChunksMap currentMap, int factor) {
        this.currentMap = currentMap;
        this.plans = new HashMap<>(currentMap.getNumOfPartitions());
        for (int i = 1; i <= currentMap.getNumOfPartitions(); i++) {
            this.plans.put(i, new HashMap<>(factor));
        }
    }

    public void initScaleInPlan(PartitionToChunksMap currentMap, int factor) {
        this.currentMap = currentMap;
        this.plans = new HashMap<>(currentMap.getNumOfPartitions());
        for (int i = currentMap.getNumOfPartitions(); i > currentMap.getNumOfPartitions() - factor; i--) {
            this.plans.put(i, new HashMap<>(factor));
        }
    }

    public PartitionToChunksMap getCurrentMap() {
        return currentMap;
    }

    public void setCurrentMap(PartitionToChunksMap currentMap) {
        this.currentMap = currentMap;
    }

    public PartitionToChunksMap getNewMap() {
        return newMap;
    }

    public void setNewMap(PartitionToChunksMap newMap) {
        this.newMap = newMap;
    }

    public Map<Integer, Map<Integer, Set<Integer>>> getPlans() {
        return plans;
    }

    public void setPlans(Map<Integer, Map<Integer, Set<Integer>>> plans) {
        this.plans = plans;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ScalePlan: \n");
        for (Map.Entry<Integer, Map<Integer, Set<Integer>>> partitionPlan : plans.entrySet()) {
            sb.append("Partition ").append(partitionPlan.getKey()).append(":");
            for (Map.Entry<Integer, Set<Integer>> copyPlan : partitionPlan.getValue().entrySet()) {
                sb.append("\t\n").append("move ").append(copyPlan.getValue().size()).append(" chunks to partition ").append(copyPlan.getKey());
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, currentMap);
        IOUtils.writeObject(out, newMap);
        IOUtils.writeShort(out, ((short) plans.size()));
        for (Map.Entry<Integer, Map<Integer, Set<Integer>>> entry : plans.entrySet()) {
            IOUtils.writeShort(out, entry.getKey().shortValue());
            IOUtils.writeShort(out, ((short) entry.getValue().size()));
            for (Map.Entry<Integer, Set<Integer>> integerSetEntry : entry.getValue().entrySet()) {
                IOUtils.writeShort(out, integerSetEntry.getKey().shortValue());
                IOUtils.writeShort(out, ((short) integerSetEntry.getValue().size()));
                for (Integer chunk : integerSetEntry.getValue()) {
                    IOUtils.writeShort(out, chunk.shortValue());
                }
            }
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.currentMap = IOUtils.readObject(in);
        this.newMap = IOUtils.readObject(in);
        int mapSize = IOUtils.readShort(in);
        this.plans = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            int oldPartition = IOUtils.readShort(in);
            int innerMapSize  = IOUtils.readShort(in);;
            HashMap<Integer, Set<Integer>> oldPartitionPlan = new HashMap<>(innerMapSize);
            for (int j = 0; j < innerMapSize; j++) {
                int newPartition  = IOUtils.readShort(in);
                int setSize = IOUtils.readShort(in);
                HashSet<Integer> newPartitionPlan = new HashSet<>(setSize);
                for (int k = 0; k < setSize; k++) {
                    newPartitionPlan.add((int) IOUtils.readShort(in));
                }
                oldPartitionPlan.put(newPartition,newPartitionPlan);
            }
            plans.put(oldPartition, oldPartitionPlan);
        }
    }


}
