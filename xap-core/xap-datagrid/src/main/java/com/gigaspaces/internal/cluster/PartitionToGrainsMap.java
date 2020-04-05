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


public class PartitionToGrainsMap implements Externalizable {
    private static final long serialVersionUID = 1L;
    public static final int GRAINS_COUNT = 4096;

    private int generation;
    private Map<Integer, Set<Integer>> partitionsToGrainsMap;
    private Map<Integer, Integer> grainsToPartitionMap;
    private int numOfPartitions;

    public PartitionToGrainsMap() {
    }

    public PartitionToGrainsMap(int partitions, int generation) {
        this.generation = generation;
        this.numOfPartitions = partitions == 0 ? 1 : partitions;
        this.partitionsToGrainsMap = new HashMap<>(this.numOfPartitions);
        this.grainsToPartitionMap = new HashMap<>(GRAINS_COUNT);
        for (int i = 1; i <= this.numOfPartitions; i++) {
            this.partitionsToGrainsMap.put(i, new TreeSet<>());
        }
    }

    public void init() {
        for (int i = 0; i < GRAINS_COUNT; i++) {
            int partitionId = (i % numOfPartitions) + 1;
            addGrain(partitionId, i);
            grainsToPartitionMap.put(i, partitionId);
        }
    }

    private void addGrain(int partitionId, int grain) {
        partitionsToGrainsMap.get(partitionId).add(grain);
    }

    private int getPartitionGrainsCount(int partitionId) {
        return partitionsToGrainsMap.get(partitionId).size();
    }

    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    public void setNumOfPartitions(int numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
    }

    public int getPartitionId(int routingValue) {
        int grain = routingValue % GRAINS_COUNT;
        return getPartitionIdImpl(grain);
    }

    public int getPartitionId(long routingValue) {
        int grain = (int) (routingValue % GRAINS_COUNT);
        return getPartitionIdImpl(grain);
    }

    private int getPartitionIdImpl(int grain) {
        return grainsToPartitionMap.get(grain) - 1;
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
        for (Map.Entry<Integer, Set<Integer>> entry : partitionsToGrainsMap.entrySet()) {
            stringBuilder.append("[").append(entry.getKey()).append("] ---> ");
            if (!entry.getValue().isEmpty()) {
                stringBuilder.append(entry.getValue().size()).append(" grains: ").append(entry.getValue());
            }
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeShort(out, (short) generation);
        IOUtils.writeShort(out, (short) numOfPartitions);
        for (Map.Entry<Integer, Set<Integer>> entry : partitionsToGrainsMap.entrySet()) {
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
        partitionsToGrainsMap = new HashMap<>(numOfPartitions);
        grainsToPartitionMap = new HashMap<>(GRAINS_COUNT);
        for (int i = 0; i < numOfPartitions; i++) {
            int key = IOUtils.readShort(in);
            Set<Integer> set = null;
            int length = IOUtils.readShort(in);
            if (length >= 0) {
                set = new TreeSet<>();
                for (int j = 0; j < length; j++) {
                    int grain = IOUtils.readShort(in);
                    set.add(grain);
                    grainsToPartitionMap.put(grain,key);
                }
            }
            partitionsToGrainsMap.put(key, set);
        }
    }
}
