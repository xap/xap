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
    private static final long serialVersionUID = 1L;
    public static final int CHUNKS_COUNT = GsEnv.propertyInt(Constants.ChunksRouting.CHUNKS_SPACE_ROUTING_COUNT).get(Constants.ChunksRouting.CHUNKS_SPACE_ROUTING_COUNT_DEFAULT);

    private int generation;
    private Map<Integer, Set<Integer>> partitionsToChunksMap;
    private transient Map<Integer, Integer> chunksToPartitionMap;
    private int numOfPartitions;

    public PartitionToChunksMap() {
    }

    public PartitionToChunksMap(int partitions) {
        this(partitions, 0);
        for (int chunk = 0; chunk < CHUNKS_COUNT; chunk++) {
            int partitionId = (chunk % partitions) + 1;
            partitionsToChunksMap.get(partitionId).add(chunk);
        }
    }

    public PartitionToChunksMap(int partitions, int generation) {
        this.generation = generation;
        this.numOfPartitions = partitions == 0 ? 1 : partitions;
        this.partitionsToChunksMap = new HashMap<>(this.numOfPartitions);
        for (int i = 1; i <= this.numOfPartitions; i++) {
            this.partitionsToChunksMap.put(i, new TreeSet<>());
        }
    }

    public int getGeneration() {
        return generation;
    }

    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    public int getPartitionId(int routingValue) {
        int chunk = routingValue % CHUNKS_COUNT;
        return getPartitionByChunk(chunk);
    }

    public int getPartitionId(long routingValue) {
        int chunk = (int) (routingValue % CHUNKS_COUNT);
        return getPartitionByChunk(chunk);
    }

    private int getPartitionByChunk(int chunk) {
        if (chunksToPartitionMap == null) {
            chunksToPartitionMap = initChunksToPartitionMap(partitionsToChunksMap);
        }
        return chunksToPartitionMap.get(chunk) - 1;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("Cluster Map [generation = "+this.getGeneration()+"]\n");
        for (Map.Entry<Integer, Set<Integer>> entry : partitionsToChunksMap.entrySet()) {
            stringBuilder.append("[").append(entry.getKey()).append("] ---> ");
            if (!entry.getValue().isEmpty()) {
                stringBuilder.append(entry.getValue().size()).append(" chunks: ").append(entry.getValue());
            }
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

    public Map<Integer, Set<Integer>> getPartitionsToChunksMap() {
        return partitionsToChunksMap;
    }

    private static Map<Integer, Integer> initChunksToPartitionMap(Map<Integer, Set<Integer>> partitionsToChunksMap) {
        Map<Integer, Integer> result = new HashMap<>();
        partitionsToChunksMap.forEach((partition, chunks) -> chunks.forEach(chunk -> result.put(chunk, partition)));
        return result;
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
        for (int i = 0; i < numOfPartitions; i++) {
            int key = IOUtils.readShort(in);
            Set<Integer> set = null;
            int length = IOUtils.readShort(in);
            if (length >= 0) {
                set = new TreeSet<>();
                for (int j = 0; j < length; j++) {
                    int chunk = IOUtils.readShort(in);
                    set.add(chunk);
                }
            }
            partitionsToChunksMap.put(key, set);
        }
    }
}
