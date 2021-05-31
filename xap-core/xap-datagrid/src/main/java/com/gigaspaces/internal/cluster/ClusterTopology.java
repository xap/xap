package com.gigaspaces.internal.cluster;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.Constants;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

public class ClusterTopology implements SmartExternalizable {
    private static final long serialVersionUID = 1L;
    public static final int CHUNKS_COUNT = GsEnv.propertyInt(Constants.ChunksRouting.CHUNKS_SPACE_ROUTING_COUNT).get(Constants.ChunksRouting.CHUNKS_SPACE_ROUTING_COUNT_DEFAULT);

    private int generation;
    private String schema;
    private int numOfInstances;
    private int numOfBackups;
    private Map<Integer, Set<Integer>> partitionsToChunksMap;
    private transient Map<Integer, Integer> chunksToPartitionMap;

    public ClusterTopology() {
    }

    /**
     * used from unit tests
     * @param numOfPartitions
     */
    public ClusterTopology(int numOfPartitions) {
        this.generation = 0;
        this.schema = "partitioned";
        this.numOfInstances = numOfPartitions;
        this.numOfBackups = 1;
        this.partitionsToChunksMap = ScalePlan.createAndInitPartitionChunksMap(numOfPartitions);
    }

    public ClusterTopology setGeneration(int generation) {
        this.generation = generation;
        return this;
    }

    public ClusterTopology setSchema(String schema) {
        this.schema = schema;
        return this;
    }

    public ClusterTopology setNumOfInstances(int numOfInstances) {
        this.numOfInstances = numOfInstances;
        return this;
    }

    public ClusterTopology setNumOfBackups(int numOfBackups) {
        this.numOfBackups = numOfBackups;
        return this;
    }

    public ClusterTopology setPartitionsToChunksMap(Map<Integer, Set<Integer>> partitionsToChunksMap) {
        this.partitionsToChunksMap = partitionsToChunksMap;
        return this;
    }

    public int getGeneration() {
        return generation;
    }

    public int getNumberOfInstances() {
        return numOfInstances;
    }

    public int getNumberOfBackups() {
        return numOfBackups;
    }

    public String getSchema() {
        return schema;
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
        StringBuilder stringBuilder = new StringBuilder("ClusterTopology{" +
                "generation=" + generation +
                ", schema='" + schema + '\'' +
                ", numOfInstances=" + numOfInstances +
                ", numOfBackups=" + numOfBackups +
                ", partitionsToChunksMap=");
        if(partitionsToChunksMap != null) {
            stringBuilder.append("\n");
            for (Map.Entry<Integer, Set<Integer>> entry : partitionsToChunksMap.entrySet()) {
                stringBuilder.append("\t[").append(entry.getKey()).append("] ---> ");
                if (!entry.getValue().isEmpty()) {
                    stringBuilder.append(entry.getValue().size()).append(" chunks: ").append(entry.getValue());
                }
                stringBuilder.append("\n");
            }
        } else {
            stringBuilder.append("null");
        }
        stringBuilder.append('}');
        return stringBuilder.toString();
    }

    public Map<Integer, Set<Integer>> getPartitionsToChunksMap() {
        return partitionsToChunksMap;
    }

    public Set<Integer> getPartitionChunks(int partition) {
        return partitionsToChunksMap.get(partition);
    }


    private static Map<Integer, Integer> initChunksToPartitionMap(Map<Integer, Set<Integer>> partitionsToChunksMap) {
        Map<Integer, Integer> result = new HashMap<>();
        partitionsToChunksMap.forEach((partition, chunks) -> chunks.forEach(chunk -> result.put(chunk, partition)));
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeShort(out, (short) generation);
        IOUtils.writeString(out, schema);
        IOUtils.writeShort(out, (short) numOfInstances);
        IOUtils.writeShort(out, (short) numOfBackups);
        IOUtils.writeShort(out, (short) (partitionsToChunksMap == null ? 0 : partitionsToChunksMap.size()));
        if(partitionsToChunksMap != null) {
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

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        generation = IOUtils.readShort(in);
        schema = IOUtils.readString(in);
        numOfInstances = IOUtils.readShort(in);
        numOfBackups = IOUtils.readShort(in);
        int size = IOUtils.readShort(in);
        if(size>=0) {
            partitionsToChunksMap = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
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

    public ClusterTopology copy() {
        return new ClusterTopology()
                .setGeneration(generation)
                .setSchema(schema)
                .setNumOfInstances(numOfInstances)
                .setNumOfBackups(numOfBackups)
                .setPartitionsToChunksMap(partitionsToChunksMap);
    }

    public boolean equivalent(ClusterTopology other) {
        return this.schema.equals(other.schema) &&
               this.numOfInstances == other.numOfInstances &&
               this.numOfBackups == other.numOfBackups;
    }
}
