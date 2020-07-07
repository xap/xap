package com.gigaspaces.internal.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ChunksRoutingManager implements Externalizable {
    private Map<Integer, PartitionToChunksMap> maps;
    private Map <Integer,Integer> partitionGenerationMap;
    private int currentGeneration;

    public ChunksRoutingManager() {
    }

    public ChunksRoutingManager(PartitionToChunksMap chunksMap) {
        this.currentGeneration = chunksMap.getGeneration();
        maps = new HashMap<>();
        maps.put(chunksMap.getGeneration(),chunksMap);
        partitionGenerationMap = new HashMap<>(chunksMap.getNumOfPartitions());
        for (int i = 1; i <= chunksMap.getNumOfPartitions(); i++) {
            partitionGenerationMap.put(i,chunksMap.getGeneration());
        }
    }

    public Map<Integer, PartitionToChunksMap> getMaps() {
        return maps;
    }

    public void setMaps(Map<Integer, PartitionToChunksMap> maps) {
        this.maps = maps;
    }

    public void setPartitionGenerationMap(Map<Integer, Integer> partitionGenerationMap) {
        this.partitionGenerationMap = partitionGenerationMap;
    }

    public void setCurrentGeneration(int currentGeneration) {
        this.currentGeneration = currentGeneration;
    }

    public PartitionToChunksMap getMapForPartition(int partitionId) {
        Integer partitionGeneration = this.partitionGenerationMap.get(partitionId);
        return partitionGeneration == null ? null : this.getMaps().get(partitionGeneration);
    }

    public void setPartitionGeneration(int partitionId, int generation) {
        this.partitionGenerationMap.put(partitionId,generation);
    }

    public void deletePartitionGeneration(int partitionId){
        this.partitionGenerationMap.remove(partitionId);
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

    public PartitionToChunksMap getLastestMap() {
        return maps.get(currentGeneration);
    }

    public void addNewMap(PartitionToChunksMap chunksMap) {
        maps.put(chunksMap.getGeneration(), chunksMap);
    }

    public void updateGeneration(){
        currentGeneration = Collections.max(maps.keySet());
    }

    public ChunksRoutingManager deepCopy(){
        ChunksRoutingManager copy = new ChunksRoutingManager();
        Map<Integer, PartitionToChunksMap> maps = new HashMap<>(this.maps.size());
        for (Map.Entry<Integer, PartitionToChunksMap> mapEntry : this.maps.entrySet()) {
            maps.put(mapEntry.getKey(),mapEntry.getValue());
        }
        copy.setMaps(maps);
        Map <Integer,Integer> partitionGenerationMap = new HashMap<>(this.partitionGenerationMap.size());
        for (Map.Entry<Integer, Integer> entry : this.partitionGenerationMap.entrySet()) {
            partitionGenerationMap.put(entry.getKey(),entry.getValue());
        }
        copy.setPartitionGenerationMap(partitionGenerationMap);
        copy.setCurrentGeneration(this.currentGeneration);
        return copy;
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
