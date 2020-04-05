package com.gigaspaces.internal.cluster;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

public class ChunksRoutingManager implements Externalizable {
    private Map<Integer, PartitionToChunksMap> maps;
    private int currentGeneration;

    public ChunksRoutingManager() {
    }

    public ChunksRoutingManager(PartitionToChunksMap chunksMap) {
        this.currentGeneration = chunksMap.getGeneration();
        maps = new HashMap<>();
        maps.put(chunksMap.getGeneration(),chunksMap);
    }

    public Map<Integer, PartitionToChunksMap> getMaps() {
        return maps;
    }

    public void setMaps(Map<Integer, PartitionToChunksMap> maps) {
        this.maps = maps;
    }

    public int getCurrentGeneration() {
        return currentGeneration;
    }

    public void setCurrentGeneration(int currentGeneration) {
        this.currentGeneration = currentGeneration;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(currentGeneration);
        out.writeShort(maps.size());
        for (PartitionToChunksMap map : maps.values()) {
            out.writeObject(map);
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
    }

    public PartitionToChunksMap getLastestMap() {
        return maps.get(currentGeneration);
    }

    public void addNewMap(PartitionToChunksMap chunksMap) {
        if(chunksMap.getGeneration() < currentGeneration){
            throw new IllegalStateException("cannot set map as latest, map.generation = "+chunksMap.getGeneration()+" while currentGeneration = "+currentGeneration+", map:\n"+chunksMap);
        }
        maps.put(chunksMap.getGeneration(), chunksMap);
        currentGeneration = chunksMap.getGeneration();
    }
}
