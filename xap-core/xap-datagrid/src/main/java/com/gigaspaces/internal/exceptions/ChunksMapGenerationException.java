package com.gigaspaces.internal.exceptions;

import com.gigaspaces.internal.cluster.PartitionToChunksMap;

public class ChunksMapGenerationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private PartitionToChunksMap newMap;


    public ChunksMapGenerationException(String msg) {
        super(msg);
    }

    public PartitionToChunksMap getNewMap() {
        return newMap;
    }

    public void setNewMap(PartitionToChunksMap newMap) {
        this.newMap = newMap;
    }

}
