package com.gigaspaces.internal.exceptions;

import com.gigaspaces.internal.cluster.ClusterTopology;

public class ChunksMapGenerationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private ClusterTopology newMap;


    public ChunksMapGenerationException(String msg) {
        super(msg);
    }

    public ClusterTopology getNewMap() {
        return newMap;
    }

    public void setNewMap(ClusterTopology newMap) {
        this.newMap = newMap;
    }

}
