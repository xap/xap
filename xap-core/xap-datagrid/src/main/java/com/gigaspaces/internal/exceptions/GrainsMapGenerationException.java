package com.gigaspaces.internal.exceptions;

import com.gigaspaces.internal.cluster.PartitionToGrainsMap;

public class GrainsMapGenerationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private PartitionToGrainsMap newMap;


    public GrainsMapGenerationException(String msg) {
        super(msg);
    }

    public PartitionToGrainsMap getNewMap() {
        return newMap;
    }

    public void setNewMap(PartitionToGrainsMap newMap) {
        this.newMap = newMap;
    }

}
