package com.gigaspaces.client.iterator.server_based;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public enum SpaceIteratorBatchStatus {
    READY("ready"),
    WAITING("waiting"),
    FAILED("failed"),
    FINISHED("finished"),;
    private String name;
    SpaceIteratorBatchStatus(String name) {
        this.name = name;
    }
    @Override
    public String toString() {
        return name;
    }
}
