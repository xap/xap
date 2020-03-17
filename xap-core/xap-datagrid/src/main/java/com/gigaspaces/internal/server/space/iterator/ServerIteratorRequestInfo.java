package com.gigaspaces.internal.server.space.iterator;

import java.util.UUID;

public class ServerIteratorRequestInfo {
    private final UUID uuid;
    private final int batchSize;
    private final int requestedBatchNumber;
    private final long maxInactiveDuration;

    public ServerIteratorRequestInfo(UUID uuid, int batchSize, int requestedBatchNumber, long maxInactiveDuration) {
        this.uuid = uuid;
        this.batchSize = batchSize;
        this.requestedBatchNumber = requestedBatchNumber;
        this.maxInactiveDuration = maxInactiveDuration;
    }

    public UUID getUuid() {
        return uuid;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getRequestedBatchNumber() {
        return requestedBatchNumber;
    }

    public long getMaxInactiveDuration() {
        return maxInactiveDuration;
    }

    public boolean isFirstTime() {
        return requestedBatchNumber == 0;
    }
}
