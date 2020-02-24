package com.gigaspaces.internal.server.space.iterator;

import java.util.UUID;

public class ServerIteratorRequestInfo {
    private final UUID uuid;
    private final long lease;
    private final int batchSize;
    private final int requestedBatchNumber;

    public ServerIteratorRequestInfo(UUID uuid, long lease, int batchSize, int requestedBatchNumber) {
        this.uuid = uuid;
        this.lease = lease;
        this.batchSize = batchSize;
        this.requestedBatchNumber = requestedBatchNumber;
    }

    public UUID getUuid() {
        return uuid;
    }

    public long getLease() {
        return lease;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getRequestedBatchNumber() {
        return requestedBatchNumber;
    }

    public boolean isFirstTime() {
        return requestedBatchNumber == 0;
    }
}
