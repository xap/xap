package com.gigaspaces.internal.server.space.iterator;

import java.util.UUID;

public class ServerIteratorRequestInfo {
    final private UUID uuid;
    final private long lease;
    final private int batchSize;
    final private boolean firstTime;

    public ServerIteratorRequestInfo(UUID uuid, long lease, int batchSize, boolean firstTime) {
        this.uuid = uuid;
        this.lease = lease;
        this.batchSize = batchSize;
        this.firstTime = firstTime;
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

    public boolean isFirstTime() {
        return firstTime;
    }
}
