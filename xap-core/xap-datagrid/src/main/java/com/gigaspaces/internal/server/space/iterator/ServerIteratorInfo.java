package com.gigaspaces.internal.server.space.iterator;

import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.GetBatchForIteratorException;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.kernel.list.IScanListIterator;

import java.util.UUID;

public class ServerIteratorInfo {
    final private Object lock = new Object();
    final private UUID uuid;
    final private int batchSize;
    final private long maxInactiveDuration;
    private volatile IScanListIterator<IEntryCacheInfo> scanListIterator;
    private volatile IEntryPacket[] storedEntryPacketsBatch;
    private volatile int storedBatchNumber;
    private volatile long expirationTime;
    private volatile boolean active;

    public ServerIteratorInfo(UUID uuid, int batchSize, long maxInactiveDuration) {
        this.uuid = uuid;
        this.batchSize = batchSize;
        this.maxInactiveDuration = maxInactiveDuration;
        this.storedBatchNumber = 0;
        this.expirationTime = System.currentTimeMillis() + maxInactiveDuration;
        this.active = true;
    }

    public UUID getUuid() {
        return uuid;
    }

    public IScanListIterator<IEntryCacheInfo> getScanListIterator() {
        return scanListIterator;
    }

    public void setScanListIterator(IScanListIterator scanListIterator) {
        this.scanListIterator = scanListIterator;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public IEntryPacket[] getStoredEntryPacketsBatch() {
        return storedEntryPacketsBatch;
    }

    public ServerIteratorInfo setStoredEntryPacketsBatch(IEntryPacket[] storedEntryPacketsBatch) {
        this.storedEntryPacketsBatch = storedEntryPacketsBatch;
        return this;
    }

    public int getStoredBatchNumber() {
        return storedBatchNumber;
    }

    public ServerIteratorInfo setStoredBatchNumber(int storedBatchNumber) {
        this.storedBatchNumber = storedBatchNumber;
        return this;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public ServerIteratorInfo setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public ServerIteratorInfo setActive(boolean active) {
        this.active = active;
        return this;
    }

    public long getMaxInactiveDuration() {
        return maxInactiveDuration;
    }

    private boolean isFirstTime(){
        return storedBatchNumber == 0;
    }

    public boolean isBatchRetrialRequest(ServerIteratorRequestInfo serverIteratorRequestInfo){
        if(serverIteratorRequestInfo.isFirstTime() && isFirstTime()) {
            return false;
        }
        int requestedBatchNumber = serverIteratorRequestInfo.getRequestedBatchNumber();
        if(Math.abs(requestedBatchNumber - storedBatchNumber) > 1 || requestedBatchNumber < storedBatchNumber){
            throw new GetBatchForIteratorException("Illegal batch request, requested batch number is " + requestedBatchNumber + ", stored batch number is " + storedBatchNumber);
        }
        return requestedBatchNumber == storedBatchNumber;
    }

    public boolean isCandidateForExpiration(){
        return expirationTime < System.currentTimeMillis();
    }

    public boolean tryDeactivateIterator(){
        if(!isActive())
            return false;
        synchronized (lock){
            if(!isActive())
                return false;
            setActive(false);
            return true;
        }
    }

    public boolean tryRenewLease(){
        synchronized (lock){
            if(!isActive())
                return false;
            setExpirationTime(System.currentTimeMillis() + maxInactiveDuration);
            return true;
        }
    }

    public boolean tryExpireIterator(){
        if(isCandidateForExpiration()) {
            synchronized(lock){
                if(isCandidateForExpiration()){
                    setActive(false);
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "ServerIteratorInfo{" +
                "uuid=" + uuid +
                ", batchSize=" + batchSize +
                ", active=" + active +
                ", scanListIterator=" + scanListIterator +
                ", storedEntryPacketsBatch=" + + (storedEntryPacketsBatch!=null ? storedEntryPacketsBatch.length : null) +
                ", storedBatchNumber=" + storedBatchNumber +
                ", expirationTime=" + expirationTime +
                '}';
    }
}
