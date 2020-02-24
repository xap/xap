package com.gigaspaces.internal.server.space.iterator;

import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.kernel.list.IScanListIterator;

import java.util.Arrays;
import java.util.UUID;

public class ServerIteratorInfo {
    final private UUID uuid;
    final private int batchSize;
    final private long lease;
    private volatile ServerIteratorStatus status;
    private volatile long expirationTime;
    private volatile IScanListIterator<IEntryCacheInfo> scanListIterator; //check if could be final
    private volatile IEntryPacket[] storedEntryPacketsBatch;
    private volatile int storedBatchNumber;
//    private volatile ServerIterator`LeaseHolder serverIteratorLeaseHolder;

    public ServerIteratorInfo(UUID uuid, int batchSize, long lease) {
        this.uuid = uuid;
        this.batchSize = batchSize;
        this.lease = lease;
        this.status = ServerIteratorStatus.ACTIVE;
        this.storedBatchNumber = 0;
    }

    public UUID getUuid() {
        return uuid;
    }

    public ServerIteratorStatus getStatus() {
        return status;
    }

    public void setStatus(ServerIteratorStatus status) {
        this.status = status;
    }

    public IScanListIterator<IEntryCacheInfo> getScanListIterator() {
        return scanListIterator;
    }

    public void setScanListIterator(IScanListIterator scanListIterator) {
        this.scanListIterator = scanListIterator;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public int getBatchSize() {
        return batchSize;
    }

//    public ServerIteratorLeaseHolder getServerIteratorLeaseHolder() {
//        return serverIteratorLeaseHolder;
//    }
//
//    public void setServerIteratorLeaseHolder(ServerIteratorLeaseHolder serverIteratorLeaseHolder) {
//        this.serverIteratorLeaseHolder = serverIteratorLeaseHolder;
//    }

    public boolean isActive(){
        return status.equals(ServerIteratorStatus.ACTIVE);
    }

    public boolean isExpired(){
        return status.equals(ServerIteratorStatus.EXPIRED);
    }

    public boolean isClosed(){
        return status.equals(ServerIteratorStatus.CLOSED);
    }

    public void renewLease(){
        if(!isExpired())
            expirationTime = System.currentTimeMillis() + lease;
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

    private boolean isFirstTime(){
        return storedBatchNumber == 0;
    }

    public boolean isBatchRetrialRequest(ServerIteratorRequestInfo serverIteratorRequestInfo){
        if(serverIteratorRequestInfo.isFirstTime() && isFirstTime()) {
            return false;
        }
        int requestedBatchNumber = serverIteratorRequestInfo.getRequestedBatchNumber();
        if(Math.abs(requestedBatchNumber - storedBatchNumber) > 1 || requestedBatchNumber < storedBatchNumber){
            throw new IllegalStateException("Illegal space iterator batch request - request for batch " + requestedBatchNumber + " stored batch number is " + storedBatchNumber);
        }
        return requestedBatchNumber == storedBatchNumber;
    }

    @Override
    public String toString() {
        return "ServerIteratorInfo{" +
                "uuid=" + uuid +
                ", batchSize=" + batchSize +
                ", lease=" + lease +
                ", status=" + status +
                ", expirationTime=" + expirationTime +
                ", scanListIterator=" + scanListIterator +
                ", storedEntryPacketsBatch=" + (storedEntryPacketsBatch!=null ? storedEntryPacketsBatch.length : null) +
                ", storedBatchNumber=" + storedBatchNumber +
                '}';
    }

    //TODO status modification and inquiry
}
