package com.gigaspaces.internal.server.space.iterator;

import com.gigaspaces.SpaceRuntimeException;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.GetBatchForIteratorException;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.context.TemplateMatchTier;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.list.CircularNumerator;

import java.util.Arrays;
import java.util.UUID;

public class ServerIteratorInfo {
    final private Object lock = new Object();
    final private UUID uuid;
    final private int batchSize;
    final private long maxInactiveDuration;
    private volatile IScanListIterator<IEntryCacheInfo> scanEntriesIter;
    private volatile boolean isTieredByTimeRule;
    private volatile TemplateMatchTier templateMatchTier;
    private volatile IEntryPacket[] storedEntryPacketsBatch;
    private volatile int storedBatchNumber;
    private volatile long expirationTime;
    private volatile boolean active;
    private volatile CircularNumerator<IServerTypeDesc> subTypesCircularNumerator;

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

    public void setScanEntriesIter(IScanListIterator<IEntryCacheInfo> scanEntriesIter) {
        this.scanEntriesIter = scanEntriesIter;
    }

    public IScanListIterator<IEntryCacheInfo> getScanEntriesIter() {
        return scanEntriesIter;
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

    public boolean isTieredByTimeRule() {
        return isTieredByTimeRule;
    }

    public void setTieredByTimeRule(boolean tieredByTimeRule) {
        this.isTieredByTimeRule = tieredByTimeRule;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public ServerIteratorInfo setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    public TemplateMatchTier getTemplateMatchTier() {
        return templateMatchTier;
    }

    public void setTemplateMatchTier(TemplateMatchTier templateMatchTier) {
        this.templateMatchTier = templateMatchTier;
    }

    public boolean isActive() {
        return active;
    }

    private void deactivate() {
        this.active = false;
        if(this.scanEntriesIter != null) {
            try {
                this.scanEntriesIter.releaseScan();
                this.scanEntriesIter = null;
            } catch (SAException e) {
                throw new SpaceRuntimeException("Failed to close scan list iterator ", e);
            }
        }
    }

    public CircularNumerator<IServerTypeDesc> getSubTypesCircularNumerator() {
        return subTypesCircularNumerator;
    }

    public ServerIteratorInfo setSubTypesCircularNumerator(CircularNumerator<IServerTypeDesc> subTypesCircularNumerator) {
        this.subTypesCircularNumerator = subTypesCircularNumerator;
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
            deactivate();
            return true;
        }
    }

    public boolean tryRenewLease(){
        if(!isActive())
            return false;
        synchronized (lock){
            if(!isActive())
                return false;
            setExpirationTime(System.currentTimeMillis() + maxInactiveDuration);
            return true;
        }
    }

    public boolean tryExpireIterator(){
        if(!isCandidateForExpiration())
            return false;
        synchronized(lock){
            if(isCandidateForExpiration()){
                deactivate();
                return true;
            }
            return false;
        }
    }

    @Override
    public String toString() {
        return "ServerIteratorInfo{" +
                "uuid=" + uuid +
                ", batchSize=" + batchSize +
                ", active=" + active +
                ", scanEntriesIter=" + scanEntriesIter +
                ", isTieredByTimeRule=" + isTieredByTimeRule +
                ", templateMatchTier=" + templateMatchTier +
                ", storedEntryPacketsBatch=" + (storedEntryPacketsBatch!=null ? storedEntryPacketsBatch.length : null) +
                ", storedBatchNumber=" + storedBatchNumber +
                ", expirationTime=" + expirationTime +
                '}';
    }
}
