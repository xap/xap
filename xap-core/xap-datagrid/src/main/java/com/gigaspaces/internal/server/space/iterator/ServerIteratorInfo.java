package com.gigaspaces.internal.server.space.iterator;

import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.kernel.list.IScanListIterator;

import java.util.UUID;

public class ServerIteratorInfo {
    final private UUID uuid;
    final private int batchSize;
    final private long lease;
    private volatile ITemplateHolder templateHolder; //check if could be final
    private volatile ServerIteratorStatus status;
    private volatile long expirationTime;
    private volatile IScanListIterator<IEntryCacheInfo> scanListIterator; //check if could be final
//    private volatile ServerIteratorLeaseHolder serverIteratorLeaseHolder;

    public ServerIteratorInfo(UUID uuid, int batchSize, long lease) {
        this.uuid = uuid;
        this.batchSize = batchSize;
        this.lease = lease;
        this.status = ServerIteratorStatus.ACTIVE;
    }

    public UUID getUuid() {
        return uuid;
    }

    public ITemplateHolder getTemplateHolder() {
        return templateHolder;
    }

    public void setTemplateHolder(ITemplateHolder templateHolder) {
        this.templateHolder = templateHolder;
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

    //TODO status modification and inquiry
}
