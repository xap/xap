/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.internal.server.space.iterator;

import com.gigaspaces.SpaceRuntimeException;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.GetBatchForIteratorException;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.list.CircularNumerator;

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

    private void deactivate() {
        this.active = false;
        if(this.scanListIterator != null) {
            try {
                this.scanListIterator.releaseScan();
                this.scanListIterator = null;
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
                ", scanListIterator=" + scanListIterator +
                ", storedEntryPacketsBatch=" + + (storedEntryPacketsBatch!=null ? storedEntryPacketsBatch.length : null) +
                ", storedBatchNumber=" + storedBatchNumber +
                ", expirationTime=" + expirationTime +
                '}';
    }
}
