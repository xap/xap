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

package com.gigaspaces.internal.client;

import com.gigaspaces.client.iterator.server_based.SpaceIteratorBatchStatus;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceIteratorBatchResult implements Serializable {
    private Object[] _entries;
    private Exception _exception;
    private Integer _partitionId;
    private SpaceIteratorBatchStatus _spaceIteratorBatchStatus;
    private boolean _firstTime;
    private UUID _uuid;

    public SpaceIteratorBatchResult() {
    }

    public SpaceIteratorBatchResult(Object[] entries, Integer partitionId, Exception exception, boolean firstTime, UUID uuid) {
        this._entries = entries;
        this._partitionId = partitionId != null ? partitionId : PartitionedClusterUtils.NO_PARTITION;
        this._exception = exception;
        this._spaceIteratorBatchStatus = determineStatus();
        this._firstTime = firstTime;
        this._uuid = uuid;
    }

    public Object[] getEntries(){return _entries;}

    public SpaceIteratorBatchStatus getIteratorStatus(){return _spaceIteratorBatchStatus;}

    public void setIteratorStatus(SpaceIteratorBatchStatus spaceIteratorBatchStatus){
        this._spaceIteratorBatchStatus = spaceIteratorBatchStatus;
    }

    public Integer getPartitionId(){return _partitionId;}

    public Exception getException(){
        return _exception;
    }

    public boolean isFirstTime() {
        return _firstTime;
    }

    public void setFirstTime(boolean firstTime) {
        this._firstTime = firstTime;
    }

    private SpaceIteratorBatchStatus determineStatus(){
        if(_exception == null){
            return SpaceIteratorBatchStatus.READY;
        }
        return SpaceIteratorBatchStatus.FAILED;
    }

    public boolean isFinished(){
        return _spaceIteratorBatchStatus.equals(SpaceIteratorBatchStatus.FINISHED);
    }

    public boolean isFailed(){
        return _spaceIteratorBatchStatus.equals(SpaceIteratorBatchStatus.FAILED);
    }

    public boolean isWaiting(){
        return _spaceIteratorBatchStatus.equals(SpaceIteratorBatchStatus.WAITING);
    }

    @Override
    public String toString() {
        return "IteratorBatchResult{" +
                "_uuid=" + _uuid +
                ", num_of_entries=" + _entries.length +
                ", _partitionId=" + _partitionId +
                ", _iteratorStatus=" + _spaceIteratorBatchStatus +
                ", _exception=" + _exception +
                ", _firstTime=" + _firstTime +
                '}';
    }
}
