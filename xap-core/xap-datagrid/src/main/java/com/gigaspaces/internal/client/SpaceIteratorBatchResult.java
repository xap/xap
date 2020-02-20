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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils;

import java.io.*;
import java.util.UUID;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceIteratorBatchResult implements Externalizable {
    private static final short FLAG_ENTRY_PACKETS = 1 << 0;
    private static final short FLAG_EXCEPTION = 1 << 1;
    private Object[] _entries;
    private Exception _exception;
    private Integer _partitionId;
    private boolean _firstTime;
    private UUID _uuid;
    private transient boolean _finished;

    public SpaceIteratorBatchResult() {
    }

    public SpaceIteratorBatchResult(Object[] entries, Integer partitionId, Exception exception, boolean firstTime, UUID uuid) {
        this._entries = entries;
        this._partitionId = partitionId != null ? partitionId : PartitionedClusterUtils.NO_PARTITION;
        this._exception = exception;
        this._firstTime = firstTime;
        this._uuid = uuid;
    }

    public Object[] getEntries(){return _entries;}

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

    public void setFinished(boolean finished) {
        this._finished = finished;
    }

    public boolean isFinished(){
        return _finished; // see if able to simplify here
    }

    public boolean isFailed(){
        return _exception != null;
    }

    public boolean isWaiting(){
        return !isFinished();
    }

    @Override
    public String toString() {
        return "IteratorBatchResult{" +
                "_uuid=" + _uuid +
                ", num_of_entries=" + _entries.length +
                ", _partitionId=" + _partitionId +
                ", _finished=" + _finished +
                ", _exception=" + _exception +
                ", _firstTime=" + _firstTime +
                '}';
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        final short flags = buildFlags();
        out.writeShort(flags);
        if (flags != 0) {
            if(_entries != null )
                IOUtils.writeObjectArray(out, _entries);
            if (_exception != null)
                IOUtils.writeObject(out, _exception);
        }
        out.writeInt(_partitionId);
        out.writeBoolean(_firstTime);
        IOUtils.writeUUID(out, _uuid);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        short flags = in.readShort();
        if (flags != 0) {
            if ((flags & FLAG_ENTRY_PACKETS) != 0)
                this._entries = IOUtils.readObjectArray(in);
            if ((flags & FLAG_EXCEPTION) != 0)
                this._exception = IOUtils.readObject(in);
        }
        this._partitionId = in.readInt();
        this._firstTime = in.readBoolean();
        this._uuid = IOUtils.readUUID(in);
    }

    private short buildFlags() {
        short flags = 0;
        if (_entries != null)
            flags |= FLAG_ENTRY_PACKETS;
        if (_exception != null)
            flags |= FLAG_EXCEPTION;
        return flags;
    }
}
