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

package com.gigaspaces.internal.cluster.node.impl.backlog.globalorder;

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.DiscardReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class GlobalOrderDiscardedReplicationPacket
        implements IReplicationOrderedPacket {
    private static final long serialVersionUID = 1L;

    private long _key;
    private long _endKey;

    public GlobalOrderDiscardedReplicationPacket() {
    }

    public GlobalOrderDiscardedReplicationPacket(long key) {
        _key = key;
        _endKey = key;
    }

    public IReplicationPacketData<?> getData() {
        return DiscardReplicationPacketData.PACKET;
    }

    public long getKey() {
        return _key;
    }

    public long getEndKey() {
        return _endKey;
    }

    public void setEndKey(long endKey) {
        _endKey = endKey;
    }

    public boolean isDataPacket() {
        return false;
    }

    @Override
    public boolean isDiscardedPacket() {
        return true;
    }

    @Override
    public GlobalOrderDiscardedReplicationPacket clone() {
        //Immutable
        return this;
    }

    @Override
    public IReplicationOrderedPacket cloneWithNewData(
            IReplicationPacketData<?> newData) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getWeight() {
        return 1;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _key = in.readLong();
        final boolean hasRange = in.readBoolean();
        _endKey = hasRange ? in.readLong() : _key;
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _key = in.readLong();
        _endKey = in.readLong();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_key);
        out.writeBoolean(hasKeyRange());
        if (hasKeyRange())
            out.writeLong(_endKey);
    }

    private boolean hasKeyRange() {
        return _key != _endKey;
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        out.writeLong(_key);
        out.writeLong(_endKey);
    }

    @Override
    public String toString() {
        return (hasKeyRange() ? "keys=" + _key + "-" + _endKey : "key=" + _key) + " data=" + getData();
    }

}
