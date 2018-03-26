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
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;

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
        return DiscardReplicationPacketData.instance();
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
        return 0;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        _key = in.readLong();
        final boolean hasRange = in.readBoolean();
        _endKey = hasRange ? in.readLong() : _key;
        PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v12_1_0) && version.lessThan(PlatformLogicalVersion.v12_3_0)) {
            in.readInt();
        }
    }

    public void readFromSwap(ObjectInput in) throws IOException, ClassNotFoundException {
        _key = in.readLong();
        _endKey = in.readLong();
        PlatformLogicalVersion version = PlatformLogicalVersion.getLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v12_1_0) && version.lessThan(PlatformLogicalVersion.v12_3_0)) {
            in.readInt();
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_key);
        out.writeBoolean(hasKeyRange());
        if (hasKeyRange())
            out.writeLong(_endKey);
        PlatformLogicalVersion endpointLogicalVersion = LRMIInvocationContext.getEndpointLogicalVersion();
        if (endpointLogicalVersion.greaterOrEquals(PlatformLogicalVersion.v12_1_0) && endpointLogicalVersion.lessThan(PlatformLogicalVersion.v12_3_0)){
            out.writeInt(0);
        }
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        out.writeLong(_key);
        out.writeLong(_endKey);
        PlatformLogicalVersion version = PlatformLogicalVersion.getLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v12_1_0) && version.lessThan(PlatformLogicalVersion.v12_3_0)) {
            out.writeInt(0);
        }
    }

    private boolean hasKeyRange() {
        return _key != _endKey;
    }

    @Override
    public String toString() {
        return (hasKeyRange() ? "keys=" + _key + "-" + _endKey : "key=" + _key) + " data=" + getData();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GlobalOrderDiscardedReplicationPacket that = (GlobalOrderDiscardedReplicationPacket) o;

        if (_key != that._key) return false;
        return _endKey == that._endKey;
    }
}
