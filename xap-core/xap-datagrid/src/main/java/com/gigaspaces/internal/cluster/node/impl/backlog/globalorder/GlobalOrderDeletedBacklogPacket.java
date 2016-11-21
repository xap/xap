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
public class GlobalOrderDeletedBacklogPacket implements IReplicationOrderedPacket {

    private static final long serialVersionUID = 1L;
    private long _startKey;
    private long _endKey;

    //Externalizable
    public GlobalOrderDeletedBacklogPacket() {
    }

    public GlobalOrderDeletedBacklogPacket(long startKey, long endKey) {
        _startKey = startKey;
        _endKey = endKey;
    }

    public IReplicationPacketData getData() {
        return DiscardReplicationPacketData.PACKET;
    }

    public long getKey() {
        return _startKey;
    }

    public long getEndKey() {
        return _endKey;
    }


    public boolean isDataPacket() {
        return false;
    }

    @Override
    public boolean isDiscardedPacket() {
        return false;
    }

    @Override
    public GlobalOrderDeletedBacklogPacket clone() {
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
        readExternalImpl(in);
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        readExternalImpl(in);
    }

    private void readExternalImpl(ObjectInput in) throws IOException {
        _startKey = in.readLong();
        _endKey = in.readLong();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        writeExternalImpl(out);
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        writeExternalImpl(out);
    }

    private void writeExternalImpl(ObjectOutput out) throws IOException {
        out.writeLong(_startKey);
        out.writeLong(_endKey);
    }

    /**
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {

        return "key=" + _startKey + " deleted backlog indicator, end key=" + _endKey;
    }


}
