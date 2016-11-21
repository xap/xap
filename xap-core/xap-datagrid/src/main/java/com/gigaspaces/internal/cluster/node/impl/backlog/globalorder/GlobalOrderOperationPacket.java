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
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class GlobalOrderOperationPacket
        implements IReplicationOrderedPacket {
    private static final long serialVersionUID = 1L;
    private long _key;
    private IReplicationPacketData _data;

    public GlobalOrderOperationPacket() {

    }

    public GlobalOrderOperationPacket(long key, IReplicationPacketData data) {
        _key = key;
        _data = data;
    }

    public IReplicationPacketData getData() {
        return _data;
    }

    public long getKey() {
        return _key;
    }

    public long getEndKey() {
        return _key;
    }

    public boolean isDataPacket() {
        return true;
    }

    @Override
    public boolean isDiscardedPacket() {
        return false;
    }

    @Override
    public GlobalOrderOperationPacket clone() {
        try {
            GlobalOrderOperationPacket clone = (GlobalOrderOperationPacket) super.clone();
            clone._data = _data.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }

    @Override
    public IReplicationOrderedPacket cloneWithNewData(
            IReplicationPacketData<?> newData) {
        try {
            GlobalOrderOperationPacket clone = (GlobalOrderOperationPacket) super.clone();
            clone._data = newData;
            return clone;
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }

    public int getWeight() {
        if(_data != null){
            return _data.getWeight();
        }
        return 1;
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _key = in.readLong();
        _data = (IReplicationPacketData) in.readObject();
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _key = in.readLong();
        _data = IOUtils.readSwapExternalizableObject(in);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(_key);
        out.writeObject(_data);
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        out.writeLong(_key);
        IOUtils.writeSwapExternalizableObject(out, _data);
    }

    @Override
    public String toString() {
        return "key=" + _key + " data=" + _data;
    }

}
