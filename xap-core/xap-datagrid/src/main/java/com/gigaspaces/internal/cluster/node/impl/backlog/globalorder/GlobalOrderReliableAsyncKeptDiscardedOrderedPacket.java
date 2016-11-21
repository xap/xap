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
public class GlobalOrderReliableAsyncKeptDiscardedOrderedPacket
        implements IReplicationOrderedPacket {
    private static final long serialVersionUID = 1L;
    private IReplicationOrderedPacket _beforeFilter;
    private IReplicationOrderedPacket _afterFilter;

    public GlobalOrderReliableAsyncKeptDiscardedOrderedPacket() {
    }

    public GlobalOrderReliableAsyncKeptDiscardedOrderedPacket(
            IReplicationOrderedPacket beforeFilter, IReplicationOrderedPacket afterFilter) {
        _beforeFilter = beforeFilter;
        _afterFilter = afterFilter;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _beforeFilter);
        IOUtils.writeObject(out, _afterFilter);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _beforeFilter = IOUtils.readObject(in);
        _afterFilter = IOUtils.readObject(in);
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        IOUtils.writeSwapExternalizableObject(out, _beforeFilter);
        IOUtils.writeSwapExternalizableObject(out, _afterFilter);
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _beforeFilter = IOUtils.readSwapExternalizableObject(in);
        _afterFilter = IOUtils.readSwapExternalizableObject(in);
    }

    public IReplicationPacketData<?> getData() {
        return _afterFilter.getData();
    }

    public long getKey() {
        return _afterFilter.getKey();
    }

    public long getEndKey() {
        return _afterFilter.getEndKey();
    }

    public boolean isDataPacket() {
        return _afterFilter.isDataPacket();
    }

    @Override
    public boolean isDiscardedPacket() {
        return _afterFilter.isDiscardedPacket();
    }

    @Override
    public GlobalOrderReliableAsyncKeptDiscardedOrderedPacket clone() {
        return new GlobalOrderReliableAsyncKeptDiscardedOrderedPacket(_beforeFilter.clone(), _afterFilter.clone());
    }

    @Override
    public IReplicationOrderedPacket cloneWithNewData(
            IReplicationPacketData<?> newData) {
        return new GlobalOrderReliableAsyncKeptDiscardedOrderedPacket(_beforeFilter.clone(), _afterFilter.cloneWithNewData(newData));
    }

    @Override
    public int getWeight() {
        if(_afterFilter != null){
            return _afterFilter.getWeight();
        }
        return 1;
    }

    public IReplicationOrderedPacket getBeforeFilterPacket() {
        return _beforeFilter;
    }

}
