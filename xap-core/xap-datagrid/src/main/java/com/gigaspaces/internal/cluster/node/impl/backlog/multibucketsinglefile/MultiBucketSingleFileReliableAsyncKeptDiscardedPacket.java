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

package com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.IMultiBucketSingleFileProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.ParallelBatchProcessingContext;
import com.gigaspaces.internal.collections.ShortObjectMap;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileReliableAsyncKeptDiscardedPacket
        implements IMultiBucketSingleFileReplicationOrderedPacket {

    private static final long serialVersionUID = 1L;
    private IMultiBucketSingleFileReplicationOrderedPacket _beforeFilter;
    private IMultiBucketSingleFileReplicationOrderedPacket _afterFilter;

    public MultiBucketSingleFileReliableAsyncKeptDiscardedPacket() {
    }

    public MultiBucketSingleFileReliableAsyncKeptDiscardedPacket(
            IMultiBucketSingleFileReplicationOrderedPacket beforeFilter,
            IMultiBucketSingleFileReplicationOrderedPacket afterFilter) {
        _beforeFilter = beforeFilter;
        _afterFilter = afterFilter;
    }

    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      IMultiBucketSingleFileProcessLog processLog,
                                                      IReplicationInFilterCallback inFilterCallback,
                                                      ParallelBatchProcessingContext context, int segmentIndex) {
        //Set the before filter packet reference so it will be used after the packet is processed to be kept
        //for the mirror
        _afterFilter.setReliableAsyncBeforeFilterPacket(_beforeFilter);
        return _afterFilter.process(sourceLookupName, processLog, inFilterCallback, context, segmentIndex);
    }

    public long processResult(String memberName,
                              MultiBucketSingleFileProcessResult processResult,
                              MultiBucketSingleFileConfirmationHolder confirmationHolder) {
        return _afterFilter.processResult(memberName, processResult, confirmationHolder);
    }

    public long getBucketKey(short bucketIndex) {
        return _afterFilter.getBucketKey(bucketIndex);
    }

    public PacketConsumeState getConsumeState(short bucketIndex) {
        return _afterFilter.getConsumeState(bucketIndex);
    }

    public boolean setConsumed() {
        return _afterFilter.setConsumed();
    }

    public short bucketCount() {
        return _afterFilter.bucketCount();
    }

    public short[] getBuckets() {
        return _afterFilter.getBuckets();
    }

    public void associateToBuckets(ShortObjectMap<List<IMultiBucketSingleFileReplicationOrderedPacket>> bucketsPackets) {
        _afterFilter.associateToBuckets(bucketsPackets);
    }

    public IMultiBucketSingleFileReplicationOrderedPacket replaceWithDiscarded() {
        return _afterFilter.replaceWithDiscarded();
    }

    public void reliableAsyncKeysUpdate(long[] bucketLastKeys,
                                        MultiBucketSingleFileConfirmationHolder confirmationHolder) {
        _afterFilter.reliableAsyncKeysUpdate(bucketLastKeys, confirmationHolder);
    }

    public IMultiBucketSingleFileReplicationOrderedPacket getReliableAsyncBeforeFilterPacket() {
        throw new IllegalStateException();
    }

    public void setReliableAsyncBeforeFilterPacket(
            IMultiBucketSingleFileReplicationOrderedPacket beforeFilterPacket) {
        throw new IllegalStateException();
    }

    public IReplicationOrderedPacket wrapWithReliableAsyncKeptDiscardedPacket(
            IMultiBucketSingleFileReplicationOrderedPacket typedAfterFilter) {
        throw new IllegalStateException();
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

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _beforeFilter);
        IOUtils.writeObject(out, _afterFilter);
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        IOUtils.writeSwapExternalizableObject(out, _beforeFilter);
        IOUtils.writeSwapExternalizableObject(out, _afterFilter);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _beforeFilter = IOUtils.readObject(in);
        _afterFilter = IOUtils.readObject(in);
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _beforeFilter = IOUtils.readSwapExternalizableObject(in);
        _afterFilter = IOUtils.readSwapExternalizableObject(in);
    }


    @Override
    public MultiBucketSingleFileReliableAsyncKeptDiscardedPacket clone() {
        return new MultiBucketSingleFileReliableAsyncKeptDiscardedPacket(_beforeFilter.clone(), _afterFilter.clone());
    }

    @Override
    public MultiBucketSingleFileReliableAsyncKeptDiscardedPacket cloneWithNewData(
            IReplicationPacketData<?> newData) {
        return new MultiBucketSingleFileReliableAsyncKeptDiscardedPacket(_beforeFilter.clone(), _afterFilter.cloneWithNewData(newData));
    }

    @Override
    public int getWeight() {
        if (_afterFilter != null) {
            return _afterFilter.getWeight();
        }
        return 1;
    }

    @Override
    public int hashCode() {
        return _beforeFilter.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MultiBucketSingleFileReliableAsyncKeptDiscardedPacket other = (MultiBucketSingleFileReliableAsyncKeptDiscardedPacket) obj;
        if (_afterFilter == null) {
            if (other._afterFilter != null)
                return false;
        } else if (!_afterFilter.equals(other._afterFilter))
            return false;
        if (_beforeFilter == null) {
            if (other._beforeFilter != null)
                return false;
        } else if (!_beforeFilter.equals(other._beforeFilter))
            return false;
        return true;
    }


}
