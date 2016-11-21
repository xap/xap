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
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.IMultiBucketSingleFileProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.ParallelBatchProcessingContext;
import com.gigaspaces.internal.io.IOUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@com.gigaspaces.api.InternalApi
public class SingleBucketOrderedPacket extends AbstractSingleBucketOrderedPacket {
    private static final long serialVersionUID = 1L;

    private IReplicationPacketData<?> _data;

    public SingleBucketOrderedPacket() {
    }

    public SingleBucketOrderedPacket(long globalKey, long bucketKey, short bucketIndex,
                                     IReplicationPacketData<?> data) {
        super(globalKey, bucketKey, bucketIndex);
        _data = data;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, _data);
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        IOUtils.writeSwapExternalizableObject(out, _data);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _data = IOUtils.readObject(in);
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        _data = IOUtils.readSwapExternalizableObject(in);
    }

    public IReplicationPacketData<?> getData() {
        return _data;
    }

    public boolean isDataPacket() {
        return true;
    }

    @Override
    public boolean isDiscardedPacket() {
        return false;
    }

    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      IMultiBucketSingleFileProcessLog processLog,
                                                      IReplicationInFilterCallback inFilterCallback, ParallelBatchProcessingContext context, int segmentIndex) {
        return processLog.process(sourceLookupName, this, inFilterCallback, context, segmentIndex);
    }

    public DiscardedSingleBucketOrderedPacket replaceWithDiscarded() {
        return new DiscardedSingleBucketOrderedPacket(getKey(), getBucketIndex(), getBucketKey());
    }


    @Override
    public SingleBucketOrderedPacket clone() {
        return new SingleBucketOrderedPacket(getKey(), getBucketKey(), getBucketIndex(), _data.clone());
    }

    @Override
    public SingleBucketOrderedPacket cloneWithNewData(
            IReplicationPacketData<?> newData) {
        return new SingleBucketOrderedPacket(getKey(), getBucketKey(), getBucketIndex(), newData);
    }

    @Override
    public int getWeight() {
        if(_data != null){
            return _data.getWeight();
        }
        return 1;
    }

}
