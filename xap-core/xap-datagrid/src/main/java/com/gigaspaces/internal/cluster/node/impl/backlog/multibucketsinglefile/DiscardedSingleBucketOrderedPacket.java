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
import com.gigaspaces.internal.cluster.node.impl.packets.data.DiscardReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.IMultiBucketSingleFileProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.ParallelBatchProcessingContext;

@com.gigaspaces.api.InternalApi
public class DiscardedSingleBucketOrderedPacket
        extends AbstractSingleBucketOrderedPacket {

    private static final long serialVersionUID = 1L;

    public DiscardedSingleBucketOrderedPacket() {
    }

    public DiscardedSingleBucketOrderedPacket(long globalKey,
                                              short bucketIndex, long bucketKey) {
        super(globalKey, bucketKey, bucketIndex);
    }

    public IReplicationPacketData<?> getData() {
        return DiscardReplicationPacketData.instance();
    }

    public boolean isDataPacket() {
        return false;
    }

    @Override
    public boolean isDiscardedPacket() {
        return true;
    }

    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      IMultiBucketSingleFileProcessLog processLog,
                                                      IReplicationInFilterCallback inFilterCallback, ParallelBatchProcessingContext context, int segmentIndex) {
        return processLog.process(sourceLookupName, this, inFilterCallback, context, segmentIndex);
    }

    public DiscardedSingleBucketOrderedPacket replaceWithDiscarded() {
        return this;
    }

    @Override
    public DiscardedSingleBucketOrderedPacket clone() {
        //This is immutable
        return this;
    }

    @Override
    public DiscardedSingleBucketOrderedPacket cloneWithNewData(
            IReplicationPacketData<?> newData) {
        throw new UnsupportedOperationException();
    }

}
