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

package com.gigaspaces.internal.cluster.node.impl.config;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class MultiBucketReplicationPolicy
        implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    final static public short DEFAULT_BUCKETS_COUNT = 1024;
    final static public Integer DEFAULT_BATCH_PARALLEL_FACTOR = null;
    final static public int DEFAULT_BATCH_PARALLEL_THRESHOLD = 50;

    private short bucketsCount = DEFAULT_BUCKETS_COUNT;
    private Integer batchParallelFactor = DEFAULT_BATCH_PARALLEL_FACTOR;
    private int batchParallelThreshold = DEFAULT_BATCH_PARALLEL_THRESHOLD;

    private interface BitMap {
        int BUCKETS_COUNT = 1 << 0;
        int BATCH_PARALLEL_FACTOR = 1 << 1;
        int BATCH_PARALLEL_THRESHOLD = 1 << 2;
    }

    public MultiBucketReplicationPolicy() {
    }

    public short getBucketCount() {
        return bucketsCount;
    }

    public void setBucketsCount(short bucketsCount) {
        this.bucketsCount = bucketsCount;
    }

    public Integer getBatchParallelFactor() {
        return batchParallelFactor;
    }

    public void setBatchParallelFactor(Integer batchParallelFactor) {
        this.batchParallelFactor = batchParallelFactor;
    }

    public int getBatchParallelThreshold() {
        return batchParallelThreshold;
    }

    public void setBatchParallelThreshold(int batchParallelThreshold) {
        this.batchParallelThreshold = batchParallelThreshold;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        int flags = 0;

        if (bucketsCount != DEFAULT_BUCKETS_COUNT)
            flags |= BitMap.BUCKETS_COUNT;

        if (batchParallelFactor != DEFAULT_BATCH_PARALLEL_FACTOR)
            flags |= BitMap.BATCH_PARALLEL_FACTOR;

        if (batchParallelThreshold != DEFAULT_BATCH_PARALLEL_THRESHOLD)
            flags |= BitMap.BATCH_PARALLEL_THRESHOLD;

        out.writeInt(flags);

        if (bucketsCount != DEFAULT_BUCKETS_COUNT)
            out.writeShort(bucketsCount);
        if (batchParallelFactor != DEFAULT_BATCH_PARALLEL_FACTOR)
            out.writeInt(batchParallelFactor);
        if (batchParallelThreshold != DEFAULT_BATCH_PARALLEL_THRESHOLD)
            out.writeInt(batchParallelThreshold);

    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        int flags = in.readInt();

        if ((flags & BitMap.BUCKETS_COUNT) != 0) {
            bucketsCount = in.readShort();
        } else {
            bucketsCount = DEFAULT_BUCKETS_COUNT;
        }

        if ((flags & BitMap.BATCH_PARALLEL_FACTOR) != 0) {
            batchParallelFactor = in.readInt();
        } else {
            batchParallelFactor = DEFAULT_BATCH_PARALLEL_FACTOR;
        }

        if ((flags & BitMap.BATCH_PARALLEL_THRESHOLD) != 0) {
            batchParallelThreshold = in.readInt();
        } else {
            batchParallelThreshold = DEFAULT_BATCH_PARALLEL_THRESHOLD;
        }

    }

}
