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
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ShortLongIterator;
import com.gigaspaces.internal.collections.ShortLongMap;
import com.gigaspaces.internal.collections.ShortObjectMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class DeletedMultiBucketOrderedPacket
        extends AbstractMultiBucketOrderedPacket {

    private static final long serialVersionUID = 1L;
    private long _globalEndKey;
    private ShortLongMap _bucketsEndKeys;
    private boolean _consumed;

    public DeletedMultiBucketOrderedPacket() {
    }

    public DeletedMultiBucketOrderedPacket(long startGlobalKey,
                                           long endGlobalKey, ShortObjectMap<BucketKey> bucketsStartKeys,
                                           ShortLongMap bucketEndKeys) {
        super(startGlobalKey, bucketsStartKeys);
        _globalEndKey = endGlobalKey;
        _bucketsEndKeys = bucketEndKeys;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        writeExternalImpl(out);
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        super.writeToSwap(out);
        writeExternalImpl(out);
    }

    private void writeExternalImpl(ObjectOutput out) throws IOException {
        out.writeLong(_globalEndKey);
        _bucketsEndKeys.serialize(out);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        readExternalImpl(in);
    }

    public void readFromSwap(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readFromSwap(in);
        readExternalImpl(in);
    }

    private void readExternalImpl(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _globalEndKey = in.readLong();
        _bucketsEndKeys = CollectionsFactory.getInstance().deserializeShortLongMap(in);
    }

    public IReplicationPacketData<?> getData() {
        return DiscardReplicationPacketData.PACKET;
    }

    public long getEndKey() {
        return _globalEndKey;
    }

    public long getBucketEndKey(short bucketIndex) {
        if (!_bucketsEndKeys.containsKey(bucketIndex))
            throw new IllegalArgumentException("No bucket end key exists for bucket index " + bucketIndex);
        return _bucketsEndKeys.get(bucketIndex);
    }

    public boolean isDataPacket() {
        return false;
    }

    @Override
    public boolean isDiscardedPacket() {
        return false;
    }

    @Override
    public DeletedMultiBucketOrderedPacket clone() {
        // Immutable
        return this;
    }

    @Override
    public IMultiBucketSingleFileReplicationOrderedPacket cloneWithNewData(
            IReplicationPacketData<?> newData) {
        throw new UnsupportedOperationException();
    }


    public MultiBucketSingleFileProcessResult process(String sourceLookupName,
                                                      IMultiBucketSingleFileProcessLog processLog,
                                                      IReplicationInFilterCallback inFilterCallback,
                                                      ParallelBatchProcessingContext context, int segmentIndex) {
        return processLog.process(sourceLookupName,
                this,
                inFilterCallback,
                context,
                segmentIndex);
    }

    @Override
    public long processResult(String memberName,
                              MultiBucketSingleFileProcessResult processResult,
                              MultiBucketSingleFileConfirmationHolder confirmationHolder) {
        for (ShortLongIterator iterator = _bucketsEndKeys.iterator(); iterator.hasNext(); ) {
            iterator.advance();
            short bucketIndex = iterator.key();
            long bucketKey = iterator.value();

            Long knownLastConfirmedKey = confirmationHolder.getBucketLastConfirmedKeys()[bucketIndex];
            if (knownLastConfirmedKey == null
                    || knownLastConfirmedKey < bucketKey)
                confirmationHolder.getBucketLastConfirmedKeys()[bucketIndex] = bucketKey;
        }
        return confirmationHolder.overrideGlobalLastConfirmedKey(getEndKey());
    }

    @Override
    public void reliableAsyncKeysUpdate(long[] bucketLastKeys,
                                        MultiBucketSingleFileConfirmationHolder confirmationHolder) {
        for (ShortLongIterator iterator = _bucketsEndKeys.iterator(); iterator.hasNext(); ) {
            iterator.advance();
            short bucketIndex = iterator.key();
            long bucketKey = iterator.value();
            bucketLastKeys[bucketIndex] = bucketKey + 1;
            confirmationHolder.getBucketLastConfirmedKeys()[bucketIndex] = bucketKey;
        }
        confirmationHolder.overrideGlobalLastConfirmedKey(getEndKey());
    }

    public PacketConsumeState getConsumeState(short bucketIndex) {
        return PacketConsumeState.CAN_CONSUME;
    }

    public boolean setConsumed() {
        // Do nothing

        // Already consumed
        if (_consumed)
            return false;
        synchronized (this) {
            // Make sure only the first caller to setConsumed will get a true
            // result
            if (_consumed)
                return false;
            _consumed = true;
            return true;
        }
    }

    public IMultiBucketSingleFileReplicationOrderedPacket replaceWithDiscarded() {
        throw new IllegalStateException("DeletedMultiBucketOrderedPacket should not be replaced by discarded packet");
    }

}
