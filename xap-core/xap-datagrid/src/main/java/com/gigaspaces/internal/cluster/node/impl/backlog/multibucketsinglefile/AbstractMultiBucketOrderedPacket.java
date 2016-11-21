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

import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessResult;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ShortObjectIterator;
import com.gigaspaces.internal.collections.ShortObjectMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractMultiBucketOrderedPacket
        implements IMultiBucketSingleFileReplicationOrderedPacket {
    private static final long serialVersionUID = 1L;
    private ShortObjectMap<BucketKey> _bucketsKeys;
    private long _globalKey;
    private transient IMultiBucketSingleFileReplicationOrderedPacket _beforeFilterPacket;

    public AbstractMultiBucketOrderedPacket() {
    }

    public AbstractMultiBucketOrderedPacket(long globalKey,
                                            ShortObjectMap<BucketKey> bucketsKeys) {
        _globalKey = globalKey;
        _bucketsKeys = bucketsKeys;
    }

    public long getBucketKey(short bucketIndex) {
        BucketKey bucketKey = _bucketsKeys.get(bucketIndex);
        if (bucketKey == null)
            throw new IllegalArgumentException("No bucket key exists for bucket index "
                    + bucketIndex);

        return bucketKey.bucketKey;
    }

    public ShortObjectMap<BucketKey> getBucketsKeys() {
        return _bucketsKeys;
    }

    public short[] getBuckets() {
        return _bucketsKeys.keys();
    }

    public short bucketCount() {
        return (short) _bucketsKeys.size();
    }

    public void associateToBuckets(
            ShortObjectMap<List<IMultiBucketSingleFileReplicationOrderedPacket>> bucketsPackets) {
        ShortObjectMap<BucketKey> bucketsKeys = _bucketsKeys;
        for (short bucketIndex : bucketsKeys.keys()) {
            List<IMultiBucketSingleFileReplicationOrderedPacket> list = bucketsPackets.get(bucketIndex);
            if (list == null) {
                list = new LinkedList<IMultiBucketSingleFileReplicationOrderedPacket>();
                bucketsPackets.put(bucketIndex, list);
            }
            list.add(this);
        }
    }

    protected void writeBucketKeys(ObjectOutput out) throws IOException {
        // Bucket size is limited by short, therefore this map cannot be bigger
        // than short
        out.writeShort(_bucketsKeys.size());
        for (ShortObjectIterator<BucketKey> iterator = _bucketsKeys.iterator(); iterator.hasNext(); ) {
            iterator.advance();
            out.writeShort(iterator.key());
            out.writeLong(iterator.value().bucketKey);
        }
    }

    protected void readBucketKeys(ObjectInput in) throws IOException {
        short size = in.readShort();
        _bucketsKeys = CollectionsFactory.getInstance().createShortObjectMap(size);
        for (int i = 0; i < size; ++i)
            _bucketsKeys.put(in.readShort(), new BucketKey(in.readLong()));
    }

    public long processResult(String memberName,
                              MultiBucketSingleFileProcessResult processResult,
                              MultiBucketSingleFileConfirmationHolder confirmationHolder) {

        for (ShortObjectIterator<BucketKey> iterator = _bucketsKeys.iterator(); iterator.hasNext(); ) {
            iterator.advance();
            short bucketIndex = iterator.key();
            long bucketKey = iterator.value().bucketKey;

            Long knownLastConfirmedKey = confirmationHolder.getBucketLastConfirmedKeys()[bucketIndex];
            if (knownLastConfirmedKey == null
                    || knownLastConfirmedKey < bucketKey)
                confirmationHolder.getBucketLastConfirmedKeys()[bucketIndex] = bucketKey;
        }
        return confirmationHolder.addGlobalKeyConfirmed(getKey());
    }

    public void reliableAsyncKeysUpdate(long[] bucketLastKeys,
                                        MultiBucketSingleFileConfirmationHolder confirmationHolder) {
        for (ShortObjectIterator<BucketKey> iterator = _bucketsKeys.iterator(); iterator.hasNext(); ) {
            iterator.advance();
            short bucketIndex = iterator.key();
            long bucketKey = iterator.value().bucketKey;
            bucketLastKeys[bucketIndex] = bucketKey + 1;
            confirmationHolder.getBucketLastConfirmedKeys()[bucketIndex] = bucketKey;
        }
        confirmationHolder.overrideGlobalLastConfirmedKey(getKey());
    }

    public IMultiBucketSingleFileReplicationOrderedPacket getReliableAsyncBeforeFilterPacket() {
        return _beforeFilterPacket;
    }

    public void setReliableAsyncBeforeFilterPacket(
            IMultiBucketSingleFileReplicationOrderedPacket beforeFilterPacket) {
        _beforeFilterPacket = beforeFilterPacket;
    }

    public long getKey() {
        return _globalKey;
    }

    public long getEndKey() {
        return _globalKey;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        writeExternalImpl(out);
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        writeExternalImpl(out);
    }

    private void writeExternalImpl(ObjectOutput out) throws IOException {
        out.writeLong(_globalKey);
        writeBucketKeys(out);
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
        _globalKey = in.readLong();
        readBucketKeys(in);
    }

    @Override
    public abstract AbstractMultiBucketOrderedPacket clone();

    @Override
    public String toString() {
        return "globalKey=" + _globalKey + " keys=" + _bucketsKeys + " data="
                + getData();
    }

    @Override
    public int hashCode() {
        return (int) (_globalKey ^ (_globalKey >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        AbstractMultiBucketOrderedPacket other = (AbstractMultiBucketOrderedPacket) obj;
        if (_bucketsKeys == null) {
            if (other._bucketsKeys != null)
                return false;
        } else if (!_bucketsKeys.equals(other._bucketsKeys))
            return false;
        if (_globalKey != other._globalKey)
            return false;
        return true;
    }

    @Override
    public int getWeight() {
        if (_beforeFilterPacket != null) {
            return _beforeFilterPacket.getWeight();
        }
        return 1;
    }




}