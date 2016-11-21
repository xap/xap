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
import com.gigaspaces.internal.collections.ShortObjectMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractSingleBucketOrderedPacket implements ISingleBucketReplicationOrderedPacket {
    private static final long serialVersionUID = 1L;

    private long _bucketKey;
    private short _bucketIndex;
    private long _globalKey;

    private transient IMultiBucketSingleFileReplicationOrderedPacket _beforeFilterPacket;

    public AbstractSingleBucketOrderedPacket() {
    }

    public AbstractSingleBucketOrderedPacket(long globalKey, long bucketKey,
                                             short bucketIndex) {
        _globalKey = globalKey;
        _bucketKey = bucketKey;
        _bucketIndex = bucketIndex;
    }

    public long getKey() {
        return _globalKey;
    }

    public long getEndKey() {
        return _globalKey;
    }

    public long getBucketKey(short bucketIndex) {
        if (_bucketIndex != bucketIndex)
            throw new IllegalArgumentException("This packet only belong to bucket " + _bucketIndex + " and not to " + bucketIndex);
        return _bucketKey;
    }

    public short[] getBuckets() {
        return new short[]{_bucketIndex};
    }

    public PacketConsumeState getConsumeState(short bucketIndex) {
        if (bucketIndex != _bucketIndex)
            throw new IllegalArgumentException("This packet only belong to bucket " + _bucketIndex + " and not to " + bucketIndex);

        return PacketConsumeState.CAN_CONSUME;
    }

    public boolean setConsumed() {
        //Do nothing, single bucket packet is consumed once always and therefore this should return true
        //always
        return true;
    }

    public short getBucketIndex() {
        return _bucketIndex;
    }

    public long getBucketKey() {
        return _bucketKey;
    }

    public short bucketCount() {
        return 1;
    }

    public void associateToBuckets(
            ShortObjectMap<List<IMultiBucketSingleFileReplicationOrderedPacket>> bucketsPackets) {
        List<IMultiBucketSingleFileReplicationOrderedPacket> list = bucketsPackets.get(_bucketIndex);
        if (list == null) {
            list = new LinkedList<IMultiBucketSingleFileReplicationOrderedPacket>();
            bucketsPackets.put(_bucketIndex, list);
        }
        list.add(this);
    }

    public long processResult(String memberName,
                              MultiBucketSingleFileProcessResult processResult,
                              MultiBucketSingleFileConfirmationHolder confirmationHolder) {
        short bucketIndex = _bucketIndex;
        long bucketKey = _bucketKey;

        Long knownLastConfirmedKey = confirmationHolder.getBucketLastConfirmedKeys()[bucketIndex];
        if (knownLastConfirmedKey == null || knownLastConfirmedKey < bucketKey)
            confirmationHolder.getBucketLastConfirmedKeys()[bucketIndex] = bucketKey;

        return confirmationHolder.addGlobalKeyConfirmed(getKey());
    }

    public void reliableAsyncKeysUpdate(long[] bucketLastKeys,
                                        MultiBucketSingleFileConfirmationHolder confirmationHolder) {
        short bucketIndex = _bucketIndex;
        long bucketKey = _bucketKey;

        bucketLastKeys[bucketIndex] = bucketKey + 1;
        confirmationHolder.overrideGlobalLastConfirmedKey(_globalKey);
        confirmationHolder.getBucketLastConfirmedKeys()[bucketIndex] = _bucketKey;
    }

    public IMultiBucketSingleFileReplicationOrderedPacket getReliableAsyncBeforeFilterPacket() {
        return _beforeFilterPacket;
    }

    public void setReliableAsyncBeforeFilterPacket(
            IMultiBucketSingleFileReplicationOrderedPacket beforeFilterPacket) {
        _beforeFilterPacket = beforeFilterPacket;
    }

    public abstract AbstractSingleBucketOrderedPacket clone();

    public void writeExternal(ObjectOutput out) throws IOException {
        writeExternalImpl(out);
    }

    public void writeToSwap(ObjectOutput out) throws IOException {
        writeExternalImpl(out);
    }

    private void writeExternalImpl(ObjectOutput out) throws IOException {
        out.writeLong(_globalKey);
        out.writeLong(_bucketKey);
        out.writeShort(_bucketIndex);
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
        _bucketKey = in.readLong();
        _bucketIndex = in.readShort();
    }

    @Override
    public String toString() {
        return "sourceGlobalKey=" + _globalKey + " bucketIndex=" + _bucketIndex + " bucketKey=" + _bucketKey + " data=" + getData();
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
        AbstractSingleBucketOrderedPacket other = (AbstractSingleBucketOrderedPacket) obj;
        if (_bucketIndex != other._bucketIndex)
            return false;
        if (_bucketKey != other._bucketKey)
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