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

package com.gigaspaces.internal.server.space.redolog;

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderOperationPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;
import com.gigaspaces.internal.utils.collections.ReadOnlyIteratorAdapter;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * A memory only based implementation of the {@link IRedoLogFile} interface. Packets are stored only
 * in the jvm memory
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class MemoryRedoLogFile<T> implements IRedoLogFile<T> {
    final private LinkedList<T> _redoFile = new LinkedList<T>();
    private long _weight;

    public void add(T replicationPacket) {
        _redoFile.addLast(replicationPacket);
        increaseWeight(replicationPacket);
    }

    public T getOldest() {
        return _redoFile.getFirst();
    }

    public boolean isEmpty() {
        return _redoFile.isEmpty();
    }

    public long getExternalStorageSpaceUsed() {
        return 0; //Memory only redo log file
    }

    public long getExternalStoragePacketsCount() {
        return 0; //Memory only redo log file
    }

    public long getMemoryPacketsCount() {
        return size();
    }

    public ReadOnlyIterator<T> readOnlyIterator(long fromIndex) {
        return new ReadOnlyIteratorAdapter<T>(_redoFile.listIterator((int) fromIndex));
    }

    public Iterator<T> iterator() {
        return _redoFile.iterator();
    }

    public ReadOnlyIterator<T> readOnlyIterator() {
        return new ReadOnlyIteratorAdapter<T>(iterator());
    }

    public T removeOldest() {
        T first = _redoFile.removeFirst();
        decreaseWeight(first);
        return first;
    }

    public long size() {
        return _redoFile.size();
    }

    public long getApproximateSize() {
        //LinkedList size method cannot cause concurrency issues but may return an inaccurate result.
        return _redoFile.size();
    }

    public void deleteOldestBatch(long batchSize) {
        if (batchSize > _redoFile.size()) {
            _redoFile.clear();
            _weight = 0;
        }
        else {
            for (long i = 0; i < batchSize; ++i){
                T first = _redoFile.removeFirst();
                decreaseWeight(first);
            }
        }
    }

    public void validateIntegrity() throws RedoLogFileCompromisedException {
        //Memory redo log cannot be compromised
    }


    public void close() {
        _redoFile.clear();
    }

    @Override
    public long getWeight() {
        return _weight;
    }

    private void increaseWeight(T packet){
        _weight += ((IReplicationOrderedPacket) packet).getWeight();
    }

    private void decreaseWeight(T packet){
        _weight -= ((IReplicationOrderedPacket) packet).getWeight();
    }
}
