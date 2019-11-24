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

import com.gigaspaces.internal.cluster.node.impl.DataTypeIntroducePacketData;
import com.gigaspaces.internal.cluster.node.impl.backlog.AbstractSingleFileGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;
import com.gigaspaces.internal.utils.collections.ReadOnlyIteratorAdapter;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.cluster.startup.RedoLogCompactionUtil;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.logging.Logger;

import static com.gigaspaces.logger.Constants.LOGGER_REPLICATION_BACKLOG;

/**
 * A memory only based implementation of the {@link IRedoLogFile} interface. Packets are stored only
 * in the jvm memory
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class MemoryRedoLogFile<T extends IReplicationOrderedPacket> implements IRedoLogFile<T> {
    final private LinkedList<T> _redoFile = new LinkedList<T>();
    private final String _name;
    private final AbstractSingleFileGroupBacklog _groupBacklog;
    private long _weight;
    private long _discardedPacketCount;
    private final Logger _logger;

    public MemoryRedoLogFile(String name, AbstractSingleFileGroupBacklog groupBacklog) {
        _name = name;
        _logger = Logger.getLogger(LOGGER_REPLICATION_BACKLOG + "." + _name);
        _groupBacklog = groupBacklog;
    }

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

    @Override
    public long getMemoryPacketsWeight() {
        return getWeight();
    }

    @Override
    public long getExternalStoragePacketsWeight() {
        return 0;//Memory only redo log file
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

    public void deleteOldestPackets(long packetsCount) {
        if (packetsCount > _redoFile.size()) {
            _redoFile.clear();
            _weight = 0;
            _discardedPacketCount = 0;
        } else {
            for (long i = 0; i < packetsCount; ++i) {
                T first = _redoFile.removeFirst();
                decreaseWeight(first);
            }
        }
    }

    private void printRedoFile(String s) {
        if (_name.contains("1_1")) {
            return;
        }
        _logger.info("");
        _logger.info(s);
        for (T t : _redoFile) {
            String dataString = t.getData().toString();
            if (t.getData() instanceof DataTypeIntroducePacketData) {
                dataString = "DataTypeIntroduce";
            }
            _logger.info("key=" + t.getKey() + ", data=" + dataString);
        }
//        Thread.dumpStack();
        _logger.info("Total weight = " + this.getWeight());
        _logger.info("");
    }

    public void validateIntegrity() throws RedoLogFileCompromisedException {
        //Memory redo log cannot be compromised
    }


    public void close() {
        _redoFile.clear();
        _weight = 0;
        _discardedPacketCount = 0;
    }

    @Override
    public long getWeight() {
        return RedoLogCompactionUtil.calculateWeight(_weight, _discardedPacketCount);
    }

    @Override
    public long getDiscardedPacketsCount() {
        return _discardedPacketCount;
    }

    @Override
    public CompactionResult performCompaction(long from, long to) {
        ListIterator<T> iterator = _redoFile.listIterator();
        final CompactionResult compactionResult =RedoLogCompactionUtil.compact(from, to, iterator);
        this._weight -= compactionResult.getDiscardedCount() + compactionResult.getDeletedFromTxn();
        this._discardedPacketCount += compactionResult.getDiscardedCount();
        return compactionResult;
    }

    private void increaseWeight(T packet) {
        if (packet.isDiscardedPacket()) {
            _discardedPacketCount++;
            if (_groupBacklog.hasMirror()) {
                _groupBacklog.increaseMirrorDiscardedCount(1);
            }
        } else {
            _weight += packet.getWeight();
        }
    }

    private void decreaseWeight(T packet) {
        if (packet.isDiscardedPacket()) {
            _discardedPacketCount--;
            if (_groupBacklog.hasMirror()) {
                _groupBacklog.decreaseMirrorDiscardedCount(1);
            }
        } else {
            _weight -= packet.getWeight();
        }
    }
}
