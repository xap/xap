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

package com.gigaspaces.internal.server.space.redolog.storage;

import com.gigaspaces.internal.cluster.node.impl.backlog.AbstractSingleFileGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.cluster.startup.RedoLogCompactionUtil;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a {@link INonBatchRedoLogFileStorage} with a cache that keeps a constant size number of
 * packets in memory which were the last appended packet. Since the most frequent read access to the
 * redo log file are to the end of the file (or the start), keeping the latest packets in memory
 * will reduce the accesses to the storage which creates a much more serious bottleneck
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class CacheLastRedoLogFileStorageDecorator<T extends IReplicationOrderedPacket> implements INonBatchRedoLogFileStorage<T> {

    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);

    private final int _bufferCapacity;
    private final INonBatchRedoLogFileStorage<T> _storage;
    private final LinkedList<T> _buffer = new LinkedList<T>();
    private final AbstractSingleFileGroupBacklog _groupBacklog;
    private long _bufferWeight;
    private long _discardedPacketCount;


    public CacheLastRedoLogFileStorageDecorator(int bufferSize, INonBatchRedoLogFileStorage<T> storage, AbstractSingleFileGroupBacklog groupBacklog) {
        this._bufferCapacity = bufferSize;
        this._storage = storage;

        if (_logger.isDebugEnabled()) {
            _logger.debug("CacheLastRedoLogFileStorageDecorator created:"
                    + "\n\tbufferSize = " + _bufferCapacity);
        }
        _bufferWeight = 0;
        _groupBacklog = groupBacklog;
    }

    public void append(T replicationPacket)
            throws StorageException, StorageFullException {
        _buffer.addLast(replicationPacket);
        if (replicationPacket.isDiscardedPacket()) {
            _discardedPacketCount++;
        } else {
            increaseBufferWeight(replicationPacket);
        }
        flushOldest();
    }

    private void increaseBufferWeight(T replicationPacket) {
        if (replicationPacket.isDiscardedPacket()) {
            _discardedPacketCount++;
            if(_groupBacklog.hasMirror()){
                _groupBacklog.increaseMirrorDiscardedCount(1);
            }
        } else {
            _bufferWeight += replicationPacket.getWeight();
        }
    }

    private void decreaseBufferWeight(T replicationPacket) {
        if (replicationPacket.isDiscardedPacket()) {
            _discardedPacketCount--;
            if(_groupBacklog.hasMirror()) {
                _groupBacklog.decreaseMirrorDiscardedCount(1);
            }
        } else {
            _bufferWeight -= replicationPacket.getWeight();
        }
    }

    public void appendBatch(List<T> replicationPackets)
            throws StorageException, StorageFullException {
        for (T replicationPacket : replicationPackets) {
            increaseBufferWeight(replicationPacket);
        }
        _buffer.addAll(replicationPackets);
        flushOldest();
    }

    private void flushOldest() throws StorageException, StorageFullException {
        try {
            while (_bufferWeight > _bufferCapacity && _buffer.size() > 1) {
                T packet = _buffer.removeFirst();
                _storage.append(packet);
                decreaseBufferWeight(packet);
            }
        } catch (StorageFullException e) {
            LinkedList newDeniedPackets = new LinkedList<T>();
            List deniedPackets = e.getDeniedPackets();
            for (int i = deniedPackets.size() - 1; i >= 0; --i) {
                _buffer.addFirst((T) deniedPackets.get(i));
                increaseBufferWeight((T) deniedPackets.get(i));
                T last = _buffer.removeLast();
                decreaseBufferWeight(last);
                newDeniedPackets.addFirst(last);
            }
            throw new StorageFullException(e.getMessage(), e.getCause(), newDeniedPackets);
        }
    }

    public void validateIntegrity() throws RedoLogFileCompromisedException {
        _storage.validateIntegrity();
    }

    public void close() {
        _buffer.clear();
        _storage.close();
        _bufferWeight = 0;
    }

    @Override
    public long getWeight() {
        return _bufferWeight + _storage.getWeight();
    }

    @Override
    public long getDiscardedPacketsCount() {
        return _discardedPacketCount;
    }

    @Override
    public CompactionResult performCompaction(long from, long to) {
        ListIterator<T> iterator = _buffer.listIterator();
        final CompactionResult compactionResult = RedoLogCompactionUtil.compact(from, to, iterator);
        this._bufferWeight -= compactionResult.getDiscardedCount() + compactionResult.getDeletedFromTxn();
        this._discardedPacketCount += compactionResult.getDiscardedCount();
        return compactionResult;
    }

    public void deleteOldestPackets(long packetsCount) throws StorageException {
        long storageSize = _storage.size();
        _storage.deleteOldestPackets(packetsCount);
        int bufferSize = _buffer.size();
        for (long i = 0; i < Math.min(bufferSize, packetsCount - storageSize); ++i) {
            T first = _buffer.removeFirst();
            decreaseBufferWeight(first);
        }
    }

    public boolean isEmpty() throws StorageException {
        return _buffer.isEmpty() && _storage.isEmpty();
    }

    public long getSpaceUsed() {
        return _storage.getSpaceUsed();
    }

    public long getExternalPacketsCount() {
        return _storage.getExternalPacketsCount();
    }

    public long getMemoryPacketsCount() {
        return _buffer.size() + _storage.getMemoryPacketsCount();
    }

    @Override
    public long getMemoryPacketsWeight() {
        return _bufferWeight + _storage.getMemoryPacketsWeight();
    }

    @Override
    public long getExternalStoragePacketsWeight() {
        return _storage.getExternalStoragePacketsWeight();
    }

    public StorageReadOnlyIterator<T> readOnlyIterator()
            throws StorageException {
        return new CacheReadOnlyIterator(_storage.readOnlyIterator());
    }

    public StorageReadOnlyIterator<T> readOnlyIterator(
            long fromIndex) throws StorageException {
        long storageSize = _storage.size();

        if (fromIndex < storageSize)
            return new CacheReadOnlyIterator(_storage.readOnlyIterator(fromIndex));

        //Can safely cast to int because if reached here the buffer size cannot be more than int
        return new CacheReadOnlyIterator((int) (fromIndex - storageSize));
    }

    public WeightedBatch<T> removeFirstBatch(int batchCapacity, long lastCompactionRangeEndKey) throws StorageException {
        WeightedBatch<T> batch = _storage.removeFirstBatch(batchCapacity, lastCompactionRangeEndKey);

        while (!_buffer.isEmpty() && batch.getWeight() < batchCapacity && !batch.isLimitReached()) {
            T first = _buffer.getFirst();

            if (batch.size() > 0 && batch.getWeight() + first.getWeight() > batchCapacity) {
                batch.setLimitReached(true);
                break;
            }

            _buffer.removeFirst();
            decreaseBufferWeight(first);
            batch.addToBatch(first);
        }
        if (batch.size() >= batchCapacity) {
            batch.setLimitReached(true);
        }
        return batch;
    }

    public long size() throws StorageException {
        return _buffer.size() + _storage.size();
    }

    @Override
    public long getCacheWeight() {
        return RedoLogCompactionUtil.calculateWeight(_bufferWeight, _discardedPacketCount);
    }

    /**
     * A read only iterator which automatically starts iterating over the buffer once the external
     * storage is exhausted
     *
     * @author eitany
     * @since 7.1
     */
    private class CacheReadOnlyIterator implements StorageReadOnlyIterator<T> {

        private final StorageReadOnlyIterator<T> _externalIterator;
        private boolean _externalIteratorExhausted;
        private Iterator<T> _bufferIterator;

        /**
         * Create an iterator which stars iterating over the packets which reside in external
         * storage
         */
        public CacheReadOnlyIterator(StorageReadOnlyIterator<T> externalIterator) {
            this._externalIterator = externalIterator;
        }

        /**
         * Create an iterator which starts directly iterating over the buffer, thus skipping the
         * external storage.
         *
         * @param fromIndex offset index to start inside the buffer
         */
        public CacheReadOnlyIterator(int fromIndex) {
            _externalIteratorExhausted = true;
            _externalIterator = null;
            _bufferIterator = _buffer.listIterator(fromIndex);
        }

        public boolean hasNext() throws StorageException {
            if (!_externalIteratorExhausted) {
                _externalIteratorExhausted = !_externalIterator.hasNext();
                if (!_externalIteratorExhausted)
                    return true;
            }

            //If here, external iterator is exhausted
            if (_bufferIterator == null)
                //Create iterator over external storage
                _bufferIterator = _buffer.iterator();

            return _bufferIterator.hasNext();
        }

        public T next() throws StorageException {
            if (!_externalIteratorExhausted) {
                try {
                    return _externalIterator.next();
                } catch (NoSuchElementException e) {
                    _externalIteratorExhausted = true;
                }
            }
            //If here, external iterator is exhausted
            if (_bufferIterator == null)
                //Create iterator over external storage
                _bufferIterator = _buffer.iterator();

            return _bufferIterator.next();
        }

        public void close() throws StorageException {
            if (_externalIterator != null)
                _externalIterator.close();
        }

    }

}
