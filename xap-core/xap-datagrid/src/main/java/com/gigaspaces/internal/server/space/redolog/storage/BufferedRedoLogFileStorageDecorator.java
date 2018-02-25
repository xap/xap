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

import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.WeightedBatch;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.cluster.startup.CompactionResult;
import com.j_spaces.core.cluster.startup.RedoLogCompactionUtil;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Wraps a {@link IRedoLogFileStorage} with a buffer, allowing adding single packets in the storage
 * which will be flushed once a specific buffer size is reached
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class BufferedRedoLogFileStorageDecorator<T extends IReplicationOrderedPacket>
        implements INonBatchRedoLogFileStorage<T> {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);

    private final int _bufferCapacity;
    private final IRedoLogFileStorage<T> _storage;
    private final LinkedList<T> _buffer = new LinkedList<T>();
    private long _bufferWeight;

    /**
     * @param bufferSize buffer size
     * @param storage    wrapped external storage
     */
    public BufferedRedoLogFileStorageDecorator(int bufferSize, IRedoLogFileStorage storage) {
        this._bufferCapacity = bufferSize;
        this._storage = storage;
        if (_logger.isLoggable(Level.CONFIG)) {
            _logger.config("BufferedRedoLogFileStorageDecorator created:"
                    + "\n\tbufferSize = " + _bufferCapacity);
        }
        _bufferWeight = 0;
    }

    public void append(T replicationPacket) throws StorageException {
        if (_bufferWeight + replicationPacket.getWeight() > _bufferCapacity && !_buffer.isEmpty()) {
            flushBuffer();
        }
        _buffer.add(replicationPacket);
        increaseWeight(replicationPacket);
        if (_bufferWeight >= _bufferCapacity && _buffer.size() >= 1)
            flushBuffer();
    }

    private void increaseWeight(T replicationPacket) {
        _bufferWeight += replicationPacket.getWeight();
    }

    private void decreaseWeight(T packet) {
        _bufferWeight -= packet.getWeight();
    }

    public void appendBatch(List<T> replicationPackets) throws StorageException {
        _buffer.addAll(replicationPackets);
        for (T replicationPacket : replicationPackets) {
            increaseWeight(replicationPacket);
        }
        if (_buffer.size() >= _bufferCapacity)
            flushBuffer();
    }

    public void deleteOldestPackets(long deleteUpToKey) throws StorageException {
        _storage.deleteOldestPackets(deleteUpToKey);
        if(_buffer.isEmpty()){
            return;
        }
        while (_buffer.getFirst().getEndKey() < deleteUpToKey){
            T firstPacket = _buffer.removeFirst();
            decreaseWeight(firstPacket);
        }
    }

    public StorageReadOnlyIterator<T> readOnlyIterator() throws StorageException {
        return new BufferedReadOnlyIterator(_storage.readOnlyIterator());
    }

    public StorageReadOnlyIterator<T> readOnlyIterator(long fromKey) throws StorageException {
        long lastKeyInStorage = _storage.getLastKeyInStorage();

        if (fromKey <= lastKeyInStorage)
            return new BufferedReadOnlyIterator(_storage.readOnlyIterator(fromKey));

        //Can safely cast to int because if reached here the buffer size cannot be more than int
        return new BufferedReadOnlyIterator((int) fromKey);
    }

    @Override
    public long getLastKeyInStorage() {
        return _buffer.isEmpty() ? _storage.getLastKeyInStorage() : _buffer.getLast().getEndKey();
    }

    public WeightedBatch<T> removeFirstBatch(int batchCapacity, long lastCompactionRangeEndKey) throws StorageException {
        WeightedBatch<T> batch = _storage.removeFirstBatch(batchCapacity, lastCompactionRangeEndKey);

        if (_logger.isLoggable(Level.FINEST))
            _logger.finest("removed a batch of weight " + batch.getWeight() + " from storage");


        while (!_buffer.isEmpty() && batch.getWeight() < batchCapacity && !batch.isLimitReached()) {
            T firstPacket = _buffer.getFirst();

            if (batch.size() > 0 && batch.getWeight() + firstPacket.getWeight() > batchCapacity) {
                batch.setLimitReached(true);
                break;
            }
            _buffer.removeFirst();
            batch.addToBatch(firstPacket);
            decreaseWeight(firstPacket);
        }

        if (batch.size() >= batchCapacity) {
            batch.setLimitReached(true);
        }
        return batch;
    }

    public long size() throws StorageException {
        return _buffer.size() + _storage.size();
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

    public boolean isEmpty() throws StorageException {
        return _buffer.isEmpty() && _storage.isEmpty();
    }

    public void validateIntegrity() throws RedoLogFileCompromisedException {
        _storage.validateIntegrity();
    }

    public void close() {
        _storage.close();
    }

    @Override
    public long getWeight() {
        return _bufferWeight + _storage.getWeight();
    }

    @Override
    public long getDiscardedPacketsCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompactionResult performCompaction(long from, long to) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCacheWeight() {
        return 0;
    }

    public IRedoLogFileStorageStatistics getUnderlyingStorage() {
        return _storage;
    }

    private void flushBuffer() throws StorageException {
        try {
            if (_logger.isLoggable(Level.FINEST))
                _logger.finest("flushing buffer to underlying storage, buffer size is " + _buffer.size());
            _storage.appendBatch(_buffer);
        } finally {
            _buffer.clear();
            _bufferWeight = 0;
        }
    }

    /**
     * A read only iterator which automatically starts iterating over the buffer once the external
     * storage is exhausted
     *
     * @author eitany
     * @since 7.1
     */
    private class BufferedReadOnlyIterator implements StorageReadOnlyIterator<T> {

        private final StorageReadOnlyIterator<T> _externalIterator;
        private boolean _externalIteratorExhausted;
        private Iterator<T> _bufferIterator;

        /**
         * Create an iterator which stars iterating over the packets which reside in external
         * storage
         */
        public BufferedReadOnlyIterator(StorageReadOnlyIterator<T> externalIterator) {
            this._externalIterator = externalIterator;
        }

        /**
         * Create an iterator which starts directly iterating over the buffer, thus skipping the
         * external storage.
         *
         * @param fromKey offset index to start inside the buffer
         */
        public BufferedReadOnlyIterator(int fromKey) {
            _externalIteratorExhausted = true;
            _externalIterator = null;
            ListIterator<T> bufferIterator = _buffer.listIterator();
            RedoLogCompactionUtil.skipToKey(bufferIterator, fromKey);
            _bufferIterator = bufferIterator;
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
