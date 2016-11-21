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

import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderOperationPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.logger.Constants;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
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
public class BufferedRedoLogFileStorageDecorator<T>
        implements INonBatchRedoLogFileStorage<T> {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);

    private final int _bufferSize;
    private final IRedoLogFileStorage<T> _storage;
    private final LinkedList<T> _buffer = new LinkedList<T>();
    private long _bufferWeight;

    /**
     * @param bufferSize buffer size
     * @param storage    wrapped external storage
     */
    public BufferedRedoLogFileStorageDecorator(int bufferSize, IRedoLogFileStorage storage) {
        this._bufferSize = bufferSize;
        this._storage = storage;
        if (_logger.isLoggable(Level.CONFIG)) {
            _logger.config("BufferedRedoLogFileStorageDecorator created:"
                    + "\n\tbufferSize = " + _bufferSize);
        }
        _bufferWeight = 0;
    }

    public void append(T replicationPacket) throws StorageException {
        _buffer.add(replicationPacket);
        increaseWeight(replicationPacket);
        if (_buffer.size() >= _bufferSize)
            flushBuffer();
    }

    private void increaseWeight(T replicationPacket) {
        _bufferWeight += ((IReplicationOrderedPacket) replicationPacket).getWeight();
    }

    private void decreaseWeight(T packet) {
        _bufferWeight -= ((IReplicationOrderedPacket) packet).getWeight();
    }

    public void appendBatch(List<T> replicationPackets) throws StorageException {
        _buffer.addAll(replicationPackets);
        for (T replicationPacket : replicationPackets) {
            increaseWeight(replicationPacket);
        }
        if (_buffer.size() >= _bufferSize)
            flushBuffer();
    }

    public void deleteFirstBatch(long batchSize) throws StorageException {
        long storageSize = _storage.size();
        _storage.deleteFirstBatch(batchSize);
        if (_logger.isLoggable(Level.FINEST))
            _logger.finest("delete a batch of size " + Math.min(batchSize, storageSize) + " from storage");
        int bufferSize = _buffer.size();
        for (long i = 0; i < Math.min(bufferSize, batchSize - storageSize); ++i) {
            T firstPacket = _buffer.removeFirst();
            decreaseWeight(firstPacket);
        }
    }

    public StorageReadOnlyIterator<T> readOnlyIterator() throws StorageException {
        return new BufferedReadOnlyIterator(_storage.readOnlyIterator());
    }

    public StorageReadOnlyIterator<T> readOnlyIterator(long fromIndex) throws StorageException {
        long storageSize = _storage.size();

        if (fromIndex < storageSize)
            return new BufferedReadOnlyIterator(_storage.readOnlyIterator(fromIndex));

        //Can safely cast to int because if reached here the buffer size cannot be more than int
        return new BufferedReadOnlyIterator((int) (fromIndex - storageSize));
    }

    public List<T> removeFirstBatch(int batchSize) throws StorageException {
        List<T> batch = _storage.removeFirstBatch(batchSize);
        int storageBatchSize = batch.size();
        if (_logger.isLoggable(Level.FINEST))
            _logger.finest("removed a batch of size " + storageBatchSize + " from storage");

        assert (storageBatchSize <= batchSize) : "Removed a batch from storage which is larger than requested (" + storageBatchSize + "/" + batchSize + ")";

        if (storageBatchSize < batchSize) {
            int remaining = batchSize - storageBatchSize;
            int bufferSize = _buffer.size();
            for (int i = 0; i < Math.min(bufferSize, remaining); ++i) {
                T firstPacket = _buffer.removeFirst();
                batch.add(firstPacket);
                decreaseWeight(firstPacket);
            }
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
         * @param fromIndex offset index to start inside the buffer
         */
        public BufferedReadOnlyIterator(int fromIndex) {
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
