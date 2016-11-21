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

import com.gigaspaces.internal.server.space.redolog.storage.INonBatchRedoLogFileStorage;
import com.gigaspaces.internal.server.space.redolog.storage.StorageException;
import com.gigaspaces.internal.server.space.redolog.storage.StorageReadOnlyIterator;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;
import com.gigaspaces.logger.Constants;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A swap based implementation of the {@link IRedoLogFile} interface, A fixed number of packets can
 * be held in the memory and once this number is exeeded the other packets are stored in a provided
 * {@link INonBatchRedoLogFileStorage}
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class FixedSizeSwapRedoLogFile<T> implements IRedoLogFile<T> {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);

    private final int _memoryMaxPackets;
    private final int _fetchBatchSize;
    private final MemoryRedoLogFile<T> _memoryRedoLogFile = new MemoryRedoLogFile<T>();
    private final INonBatchRedoLogFileStorage<T> _externalStorage;
    //Not volatile because this is not a thread safe structure, assume flushing of thread cache
    //changes because lock is held at upper layer
    private boolean _insertToExternal = false;

    /**
     * Constructs a fixed size swap redo log file
     */
    public FixedSizeSwapRedoLogFile(FixedSizeSwapRedoLogFileConfig config) {
        this._memoryMaxPackets = config.getMemoryMaxPackets();
        this._externalStorage = config.getRedoLogFileStorage();
        this._fetchBatchSize = config.getFetchBatchSize();
        if (_logger.isLoggable(Level.CONFIG)) {
            _logger.config("FixedSizeSwapRedoLogFile created:"
                    + "\n\tmemoryMaxPackets = " + _memoryMaxPackets
                    + "\n\tfetchBatchSize = " + _fetchBatchSize);
        }
    }

    public void add(T replicationPacket) {
        if (!_insertToExternal && _memoryRedoLogFile.size() >= _memoryMaxPackets)
            _insertToExternal = true;

        if (_insertToExternal)
            addToStorage(replicationPacket);
        else
            _memoryRedoLogFile.add(replicationPacket);
    }

    public T getOldest() {
        if (!_memoryRedoLogFile.isEmpty())
            return _memoryRedoLogFile.getOldest();

        return getOldestFromDataStorage();
    }

    private T getOldestFromDataStorage() {
        try {
            StorageReadOnlyIterator<T> storageIterator = _externalStorage.readOnlyIterator();
            T oldest = storageIterator.next();
            storageIterator.close();
            return oldest;
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    public boolean isEmpty() {
        //return true if both the memory redo log file is empty and the external storage
        try {
            return _memoryRedoLogFile.isEmpty() && (_insertToExternal ? _externalStorage.isEmpty() : true);
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    public ReadOnlyIterator<T> readOnlyIterator(long fromIndex) {
        long memRedoFileSize = _memoryRedoLogFile.size();
        if (fromIndex < memRedoFileSize)
            return new SwapReadOnlyIterator(_memoryRedoLogFile.readOnlyIterator(fromIndex));
        //Skip entire memory redo log, can safely cast to int because here memRedoFileSize cannot be more than int
        return new SwapReadOnlyIterator(fromIndex - memRedoFileSize);
    }

    public T removeOldest() {
        if (!_memoryRedoLogFile.isEmpty())
            return _memoryRedoLogFile.removeOldest();

        moveOldestBatchFromStorage();

        return _memoryRedoLogFile.removeOldest();
    }

    public long size() {
        //Returns the size of the redo log file taking both the swapped packets
        //and the memory residing packets into consideration
        try {
            return _memoryRedoLogFile.size() + (_insertToExternal ? _externalStorage.size() : 0);
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    public long getApproximateSize() {
        try {
            return _memoryRedoLogFile.getApproximateSize() + _externalStorage.size();
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    public Iterator<T> iterator() {
        //TODO This iterator which is not read only does not propogate to the swapped redo log
        //However the only usage of it does not really need it to be propogated and it is currently
        //done so to simplify the IExternalRedoLogFileStorage interface to support only read only iterators
        return _memoryRedoLogFile.iterator();
    }

    public ReadOnlyIterator<T> readOnlyIterator() {
        ReadOnlyIterator<T> memoryIterator = _memoryRedoLogFile.readOnlyIterator();
        return new SwapReadOnlyIterator(memoryIterator);
    }

    public void deleteOldestBatch(long batchSize) {
        long memorySize = _memoryRedoLogFile.size();
        _memoryRedoLogFile.deleteOldestBatch(batchSize);

        if (memorySize < batchSize)
            deleteOldestBatchFromStorage(batchSize - memorySize);

        if (_memoryRedoLogFile.isEmpty() && _insertToExternal)
            moveOldestBatchFromStorage();
    }

    private void deleteOldestBatchFromStorage(long deleteBatchFromSwap) {
        try {
            _externalStorage.deleteFirstBatch(deleteBatchFromSwap);
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    private void moveOldestBatchFromStorage() {
        try {
            List<T> batch = _externalStorage.removeFirstBatch(_fetchBatchSize);

            if (_logger.isLoggable(Level.FINEST))
                _logger.finest("Moving a batch of packet from storage into memory, batch size is " + batch.size());

            assert (batch.size() <= _fetchBatchSize) : "Removed a batch from storage which is larger than requested (" + batch.size() + "/" + _memoryMaxPackets + ")";

            for (T packet : batch)
                _memoryRedoLogFile.add(packet);

            if (_externalStorage.isEmpty() && batch.size() < _memoryMaxPackets)
                _insertToExternal = false;
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    private void addToStorage(T replicationPacket) {
        try {
            _externalStorage.append(replicationPacket);
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    public long getMemoryPacketCount() {
        return _memoryRedoLogFile.size();
    }

    public long getStoragePacketCount() {
        try {
            return _externalStorage.size();
        } catch (StorageException e) {
            throw new SwapStorageException(e);
        }
    }

    public long getExternalStorageSpaceUsed() {
        return _externalStorage.getSpaceUsed();
    }

    public long getExternalStoragePacketsCount() {
        return _externalStorage.getExternalPacketsCount();
    }

    public long getMemoryPacketsCount() {
        return _memoryRedoLogFile.getMemoryPacketsCount() + _externalStorage.getMemoryPacketsCount();
    }

    public void validateIntegrity() throws RedoLogFileCompromisedException {
        _memoryRedoLogFile.validateIntegrity();
        _externalStorage.validateIntegrity();
    }

    public void close() {
        _memoryRedoLogFile.close();
        _externalStorage.close();
    }

    @Override
    public long getWeight() {
        return _memoryRedoLogFile.getWeight() + _externalStorage.getWeight();
    }

    /**
     * A read only iterator which iterate over the memory redo log file, and once completed
     * iterating over it, it continue to iterate over the external storage
     *
     * @author eitany
     * @since 7.1
     */
    private class SwapReadOnlyIterator
            implements ReadOnlyIterator<T> {

        private final ReadOnlyIterator<T> _memoryIterator;
        private boolean _memoryIteratorExhausted = false;
        private StorageReadOnlyIterator<T> _externalIterator = null;

        /**
         * Create an iterator which stars iterating over the packets which reside in the memory redo
         * log file
         */
        public SwapReadOnlyIterator(
                ReadOnlyIterator<T> memoryIterator) {
            this._memoryIterator = memoryIterator;
        }


        /**
         * Create an iterator which starts directly iterating over the storage, thus skipping the
         * memory redo log file
         *
         * @param inSwapStartIndex offset index to start inside the storage
         */
        public SwapReadOnlyIterator(long inSwapStartIndex) {
            _memoryIteratorExhausted = true;
            _memoryIterator = null;
            try {
                _externalIterator = _externalStorage.readOnlyIterator(inSwapStartIndex);
            } catch (StorageException e) {
                throw new SwapStorageException(e);
            }
        }


        public boolean hasNext() {
            if (!_memoryIteratorExhausted) {
                _memoryIteratorExhausted = !_memoryIterator.hasNext();
                if (!_memoryIteratorExhausted)
                    return true;
            }

            try {
                //If here, memory iterator is exhausted
                if (_externalIterator == null)
                    _externalIterator = _externalStorage.readOnlyIterator();

                return _externalIterator.hasNext();
            } catch (StorageException e) {
                throw new SwapStorageException(e);
            }
        }

        public T next() {
            if (!_memoryIteratorExhausted) {
                try {
                    return _memoryIterator.next();
                } catch (NoSuchElementException e) {
                    _memoryIteratorExhausted = true;
                }
            }

            try {
                //If here, memory iterator is exhausted (support iteration using only next())
                if (_externalIterator == null)
                    _externalIterator = _externalStorage.readOnlyIterator();

                return _externalIterator.next();
            } catch (StorageException e) {
                throw new SwapStorageException(e);
            }
        }


        public void close() {
            if (_memoryIterator != null)
                _memoryIterator.close();
            if (_externalIterator != null)
                try {
                    _externalIterator.close();
                } catch (StorageException e) {
                    throw new SwapStorageException(e);
                }
        }

    }

}
