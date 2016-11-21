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

import com.gigaspaces.internal.server.space.redolog.RedoLogFileCompromisedException;
import com.gigaspaces.internal.utils.concurrent.GSThread;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Wraps a {@link IRedoLogFileStorage} with a buffer, allowing adding single packets in the storage.
 * all packets addition are flushed to the underlying storage asynchronously and the adding method
 * immediately returns
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class AsyncRedoLogFileStorageDecorator<T>
        implements INonBatchRedoLogFileStorage<T> {

    private final IRedoLogFileStorage<T> _storage;
    private final ReadWriteLock _lock = new ReentrantReadWriteLock();
    private final LinkedList<T> _buffer = new LinkedList<T>();
    private final long _interval;
    private final int _maxFlushSize;
    private Flusher _fluser;

    public AsyncRedoLogFileStorageDecorator(IRedoLogFileStorage<T> storage, long interval, TimeUnit timeUnit, int maxFlushSize) {
        this._storage = storage;
        this._interval = timeUnit.toMillis(interval);
        this._maxFlushSize = maxFlushSize;
    }

    public void append(T replicationPacket)
            throws StorageException {
        initFlusherIfNeeder();
        //Protect unflushed packets because the flusher thread can asynchronously
        //flush the packets to the storage
        synchronized (_buffer) {
            _buffer.add(replicationPacket);
        }
    }

    public void appendBatch(List<T> replicationPackets)
            throws StorageException, StorageFullException {
        initFlusherIfNeeder();
        //Protect unflushed packets because the flusher thread can asynchronously
        //flush the packets to the storage
        synchronized (_buffer) {
            _buffer.addAll(replicationPackets);
        }
    }

    public synchronized void validateIntegrity() throws RedoLogFileCompromisedException {
        _storage.validateIntegrity();
    }

    public synchronized void close() {
        if (_fluser != null) {
            _fluser.interrupt();
            try {
                _fluser.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            _fluser = null;
        }

        _storage.close();
    }

    @Override
    public long getWeight() {
        throw new UnsupportedOperationException("getWeight not supported in class AsyncRedoLogFileStorageDecorator");
    }

    public void deleteFirstBatch(long batchSize) throws StorageException {
        long storageSize;
        _lock.writeLock().lock();
        try {
            storageSize = _storage.size();
            _storage.deleteFirstBatch(batchSize);
            //Protect unflushed packets because the flusher thread can asynchronously
            //flush the packets to the storage
            synchronized (_buffer) {
                int bufferSize = _buffer.size();
                for (long i = 0; i < Math.min(bufferSize, batchSize - storageSize); ++i)
                    _buffer.removeFirst();
            }
        } finally {
            _lock.writeLock().unlock();
        }
    }

    public boolean isEmpty() throws StorageException {
        if (_buffer.isEmpty()) {
            _lock.readLock().lock();
            try {
                return _storage.isEmpty();
            } finally {
                _lock.readLock().unlock();
            }
        }

        return false;
    }

    public long getSpaceUsed() {
        _lock.readLock().lock();
        try {
            return _storage.getSpaceUsed();
        } finally {
            _lock.readLock().unlock();
        }
    }

    public long getExternalPacketsCount() {
        _lock.readLock().lock();
        try {
            return _storage.getExternalPacketsCount();
        } finally {
            _lock.readLock().unlock();
        }
    }

    public long getMemoryPacketsCount() {
        _lock.readLock().lock();
        try {
            return _buffer.size() + _storage.getMemoryPacketsCount();
        } finally {
            _lock.readLock().unlock();
        }
    }

    public StorageReadOnlyIterator<T> readOnlyIterator()
            throws StorageException {
        //Takes reader lock to keep the contract of multi reader single writer mutual exclusion
        //the read lock will be released by the iterator when it is closed (Flusher thread can
        //write to the storage if this lock is not held)
        _lock.readLock().lock();
        return new AsyncReadOnlyIterator(_storage.readOnlyIterator());
    }

    public StorageReadOnlyIterator<T> readOnlyIterator(
            long fromIndex) throws StorageException {
        //Takes reader lock to keep the contract of multi reader single writer mutual exclusion
        //the read lock will be released by the iterator when it is closed (Flusher thread can
        //write to the storage if this lock is not held)
        _lock.readLock().lock();

        long storageSize = _storage.size();
        if (fromIndex < storageSize)
            return new AsyncReadOnlyIterator(_storage.readOnlyIterator(fromIndex));
        //Skip entire storage and iterate only over the memory, can safely cast to int because
        //unflushed data can not be more than int
        return new AsyncReadOnlyIterator((int) (fromIndex - storageSize));
    }

    public List<T> removeFirstBatch(int batchSize)
            throws StorageException {
        _lock.writeLock().lock();
        try {
            List<T> batch = _storage.removeFirstBatch(batchSize);
            int storageBatchSize = batch.size();

            assert (storageBatchSize <= batchSize) : "Removed a batch from storage which is larger than requested (" + storageBatchSize + "/" + batchSize + ")";

            if (storageBatchSize < batchSize) {
                int remaining = batchSize - storageBatchSize;
                //Protect unflushed packets because the flusher thread can asynchronously
                //flush the packets to the storage
                synchronized (_buffer) {
                    int bufferSize = _buffer.size();
                    for (int i = 0; i < Math.min(bufferSize, remaining); ++i)
                        batch.add(_buffer.removeFirst());
                }
            }
            return batch;
        } finally {
            _lock.writeLock().unlock();
        }
    }

    public long size() throws StorageException {
        _lock.readLock().lock();
        try {
            return _storage.size() + _buffer.size();
        } finally {
            _lock.readLock().unlock();
        }
    }

    private void initFlusherIfNeeder() {
        if (_fluser == null)
            synchronized (this) {
                if (_fluser == null) {
                    _fluser = new Flusher();
                    _fluser.start();
                }
            }

    }

    private class Flusher extends GSThread {
        public Flusher() {
            super("AsyncRedoLogFileStorage-Flusher");
        }

        ArrayList<T> _flushBuffer = new ArrayList<T>();

        @Override
        public void run() {
            while (true) {
                //Protect from concurrent additions to the _unflushedPackets
                synchronized (_buffer) {
                    if (!_buffer.isEmpty()) {
                        int unflushedSize = _buffer.size();
                        for (int i = 0; i < Math.min(_maxFlushSize, unflushedSize); ++i) {
                            T packet = _buffer.removeFirst();
                            _flushBuffer.add(packet);
                        }
                    }
                }
                //If there are packets to flush
                if (!_flushBuffer.isEmpty()) {
                    _lock.writeLock().lock();
                    try {
                        try {
                            //Flush to storage
                            _storage.appendBatch(_flushBuffer);
                            _flushBuffer.clear();
                        } catch (StorageFullException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (StorageException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    } finally {
                        _lock.writeLock().unlock();
                    }
                }
                try {
                    //Go to sleep
                    Thread.sleep(_interval);
                } catch (InterruptedException e) {
                    //Break this thread
                    break;
                }
            }
        }

    }


    private class AsyncReadOnlyIterator
            implements StorageReadOnlyIterator<T> {
        private final StorageReadOnlyIterator<T> _storageIterator;
        private boolean _storageIteratorExhausted;
        private Iterator<T> _bufferIterator;
        private volatile boolean _releasedLock;

        public AsyncReadOnlyIterator(
                StorageReadOnlyIterator<T> readOnlyIterator) {
            this._storageIterator = readOnlyIterator;
        }

        public AsyncReadOnlyIterator(int fromIndex) {
            _storageIteratorExhausted = true;
            _storageIterator = null;
            _bufferIterator = _buffer.iterator();
            for (int i = 0; i < fromIndex; ++i) {
                if (_bufferIterator.hasNext())
                    _bufferIterator.next();
                else
                    break;
            }
        }

        public void close() throws StorageException {
            if (_storageIterator != null)
                _storage.close();
            releaseLock();
        }

        public boolean hasNext() throws StorageException {
            if (!_storageIteratorExhausted) {
                _storageIteratorExhausted = !_storageIterator.hasNext();
                if (!_storageIteratorExhausted)
                    return true;
            }

            //If here, external iterator is exhausted
            if (_bufferIterator == null)
                //Create iterator over external storage
                _bufferIterator = _buffer.iterator();

            boolean hasNext = _bufferIterator.hasNext();
            if (!hasNext)
                releaseLock();
            return hasNext;
        }

        public T next() throws StorageException {
            if (!_storageIteratorExhausted) {
                try {
                    return _storageIterator.next();
                } catch (NoSuchElementException e) {
                    _storageIteratorExhausted = true;
                }
            }
            //If here, external iterator is exhausted
            if (_bufferIterator == null)
                //Create iterator over external storage
                _bufferIterator = _buffer.iterator();

            return _bufferIterator.next();
        }

        @Override
        protected void finalize() throws Throwable {
            releaseLock();
        }

        private void releaseLock() {
            if (!_releasedLock) {
                synchronized (this) {
                    if (!_releasedLock) {
                        _lock.readLock().unlock();
                        _releasedLock = true;
                    }
                }
            }
        }
    }

}
