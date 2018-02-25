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

package com.gigaspaces.internal.server.space.redolog.storage.bytebuffer;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A single segment inside a {@link ByteBufferRedoLogFileStorage}
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class StorageSegment {
    private final static Index START_SEGMENT = new Index(0, 0);

    private final SegmentCursor _cursor;
    private final IByteBufferStorage _storage;
    private final IStorageSegmentsMediator _segmentsMediator;
    private long _numberOfPackets;
    private int _deletedPacketsCount;
    private long _weight;
    private long _lastKeyInSegment;

    private final ArrayList<Index> _indexes = new ArrayList<Index>();

    private Index _lastIndex = START_SEGMENT;
    private boolean _sealedWriter = false;
    private long _writerPosition;
    private int _unindexedLength;
    private int _unindexedPacketsCount;

    public StorageSegment(IByteBufferStorageFactory storageProvider, IStorageSegmentsMediator segmentsMediator, int maxReaders) throws ByteBufferStorageException {
        _segmentsMediator = segmentsMediator;
        _storage = storageProvider.createStorage();
        _cursor = new SegmentCursor(_storage.getCursor());
    }


    public String getName() {
        return _storage.getName();
    }

    public SegmentCursor getCursorForWriting() {
        if (_sealedWriter)
            throw new IllegalStateException("Cannot request cursor for writing after segment has been sealed");
        _cursor.setAsWriter();
        return _cursor;
    }

    public void sealWriter() {
        _sealedWriter = true;
    }

    public SegmentCursor getCursorForReading() throws ByteBufferStorageException {
        //Acquire to avoid from being closed while working due to open readers limit
        _cursor.acquire();
        //Might been closed because readers limit have been reached
        try {
            _cursor.initIfNeeded();
        } catch (Exception e) {
            _cursor.release();
            if (e instanceof ByteBufferStorageException)
                throw (ByteBufferStorageException) e;
            throw new ByteBufferStorageException("error creating cursor", e);
        }
        return _cursor;
    }

    public void clear() throws ByteBufferStorageException {
        _storage.clear();
        _numberOfPackets = 0;
        _deletedPacketsCount = 0;
        _writerPosition = 0;
        _indexes.clear();
        _unindexedLength = 0;
        _unindexedPacketsCount = 0;
        _weight = 0;
        _lastIndex = START_SEGMENT;
    }

    public long getPosition() {
        return _writerPosition;
    }

    public void increaseNumOfPackets(long numOfPackets) {
        _numberOfPackets += numOfPackets;
    }

    public long getNumOfPackets() {
        return _numberOfPackets;
    }

    public int getNumOfDeletedPackets() {
        return _deletedPacketsCount;
    }

    public void delete() {
        _storage.close();
    }

    public void decreaseNumOfPackets(long numOfPackets) {
        _deletedPacketsCount += numOfPackets;
        _numberOfPackets -= numOfPackets;
    }

    public void setUnindexedState(int unindexedLength, int unindexedPacketsCount) {
        this._unindexedLength = unindexedLength;
        this._unindexedPacketsCount = unindexedPacketsCount;
    }

    public int getUnindexedLength() {
        return _unindexedLength;
    }

    public int getUnindexedPackets() {
        return _unindexedPacketsCount;
    }

    public void addIndex(int packetCountFromLastIndex, long distanceFromLastIndex) {
        Index newIndex = new Index(_lastIndex.getPacketCount() + packetCountFromLastIndex, _lastIndex.getPosition() + distanceFromLastIndex);
        _indexes.add(newIndex);
        _lastIndex = newIndex;
    }

    public long adjustReader(IByteBufferStorageCursor reader, long packetPosition) {
        long actualPosition = packetPosition + _deletedPacketsCount;
        Index closestBlockStart = searchIndex(actualPosition);
        reader.setPosition(closestBlockStart.getPosition());
        return actualPosition - closestBlockStart.getPacketCount();
    }

    //This will return closest index position of packet position including deleted packets
    public long getIndexPosition(int packetPosition) {
        long actualPosition = packetPosition;
        Index closestBlockStart = searchIndex(actualPosition);
        return closestBlockStart.getPosition();
    }

    public Index searchIndex(long packetPosition) {
        if (_indexes.isEmpty())
            return START_SEGMENT;
        return binarySearch(packetPosition, 0, _indexes.size() - 1);
    }

    private Index binarySearch(long packetPosition, int startInd, int endInd) {
        //End condition
        if (startInd == endInd) {
            Index index = _indexes.get(startInd);
            if (index.getPacketCount() > packetPosition) {
                if (startInd == 0)
                    return START_SEGMENT;
                return _indexes.get(startInd - 1);
            }
            if (index.getPacketCount() <= packetPosition)
                return index;
        }
        int middleIndex = (endInd - startInd) / 2 + startInd;
        Index index = _indexes.get(middleIndex);
        if (index.getPacketCount() < packetPosition)
            return binarySearch(packetPosition, middleIndex + 1, endInd);
        if (index.getPacketCount() > packetPosition)
            return binarySearch(packetPosition, startInd, middleIndex);
        //found position
        return index;
    }

    public void increaseWeight(long weight){
        _weight += weight;
    }

    public void decreaseWeight(long weight){
        _weight -= weight;
    }

    public long getLastKeyInSegment() {
        return _lastKeyInSegment;
    }

    public void setLastKeyInSegment(long _lastKeyInSegment) {
        this._lastKeyInSegment = _lastKeyInSegment;
    }

    public long getWeight() {
        return _weight;
    }

    private static class Index {
        private final long _packetCount;
        private final long _position;

        public Index(long packetCount, long position) {
            _packetCount = packetCount;
            _position = position;
        }

        public long getPacketCount() {
            return _packetCount;
        }

        public long getPosition() {
            return _position;
        }

        @Override
        public String toString() {
            return "Index[packetCount=" + _packetCount + ", position=" + _position + "]";
        }
    }

    public class SegmentCursor implements IByteBufferStorageCursor {
        private IByteBufferStorageCursor _storageCursor;
        private final Lock _lock = new ReentrantLock();
        private volatile boolean _actAsWriter;

        public SegmentCursor(IByteBufferStorageCursor cursor) {
            this._storageCursor = cursor;
        }

        public void setAsWriter() {
            _actAsWriter = true;
            _storageCursor.setPosition(_writerPosition);
        }

        public boolean tryAcquire() {
            return _lock.tryLock();
        }

        public void acquire() {
            _lock.lock();
        }

        public void initIfNeeded() throws ByteBufferStorageException {
            if (_storageCursor == null) {
                _segmentsMediator.allocatedNewResource(this);
                _storageCursor = _storage.getCursor();
            }
        }

        public void release() {
            //If writer, keep the position
            if (_actAsWriter) {
                try {
                    _writerPosition = _storageCursor.getPosition();
                    if (_sealedWriter)
                        clearStorageCursor();
                } finally {
                    _actAsWriter = false;
                }
            } else {
                _lock.unlock();
            }
        }

        public void close() {
            _storageCursor.close();
        }

        public long getPosition() {
            return _storageCursor.getPosition();
        }

        public void movePosition(long offset) {
            _storageCursor.movePosition(offset);
        }

        public byte readByte() {
            return _storageCursor.readByte();
        }

        public void readBytes(byte[] result, int offset, int length) {
            _storageCursor.readBytes(result, offset, length);
        }

        public int readInt() {
            return _storageCursor.readInt();
        }

        public long readLong() {
            return _storageCursor.readLong();
        }

        public void setPosition(long position) {
            _storageCursor.setPosition(position);
        }

        public void writeByte(byte value) {
            _storageCursor.writeByte(value);
        }

        public void writeBytes(byte[] array, int offset, int length) {
            _storageCursor.writeBytes(array, offset, length);
        }

        public void writeInt(int value) {
            _storageCursor.writeInt(value);
        }

        public void writeLong(long value) {
            _storageCursor.writeLong(value);
        }

        public boolean isSegmentSealedForWriting() {
            return _sealedWriter;
        }

        //Flush to main memory (just for safety)
        public synchronized void clearStorageCursor() {
            _storageCursor.close();
            _storageCursor = null;
        }
    }

}
