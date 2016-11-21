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

import com.gigaspaces.internal.server.space.redolog.storage.IRedoLogFileStorage;
import com.gigaspaces.internal.server.space.redolog.storage.StorageException;
import com.gigaspaces.internal.server.space.redolog.storage.StorageFullException;
import com.gigaspaces.internal.server.space.redolog.storage.StorageReadOnlyIterator;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.StorageSegment.SegmentCursor;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.TraceableLogger;
import com.j_spaces.kernel.pool.IResourceFactory;
import com.j_spaces.kernel.pool.IResourcePool;
import com.j_spaces.kernel.pool.Resource;
import com.j_spaces.kernel.pool.ResourcePool;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of redo log file storage which is based on an underlying byte buffer storages
 * provider which simulated an endless byte buffer
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ByteBufferRedoLogFileStorage<T>
        implements IRedoLogFileStorage<T>, IStorageSegmentsMediator {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);
    private static final TraceableLogger _traceableLogger = TraceableLogger.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);

    private final StorageReadOnlyIterator<T> EMPTY_ITERATOR = new EmptyStorageReadOnlyIterator<T>();
    private static final int END_OF_PACKETS_MARKER = -1;
    private static final int END_OF_SEGMENT_MARKER = -2;

    private final IByteBufferStorageFactory _byteBufferStorageProvider;
    private final PacketSerializer<T> _packetSerializer;
    private final IResourcePool<ByteArrayResource> _byteArrayPool = new ResourcePool<ByteArrayResource>(new IResourceFactory<ByteArrayResource>() {

        public ByteArrayResource allocate() {
            return new ByteArrayResource(1024);
        }

    }, 0, 128);

    private final LinkedList<StorageSegment> _segments = new LinkedList<StorageSegment>();
    private final LinkedList<SegmentCursor> _allocatedReaders = new LinkedList<SegmentCursor>();
    private final int _maxCursors;

    private ByteBuffer _writerBuffer;
    private final long _maxSwapSize;
    private final long _maxSizePerSegment;
    private final int _maxWriteBufferSize;
    private final int _maxScanLength;

    private boolean _initialized = false;
    private long _dataStartPos = 0;
    private long _size;
    private static final int INTEGER_SERIALIZE_LENGTH = 4;
    private static final int PACKET_PROTOCOL_OVERHEAD = 1 * INTEGER_SERIALIZE_LENGTH;


    public ByteBufferRedoLogFileStorage(IByteBufferStorageFactory byteBufferStorageProvider) {
        this(byteBufferStorageProvider, new ByteBufferRedoLogFileConfig<T>());
    }

    public ByteBufferRedoLogFileStorage(IByteBufferStorageFactory byteBufferStorageProvider, ByteBufferRedoLogFileConfig<T> config) {
        this._byteBufferStorageProvider = byteBufferStorageProvider;
        this._maxWriteBufferSize = config.getWriterMaxBufferSize();
        this._maxSwapSize = config.getMaxSwapSize();
        this._maxSizePerSegment = config.getMaxSizePerSegment();
        this._maxScanLength = config.getMaxScanLength();
        this._maxCursors = config.getMaxOpenStorageCursors() - 1; //There's always an open cursor at the last segment for writing
        this._writerBuffer = ByteBuffer.allocate(config.getWriterBufferSize());
        this._packetSerializer = new PacketSerializer<T>(config.getPacketStreamSerializer());

        if (_logger.isLoggable(Level.CONFIG)) {
            _logger.config("ByteBufferRedoLogFileStorage created:"
                    + "\n\tmaxWriteBufferSize = " + (config.getWriterMaxBufferSize() / 1024) + "kb"
                    + "\n\tmaxSwapSize = " + (config.getMaxSwapSize() == -1 ? "UNLIMITED" : (_maxSwapSize / (1024 * 1024)) + "mb")
                    + "\n\tmaxSizePerSegment = " + (config.getMaxSizePerSegment() / (1024 * 1024)) + "mb"
                    + "\n\tmaxScanLength = " + (config.getMaxScanLength() / 1024) + "kb"
                    + "\n\tmaxCursors = " + config.getMaxOpenStorageCursors());
        }
    }

    public void appendBatch(List<T> replicationPackets) throws StorageException, StorageFullException {
        initIfNeeded();
        StorageSegment initLastSegment = getLastSegment();
        //Get current segment position which indicates its size
        long segmentSizeAfterPacket = initLastSegment.getPosition() - INTEGER_SERIALIZE_LENGTH /*the position will be moved by -4 once start to write*/;
        SegmentCursor writer = null;
        boolean firstPacket = true;
        int segmentAddedPackets = 0;
        int writtenPackets = 0;
        int unindexedLength = initLastSegment.getUnindexedLength();
        int unindexedPacketsCount = initLastSegment.getUnindexedPackets();

        try {
            for (T packet : replicationPackets) {
                ByteBuffer bufferedPacket = _packetSerializer.serializePacket(packet);

                int packetSerializeLength = bufferedPacket.remaining() + PACKET_PROTOCOL_OVERHEAD /*over head per packet*/;
                segmentSizeAfterPacket += packetSerializeLength;
                boolean hasRemaining = segmentSizeAfterPacket <= _maxSizePerSegment;
                if (!hasRemaining) {
                    //Size exceeded, flush current content and create new segment
                    StorageSegment currentLastSegment = getLastSegment();
                    //if writer is null no packets have been written so far{
                    if (writer == null) {
                        writer = currentLastSegment.getCursorForWriting();
                        writer.movePosition(-INTEGER_SERIALIZE_LENGTH);
                    }
                    //Mark end of segment
                    flushWriterBufferToStorage(writer);
                    markEndOfSegment(writer);
                    //Increase number of packets stored in segment
                    currentLastSegment.increaseNumOfPackets(segmentAddedPackets);
                    //Create new segment
                    createNewSegment();
                    StorageSegment lastSegment = getLastSegment();
                    segmentSizeAfterPacket = packetSerializeLength;
                    segmentAddedPackets = 0;
                    unindexedLength = 0;
                    unindexedPacketsCount = 0;
                    //We can seal this segment writer, because this segment will not contain anymore packets
                    currentLastSegment.sealWriter();
                    if (_logger.isLoggable(Level.FINEST))
                        _logger.finest("segment sealed, a new segment created, current segment count is " + _segments.size());
                    //Make sure we do not double release this writer if an exception occurd at the release operation
                    final SegmentCursor tempWriter = writer;
                    writer = null;
                    tempWriter.release();
                    //Update writer                        
                    writer = lastSegment.getCursorForWriting();
                }

                if (firstPacket) {
                    //Could be that the first packet caused a new segment to be created
                    if (writer == null) {
                        writer = getLastSegment().getCursorForWriting();
                        writer.movePosition(-INTEGER_SERIALIZE_LENGTH);
                    }
                    firstPacket = false;
                }
                boolean wasAppended = appendPacketToWriterBuffer(bufferedPacket);
                if (!wasAppended) {
                    //Could not append because reached max size, flush current content.
                    flushWriterBufferToStorage(writer);
                    writePacketByteBufferToStorage(writer, bufferedPacket);
                }
                //Increase number of packets added to the current segment
                segmentAddedPackets++;
                writtenPackets++;
                //Keep track of unindexed length
                unindexedLength += packetSerializeLength;
                unindexedPacketsCount++;
                if (unindexedLength >= _maxScanLength) {
                    getLastSegment().addIndex(unindexedPacketsCount, unindexedLength);
                    unindexedPacketsCount = 0;
                    unindexedLength = 0;
                }
            }

            //If remaining is less then capacity, it means buffer has bending packets to flush
            if (_writerBuffer.remaining() < _writerBuffer.capacity())
                flushWriterBufferToStorage(writer);

            //Increase number of packets in last segment
            getLastSegment().increaseNumOfPackets(segmentAddedPackets);
            getLastSegment().setUnindexedState(unindexedLength, unindexedPacketsCount);

            markEndOfPackets(writer);
        } catch (StorageFullException e) {
            //If storage was full on first packet, the writer was not set and no data was written
            if (writer != null) {
                writer.movePosition(-INTEGER_SERIALIZE_LENGTH);
                //We want to leave the storage not corrupted, mark end
                markEndOfPackets(writer);
            }
            //Fill denied packets
            ArrayList<T> deniedPackets = new ArrayList<T>();
            for (int i = writtenPackets; i < replicationPackets.size(); ++i)
                deniedPackets.add(replicationPackets.get(i));
            if (_logger.isLoggable(Level.FINE))
                _logger.fine("Storage is full, denied " + deniedPackets.size() + " packets");
            throw new StorageFullException(e.getMessage(), e.getCause(), deniedPackets);
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE))
                _logger.log(Level.SEVERE, "error appending replications packets to storage", e);
            throw new StorageException("error appending replications packets to storage", e);
        } finally {
            if (writer != null)
                writer.release();
            _size += writtenPackets;
        }

    }


    private void createNewSegment() throws StorageException, StorageFullException {
        StorageSegment newSegment;
        try {
            if (_maxSwapSize != ByteBufferRedoLogFileConfig.UNLIMITED) {
                long swapSize = _segments.size() * _maxSizePerSegment;
                //Next segment can breach the size
                if (swapSize + _maxSizePerSegment > _maxSwapSize)
                    throw new StorageFullException("storage is full", null);
            }
            newSegment = new StorageSegment(_byteBufferStorageProvider, this, _maxCursors);
        } catch (ByteBufferStorageException e) {
            throw new StorageException("error while creating a new segment", e);
        }
        _segments.addLast(newSegment);
    }

    private StorageSegment getLastSegment() {
        return _segments.getLast();
    }

    private void flushWriterBufferToStorage(IByteBufferStorageCursor writer) {
        _writerBuffer.limit(_writerBuffer.position());
        _writerBuffer.position(0);
        writer.writeBytes(_writerBuffer.array(), 0, _writerBuffer.limit());
        rewindWriterBuffer();
    }

    private boolean appendPacketToWriterBuffer(ByteBuffer buffer) {
        final int packetLength = buffer.remaining();
        //Need to resize the buffer
        if (_writerBuffer.remaining() < packetLength + PACKET_PROTOCOL_OVERHEAD /*keep space for length*/) {
            int newCapacity = (int) ((_writerBuffer.capacity() + packetLength + PACKET_PROTOCOL_OVERHEAD) * 1.5);
            if (newCapacity > _maxWriteBufferSize)
                return false;
            ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
            newBuffer.limit(newBuffer.capacity());
            _writerBuffer.limit(_writerBuffer.position());
            _writerBuffer.position(0);
            newBuffer.put(_writerBuffer);
            _writerBuffer = newBuffer;
        }
        //Write packet length
        _writerBuffer.putInt(packetLength);
        _writerBuffer.put(buffer);
        return true;
    }

    private void rewindWriterBuffer() {
        _writerBuffer.rewind();
        _writerBuffer.limit(_writerBuffer.capacity());
    }

    public void deleteFirstBatch(long batchSize) throws StorageException {
        if (!_initialized)
            return;
        //First delete entire possible segments
        long remainingBatch = deleteEntireSegments(batchSize);
        if (remainingBatch <= 0)
            return;
        SegmentCursor reader = null;
        try {
            //Init arguments, start from first segment            
            StorageSegment currentSegment = getFirstSegment();
            reader = currentSegment.getCursorForReading();
            reader.setPosition(_dataStartPos);

            for (int i = 0; i < remainingBatch; ++i) {
                int lengthOfCurrentPacket = reader.readInt();
                reader.movePosition(lengthOfCurrentPacket);
            }
            _size -= Math.min(remainingBatch, _size);
            //Move start position after deleted elements
            _dataStartPos = reader.getPosition();
            currentSegment.decreaseNumOfPackets(remainingBatch);
        } catch (ByteBufferStorageException e) {
            throw new StorageException("error when deleting first batch from storage", e);
        } finally {
            if (reader != null)
                reader.release();
        }
    }

    private StorageSegment getFirstSegment() {
        return _segments.getFirst();
    }

    private long deleteEntireSegments(long batchSize) throws StorageException {
        int numberOfSegments = _segments.size();
        int currentSegmentCounter = 0;
        int deletedSegments = 0;
        long remaining = batchSize;
        for (StorageSegment segment : _segments) {
            currentSegmentCounter++;
            long numOfPackets = segment.getNumOfPackets();
            //Delete this segment because it has less packets than needed to delete
            if (remaining >= numOfPackets) {
                //If not the last segment
                if (currentSegmentCounter < numberOfSegments) {
                    if (_logger.isLoggable(Level.FINEST))
                        _logger.finest("deleted segment " + currentSegmentCounter);
                    segment.delete();
                    remaining -= numOfPackets;
                    deletedSegments++;
                    _size -= numOfPackets;
                    _dataStartPos = 0;
                } else {
                    //last segment we only clear and not delete
                    try {
                        clearStorageAndReset();
                    } catch (ByteBufferStorageException e) {
                        throw new StorageException("error clearing storage", e);
                    }
                    _size = 0;
                    remaining = 0;
                }
            } else
                break;
        }
        removeDeletedSegments(deletedSegments);
        return remaining;
    }

    public List<T> removeFirstBatch(int batchSize) throws StorageException {
        ArrayList<T> result = new ArrayList<T>();
        if (_initialized) {
            SegmentCursor reader = null;
            try {
                //Init arguments, start from first segment
                Iterator<StorageSegment> segments = _segments.iterator();
                StorageSegment currentSegment = segments.next();
                reader = currentSegment.getCursorForReading();
                reader.setPosition(_dataStartPos);
                int removedFromCurrentSegment = 0;
                int numberOfDeletedSegments = 0;

                for (int i = 0; i < batchSize; ++i) {
                    //Read packet
                    T packet = readSinglePacketFromStorage(reader);
                    //End of current segment
                    if (packet == null) {
                        //End of segment reached, we can delete it
                        if (segments.hasNext()) {
                            reader.release();
                            currentSegment.delete();
                            if (_logger.isLoggable(Level.FINEST))
                                _logger.finest("deleted segment " + numberOfDeletedSegments);
                            numberOfDeletedSegments++;
                            currentSegment = segments.next();
                            removedFromCurrentSegment = 0;
                            //Move reader to next segment and set its position
                            reader = currentSegment.getCursorForReading();
                            reader.setPosition(0);
                            //Read packet again
                            packet = readSinglePacketFromStorage(reader);
                            //Packet can't be null here, it points to empty last storage
                        } else {
                            //This was the last segment
                            break;
                        }
                    }
                    removedFromCurrentSegment++;
                    result.add(packet);
                }

                currentSegment.decreaseNumOfPackets(removedFromCurrentSegment);
                //Fix start position
                _dataStartPos = reader.getPosition();
                //Check if last segment should be deleted
                if (0 == currentSegment.getNumOfPackets()) {
                    //This is not the last segment
                    if (numberOfDeletedSegments < _segments.size() - 1) {
                        numberOfDeletedSegments++;
                        reader.release();
                        reader = null;
                        currentSegment.delete();
                        _dataStartPos = 0;
                    }
                    //This is last segment
                    else {
                        reader.release();
                        reader = null;
                        clearStorageAndReset();
                    }
                }
                _size -= result.size();
                removeDeletedSegments(numberOfDeletedSegments);
            } catch (IOException e) {
                throw new StorageException("error when removing first batch from storage", e);
            } catch (ClassNotFoundException e) {
                throw new StorageException("error when removing first batch from storage", e);
            } catch (ByteBufferStorageException e) {
                throw new StorageException("error when removing first batch from storage", e);
            } finally {
                if (reader != null)
                    reader.release();
            }
        }
        return result;
    }

    public StorageReadOnlyIterator<T> readOnlyIterator() throws StorageException {
        return createReadOnlyIterator(0);
    }

    public StorageReadOnlyIterator<T> readOnlyIterator(long fromIndex) throws StorageException {
        return createReadOnlyIterator(fromIndex);
    }

    public long size() throws StorageException {
        return _size;
    }

    public boolean isEmpty() throws StorageException {
        return size() == 0;
    }

    public long getSpaceUsed() {
        int segmentCount = _segments.size();
        if (segmentCount == 0)
            return 0;

        return (segmentCount - 1) * _maxSizePerSegment + getLastSegment().getPosition();
    }

    public long getExternalPacketsCount() {
        return _size;
    }

    public long getMemoryPacketsCount() {
        return 0; //no packets held in memory at this layer
    }

    public synchronized void close() {
        for (StorageSegment segment : _segments)
            segment.delete();
        _segments.clear();
        _initialized = false;
        _dataStartPos = 0;
        _size = 0;
        if (_logger.isLoggable(Level.FINE))
            _logger.fine("closed storage");
    }

    @Override
    public long getWeight() {
        try {
            return size();
            //TODO: keep real weight (not size), phase : getWeight() from disk
        } catch (StorageException e) {
            throw new IllegalStateException("failed to get weight from ByteBufferRedoLogFileStorage", e);
        }
    }


    private void initIfNeeded() throws StorageException {
        if (!_initialized) {
            synchronized (this) {
                if (!_initialized) {
                    SegmentCursor writer = null;
                    try {
                        StorageSegment firstSegment = new StorageSegment(_byteBufferStorageProvider, this, _maxCursors);
                        writer = firstSegment.getCursorForWriting();
                        //Put size in start of stream
                        markEndOfPackets(writer);
                        _segments.add(firstSegment);
                        _initialized = true;
                        if (_logger.isLoggable(Level.FINE))
                            _logger.fine("disk is used for the first time, created first storage segment");
                    } catch (ByteBufferStorageException e) {
                        throw new StorageException(e);
                    } finally {
                        if (writer != null)
                            writer.release();
                    }
                }
            }
        }

    }

    private void removeDeletedSegments(int numberOfDeletedSegments) {
        for (int i = 0; i < numberOfDeletedSegments; ++i)
            _segments.removeFirst();
    }

    private void writePacketByteBufferToStorage(IByteBufferStorageCursor writer, ByteBuffer bufferedPacket) {
        int length = bufferedPacket.remaining();
        writer.writeInt(length);
        writer.writeBytes(bufferedPacket.array(), 0, bufferedPacket.limit());
    }

    private void clearStorageAndReset() throws ByteBufferStorageException {
        StorageSegment segment = getLastSegment();
        segment.clear();
        _dataStartPos = 0;
        if (_logger.isLoggable(Level.FINEST))
            _logger.finest("first segments cleared");
        SegmentCursor writer = segment.getCursorForWriting();
        try {
            markEndOfPackets(writer);
        } finally {
            writer.release();
        }

    }

    private ByteArrayResource readBytesFromStorage(IByteBufferStorageCursor reader) {
        int length = reader.readInt();
        if (length == END_OF_PACKETS_MARKER)
            return null;
        if (length == END_OF_SEGMENT_MARKER)
            return null;
        ByteArrayResource byteArrayResource = getByteArrayResource(length);
        reader.readBytes(byteArrayResource.array(), 0, length);
        return byteArrayResource;
    }

    private ByteArrayResource getByteArrayResource(int length) {
        ByteArrayResource resource = _byteArrayPool.getResource();
        resource.ensureCapacity(length);
        return resource;
    }

    private void markEndOfPackets(IByteBufferStorageCursor writer) {
        writer.writeInt(END_OF_PACKETS_MARKER);
    }

    private void markEndOfSegment(IByteBufferStorageCursor writer) {
        writer.writeInt(END_OF_SEGMENT_MARKER);
    }

    private StorageReadOnlyIterator<T> createReadOnlyIterator(
            long fromIndex) throws StorageException {
        if (!_initialized)
            return EMPTY_ITERATOR;

        if (fromIndex >= _size)
            return EMPTY_ITERATOR;

        try {
            return new ByteBufferReadOnlyIterator(fromIndex);
        } catch (ByteBufferStorageException e) {
            throw new StorageException("error creating a readonly iterator over the storage", e);
        }
    }

    private T readSinglePacketFromStorage(IByteBufferStorageCursor reader) throws IOException,
            ClassNotFoundException {
        ByteArrayResource serializedPacket = readBytesFromStorage(reader);
        if (serializedPacket == null)
            return null;
        try {

            return _packetSerializer.deserializePacket(serializedPacket.array());
        } finally {
            serializedPacket.release();
        }
    }

    public synchronized void allocatedNewResource(SegmentCursor resource) {
        if (_allocatedReaders.size() >= _maxCursors) {
            //Attempt to clear the storage reader of the older created reader
            Iterator<SegmentCursor> readers = _allocatedReaders.iterator();
            while (readers.hasNext()) {
                SegmentCursor reader = readers.next();
                //Found free oldest reader
                if (reader.isSegmentSealedForWriting()) {
                    if (reader.tryAcquire()) {
                        try {
                            //Closed the underlying storage reader
                            reader.clearStorageCursor();
                            break;
                        } finally {
                            //Remove it from the list
                            readers.remove();
                            //Release the resource, next time it is acquired it will
                            //create a new storage reader
                            reader.release();
                        }
                    }
                }
            }
        }

        _allocatedReaders.addLast(resource);
    }

    /**
     * Performs an integrity check that verifies the storage state and data is not compromised
     */
    public void validateIntegrity() throws ByteBufferStorageCompromisedException {
        synchronized (this) {
            if (!_initialized)
                return;
        }
        try {
            SegmentCursor reader = null;
            int segmentIndex = 0;
            int packetCount = 0;
            long unindexedLength = 0;
            int packetInSegment = 0;
            long position = 0;

            //Validate integrity objectively (using built in protocol knowledge and not
            //other methods)
            try {
                StorageSegment currentSegment = getFirstSegment();
                reader = currentSegment.getCursorForReading();
                reader.setPosition(0);
                boolean firstIteration = true;
                while (true) {
                    position = reader.getPosition();
                    if (_traceableLogger.isLoggable(Level.FINEST))
                        _traceableLogger.finest("start index integrity iteration [" + packetCount + "], position=" + position + " segment index [" + segmentIndex + "] in segment packet index [" + packetInSegment + "] unindexed length [" + unindexedLength + "]");
                    //Validate index integrity
                    if (!firstIteration && unindexedLength >= _maxScanLength) {
                        long indexPos = currentSegment.getIndexPosition(packetInSegment);
                        if (_traceableLogger.isLoggable(Level.FINEST))
                            _traceableLogger.finest("index position=" + indexPos + " reader position=" + position);
                        if (indexPos != reader.getPosition())
                            throw new ByteBufferStorageCompromisedException("Error while checking index integrity, segment index [" + segmentIndex + "]" +
                                    " packet index [" + packetCount + "] in segment packet index [" + packetInSegment + "] expected index [" + reader.getPosition() + "] actual [" + indexPos + "]");

                        unindexedLength = 0;
                    }
                    firstIteration = false;
                    int length = reader.readInt();
                    if (_traceableLogger.isLoggable(Level.FINEST))
                        _traceableLogger.finest("next packet length=" + length);
                    //End of redo log file
                    if (length == END_OF_PACKETS_MARKER) {
                        if (_traceableLogger.isLoggable(Level.FINEST))
                            _traceableLogger.finest("reached end of packets marker");
                        break;
                    }
                    if (length == END_OF_SEGMENT_MARKER) {
                        if (_traceableLogger.isLoggable(Level.FINEST))
                            _traceableLogger.finest("reached end of segment marker");
                        //No double release on exception
                        SegmentCursor tempReader = reader;
                        reader = null;
                        if (_traceableLogger.isLoggable(Level.FINEST))
                            _traceableLogger.finest("releasing reader");
                        tempReader.release();
                        //Move to next segment
                        currentSegment = _segments.get(++segmentIndex);
                        reader = currentSegment.getCursorForReading();
                        reader.setPosition(0);
                        position = 0;
                        packetInSegment = 0;
                        unindexedLength = 0;
                        if (_traceableLogger.isLoggable(Level.FINEST))
                            _traceableLogger.finest("moved to next segment");
                        continue;
                    }

                    byte[] packetBuffer = new byte[length];
                    reader.readBytes(packetBuffer, 0, length);
                    //Deserialize packet to check stream not corrupted
                    if (_traceableLogger.isLoggable(Level.FINEST))
                        _traceableLogger.finest("deserializing packet");
                    T deserializePacket = _packetSerializer.deserializePacket(packetBuffer);
                    if (deserializePacket == null)
                        throw new ByteBufferStorageCompromisedException("Error while checking data integrity, packet at index " + packetCount + " is null");
                    if (packetInSegment >= currentSegment.getNumOfDeletedPackets())
                        packetCount++;
                    packetInSegment++;
                    unindexedLength += reader.getPosition() - position;
                }
                if (_traceableLogger.isLoggable(Level.FINEST))
                    _traceableLogger.finest("done index integrity check, releasing last reader");
                //Release last reader, upon exception make sure we do not release it twice
                SegmentCursor tempReader = reader;
                reader = null;
                if (tempReader != null)
                    tempReader.release();
            } catch (Exception e) {
                if (reader != null)
                    try {
                        reader.release();
                    } catch (Exception ei) {
                        throw new ByteBufferStorageCompromisedException("Error while checking data integrity, reached packet index " + packetCount
                                + " additionally exception occurred when releasing the reader " + ei.toString(), e);
                    }
                if (e instanceof ByteBufferStorageCompromisedException)
                    throw (ByteBufferStorageCompromisedException) e;
                throw new ByteBufferStorageCompromisedException("Error while checking data integrity, reached packet index " + packetCount, e);
            }

            if (_traceableLogger.isLoggable(Level.FINEST))
                _traceableLogger.finest("validating size integrity, expected size=" + packetCount + " known size=" + _size);

            //Validate size integrity
            if (packetCount != _size)
                throw new ByteBufferStorageCompromisedException("Calculated size [" + packetCount + "] does not match kept size [" + _size + "]");

            if (_traceableLogger.isLoggable(Level.FINEST))
                _traceableLogger.finest("perform one entire scan using iterator");
            StorageReadOnlyIterator<T> iterator = null;
            int count = 0;
            //Perform one entire scan using iterator
            try {
                iterator = readOnlyIterator();

                while (iterator.hasNext()) {
                    if (iterator.next() == null)
                        throw new ByteBufferStorageCompromisedException("Error while checking full iteration indexes, at packet index " + count + " iterator returned null packet");
                    if (_traceableLogger.isLoggable(Level.FINEST))
                        _traceableLogger.finest("iteration count " + count);
                    count++;
                }
                if (_traceableLogger.isLoggable(Level.FINEST))
                    _traceableLogger.finest("done iterator integrity check, closing iterator");
                StorageReadOnlyIterator<T> tempIterator = iterator;
                iterator = null;
                tempIterator.close();
                if (_traceableLogger.isLoggable(Level.FINEST))
                    _traceableLogger.finest("validating iterator size integrity, expected size=" + count + " known size=" + _size);
                //Validate size integrity
                if (count != _size)
                    throw new ByteBufferStorageCompromisedException("Full iteration size [" + packetCount + "] does not match kept size [" + _size + "]");
            } catch (Exception e) {
                if (iterator != null)
                    try {
                        iterator.close();
                    } catch (StorageException ei) {
                        throw new ByteBufferStorageCompromisedException("Error while checking full iteration integrity, at packet index " + count
                                + " additionally an error occurred while closing iterator " + ei.toString(), e);
                    }

                if (e instanceof ByteBufferStorageCompromisedException)
                    throw (ByteBufferStorageCompromisedException) e;
                throw new ByteBufferStorageCompromisedException("Error while checking full iteration integrity, at packet index " + count, e);
            }

            if (_traceableLogger.isLoggable(Level.FINEST))
                _traceableLogger.finest("validating fetch iterator by index integrity");
            //Validate indexes integrity
            iterator = null;
            int i = 0;
            try {
                for (; i < _size; ++i) {
                    iterator = readOnlyIterator(i);
                    if (!iterator.hasNext())
                        throw new ByteBufferStorageCompromisedException("Error while validating indexes, at packet index " + i + " iterator has no next");
                    T packet = iterator.next();
                    if (packet == null)
                        throw new ByteBufferStorageCompromisedException("Error while validating indexes, at packet index " + i + " iterator returned null packet");
                    if (_traceableLogger.isLoggable(Level.FINEST))
                        _traceableLogger.finest("validated fetch iterator by index [" + i + "] integrity done, closing iterator");
                    //Avoid double close on exception
                    StorageReadOnlyIterator<T> tempIterator = iterator;
                    iterator = null;
                    tempIterator.close();
                }
            } catch (Exception e) {
                if (iterator != null)
                    try {
                        iterator.close();
                    } catch (StorageException ei) {
                        throw new ByteBufferStorageCompromisedException("Error while validating indexes, at packet index " + i
                                + " additionally an error occurred while closing iterator " + ei.toString(), e);
                    }
                if (e instanceof ByteBufferStorageCompromisedException)
                    throw (ByteBufferStorageCompromisedException) e;
                throw new ByteBufferStorageCompromisedException("Error while validating indexes, at packet index " + i, e);
            }
        } catch (ByteBufferStorageCompromisedException e) {
            logGeneralStatusOnError();
            if (_traceableLogger.isLoggable(Level.FINEST))
                _traceableLogger.finest(e.getMessage());
            _traceableLogger.showThreadTrace();
            throw e;
        } catch (Exception e) {
            logGeneralStatusOnError();
            if (_traceableLogger.isLoggable(Level.FINEST))
                _traceableLogger.finest(e.getMessage());
            _traceableLogger.showThreadTrace();
            throw new ByteBufferStorageCompromisedException(e);
        } finally {
            _traceableLogger.clearThreadTrace();
        }
    }

    private void logGeneralStatusOnError() {
        if (_logger.isLoggable(Level.SEVERE))
            _logger.severe("integrity validation failed, general state:"
                    + StringUtils.NEW_LINE + "dataStartPos="
                    + _dataStartPos + StringUtils.NEW_LINE + "known size="
                    + _size + StringUtils.NEW_LINE + "number of segments="
                    + _segments.size() + StringUtils.NEW_LINE
                    + "configured segment size=" + _maxSizePerSegment
                    + StringUtils.NEW_LINE + "configured max scan length="
                    + _maxScanLength + StringUtils.NEW_LINE
                    + "segments details:" + StringUtils.NEW_LINE
                    + displaySegmentDetails());
    }

    private String displaySegmentDetails() {
        StringBuilder result = new StringBuilder("segments count=");
        result.append(_segments.size());
        result.append(StringUtils.NEW_LINE);
        int index = 0;
        for (StorageSegment segment : _segments) {
            result.append("segment ");
            result.append(index++);
            result.append(": name=");
            result.append(segment.getName());
            result.append(" held packets=");
            result.append(segment.getNumOfPackets());
            result.append(" deleted packets=");
            result.append(segment.getNumOfDeletedPackets());
            result.append(" position=");
            result.append(segment.getPosition());
            result.append(" unindexed length=");
            result.append(segment.getUnindexedLength());
            result.append(" unindexed packets=");
            result.append(segment.getUnindexedPackets());
            result.append(StringUtils.NEW_LINE);
        }
        return result.toString();
    }

    private class ByteBufferReadOnlyIterator implements StorageReadOnlyIterator<T> {

        private SegmentCursor _reader;
        private T _next;
        private int _segmentIndex = 0;
        private volatile boolean _released = false;

        public ByteBufferReadOnlyIterator(long fromIndex) throws ByteBufferStorageException {
            advanceToPacket(fromIndex);
        }

        private void advanceToPacket(long fromIndex) throws ByteBufferStorageException {
            long firstPacketInSegment = 0;
            long firstPacketInNextSegment = 0;
            StorageSegment startSegment = null;
            //Locate for segment containing requested packet
            for (StorageSegment segment : _segments) {
                firstPacketInNextSegment += segment.getNumOfPackets();
                //Located
                if (fromIndex < firstPacketInNextSegment) {
                    //If first segment start from dataStartPos, otherwise the data start from 0
                    startSegment = segment;
                    break;
                } else {
                    //Advance to next segment
                    firstPacketInSegment = firstPacketInNextSegment;
                    _segmentIndex++;
                }
            }

            try {

                if (startSegment == null) {
                    //Set reader to end of log
                    StorageSegment lastSegment = getLastSegment();
                    _reader = lastSegment.getCursorForReading();
                    _reader.setPosition(lastSegment.getPosition() - INTEGER_SERIALIZE_LENGTH);
                    return;
                }

                _reader = startSegment.getCursorForReading();
                //If start from 0, we start at _dataStartPos
                if (fromIndex == 0)
                    _reader.setPosition(_dataStartPos);
                else {
                    long positionInBlock = startSegment.adjustReader(_reader, fromIndex - firstPacketInSegment);

                    for (long i = 0; i < positionInBlock; ++i) {
                        //advance in storage
                        int length = _reader.readInt();
                        _reader.movePosition(length);
                    }
                }
            } catch (ByteBufferStorageException e) {
                releaseReader();
                throw e;
            } catch (Exception e) {
                releaseReader();
                throw new ByteBufferStorageException(e);
            }

        }

        public boolean hasNext() throws StorageException {
            boolean hasNext = false;
            try {
                if (_reader == null)
                    return false;
                while (true) {
                    _next = readSinglePacketFromStorage(_reader);
                    if (_next != null)
                        break;

                    if (++_segmentIndex >= _segments.size())
                        break;

                    try {
                        //Move to next segment
                        _reader.release();
                        _reader = _segments.get(_segmentIndex).getCursorForReading();
                        _reader.setPosition(0);
                    } catch (ByteBufferStorageException e) {
                        throw new StorageException("error while iterating over the storage", e);
                    }
                }
                hasNext = _next != null;
                return hasNext;
            } catch (Exception e) {
                throw new StorageException("error while iterating over the storage", e);
            } finally {
                if (!hasNext)
                    releaseReader();
            }
        }

        public T next() throws StorageException {
            if (_next == null && !hasNext())
                throw new NoSuchElementException();
            T result = _next;
            _next = null;
            return result;
        }

        @Override
        protected void finalize() throws Throwable {
            releaseReader();
        }

        public void close() throws StorageException {
            releaseReader();
        }

        private void releaseReader() {
            if (!_released) {
                if (_reader != null) {
                    _reader.release();
                    _reader = null;
                }
                _released = true;
            }
        }
    }

    /**
     * A resource for byte array, ensure capacity must be called each time this resource is taken
     *
     * @author eitany
     * @since 7.1
     */
    public class ByteArrayResource extends Resource {
        private SoftReference<byte[]> _bufferRef;
        private byte[] _tempBuffer;

        public ByteArrayResource(int length) {
            _bufferRef = new SoftReference<byte[]>(new byte[length]);
        }

        public void ensureCapacity(int length) {
            //Creates a strong reference to a temp buffer with the specified length
            _tempBuffer = _bufferRef.get();
            if (_tempBuffer == null || _tempBuffer.length < length) {
                _tempBuffer = new byte[length];
                _bufferRef = new SoftReference<byte[]>(_tempBuffer);
            }
        }

        @Override
        public void clear() {
            //Clears the strong reference allowing the buffer to be garbage collected
            _tempBuffer = null;
        }

        /**
         * Must be called first when the resource is acquired only after ensureCapacity was called
         */
        public byte[] array() {
            return _tempBuffer;
        }
    }

}
