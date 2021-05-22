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

package com.j_spaces.core.cluster;

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ByteBufferRedoLogFileConfig;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class SwapBacklogConfig
        implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    final static public int FLUSH_BUFFER_PACKETS_COUNT_DEFAULT = 500;
    final static public int FETCH_BUFFER_PACKETS_COUNT_DEFAULT = 500;
    final static public long SEGMENT_SIZE_DEFAULT = ByteBufferRedoLogFileConfig.DEFAULT_SEGMENT_SIZE;
    final static public int MAX_SCAN_LENGTH_DEFAULT = ByteBufferRedoLogFileConfig.DEFAULT_MAX_SCAN;
    final static public int MAX_OPEN_CURSORS_DEFAULT = ByteBufferRedoLogFileConfig.DEFAULT_MAX_STORAGE_CURSORS;
    final static public int WRITE_BUFFER_SIZE_DEFAULT = ByteBufferRedoLogFileConfig.DEFAULT_MAX_BUFFER_SIZE;

    private int _flushBufferPacketsCount = FLUSH_BUFFER_PACKETS_COUNT_DEFAULT;
    private int _fetchBufferPacketsCount = FETCH_BUFFER_PACKETS_COUNT_DEFAULT;
    private long _segmentSize = SEGMENT_SIZE_DEFAULT;
    private int _maxScanLength = MAX_SCAN_LENGTH_DEFAULT;
    private int _maxOpenCursors = MAX_OPEN_CURSORS_DEFAULT;
    private int _writerBufferSize = WRITE_BUFFER_SIZE_DEFAULT;

    private interface BitMap {
        int FLUSH_BUFFER_PACKETS_COUNT = 1 << 0;
        int FETCH_BUFFER_PACKETS_COUNT = 1 << 1;
        int SEGMENT_SIZE = 1 << 2;
        int MAX_SCAN_LENGTH = 1 << 3;
        int MAX_OPEN_CURSORS = 1 << 4;
        int WRITE_BUFFER_SIZE = 1 << 5;
    }

    public int getFlushBufferPacketsCount() {
        return _flushBufferPacketsCount;
    }

    public void setFlushBufferPacketsCount(int flushBufferPacketsCount) {
        _flushBufferPacketsCount = flushBufferPacketsCount;
    }

    public int getFetchBufferPacketsCount() {
        return _fetchBufferPacketsCount;
    }

    public void setFetchBufferPacketsCount(int fetchBufferPacketsCount) {
        _fetchBufferPacketsCount = fetchBufferPacketsCount;
    }

    public long getSegmentSize() {
        return _segmentSize;
    }

    public void setSegmentSize(long segmentSize) {
        _segmentSize = segmentSize;
    }

    public int getMaxScanLength() {
        return _maxScanLength;
    }

    public void setMaxScanLength(int maxScanLength) {
        _maxScanLength = maxScanLength;
    }

    public int getMaxOpenCursors() {
        return _maxOpenCursors;
    }

    public void setMaxOpenCursors(int maxOpenCursors) {
        _maxOpenCursors = maxOpenCursors;
    }

    public int getWriterBufferSize() {
        return _writerBufferSize;
    }

    public void setWriterBufferSize(int writerBufferSize) {
        _writerBufferSize = writerBufferSize;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        int flags = 0;

        if (_flushBufferPacketsCount != FLUSH_BUFFER_PACKETS_COUNT_DEFAULT)
            flags |= BitMap.FLUSH_BUFFER_PACKETS_COUNT;

        if (_fetchBufferPacketsCount != FETCH_BUFFER_PACKETS_COUNT_DEFAULT)
            flags |= BitMap.FETCH_BUFFER_PACKETS_COUNT;

        if (_segmentSize != SEGMENT_SIZE_DEFAULT)
            flags |= BitMap.SEGMENT_SIZE;

        if (_maxScanLength != MAX_SCAN_LENGTH_DEFAULT)
            flags |= BitMap.MAX_SCAN_LENGTH;

        if (_maxOpenCursors != MAX_OPEN_CURSORS_DEFAULT)
            flags |= BitMap.MAX_OPEN_CURSORS;

        if (_writerBufferSize != WRITE_BUFFER_SIZE_DEFAULT)
            flags |= BitMap.WRITE_BUFFER_SIZE;

        out.writeInt(flags);

        if (_flushBufferPacketsCount != FLUSH_BUFFER_PACKETS_COUNT_DEFAULT)
            out.writeInt(_flushBufferPacketsCount);
        if (_fetchBufferPacketsCount != FETCH_BUFFER_PACKETS_COUNT_DEFAULT)
            out.writeInt(_fetchBufferPacketsCount);
        if (_segmentSize != SEGMENT_SIZE_DEFAULT)
            out.writeLong(_segmentSize);
        if (_maxScanLength != MAX_SCAN_LENGTH_DEFAULT)
            out.writeInt(_maxScanLength);
        if (_maxOpenCursors != MAX_OPEN_CURSORS_DEFAULT)
            out.writeInt(_maxOpenCursors);
        if (_writerBufferSize != WRITE_BUFFER_SIZE_DEFAULT)
            out.writeInt(_writerBufferSize);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        int flags = in.readInt();

        if ((flags & BitMap.FLUSH_BUFFER_PACKETS_COUNT) != 0) {
            _flushBufferPacketsCount = in.readInt();
        } else {
            _flushBufferPacketsCount = FLUSH_BUFFER_PACKETS_COUNT_DEFAULT;
        }
        if ((flags & BitMap.FETCH_BUFFER_PACKETS_COUNT) != 0) {
            _fetchBufferPacketsCount = in.readInt();
        } else {
            _fetchBufferPacketsCount = FETCH_BUFFER_PACKETS_COUNT_DEFAULT;
        }
        if ((flags & BitMap.SEGMENT_SIZE) != 0) {
            _segmentSize = in.readLong();
        } else {
            _segmentSize = SEGMENT_SIZE_DEFAULT;
        }
        if ((flags & BitMap.MAX_SCAN_LENGTH) != 0) {
            _maxScanLength = in.readInt();
        } else {
            _maxScanLength = MAX_SCAN_LENGTH_DEFAULT;
        }
        if ((flags & BitMap.MAX_OPEN_CURSORS) != 0) {
            _maxOpenCursors = in.readInt();
        } else {
            _maxOpenCursors = MAX_OPEN_CURSORS_DEFAULT;
        }
    }

    @Override
    public String toString() {
        return "SwapRedologConfig [_flushBufferPacketsCount="
                + _flushBufferPacketsCount + ", _fetchBufferPacketsCount="
                + _fetchBufferPacketsCount + ", _segmentSize=" + _segmentSize
                + ", _maxScanLength=" + _maxScanLength + ", _maxOpenCursors="
                + _maxOpenCursors + ", _writerBufferSize=" + _writerBufferSize
                + "]";
    }


}
