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

package com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.raf;

import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.ByteBufferStorageException;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.IByteBufferStorage;
import com.gigaspaces.internal.server.space.redolog.storage.bytebuffer.IByteBufferStorageCursor;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * {@link IByteBufferStorage} implementation that is based on {@link RandomAccessFile}
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class RAFByteBufferStorage
        implements IByteBufferStorage {

    private final File _file;
    private volatile Cursor _cursor;
    private volatile boolean _closed;

    public RAFByteBufferStorage(String fileName) throws ByteBufferStorageException {
        try {
            File workLocation = SystemLocations.singleton().work().toFile();
            workLocation.mkdirs();
            File replicationDirName = new File("replication");
            File replicationDirPath = new File(workLocation, replicationDirName.getPath());
            replicationDirPath.mkdirs();
            _file = File.createTempFile(fileName, "tmp", replicationDirPath);
        } catch (IOException e) {
            throw new ByteBufferStorageException("error creating temp file", e);
        }
        try {
            getFile().deleteOnExit();
        } catch (Throwable t) {
            //Do nothing, this can occurr if deleteOnExit is called when the jvm is during shutdown.
        }
    }

    public void clear() throws ByteBufferStorageException {
        if (_cursor == null)
            getCursor();

        _cursor.clear();
    }

    public synchronized void close() {
        if (_cursor != null)
            _cursor.close();
        _cursor = null;
        getFile().delete();
        _closed = true;
    }

    public File getFile() {
        return _file;
    }

    public synchronized IByteBufferStorageCursor getCursor()
            throws ByteBufferStorageException {
        if (_closed)
            throw new RAFByteBufferStorageException("storage is closed");
        if (_cursor == null)
            try {
                _cursor = new Cursor();
            } catch (FileNotFoundException e) {
                throw new ByteBufferStorageException("error creating cursor over the temp file", e);
            }

        return _cursor;
    }

    public String getName() {
        try {
            return _file.getAbsolutePath();
        } catch (Throwable t) {
            //We dont want to throw exception from here
            return null;
        }
    }

    private class Cursor implements IByteBufferStorageCursor {

        private final RandomAccessFile _raf;

        public Cursor() throws FileNotFoundException {
            _raf = new RandomAccessFile(_file, "rw");
        }

        public void clear() {
            try {
                _raf.setLength(0);
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }
        }

        public void close() {
            try {
                _raf.close();
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            } finally {
                _cursor = null;
            }
        }

        public long getPosition() {
            try {
                return _raf.getFilePointer();
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }
        }

        public void movePosition(long offset) {
            try {
                _raf.seek(_raf.getFilePointer() + offset);
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }
        }

        public byte readByte() {
            try {
                return _raf.readByte();
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }
        }

        public void readBytes(byte[] result, int offset, int length) {
            try {
                _raf.read(result, offset, length);
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }

        }

        public int readInt() {
            try {
                return _raf.readInt();
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }
        }

        public long readLong() {
            try {
                return _raf.readLong();
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }
        }

        public void setPosition(long position) {
            try {
                _raf.seek(position);
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }
        }

        public void writeByte(byte value) {
            try {
                _raf.writeByte(value);
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }

        }

        public void writeBytes(byte[] array, int offset, int length) {
            try {
                _raf.write(array, offset, length);
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }
        }

        public void writeInt(int value) {
            try {
                _raf.writeInt(value);
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }
        }

        public void writeLong(long value) {
            try {
                _raf.writeLong(value);
            } catch (IOException e) {
                throw new RAFByteBufferStorageException(e);
            }
        }
    }

}
