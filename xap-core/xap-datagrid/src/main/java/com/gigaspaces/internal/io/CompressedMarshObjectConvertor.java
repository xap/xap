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

package com.gigaspaces.internal.io;

import com.gigaspaces.internal.utils.ReflectionUtils;
import com.gigaspaces.internal.utils.pool.IMemoryAwareResourceFactory;
import com.gigaspaces.internal.utils.pool.IMemoryAwareResourcePool;
import com.gigaspaces.logger.Constants;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.pool.IResourceFactory;
import com.j_spaces.kernel.pool.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;


/**
 * Notice! this implementation is not Thread safe and should be use in conjunction with {@link
 * com.j_spaces.kernel.pool.ResourcePool} only.
 *
 * @author Guy Korland
 * @since 4.1
 */
@com.gigaspaces.api.InternalApi
public class CompressedMarshObjectConvertor extends Resource implements MarshObjectConvertorResource {
    private int zipEntryCounter = 0;
    private static final int MAX_ENTRIES = 100;
    private boolean _idempotent;

    private int _level;

    private GSByteArrayOutputStream _bao;
    private final SmartByteArrayCache _byteArrayCache;
    private ZipOutputStream _zo;
    private ObjectOutputStream _oo;

    private GSByteArrayInputStream _bai;
    private ZipInputStream _zi;
    private ObjectInputStream _oi;

    private final ObjectInputStreamFactory objectInputStreamFactory;

    static private CompressedMarshObjectConvertorFactory _factory = null;

    // logger
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_LRMI);
    private static final byte[] DUMMY_BUFFER = new byte[0];

    private static SmartByteArrayCache createSerializationByteArrayCache(ISmartLengthBasedCacheCallback cacheCallback) {
        final int maxBufferSize = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_CACHED_BUFFER_SIZE, SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_CACHED_BUFFER_SIZE_DEFAULT);
        final double expungeRatio = Double.parseDouble(System.getProperty(SystemProperties.STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_RATIO, String.valueOf(SystemProperties.STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_RATIO_DEFAULT)));
        final int expungeCount = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD, SystemProperties.STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD_DEFAULT);
        return new SmartByteArrayCache(maxBufferSize, expungeRatio, expungeCount, 1024, cacheCallback);
    }


    /**
     * @param level the compression level (0-9), The default setting is DEFAULT_COMPRESSION.
     * @throws IllegalArgumentException if the compression level is invalid
     */
    public CompressedMarshObjectConvertor(int level) {
        this(level, null);
    }

    public CompressedMarshObjectConvertor(int level, IMemoryAwareResourcePool resourcePool) {
        this(level, resourcePool, ObjectInputStreamFactory.Default.instance, false);
    }

    public CompressedMarshObjectConvertor(int level, IMemoryAwareResourcePool resourcePool, ObjectInputStreamFactory objectInputStreamFactory, boolean idempotent) {
        this.objectInputStreamFactory = objectInputStreamFactory;
        ISmartLengthBasedCacheCallback cacheCallback = resourcePool == null ? null : SmartLengthBasedCache.toCacheCallback(resourcePool);
        _byteArrayCache = createSerializationByteArrayCache(cacheCallback);
        _level = level;
        this._idempotent = idempotent;
        try {
            _bao = new GSByteArrayOutputStream();
            _zo = createZipOutputStream(_bao);
            _zo.setLevel(_level);
            _zo.putNextEntry(createZipEntry(_idempotent ? 0 : zipEntryCounter++));
            _oo = getObjectOutputStream(_zo);

            _bai = new GSByteArrayInputStream(new byte[0]);
            _zi = new ZipInputStream(_bai);

            fromBinary(serializeToByteArray("")); // remove header from
            // in/out
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    protected byte[] serializeToByteArray(Object o) throws IOException {
        _oo.writeObject(o);
        _oo.flush();
        _zo.closeEntry();

        return _bao.toByteArray();
    }

    @Override
    public byte[] toBinary(Object o) throws IOException {
        _bao.setBuffer(_byteArrayCache.get());
        _bao.reset();
        // check for next time
        if (_idempotent) {
            _zo.putNextEntry(createZipEntry(0));
            _oo.reset();
        } else if (++zipEntryCounter < MAX_ENTRIES) {
            _zo.putNextEntry(createZipEntry(zipEntryCounter));
            _oo.reset();
        } else // open new zip OutputStream for next time
        {
            zipEntryCounter = 0;
            _zo = createZipOutputStream(_bao);
            _zo.setLevel(_level);
            _zo.putNextEntry(createZipEntry(zipEntryCounter));
            _oo = getObjectOutputStream(_zo);

            // remove ObjectOutputStream header from zip stream
            _zo.closeEntry();
            _bao.reset();

            _zo.putNextEntry(createZipEntry(++zipEntryCounter));
            _oo.reset();
        }

        byte[] bc = serializeToByteArray(o);
        _byteArrayCache.notifyUsedSize(bc.length);
        _bao.setBuffer(DUMMY_BUFFER);

        return bc;
    }

    @Override
    public Object fromBinary(byte[] data) throws IOException, ClassNotFoundException {
        _bai.setBuffer(data);
        _zi.getNextEntry();

        if (_oi == null) {
            _oi = getObjectInputStream(_zi);
        }

        Object object = _oi.readObject();
        // It seems ZipInputStream has some internal state and it needs
        // to be cleared both before and after setting the underlying
        // InputStream
        // buffer
        _zi.getNextEntry();
        _bai.setBuffer(DUMMY_BUFFER);
        return object;
    }

    protected ObjectOutputStream getObjectOutputStream(OutputStream is)
            throws IOException {
        return new ObjectOutputStream(is);
    }

    protected ObjectInputStream getObjectInputStream(InputStream is) throws IOException {
        return objectInputStreamFactory.create(is);
    }

    @Override
    public void clear() {
        // No clear needed
    }

    public static IResourceFactory<CompressedMarshObjectConvertor> getFactory() {
        if (_factory == null)
            _factory = new CompressedMarshObjectConvertorFactory();

        return _factory;
    }

    protected static class CompressedMarshObjectConvertorFactory implements IMemoryAwareResourceFactory<CompressedMarshObjectConvertor> {
        public CompressedMarshObjectConvertor allocate() {
            return allocate(null);
        }

        @Override
        public CompressedMarshObjectConvertor allocate(IMemoryAwareResourcePool resourcePool) {
            return new CompressedMarshObjectConvertor(9, resourcePool);
        }
    }

    @Override
    public long getUsedMemory() {
        return _byteArrayCache.getLength();
    }

    private ZipOutputStream createZipOutputStream(OutputStream out) {
        return _idempotent ? new IdempotentZipOutputStream(out) : new ZipOutputStream(out);
    }

    private ZipEntry createZipEntry(int id) {
        String s = id == 0 ? "0" : Integer.toString(id);
        ZipEntry result = new ZipEntry(s);
        if (_idempotent)
            result.setTime(0);
        return result;
    }

    /**
     * Workaround: Create a reusable zip output stream by using reflection to override the duplicate entries protection.
     */
    private static class IdempotentZipOutputStream extends ZipOutputStream {
        private IdempotentZipOutputStream(OutputStream out) {
            super(out);
            try {
                ReflectionUtils.setField(this, "names", new DummyHashSet<>());
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Failed to create idempotent zip output stream");
            }
        }

        private static class DummyHashSet<E> extends HashSet<E> {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean add(E e) {
                return true;
            }
        }
    }
}
