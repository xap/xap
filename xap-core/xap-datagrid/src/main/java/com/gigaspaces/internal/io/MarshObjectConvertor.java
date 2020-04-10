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
import java.io.ObjectStreamConstants;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notice! this implementation is not Thread safe and should be use in conjuction with {@link
 * com.j_spaces.kernel.pool.ResourcePool} only.
 *
 * @author Guy Korland
 * @since 4.1
 */
@com.gigaspaces.api.InternalApi
public class MarshObjectConvertor extends Resource implements MarshObjectConvertorResource {
    private GSByteArrayOutputStream _bao;
    private final SmartByteArrayCache _byteArrayCache;
    private ObjectOutputStream _oo;

    private GSByteArrayInputStream _bai;
    private ObjectInputStream _oi;

    static private MarshObjectConvertorFactory _factory = null;
    private final ObjectInputStreamFactory objectInputStreamFactory;

    // logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);
    final private static byte[] DUMMY_BUFFER = new byte[0];
    // byte array that is used to clear the ObjectInputStream tables after each read
    // the byte array is written to be stream as if it was sent over the network
    // to simulate TC_RESET
    final private static byte[] RESET_BUFFER = new byte[]{ObjectStreamConstants.TC_RESET, ObjectStreamConstants.TC_NULL};

    private static SmartByteArrayCache createSerializationByteArrayCache(ISmartLengthBasedCacheCallback cacheCallback) {
        final int maxBufferSize = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_CACHED_BUFFER_SIZE, SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_CACHED_BUFFER_SIZE_DEFAULT);
        final double expungeRatio = Double.parseDouble(System.getProperty(SystemProperties.STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_RATIO, String.valueOf(SystemProperties.STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_RATIO_DEFAULT)));
        final int expungeCount = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD, SystemProperties.STORAGE_TYPE_SERIALIZATION_CACHED_BUFFER_EXPUNGE_TIMES_THRESHOLD_DEFAULT);
        return new SmartByteArrayCache(maxBufferSize, expungeRatio, expungeCount, 1024, cacheCallback);
    }

    public MarshObjectConvertor() {
        this(null);
    }

    public MarshObjectConvertor(IMemoryAwareResourcePool resourcePool) {
        this(resourcePool, ObjectInputStreamFactory.Default.instance);
    }

    public MarshObjectConvertor(IMemoryAwareResourcePool resourcePool, ObjectInputStreamFactory objectInputStreamFactory) {
        ISmartLengthBasedCacheCallback cacheCallback = resourcePool == null ? null : SmartLengthBasedCache.toCacheCallback(resourcePool);
        this.objectInputStreamFactory = objectInputStreamFactory;
        _byteArrayCache = createSerializationByteArrayCache(cacheCallback);
        try {
            _bao = new GSByteArrayOutputStream();
            _oo = getObjectOutputStream(_bao);

            _bai = new GSByteArrayInputStream(new byte[0]);

            try {
                fromBinary(serializeToByteArray(""));
            } catch (ClassNotFoundException e) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(e.getMessage(), e);
                }
            }
        } catch (IOException e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public byte[] toBinary(Object o) throws IOException {
        // We need to reset state and pass this indication to the
        // deserializing stream
        _bao.setBuffer(_byteArrayCache.get());
        _oo.reset();

        byte[] bc = serializeToByteArray(o);

        _byteArrayCache.notifyUsedSize(bc.length);
        _bao.setBuffer(DUMMY_BUFFER);
        _oo.reset();

        return bc;
    }

    protected byte[] serializeToByteArray(Object o) throws IOException {
        _oo.writeObject(o);
        _oo.flush();

        return _bao.toByteArray();
    }

    @Override
    public Object fromBinary(byte[] data) throws IOException, ClassNotFoundException {
        _bai.setBuffer(data);

        if (_oi == null) {
            _oi = getObjectInputStream(_bai);
        }

        Object object = _oi.readObject();
        _bai.setBuffer(RESET_BUFFER);
        _oi.readObject();
        return object;
    }

    @Override
    public void clear() {
        // No clear needed
    }

    /**
     * Wrap given InputStream with ObjectInputStream
     */
    protected ObjectInputStream getObjectInputStream(InputStream is) throws IOException {
        return objectInputStreamFactory.create(is);
    }

    /**
     * Wrap given OutputStream with ObjectInputStream
     */
    protected ObjectOutputStream getObjectOutputStream(OutputStream os)
            throws IOException {
        return new ObjectOutputStream(os);
    }

    /**
     * @return
     */
    public static IResourceFactory<MarshObjectConvertor> getFactory() {
        if (_factory == null)
            _factory = new MarshObjectConvertorFactory();

        return _factory;
    }

    /**
     * @author anna
     * @version 1.0
     * @since 5.1
     */
    protected static class MarshObjectConvertorFactory
            implements IMemoryAwareResourceFactory<MarshObjectConvertor> {

        public MarshObjectConvertor allocate() {
            return allocate(null);
        }

        @Override
        public MarshObjectConvertor allocate(IMemoryAwareResourcePool resourcePool) {
            return new MarshObjectConvertor(resourcePool);
        }
    }

    @Override
    public long getUsedMemory() {
        return _byteArrayCache.getLength();
    }
}
