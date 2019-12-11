/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.io.*;
import com.gigaspaces.internal.utils.pool.IMemoryAwareResourceFactory;
import com.gigaspaces.internal.utils.pool.IMemoryAwareResourcePool;
import com.gigaspaces.internal.utils.pool.MemoryBoundedResourcePool;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;

/**
 * @author Niv Ingberg
 * @since 15.2
 */
@InternalApi
public class PooledObjectConverter {
    private static final int maxResources = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_POOL_RESOURCE_COUNT_SIZE, 100);
    private static final int poolMemoryBounds = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_POOL_MEMORY_SIZE, 256 * 1024 * 1024);
    private static final int zipCompressionLevel = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_ZIP_COMPRESSION_LEVEL, 9);

    private static MemoryBoundedResourcePool<MarshObjectConvertor> binaryConverterPool = new BinaryFactory().toConverterPool();
    private static MemoryBoundedResourcePool<CompressedMarshObjectConvertor> zipConverterPool = new ZipFactory().toConverterPool();

    public static byte[] serialize(Object value) throws IOException {
        return toBinary(value, binaryConverterPool);
    }

    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        return fromBinary(data, binaryConverterPool);
    }

    public static byte[] zip(Object value) throws IOException {
        return toBinary(value, zipConverterPool);
    }

    public static Object unzip(byte[] data) throws IOException, ClassNotFoundException {
        return fromBinary(data, zipConverterPool);
    }

    private static <T extends MarshObjectConvertorResource> byte[] toBinary(Object value, MemoryBoundedResourcePool<T> converterPool) throws IOException {
        T converter = converterPool.getResource();
        try {
            return converter.toBinary(value);
        } finally {
            converterPool.freeResource(converter);
        }
    }

    private static <T extends MarshObjectConvertorResource> Object fromBinary(byte[] data, MemoryBoundedResourcePool<T> converterPool) throws IOException, ClassNotFoundException {
        T converter = converterPool.getResource();
        try {
            return converter.fromBinary(data);
        } finally {
            converterPool.freeResource(converter);
        }
    }

    private static class BinaryFactory implements IMemoryAwareResourceFactory<MarshObjectConvertor> {
        @Override
        public MarshObjectConvertor allocate() {
            return allocate(null);
        }

        @Override
        public MarshObjectConvertor allocate(IMemoryAwareResourcePool resourcePool) {
            return new MarshObjectConvertor(resourcePool, ContextClassResolverObjectInputStream.Factory.instance);
        }

        private MemoryBoundedResourcePool<MarshObjectConvertor> toConverterPool() {
            return new MemoryBoundedResourcePool<>(this, 0, maxResources, poolMemoryBounds);
        }
    }

    private static class ZipFactory implements IMemoryAwareResourceFactory<CompressedMarshObjectConvertor> {
        @Override
        public CompressedMarshObjectConvertor allocate() {
            return allocate(null);
        }

        @Override
        public CompressedMarshObjectConvertor allocate(IMemoryAwareResourcePool resourcePool) {
            return new CompressedMarshObjectConvertor(zipCompressionLevel, resourcePool, ContextClassResolverObjectInputStream.Factory.instance);
        }

        private MemoryBoundedResourcePool<CompressedMarshObjectConvertor> toConverterPool() {
            return new MemoryBoundedResourcePool<>(this, 0, maxResources, poolMemoryBounds);
        }
    }
}
