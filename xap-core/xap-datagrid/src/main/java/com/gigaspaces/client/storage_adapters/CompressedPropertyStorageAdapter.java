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

package com.gigaspaces.client.storage_adapters;

import com.gigaspaces.client.storage_adapters.internal.PooledObjectSerializer;
import com.gigaspaces.internal.io.CompressedMarshObjectConvertor;
import com.gigaspaces.internal.io.ContextClassResolverObjectInputStream;
import com.gigaspaces.internal.io.MarshObject;
import com.gigaspaces.internal.utils.pool.IMemoryAwareResourceFactory;
import com.gigaspaces.internal.utils.pool.IMemoryAwareResourcePool;

import java.io.IOException;

/**
 * Adapter for compressing properties using standard zip compression and storing them in space in binary form.
 *
 * @author Niv Ingberg
 * @since 15.2
 */
public class CompressedPropertyStorageAdapter implements PropertyStorageAdapter {

    protected static final int DEFAULT_COMPRESSION_LEVEL = 9;

    private final PooledObjectSerializer<CompressedMarshObjectConvertor> serializer;

    public CompressedPropertyStorageAdapter() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    protected CompressedPropertyStorageAdapter(int level) {
        this.serializer = new PooledObjectSerializer<>(new Factory(level));
    }

    @Override
    public Object toSpace(Object value) throws IOException {
        return serializer.serialize(value);
    }

    @Override
    public Object fromSpace(Object value) throws IOException, ClassNotFoundException {
        return serializer.deserialize((MarshObject) value);
    }

    private static class Factory implements IMemoryAwareResourceFactory<CompressedMarshObjectConvertor> {
        private final int level;

        private Factory(int level) {
            this.level = level;
        }

        @Override
        public CompressedMarshObjectConvertor allocate() {
            return allocate(null);
        }

        @Override
        public CompressedMarshObjectConvertor allocate(IMemoryAwareResourcePool resourcePool) {
            return new CompressedMarshObjectConvertor(level, resourcePool, ContextClassResolverObjectInputStream.Factory.instance);
        }
    }
}
