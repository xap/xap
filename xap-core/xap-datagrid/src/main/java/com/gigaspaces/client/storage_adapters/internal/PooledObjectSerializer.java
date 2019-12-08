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

package com.gigaspaces.client.storage_adapters.internal;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.internal.io.MarshObject;
import com.gigaspaces.internal.io.MarshObjectConvertorResource;
import com.gigaspaces.internal.utils.pool.IMemoryAwareResourceFactory;
import com.gigaspaces.internal.utils.pool.MemoryBoundedResourcePool;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;

/**
 * @author Niv Ingberg
 * @since 15.2
 */
@InternalApi
public class PooledObjectSerializer<T extends MarshObjectConvertorResource> {
    private final MemoryBoundedResourcePool<T> converterPool;

    public PooledObjectSerializer(IMemoryAwareResourceFactory<T> factory) {
        int maxResources = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_POOL_RESOURCE_COUNT_SIZE, SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_POOL_RESOURCE_COUNT_SIZE_DEFAULT);
        int poolMemoryBounds = Integer.getInteger(SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_POOL_MEMORY_SIZE, SystemProperties.STORAGE_TYPE_SERIALIZATION_MAX_POOL_MEMORY_SIZE_DEFAULT);
        converterPool = new MemoryBoundedResourcePool<T>(factory, 0, maxResources, poolMemoryBounds);
    }

    public MarshObject serialize(Object value) throws IOException {
        T converter = converterPool.getResource();
        try {
            return converter.getMarshObject(value);
        } finally {
            converterPool.freeResource(converter);
        }
    }

    public Object deserialize(MarshObject value) throws IOException, ClassNotFoundException {
        T converter = converterPool.getResource();
        try {
            return converter.getObject(value);
        } finally {
            converterPool.freeResource(converter);
        }
    }
}
