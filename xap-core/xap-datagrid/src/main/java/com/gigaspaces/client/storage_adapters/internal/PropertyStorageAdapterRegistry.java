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
import com.gigaspaces.client.storage_adapters.BinaryPropertyStorageAdapter;
import com.gigaspaces.client.storage_adapters.CompressedPropertyStorageAdapter;
import com.gigaspaces.client.storage_adapters.PropertyStorageAdapter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Niv Ingberg
 * @since 15.2
 */
@InternalApi
public class PropertyStorageAdapterRegistry {
    private static final PropertyStorageAdapterRegistry instance = new PropertyStorageAdapterRegistry();

    private final Map<Class<? extends PropertyStorageAdapter>, PropertyStorageAdapter> map = new ConcurrentHashMap<>();

    private PropertyStorageAdapterRegistry() {
        getOrCreate(BinaryPropertyStorageAdapter.class);
        getOrCreate(CompressedPropertyStorageAdapter.class);
    }

    public static PropertyStorageAdapterRegistry getInstance() {
        return instance;
    }

    public PropertyStorageAdapter get(Class<? extends PropertyStorageAdapter> key) {
        return map.get(key);
    }

    public PropertyStorageAdapter getOrCreate(Class<? extends PropertyStorageAdapter> key) {
        if (!map.containsKey(key)) {
            synchronized (map) {
                if (!map.containsKey(key)) {
                    PropertyStorageAdapter value = null;
                    try {
                        value = key.newInstance();
                    } catch (ReflectiveOperationException e) {
                        throw new RuntimeException("Failed to instantiate " + key, e);
                    }
                    map.put(key, value);
                }
            }
        }
        return map.get(key);
    }
}
