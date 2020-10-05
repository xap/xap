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

package com.gigaspaces.client.storage_adapters.class_storage_adapters;

import com.gigaspaces.api.InternalApi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Yael Nahon
 * @since 15.8
 */
@InternalApi
public class ClassBinaryStorageAdapterRegistry {
    private static final ClassBinaryStorageAdapterRegistry instance = new ClassBinaryStorageAdapterRegistry();

    private final Map<Class<? extends ClassBinaryStorageAdapter>, ClassBinaryStorageAdapter> map = new ConcurrentHashMap<>();

    private ClassBinaryStorageAdapterRegistry() {
    }

    public static ClassBinaryStorageAdapterRegistry getInstance() {
        return instance;
    }

    public ClassBinaryStorageAdapter get(Class<? extends ClassBinaryStorageAdapter> key) {
        return map.get(key);
    }

    public ClassBinaryStorageAdapter getOrCreate(Class<? extends ClassBinaryStorageAdapter> key) {
        if (!map.containsKey(key)) {
            synchronized (map) {
                if (!map.containsKey(key)) {
                    ClassBinaryStorageAdapter value = null;
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
