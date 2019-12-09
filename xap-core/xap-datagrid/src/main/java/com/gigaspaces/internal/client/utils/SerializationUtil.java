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

package com.gigaspaces.internal.client.utils;

import com.gigaspaces.client.storage_adapters.PropertyStorageAdapter;
import com.gigaspaces.client.storage_adapters.internal.PropertyStorageAdapterRegistry;
import com.gigaspaces.metadata.StorageType;

import java.io.IOException;

/**
 * A utility that provides a way to serialize and deserialize an entry field
 *
 * @author Guy Korland
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class SerializationUtil {
    private static final PropertyStorageAdapter binaryAdapter = PropertyStorageAdapterRegistry.getInstance().get(StorageType.BINARY.getStorageAdapterClass());
    private static final PropertyStorageAdapter compressedAdapter = PropertyStorageAdapterRegistry.getInstance().get(StorageType.COMPRESSED.getStorageAdapterClass());

    public static Object serializeFieldValue(Object value, StorageType storageType) throws IOException {
        if (value == null)
            return null;

        PropertyStorageAdapter adapter = getSerializationAdapter(storageType);
        return adapter == null ? value : adapter.toSpace(value);
    }

    public static Object deSerializeFieldValue(Object value, StorageType storageType) throws ClassNotFoundException, IOException {
        if (value == null)
            return null;

        PropertyStorageAdapter adapter = getSerializationAdapter(storageType);
        return adapter == null ? value : adapter.fromSpace(value);
    }

    private static PropertyStorageAdapter getSerializationAdapter(StorageType storageType) {
        switch (storageType) {
            case OBJECT: return null;
            case BINARY: return binaryAdapter;
            case COMPRESSED: return compressedAdapter;
            default: throw new IllegalArgumentException("Invalid storage Type : " + storageType);
        }
    }
}
