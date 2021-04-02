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
package com.gigaspaces.metadata;

import com.gigaspaces.client.storage_adapters.class_storage_adapters.ClassBinaryStorageAdapter;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.DirectClassBinaryStorageAdapter;
import com.gigaspaces.client.storage_adapters.class_storage_adapters.SequentialClassBinaryStorageAdapter;
import com.gigaspaces.internal.utils.GsEnv;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.SystemProperties;

/**
 * @author Niv Ingberg
 * @since 15.8
 */
public enum ClassBinaryStorageLayout {
    DEFAULT,
    SEQUENTIAL,
    DIRECT;

    private final Class<? extends ClassBinaryStorageAdapter> adapterClass;

    ClassBinaryStorageLayout() {
        this.adapterClass = initAdapter(this.name());
    }

    private static Class<? extends ClassBinaryStorageAdapter> initAdapter(String name) {
        String value = GsEnv.property(SystemProperties.CLASS_BINARY_STORAGE_ADAPTER + name.toLowerCase()).get();
        if (value != null) {
            try {
                return ClassLoaderHelper.loadClass(value);
            } catch (ClassNotFoundException e) {
                throw new SpaceMetadataException("Failed to load class [" + value + "]");
            }
        }
        return name.equals("DIRECT") ? DirectClassBinaryStorageAdapter.class : SequentialClassBinaryStorageAdapter.class;
    }

    public Class<? extends ClassBinaryStorageAdapter> getAdapterClass() {
        return adapterClass;
    }
}
