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

import com.gigaspaces.client.storage_adapters.BinaryPropertyStorageAdapter;
import com.gigaspaces.client.storage_adapters.CompressedPropertyStorageAdapter;
import com.gigaspaces.client.storage_adapters.PropertyStorageAdapter;

/**
 * @author yael
 * @since 9.0.0
 */
public enum StorageType {
    DEFAULT(null),
    OBJECT(null),
    BINARY(BinaryPropertyStorageAdapter.class),
    COMPRESSED(CompressedPropertyStorageAdapter.class);

    private final Class<? extends PropertyStorageAdapter> storageAdapterClass;

    StorageType(Class<? extends PropertyStorageAdapter> storageAdapterClass) {
        this.storageAdapterClass = storageAdapterClass;
    }

    public Class<? extends PropertyStorageAdapter> getStorageAdapterClass() {
        return storageAdapterClass;
    }

    private static final int OBJECT_CODE = 0;
    private static final int BINARY_CODE = 1;
    private static final int FULL_CODE = 2;
    private static final int COMPRESSED_CODE = 3;

    public static StorageType fromCode(int code) {
        switch (code) {
            case OBJECT_CODE:
                return OBJECT;
            case BINARY_CODE:
                return BINARY;
            case COMPRESSED_CODE:
                return COMPRESSED;
            case FULL_CODE:
                throw new IllegalArgumentException("StorageType FULL (2) is no longer supported - use " + BINARY + " (" + BINARY_CODE + ") instead");
            default:
                throw new IllegalArgumentException("Unsupported StorageType code: " + code);
        }
    }

    public int getCode() {
        switch (this) {
            case OBJECT:
                return OBJECT_CODE;
            case BINARY:
                return BINARY_CODE;
            case COMPRESSED:
                return COMPRESSED_CODE;
            default:
                throw new IllegalArgumentException("Unsupported StorageType: " + this);
        }
    }
}
