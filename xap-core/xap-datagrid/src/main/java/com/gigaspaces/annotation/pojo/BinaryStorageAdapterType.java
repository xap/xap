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
package com.gigaspaces.annotation.pojo;

public enum BinaryStorageAdapterType {
    ALL, EXCLUDE_INDEXES, CUSTOM;

    public static BinaryStorageAdapterType fromCode(int code) {
        switch (code) {
            case 1:
                return ALL;
            case 2:
                return EXCLUDE_INDEXES;
            case 3:
                return CUSTOM;
            default:
                throw new IllegalStateException("No StorageAdapterType found for code: " + code);
        }
    }

    public static int toCode(BinaryStorageAdapterType type) {
        switch (type) {
            case ALL:
                return 1;
            case EXCLUDE_INDEXES:
                return 2;
            case CUSTOM:
                return 3;
            default:
                throw new IllegalStateException("No StorageAdapterType found [" + type + "]");
        }
    }
}
