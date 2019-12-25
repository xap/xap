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

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.io.MarshObject;
import com.gigaspaces.internal.io.PooledObjectConverter;

import java.io.IOException;
import java.util.Base64;

/**
 * Interface for adapting space properties values before storing them in space or after retrieving them.
 *
 * @author Niv Ingberg
 * @since 15.2
 */
@ExperimentalApi
public interface PropertyStorageAdapter {
    String getName();
    Object toSpace(Object value) throws IOException;
    Object fromSpace(Object value) throws IOException, ClassNotFoundException;

    default Class<?> getStorageClass() {
        return useBase64Wrapper() ? String.class : null;
    }

    default byte[] serialize(Object value) throws IOException {
        return PooledObjectConverter.serialize(value);
    }

    default Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        return PooledObjectConverter.deserialize(data);
    }

    default byte[] zip(Object value) throws IOException {
        return PooledObjectConverter.zip(value);
    }

    default Object unzip(byte[] data) throws IOException, ClassNotFoundException {
        return PooledObjectConverter.unzip(data);
    }

    default Object wrap(byte[] data) {
        return useBase64Wrapper()
                ? base64Encode(data)
                : wrapBinary(data);
    }

    default byte[] unwrap(Object spaceValue) {
        return useBase64Wrapper()
                ? base64Decode((String) spaceValue)
                : ((BinaryWrapper) spaceValue).getBytes();
    }

    default BinaryWrapper wrapBinary(byte[] bytes) {
        return new MarshObject(bytes);
    }

    default String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    default byte[] base64Decode(String s) {
        return Base64.getDecoder().decode(s);
    }

    default boolean useBase64Wrapper() {
        return false;
    }
}
