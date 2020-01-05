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

import com.gigaspaces.internal.io.MarshObject;
import com.gigaspaces.internal.io.PooledObjectConverter;

import java.io.IOException;
import java.util.Base64;

/**
 * Base class for adapting space properties values before storing them in space or after retrieving them.
 *
 * @author Niv Ingberg
 * @since 15.2
 */
public abstract class PropertyStorageAdapter {

    /**
     * Triggered when a property value is retrieved from the user's object and is about to be sent to the space.
     * @param value The original property value
     * @return The value which should be stored in space
     * @throws IOException Thrown when processing the property value fails.
     */
    public abstract Object toSpace(Object value) throws IOException;

    /**
     * Triggered when a property value arrives from the space and is about to be injected in the user's object.
     * @param value The value which was stored in the space
     * @return The value which should be set in the user's object.
     * @throws IOException Thrown when processing the property value fails.
     * @throws ClassNotFoundException Thrown when processing the property value fails due to a class loading issue.
     */
    public abstract Object fromSpace(Object value) throws IOException, ClassNotFoundException;

    /**
     * Returns a name used for display in monitoring tools.
     */
    public String getName() {
        return this.getClass().getSimpleName();
    }

    /**
     * Returns the class of the values which will be stored in the space.
     */
    public Class<?> getStorageClass() {
        return useBase64Wrapper() ? String.class : null;
    }

    /**
     * Determines if binary content should be stored in the space as a string in base64 encoding. Defaults to false.
     */
    public boolean useBase64Wrapper() {
        return false;
    }

    /**
     * Helper method for serializing a serializable object to a byte array.
     */
    protected byte[] serialize(Object value) throws IOException {
        return PooledObjectConverter.serialize(value);
    }

    /**
     * Helper method for deserializing a byte array to a serializable object.
     */
    protected Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        return PooledObjectConverter.deserialize(data);
    }

    /**
     * Helper method for serializing and compressing a serializable object to a byte array.
     */
    protected byte[] zip(Object value) throws IOException {
        return PooledObjectConverter.zip(value);
    }

    /**
     * Helper method for uncompressing and deserializing a byte array to a serializable object.
     */
    protected Object unzip(byte[] data) throws IOException, ClassNotFoundException {
        return PooledObjectConverter.unzip(data);
    }

    /**
     * Helper method for wrapping a byte array in a container object for space storage.
     */
    protected Object wrapBinary(byte[] data) {
        return useBase64Wrapper()
                ? base64Encode(data)
                : toBinaryWrapper(data);
    }

    /**
     * Helper method for unwrapping a byte array from its container object.
     */
    protected byte[] unwrapBinary(Object spaceValue) {
        return useBase64Wrapper()
                ? base64Decode((String) spaceValue)
                : ((BinaryWrapper) spaceValue).getBytes();
    }

    /**
     * Default factory for wrapping a byte array in a container for space storage.
     */
    protected BinaryWrapper toBinaryWrapper(byte[] bytes) {
        return new MarshObject(bytes);
    }

    /**
     * Helper method for encoding a byte array to a base64-encoded string.
     */
    protected String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * Helper method for decoding a base64-encoded string to a byte array.
     */
    protected byte[] base64Decode(String s) {
        return Base64.getDecoder().decode(s);
    }
}
