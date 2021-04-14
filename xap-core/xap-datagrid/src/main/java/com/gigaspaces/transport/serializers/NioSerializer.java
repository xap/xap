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
package com.gigaspaces.transport.serializers;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class NioSerializer implements Closeable {
    private static final int BYTES_INTEGER = 4;

    public abstract ByteBuffer serialize(Object obj) throws IOException;
    public abstract ByteBuffer serializeWithLength(Object obj) throws IOException;

    public abstract void serialize(ByteBuffer buffer, Object obj) throws IOException;

    public abstract <T> T deserialize(ByteBuffer buffer) throws IOException;

    public void serializeWithLength(ByteBuffer buffer, Object obj) throws IOException {
        // Save position before write:
        int prevPos = buffer.position();
        // Skip ahead, save bytes for unknown length (int):
        buffer.position(prevPos + BYTES_INTEGER);
        // serialize:
        serialize(buffer, obj);
        // Prepend length in before payload:
        buffer.putInt(prevPos, buffer.position() - prevPos - BYTES_INTEGER);
    }
}
