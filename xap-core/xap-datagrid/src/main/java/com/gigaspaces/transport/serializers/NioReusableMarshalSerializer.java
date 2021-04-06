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

import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.io.MarshalOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NioReusableMarshalSerializer extends NioSerializer {
    private final ByteBufferBackedOutputStream bos = new ByteBufferBackedOutputStream();
    private final ByteBufferBackedInputStream bis = new ByteBufferBackedInputStream();
    private MarshalOutputStream oos;
    private MarshalInputStream ois;

    @Override
    public void close() throws IOException {
        if (oos != null)
            oos.close();
        if (ois != null)
            ois.close();
    }

    @Override
    public ByteBuffer serialize(Object obj) throws IOException {
        bos.setBuffer(null);
        serializeImpl(obj);
        return bos.getBuffer();
    }

    @Override
    public ByteBuffer serializeWithLength(Object obj) throws IOException {
        bos.setBuffer(null);
        // Hack - placeholder for header.
        bos.getBuffer().putInt(0);
        // Serialize to dynamic buffer (reference might change):
        serializeImpl(obj);
        // Get ref to buffer with serialized data:
        ByteBuffer result = bos.getBuffer();
        // Prepend length:
        result.putInt(0, result.position() - 4);
        result.flip();
        return result;
    }

    @Override
    public void serialize(ByteBuffer buffer, Object obj) throws IOException {
        bos.setBuffer(buffer);
        serializeImpl(obj);
    }

    private void serializeImpl(Object obj) throws IOException {
        if (oos == null)
            oos = new MarshalOutputStream(bos);
        else
            oos.reset();
        oos.writeObject(obj);
        oos.flush();
    }

    @Override
    public <T> T deserialize(ByteBuffer buffer) throws IOException {
        bis.setBuffer(buffer);
        if (ois == null)
            ois = new MarshalInputStream(bis);

        try {
            return (T) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
