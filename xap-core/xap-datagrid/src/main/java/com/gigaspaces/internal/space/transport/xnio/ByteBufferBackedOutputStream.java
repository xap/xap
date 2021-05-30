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
package com.gigaspaces.internal.space.transport.xnio;

import com.gigaspaces.api.ExperimentalApi;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public class ByteBufferBackedOutputStream extends OutputStream {
    private static final int QUANTUM = 1024;
    private ByteBuffer buf;
    private boolean resizable;

    public ByteBufferBackedOutputStream() {
        this(null);
    }

    public ByteBufferBackedOutputStream(ByteBuffer buf) {
        setBuffer(buf);
    }

    public ByteBuffer getBuffer() {
        return buf;
    }

    public void setBuffer(ByteBuffer buf) {
        this.resizable = buf == null;
        this.buf = buf != null ? buf : ByteBuffer.allocate(QUANTUM);
    }

    @Override
    public void write(int b) throws IOException {
        if (resizable)
            ensureCapacity(1);
        buf.put((byte) b);
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        if (resizable)
            ensureCapacity(len);
        buf.put(bytes, off, len);
    }

    private void ensureCapacity(int delta) {
        if (buf.remaining() < delta) {
            int addition = Math.max(delta - buf.remaining(), QUANTUM);
            ByteBuffer newBuf = ByteBuffer.allocate(buf.capacity() + addition);
            buf.flip();
            newBuf.put(buf);
            buf = newBuf;
        }
    }
}
