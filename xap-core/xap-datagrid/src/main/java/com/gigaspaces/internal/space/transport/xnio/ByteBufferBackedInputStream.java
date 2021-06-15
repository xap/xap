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
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public class ByteBufferBackedInputStream extends InputStream {

    private ByteBuffer buf;

    public ByteBufferBackedInputStream() {
    }
    public ByteBufferBackedInputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    public void setBuffer(ByteBuffer buf) {
        this.buf = buf;
    }

    @Override
    public int read() throws IOException {
        return !buf.hasRemaining() ? -1 : buf.get() & 0xFF;
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }

        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
    }

    @Override
    public int available() throws IOException {
        return buf.remaining();
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0)
            return 0;
        if (n > Integer.MAX_VALUE)
            throw new IllegalArgumentException("n > Integer.MAX_VALUE");

        n = Math.min(n, buf.remaining());
        buf.position(buf.position() + (int)n);
        return n;
    }
}
