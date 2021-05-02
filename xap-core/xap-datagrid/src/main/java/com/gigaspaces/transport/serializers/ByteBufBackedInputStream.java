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

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;

public class ByteBufBackedInputStream extends InputStream {

    private ByteBuf buf;

    public ByteBufBackedInputStream() {
    }

    public ByteBufBackedInputStream(ByteBuf buf) {
        this.buf = buf;
    }

    public void setBuffer(ByteBuf buf) {
        this.buf = buf;
    }

    public int read() throws IOException {
        return available() == 0 ? -1 : buf.readByte() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len) throws IOException {
        int available = available();
        if (available == 0) {
            return -1;
        }

        len = Math.min(available, len);
        buf.readBytes(bytes, off, len);
        return len;
    }

    @Override
    public int available() throws IOException {
        return buf.readableBytes();
    }

    @Override
    public long skip(long n) throws IOException {
        int nBytes = Math.min(available(), n > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n);
        buf.skipBytes(nBytes);
        return nBytes;
    }
}
