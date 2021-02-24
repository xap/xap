package com.gigaspaces.transport;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferBackedInputStream extends InputStream {

    private final ByteBuffer buf;

    public ByteBufferBackedInputStream(ByteBuffer buf) {
        this.buf = buf;
    }

    public int read() throws IOException {
        return !buf.hasRemaining() ? -1 : buf.get() & 0xFF;
    }

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