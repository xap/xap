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
