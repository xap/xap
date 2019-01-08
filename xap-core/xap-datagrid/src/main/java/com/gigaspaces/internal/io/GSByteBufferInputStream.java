package com.gigaspaces.internal.io;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class GSByteBufferInputStream extends InputStream {
    private final ByteBuffer byteBuffer;

    public GSByteBufferInputStream(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public int read() {
        return byteBuffer.get();
    }

    @Override
    public int read(byte b[], int off, int len) {
        int remaining = byteBuffer.remaining();
        if (remaining == 0)
            return -1;
        int actualLen = len <= remaining ? len : remaining;
        byteBuffer.get(b, off, actualLen);
        return actualLen;
    }

    @Override
    public long skip(long n) {
        int remaining = byteBuffer.remaining();
        int actualN = n <= remaining ? (int)n : remaining;
        int pos = byteBuffer.position();
        byteBuffer.position(pos + actualN);
        return actualN;
    }

    @Override
    public int available() {
        return byteBuffer.remaining();
    }
}
