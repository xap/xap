package com.gigaspaces.internal.io;

import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class GSByteBufferOutputStream extends OutputStream {
    private final ByteBuffer byteBuffer;

    public GSByteBufferOutputStream(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public void write(int b) {
        byteBuffer.put((byte) b);
    }

    @Override
    public void write(byte b[], int off, int len) {
        if (len == 0)
            return;
        if (len > byteBuffer.remaining()) {
            throw new RuntimeException("buffer capacity = "+byteBuffer.capacity()+", len = "+len);
        }
        byteBuffer.put(b, off, len);
    }
}
