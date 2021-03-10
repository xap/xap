package com.gigaspaces.transport.serializers;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;

public class ByteBufBackedOutputStream extends OutputStream {
    private ByteBuf buf;

    public ByteBufBackedOutputStream() {
    }

    public ByteBufBackedOutputStream(ByteBuf buffer) {
        this.buf = buffer;
    }

    public void setBuffer(ByteBuf buffer) {
        this.buf = buffer;
    }

    public void write(int b) throws IOException {
        buf.writeByte(b);
    }

    public void write(byte[] bytes, int off, int len) throws IOException {
        buf.writeBytes(bytes, off, len);
    }
}