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
