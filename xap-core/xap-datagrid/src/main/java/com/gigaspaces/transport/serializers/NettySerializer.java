package com.gigaspaces.transport.serializers;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

public abstract class NettySerializer implements Closeable {
    public abstract <T> void serialize(ByteBuf buffer, T obj) throws IOException;
    public abstract <T> T deserialize(ByteBuf buffer) throws IOException;
}
