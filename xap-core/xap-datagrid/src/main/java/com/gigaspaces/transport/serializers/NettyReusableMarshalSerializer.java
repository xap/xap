package com.gigaspaces.transport.serializers;

import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.io.MarshalOutputStream;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

public class NettyReusableMarshalSerializer extends NettySerializer {
    private final ByteBufBackedOutputStream bos = new ByteBufBackedOutputStream();
    private final ByteBufBackedInputStream bis = new ByteBufBackedInputStream();
    private MarshalOutputStream oos;
    private MarshalInputStream ois;

    @Override
    public void close() throws IOException {
        if (oos != null)
            oos.close();
        if (ois != null)
            ois.close();
    }

    @Override
    public <T> void serialize(ByteBuf buffer, T obj) throws IOException {
        bos.setBuffer(buffer);
        if (oos == null)
            oos = new MarshalOutputStream(bos);
        else
            oos.reset();
        oos.writeObject(obj);
        oos.flush();
    }

    @Override
    public <T> T deserialize(ByteBuf buffer) throws IOException {
        bis.setBuffer(buffer);
        if (ois == null)
            ois = new MarshalInputStream(bis);

        try {
            return (T) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
