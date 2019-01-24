package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.lrmi.nio.ByteBufferPacketSerializer;
import com.gigaspaces.lrmi.nio.IPacket;
import com.ibm.disni.verbs.SVCPostSend;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

public class LrmiRdmaResource extends RdmaResource {
    private ByteBufferPacketSerializer serializer;

    public LrmiRdmaResource(short id, ByteBuffer buffer, SVCPostSend postSend) {
        super(id, buffer, postSend);
        try {
            this.serializer = new ByteBufferPacketSerializer(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void serialize(Object payload) throws IOException {
        Logger.getLogger("RdmaLogger").info("serializing payload "+payload);
        serializer.serialize((IPacket) payload);
        Logger.getLogger("Rdma").info("buffer position = "+getBuffer().position());
    }
}
