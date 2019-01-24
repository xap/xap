package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.lrmi.nio.ByteBufferPacketSerializer;
import com.gigaspaces.lrmi.nio.IPacket;
import com.ibm.disni.verbs.SVCPostSend;

import java.io.IOException;
import java.nio.ByteBuffer;

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
    public void serialize(long id, Object payload) throws IOException {
        getBuffer().putLong(id);
        serializer.serialize((IPacket) payload);
    }
}
