package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.lrmi.nio.ByteBufferPacketSerializer;
import com.gigaspaces.lrmi.nio.IPacket;
import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.SVCPostSend;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LrmiRdmaResource extends RdmaResource {
    private ByteBufferPacketSerializer serializer;
    private Logger logger = DiSNILogger.getLogger();

    public LrmiRdmaResource(short id, ByteBuffer buffer, RdmaActiveEndpoint endpoint) throws IOException {
        super(id, buffer, endpoint);
        try {
            this.serializer = new ByteBufferPacketSerializer(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void serialize(Object payload) throws IOException {
        if(logger.isDebugEnabled()) {
            logger.debug("serializing payload " + payload);
        }
        serializer.serialize((IPacket) payload);
        if(logger.isDebugEnabled()) {
            logger.debug("buffer position = " + getBuffer().position());
        }
    }
}
