package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.SVCPostSend;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

public class RdmaResource {

    private final short id;
    private final ByteBuffer buffer;
    private final SVCPostSend postSend;
    private Logger logger = DiSNILogger.getLogger();

    public RdmaResource(short id, ByteBuffer buffer, SVCPostSend postSend) {
        this.id = id;
        this.buffer = buffer;
        this.postSend = postSend;
    }

    public short getId() {
        return id;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public SVCPostSend getPostSend() {
        return postSend;
    }

    public void serialize(Object payload) throws IOException {
        if(logger.isDebugEnabled()) {
            logger.debug("serializing payload " + payload);
        }
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
        oos.writeObject(payload);
        oos.flush();
        byte[] bytes = bytesOut.toByteArray();
        bytesOut.close();
        oos.close();
        buffer.put(bytes);
    }
}
