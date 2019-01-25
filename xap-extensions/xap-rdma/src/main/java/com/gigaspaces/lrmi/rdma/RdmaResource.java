package com.gigaspaces.lrmi.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.SVCPostSend;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class RdmaResource {

    private final short id;
    private final ByteBuffer buffer;
    private final SVCPostSend postSend;
    private final LinkedList<IbvSendWR> wr_list;
    private final RdmaActiveEndpoint endpoint;
    private Logger logger = DiSNILogger.getLogger();

    public RdmaResource(short id, ByteBuffer buffer, RdmaActiveEndpoint endpoint) throws IOException {
        this.endpoint = endpoint;
        this.id = id;
        this.buffer = buffer;
        IbvMr mr = endpoint.registerMemory(buffer).execute().free().getMr();
        this.wr_list = ClientTransport.createSendWorkRequest(id, mr);
        this.postSend = RdmaConstants.JNI_CACHE_ENABLED ? endpoint.postSend(wr_list) : null;
    }

    public short getId() {
        return id;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void execute() throws IOException {
        if (RdmaConstants.JNI_CACHE_ENABLED)
            postSend.execute();
        else
            endpoint.postSend(wr_list).execute().free();
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
