package com.gigaspaces.rdma;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

public class RdmaSender implements Runnable {

    private final Client.CustomClientEndpoint endpoint;
    private final ArrayBlockingQueue<RdmaMsg> writeRequests;
    private final ByteBuffer sendBuf = ByteBuffer.allocateDirect(100);

    public RdmaSender(Client.CustomClientEndpoint endpoint,
                      ArrayBlockingQueue<RdmaMsg> writeRequests) {
        this.endpoint = endpoint;
        this.writeRequests = writeRequests;
    }

    public ByteBuffer getSendBuf() {
        return sendBuf;
    }


    @Override
    public void run() {
        while (true) {
            try {
                RdmaMsg rdmaMsg = writeRequests.take();
                try {
                    long id = rdmaMsg.getId();
                    ClientTransport.serializeToBuffer(sendBuf, rdmaMsg, id);
                    ClientTransport.rdmaSendBuffer(id, sendBuf, endpoint).execute().free();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
