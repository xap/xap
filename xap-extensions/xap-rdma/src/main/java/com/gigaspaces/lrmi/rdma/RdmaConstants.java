package com.gigaspaces.lrmi.rdma;

import java.util.concurrent.atomic.AtomicInteger;

public class RdmaConstants {
    public static boolean ENABLED = Boolean.getBoolean("com.gs.rdma.enabled");;
    public static boolean NETTY_ENABLED = Boolean.getBoolean("com.gs.netty.enabled");;
    public static boolean JNI_CACHE_ENABLED = false;
    public static final int MAX_INCOMMING_REQUESTS = 1000;
    public static final int BUFFER_SIZE = 4 * 1024;
    public static final int RDMA_CONNECT_TIMEOUT = 5000;
    public static final int RDMA_SYNC_OP_TIMEOUT = 5000;
    public static final int RDMA_CLIENT_SEND_ID = 2000;
    public static final int RDMA_CLIENT_RECV_ID = 2001;
    public static final int RDMA_SERVER_SEND_ID = 1000;
    public static final int RDMA_SERVER_RECV_ID = 1001;

    private static AtomicInteger workRequestIdGenerator = new AtomicInteger(1000);

    public static int nextId(){
        return workRequestIdGenerator.incrementAndGet();
    }

    public static int bufferSize(){
        return BUFFER_SIZE;
    }

}
