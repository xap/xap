package com.gigaspaces.lrmi.rdma;

import java.util.concurrent.atomic.AtomicInteger;

public class RdmaConstants {
    public static boolean ENABLED = Boolean.getBoolean("com.gs.rdma.enabled");
    public static final int MAX_INCOMMING_REQUESTS = 1000;
    public static final int BUFFER_SIZE = 1000;
    public static final int RDMA_CONNECT_TIMEOUT = 5000;
    public static final int RDMA_SYNC_OP_TIMEOUT = 5000;
    private static AtomicInteger workRequestIdGenerator = new AtomicInteger(1000);

    public static int nextId(){
        return workRequestIdGenerator.incrementAndGet();
    }

}
