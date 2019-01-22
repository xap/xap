package com.gigaspaces.lrmi.rdma;

public class RdmaConstants {
    public static boolean ENABLED = Boolean.getBoolean("com.gs.rdma.enabled");
    public static int BUFFER_SIZE = 1000;
    public static int RDMA_CONNECT_TIMEOUT = 5000;
    public static int RDMA_SYNC_OP_TIMEOUT = 5000;

}
