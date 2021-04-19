package com.gigaspaces.transport;

import com.gigaspaces.internal.utils.GsEnv;

public class PocSettings {
    public static final ServerType serverType = Enum.valueOf(ServerType.class, GsEnv.property("com.gs.nio.type").get("lrmi").toUpperCase());
    public static final String host = GsEnv.property("com.gs.nio.host").get("localhost");
    public static final int port = GsEnv.propertyInt("com.gs.nio.port").get(8080);
    public static final boolean directBuffers = GsEnv.propertyBoolean("com.gs.nio.direct-buffers").get(false);
    public static final boolean directExternalizable = GsEnv.propertyBoolean("com.gs.nio.direct-externalizable").get(false);
    public static final boolean directVersion = GsEnv.propertyBoolean("com.gs.nio.direct-version").get(false);
    public static final boolean cacheRequest = GsEnv.propertyBoolean("com.gs.nio.cache-request").get(false);
    public static final boolean cacheResponse = GsEnv.propertyBoolean("com.gs.nio.cache-response").get(false);
    //public static final boolean customMarshal = GsEnv.propertyBoolean("com.gs.nio.custom-marshal").get(true);
    public static final String clientConnectionPoolType = GsEnv.property("com.gs.nio.client.connection-pool.type").get("thread-local");
    public static final int clientConnectionPoolSize = GsEnv.propertyInt("com.gs.nio.client.connection-pool.size").get(4);
    public static final int serverReaderPoolSize = GsEnv.propertyInt("com.gs.nio.server.reader-pool-size").get(4);
    public static final boolean serverLrmiExecutor = GsEnv.propertyBoolean("com.gs.nio.server.lrmi-executor").get(true);

    public static boolean isEnabled() {
        return serverType != ServerType.LRMI;
    }

    public static String dump() {
        return "host: " + host + ", " +
                "port: " + port + ", " +
                "direct-buffers: " + directBuffers + ", " +
                //"custom-marshal: " + customMarshal + ", " +
                "client.connection-pool.type: " + clientConnectionPoolType + ", " +
                "client.connection-pool.size: " + clientConnectionPoolSize + ", " +
                "server.reader-pool-size: " + serverReaderPoolSize;
    }

    public enum ServerType {
        LRMI,
        NIO,
        NETTY,
        NETTY_NIO,
        NETTY_EPOLL,
        NETTY_IOURING,
    }
}
