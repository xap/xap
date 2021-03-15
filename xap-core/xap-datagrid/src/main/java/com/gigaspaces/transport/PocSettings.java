/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.transport;

import com.gigaspaces.internal.utils.GsEnv;

public class PocSettings {
    public static final ServerType serverType = Enum.valueOf(ServerType.class, GsEnv.property("com.gs.nio.type").get("lrmi").toUpperCase());
    public static final String host = GsEnv.property("com.gs.nio.host").get("localhost");
    public static final int port = GsEnv.propertyInt("com.gs.nio.port").get(8080);
    public static final boolean directBuffers = GsEnv.propertyBoolean("com.gs.nio.direct-buffers").get(false);
    //public static final boolean customMarshal = GsEnv.propertyBoolean("com.gs.nio.custom-marshal").get(true);
    public static final boolean clientConnectionPoolDynamic = GsEnv.propertyBoolean("com.gs.nio.client.connection-pool.dynamic").get(false);
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
                "client.connection-pool.dynamic: " + clientConnectionPoolDynamic + ", " +
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
