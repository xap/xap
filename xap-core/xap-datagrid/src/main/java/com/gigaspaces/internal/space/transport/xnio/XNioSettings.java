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
package com.gigaspaces.internal.space.transport.xnio;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.utils.GsEnv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public class XNioSettings {
    // Enable/Disable XNio feature
    public static final boolean ENABLED = GsEnv.propertyBoolean("XNIO_ENABLED").get(false);
    // Determines the number of selector threads used by the server
    public static final int SERVER_IO_THREADS = GsEnv.propertyInt("XNIO_SERVER_IO_THREADS").get(4);
    // Determines if the allocated byte buffers are direct or not
    public static final boolean DIRECT_BUFFERS = GsEnv.propertyBoolean("XNIO_DIRECT_BUFFERS").get(false);
    // Determines the type of the client connection pool
    public static final String CLIENT_CONNECTION_POOL_TYPE = GsEnv.property("XNIO_CLIENT_CONNECTION_POOL_TYPE").get("thread-local");
    // Determines the size of the client connection pool
    public static final int CLIENT_CONNECTION_POOL_SIZE = GsEnv.propertyInt("XNIO_CLIENT_CONNECTION_POOL_SIZE").get(64);

    // Since there's no way for an xnio client to directly discover an xnio server, we're using a temp hack:
    // * Define a port delta (100 by default, compliant with the lrmi port range 8200-8299 width)
    // * The xnio server binds to the lrmi port + delta.
    // * The xnio client knows the lrmi port and adds delta to connect to the xnio server.
    private static final int PORT_DELTA = GsEnv.propertyInt("XNIO_PORT_DELTA").get(100);
    private static final boolean CHANNEL_TCP_NODELAY = GsEnv.propertyBoolean("XNIO_TCP_NODELAY").get(true);

    public static InetSocketAddress getXNioBindAddress(String host, int port) {
        return new InetSocketAddress(host, port + PORT_DELTA);
    }

    public static void initSocketChannel(SocketChannel channel)
            throws IOException {
        channel.setOption(StandardSocketOptions.TCP_NODELAY, CHANNEL_TCP_NODELAY);
    }
}
