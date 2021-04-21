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
package com.gigaspaces.transport.client;

import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class NioConnectionPoolSingleton implements NioConnectionPool {

    private final InetSocketAddress address;
    private final int connectionTimeout;
    private NioChannel instance;

    public NioConnectionPoolSingleton() {
        this(new InetSocketAddress(PocSettings.host, PocSettings.port), 10_000);
    }

    public NioConnectionPoolSingleton(InetSocketAddress address, int connectionTimeout) {
        this.address = address;
        this.connectionTimeout = connectionTimeout;
    }

    public NioChannel acquire() throws IOException {
        if (instance == null)
            instance = new NioChannel(createChannel(address, connectionTimeout));
        return instance;
    }

    public void release(NioChannel channel) {
    }

    @Override
    public void close() throws IOException {
        instance.close();
    }

    private SocketChannel createChannel(InetSocketAddress serverAddress, int connectionTimeout) {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            //LRMIUtilities.initNewSocketProperties(socketChannel);
            socketChannel.socket().connect(serverAddress, connectionTimeout);
            return socketChannel;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
