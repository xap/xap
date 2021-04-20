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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public class NioConnectionPoolThreadLocal implements NioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(NioConnectionPoolThreadLocal.class);

    private final InetSocketAddress serverAddress;
    private final ThreadLocal<NioChannel> threadLocal;
    private final int connectionTimeout;

    public NioConnectionPoolThreadLocal() {
        this(new InetSocketAddress(PocSettings.host, PocSettings.port), 10_000);
    }

    public NioConnectionPoolThreadLocal(InetSocketAddress address, int connectionTimeout) {
        this.serverAddress = address;
        this.connectionTimeout = connectionTimeout;
        threadLocal = ThreadLocal.withInitial(() -> new NioChannel(createChannel()));
    }

    public NioChannel acquire() throws IOException {
        return threadLocal.get();
    }

    public void release(NioChannel channel) {
    }

    @Override
    public void close() throws IOException {
        threadLocal.get().close();
        threadLocal.remove();
    }

    private SocketChannel createChannel()  {
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

    private void closeSilently(NioChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warn("Failed to close socket channel", e);
        }
    }
}
