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
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

public class NioConnectionPoolDynamic implements NioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(NioConnectionPoolDynamic.class);

    private final InetSocketAddress serverAddress;
    private final Queue<NioChannel> pool;
    private final int connectionTimeout;

    public NioConnectionPoolDynamic() {
        this(new InetSocketAddress(PocSettings.host, PocSettings.port), PocSettings.clientConnectionPoolSize, 10_000);
    }

    public NioConnectionPoolDynamic(InetSocketAddress address, int capacity, int connectionTimeout) {
        this.serverAddress = address;
        this.pool = new LinkedBlockingDeque<>(capacity);
        this.connectionTimeout = connectionTimeout;
    }

    public NioChannel acquire() throws IOException {
        NioChannel result = pool.poll();
        if (result == null) {
            logger.debug("No pooled resource - creating a new one");
            result = new NioChannel(createChannel(serverAddress, connectionTimeout));
        }
        return result;
    }

    public void release(NioChannel channel) {
        if (!pool.offer(channel)) {
            logger.debug("Resource pool is full - closing released resource");
            closeSilently(channel);
        }
    }

    @Override
    public void close() {
        while (!pool.isEmpty()) {
            NioChannel connection = pool.poll();
            if (connection != null)
                closeSilently(connection);
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
