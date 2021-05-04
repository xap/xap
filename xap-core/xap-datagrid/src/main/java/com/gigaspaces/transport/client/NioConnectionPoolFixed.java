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

public class NioConnectionPoolFixed implements NioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(NioConnectionPoolFixed.class);

    private final NioChannel[] pool;

    public NioConnectionPoolFixed(InetSocketAddress address) {
        this(address, PocSettings.clientConnectionPoolSize, 10_000);
    }

    public NioConnectionPoolFixed(InetSocketAddress address, int capacity, int connectionTimeout) {
        this.pool = new NioChannel[capacity];
        for (int i = 0; i < capacity; i++) {
            pool[i] = new NioChannel(createChannel(address, connectionTimeout));
        }
    }

    public NioChannel acquire() throws IOException {
        for (NioChannel channel : pool) {
            if (channel.tryAcquire())
                return channel;
        }
        throw new IOException("No free channel");
    }

    public void release(NioChannel channel) {
        channel.release();
    }

    @Override
    public void close() {
        for (NioChannel channel : pool) {
            if (channel != null)
                closeSilently(channel);
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
