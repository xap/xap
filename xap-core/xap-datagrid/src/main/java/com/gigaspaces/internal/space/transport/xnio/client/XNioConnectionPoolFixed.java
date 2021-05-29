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
package com.gigaspaces.internal.space.transport.xnio.client;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.space.transport.xnio.XNioChannel;
import com.gigaspaces.internal.space.transport.xnio.XNioSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public class XNioConnectionPoolFixed implements XNioConnectionPool {
    private static final Logger logger = LoggerFactory.getLogger(XNioConnectionPoolFixed.class);

    private final XNioChannel[] pool;

    public XNioConnectionPoolFixed(InetSocketAddress address) {
        this(address, XNioSettings.CLIENT_CONNECTION_POOL_SIZE, 10_000);
    }

    public XNioConnectionPoolFixed(InetSocketAddress address, int capacity, int connectionTimeout) {
        this.pool = new XNioChannel[capacity];
        for (int i = 0; i < capacity; i++) {
            pool[i] = new XNioChannel(createChannel(address, connectionTimeout));
        }
    }

    public XNioChannel acquire() throws IOException {
        for (XNioChannel channel : pool) {
            if (channel.tryAcquire())
                return channel;
        }
        throw new IOException("No free channel");
    }

    public void release(XNioChannel channel) {
        channel.release();
    }

    @Override
    public void close() {
        for (XNioChannel channel : pool) {
            if (channel != null)
                closeSilently(channel);
        }
    }

    private void closeSilently(XNioChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            logger.warn("Failed to close socket channel", e);
        }
    }
}
