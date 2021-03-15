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
package com.gigaspaces.transport.server;

import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.transport.PocSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class NioServer implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(NioServer.class);
    private final SpaceImpl space;
    private final ServerSocketChannel ssc;
    private final AcceptMonitor acceptMonitor;
    private final ReadMonitor[] readMonitors;
    private int nextReader;

    public NioServer(SpaceImpl space) throws IOException {
        this(space, new InetSocketAddress(PocSettings.host, PocSettings.port), PocSettings.serverReaderPoolSize);
    }
    public NioServer(SpaceImpl space, InetSocketAddress address, int readersPoolSize) throws IOException {
        logger.info("Creating NioServer (address {}, readers: {}, lrmiExecutor: {}", address, readersPoolSize, PocSettings.serverLrmiExecutor);
        this.space = space;

        ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ssc.socket().bind(address);

        this.readMonitors = new ReadMonitor[readersPoolSize];
        for (int i = 0; i < readMonitors.length; i++) {
            readMonitors[i] = new ReadMonitor(this, i);
        }

        acceptMonitor = new AcceptMonitor(this);
        acceptMonitor.register(ssc, SelectionKey.OP_ACCEPT, null);
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing");
        // Close accept monitor first to stop readers from receiving new channels:
        acceptMonitor.close();
        // Close readers:
        for (SelectMonitor readMonitor : readMonitors) {
            readMonitor.close();
        }
        ssc.close();
    }

    public SocketChannel accept() throws IOException {
        return ssc.accept();
    }

    public SelectMonitor getNextReadMonitor() {
        SelectMonitor result = readMonitors[nextReader++];
        if (nextReader == readMonitors.length)
            nextReader = 0;
        return result;
    }

    public SpaceImpl getSpace() {
        return space;
    }
}
