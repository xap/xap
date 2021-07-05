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
package com.gigaspaces.internal.space.transport.xnio.server;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.config.ConfigurationException;
import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.space.transport.xnio.XNioChannel;
import com.gigaspaces.internal.space.transport.xnio.XNioSettings;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.ProtocolAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Experimental Nio (xnio) server to execute operations more efficently than the lrmi execution.
 * This is an experimental partial implementation which should not be used for production - it's only intended to show
 * the potential performance gain from switching to a faster nio implementation.
 *
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public class XNioServer implements Closeable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ServerSocketChannel ssc;
    private final SelectorHandler boss;
    private final SelectorHandler[] workers;
    private final SpaceImpl space;
    private int nextWorker;

    public static XNioServer create(SpaceImpl space) {
        try {
            ProtocolAdapter<?> protocolAdapter = LRMIRuntime.getRuntime().getProtocolRegistry().get(NIOConfiguration.PROTOCOL_NAME);
            if (protocolAdapter == null) {
                try {
                    LRMIRuntime.getRuntime().initServerSide();
                } catch (ConfigurationException e) {
                    throw new IOException(e);
                }
                protocolAdapter = LRMIRuntime.getRuntime().getProtocolRegistry().get(NIOConfiguration.PROTOCOL_NAME);
            }
            InetSocketAddress bindAddress = XNioSettings.getXNioBindAddress(protocolAdapter.getHostName(), protocolAdapter.getPort());
            return new XNioServer(space, bindAddress);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private XNioServer(SpaceImpl space, InetSocketAddress bindAddress) throws IOException {
        int numOfWorkers = XNioSettings.SERVER_IO_THREADS;
        logger.info("Listening to incoming connections at {} (I/O workers: {})", bindAddress, numOfWorkers);
        this.space = space;
        ssc = ServerSocketChannel.open();
        ssc.configureBlocking(false);
        ssc.socket().bind(bindAddress);
        boss = new SelectorHandler("boss");
        workers = numOfWorkers != -1 ? initWorkers(numOfWorkers) : new SelectorHandler[] {boss};
        ssc.register(boss.selector, SelectionKey.OP_ACCEPT);
        initDaemon(boss, "boss");
    }

    @Override
    public void close() throws IOException {
        // TODO: Implement close
    }

    private SelectorHandler[] initWorkers(int numOfWorkers) throws IOException {
        SelectorHandler[] result = new SelectorHandler[numOfWorkers];
        for (int i = 0; i < numOfWorkers; i++) {
            String name = "worker-" + i;
            result[i] = new SelectorHandler(name);
            initDaemon(result[i], name);
        }
        return result;
    }

    private void initDaemon(Runnable runnable, String name) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(true);
        thread.start();
    }

    private SelectorHandler getWorker() {
        SelectorHandler result = workers[nextWorker++];
        if (nextWorker == workers.length)
            nextWorker = 0;
        return result;
    }

    private void processAccept(SelectionKey key) throws IOException {
        SocketChannel clientSocket = ssc.accept();
        XNioChannel channel = new XNioChannel(clientSocket);
        channel.setReadProcessor(new SpaceReadProcessor(space));
        clientSocket.configureBlocking(false);
        XNioSettings.initSocketChannel(clientSocket);
        getWorker().register(channel);
    }

    private void processRead(SelectionKey key) throws IOException {
        XNioChannel channel = (XNioChannel) key.attachment();
        channel.getReadProcessor().read(channel);
    }

    private class SelectorHandler implements Runnable {
        private final Logger logger;
        private final Selector selector;
        private final BlockingQueue<RegistrationRequest> registrationRequests = new LinkedBlockingQueue<>();

        public SelectorHandler(String name) throws IOException {
            logger = LoggerFactory.getLogger(this.getClass().getName() + "." + name);
            selector = Selector.open();
        }

        public void register(XNioChannel channel) throws ClosedChannelException {
            if (this == boss)
                registerImpl(channel);
            else
                registrationRequests.offer(new RegistrationRequest(channel));
        }

        private void registerImpl(XNioChannel channel) throws ClosedChannelException {
            channel.getSocketChannel().register(selector, SelectionKey.OP_READ, channel);
            logger.info("Added new client {} with processor {}", channel.getSocketChannel(), channel.getReadProcessor().getName());
        }

        @Override
        public void run() {
            try {
                while (true) {
                    while (!registrationRequests.isEmpty()) {
                        RegistrationRequest request = registrationRequests.poll();
                        registerImpl(request.channel);
                    }
                    if (selector.select(100) != 0) {
                        Set<SelectionKey> readySet = selector.selectedKeys();
                        for (Iterator<SelectionKey> it = readySet.iterator(); it.hasNext(); ) {
                            final SelectionKey key = it.next();
                            it.remove();
                            try {
                                if (key.isAcceptable()) {
                                    processAccept(key);
                                } else if (key.isReadable()) {
                                    processRead(key);
                                }
                            } catch (IOException e) {
                                logger.warn("Failed to read from {} - cancelling key", key.channel(), e);
                                key.cancel();
                            }
                        }
                    }
                }
            } catch(IOException e) {
                logger.error("Failed to process selector", e);
            }
        }
    }

    private static class RegistrationRequest {
        private final XNioChannel channel;

        private RegistrationRequest(XNioChannel channel) {
            this.channel = channel;
        }
    }
}
