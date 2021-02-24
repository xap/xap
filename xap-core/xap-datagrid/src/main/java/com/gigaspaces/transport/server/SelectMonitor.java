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

import com.gigaspaces.internal.utils.concurrent.GSThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class SelectMonitor implements Runnable, Closeable {
    protected final Logger logger;
    private final String name;
    private final Selector selector;
    private final Queue<RegistrationRequest> pendingRegistrations = new LinkedBlockingQueue<>();
    private Thread thread;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile boolean closed;

    public SelectMonitor(String name) throws IOException {
        this.logger = LoggerFactory.getLogger(SelectMonitor.class + "." + name);
        this.name = name;
        this.selector = Selector.open();
    }

    public void register(SelectableChannel channel, int ops, Object attachment) throws ClosedChannelException {
        // Registrations are queued and handled on the selector monitoring thread to avoid broken concurrency issues.
        pendingRegistrations.add(new RegistrationRequest(channel, ops, attachment));
        if (started.compareAndSet(false, true)) {
            logger.info("Starting selector monitor thread");
            this.thread = new GSThread(this, "nio-" + name);
            this.thread.setDaemon(true);
            this.thread.start();
        }
    }

    @Override
    public void run() {
        while (!closed) {
            if (!pendingRegistrations.isEmpty())
                processRegistrationRequests();
            try {
                if (selector.select(1000) != 0) {
                    if (logger.isDebugEnabled())
                        logger.debug("Ready keys: {}", selector.selectedKeys().size());
                    Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                    while (it.hasNext()) {
                        SelectionKey key = it.next();
                        it.remove();
                        try {
                            onReady(key);
                        } catch (IOException e) {
                            if (e.getMessage().equals("An existing connection was forcibly closed by the remote host")) {
                                logger.warn("Channel {} is closed, cancelling key", key.channel(), e);
                                key.cancel();
                            } else {
                                logger.error("Failed to process key from channel {}", key.channel(), e);
                            }
                        }
                    }
                }
            } catch (IOException e) {
                logger.warn("Error while monitoring selected keys", e);
            }
        }
        // Cleanup keys:
        for (SelectionKey key : selector.keys()) {
            Object attachment = key.attach(null);
            if (attachment instanceof Closeable) {
                try {
                    ((Closeable) attachment).close();
                } catch (IOException e) {
                    logger.warn("Failed to close attachment", e);
                }
            }
        }
    }

    private void processRegistrationRequests() {
        RegistrationRequest request;
        while ((request = pendingRegistrations.poll()) != null) {
            try {
                request.channel.register(selector, request.ops, request.attachment);
                logger.info("Registered {} for {}", request.channel, request.ops);
            } catch (ClosedChannelException e) {
                logger.warn("Failed to register channel {} for selection with ops {}", request.channel, request.ops, e);
            }
        }
    }

    protected abstract void onReady(SelectionKey key) throws IOException;

    @Override
    public void close() throws IOException {
        // signal thread to stop polling for select events
        closed = true;
        selector.wakeup();
        // wait for thread to complete cleanup
        try {
            thread.join();
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for selector thread to close");
            Thread.currentThread().interrupt();
        }
        // Close selector
        selector.close();
    }

    private static class RegistrationRequest {
        public final SelectableChannel channel;
        public final int ops;
        public final Object attachment;

        private RegistrationRequest(SelectableChannel channel, int ops, Object attachment) {
            this.channel = channel;
            this.ops = ops;
            this.attachment = attachment;
        }
    }
}
