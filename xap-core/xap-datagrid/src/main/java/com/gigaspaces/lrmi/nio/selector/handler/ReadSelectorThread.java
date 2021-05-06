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

package com.gigaspaces.lrmi.nio.selector.handler;

import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.lrmi.nio.ChannelEntry;
import com.gigaspaces.lrmi.nio.ChannelEntry.State;
import com.gigaspaces.lrmi.nio.Pivot;
import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.Reader.ProtocolValidationContext;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handle read events from selector.
 *
 * @author Guy Korland
 * @since 6.0.4, 6.5
 */
@com.gigaspaces.api.InternalApi
public class ReadSelectorThread extends AbstractSelectorThread {
    private final Pivot _pivot;

    public ReadSelectorThread(Pivot pivot, String name) throws IOException {
        super();
        this._pivot = pivot;
        GSThread.daemon(this, name).start();
    }

    final private Queue<SelectionKey> _keysToEnable = new ConcurrentLinkedQueue<SelectionKey>();
    final private AtomicInteger _keysToEnableCounter = new AtomicInteger();
    final private Queue<SocketChannel> _keysToCreate = new ConcurrentLinkedQueue<SocketChannel>();
    final private AtomicInteger _keysToCreateCounter = new AtomicInteger();

    @Override
    protected void handleConnection(SelectionKey key) throws IOException, InterruptedException {
        if (key.isReadable()) {
            handleRead(key);
        }
    }

    private void handleRead(SelectionKey key) throws IOException {
        // disable OP_READ on key before doing anything else
        key.interestOps(key.interestOps() & (~SelectionKey.OP_READ));

        SocketChannel channel = (SocketChannel) key.channel();
        ChannelEntry channelEntry = _pivot.getChannelEntryFromChannel(channel);

        // in case the client already disconnected_keysToCreate
        if (channelEntry != null) {
            //Check if we need protocol validation
            if (_pivot.isProtocolValidationEnabled() && !channelEntry.isProtocolValidated()) {
                Reader.ProtocolValidationContext ctx = (ProtocolValidationContext) key.attachment();
                if (ctx == null) {
                    ctx = new ProtocolValidationContext(key);
                    key.attach(ctx);
                }
                _pivot.handleProtocolValidation(channelEntry, ctx, this);
            } else {
                // this will make the Pivot to dispatch a Bus Packet to a worker thread
                Reader.Context ctx = (Reader.Context) key.attachment();
                if (ctx == null) {
                    channelEntry.setChannelState(State.PROGRESS);
                    ctx = new Reader.Context(key);
                    key.attach(ctx);
                }

                _pivot.handleReadRequest(channelEntry, ctx, this);
            }
        }
    }

    @Override
    protected void enableSelectionKeys() {
        int size = _keysToEnableCounter.get();
        for (int i = 0; i < size; i++) {
            SelectionKey selectionKey = _keysToEnable.poll();
            _keysToEnableCounter.decrementAndGet();
            if (selectionKey.isValid()) {
                selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_READ);
            }
        }

        size = _keysToCreateCounter.get();
        for (int i = 0; i < size; i++) {
            SocketChannel channel = _keysToCreate.poll();
            _keysToCreateCounter.decrementAndGet();
            SelectionKey readKey;
            try {
                readKey = register(channel, SelectionKey.OP_READ);
            } catch (ClosedChannelException e) {
                // what can you do ?
                continue;
            }
            _pivot.newConnection(this, readKey);
        }
    }

    /**
     * called after the reading has finished
     */
    public void registerKey(SelectionKey key) {
        if (key == null)
            return;

        // add SelectionKey & Op to list of Ops to enable
        _keysToEnable.add(key);
        _keysToEnableCounter.incrementAndGet();

        // tell the Selector Thread there's some ops to _keysToCreateenable
        // wakeup() will force the SelectorThread to bail out
        // of select() to process your registered request
        getSelector().wakeup();
    }

    /**
     * called after the reading has finished
     */
    public void createKey(SocketChannel channel) {
        // add SelectionKey & Op to list of Ops to create a new key
        _keysToCreate.add(channel);
        _keysToCreateCounter.incrementAndGet();

        // tell the Selector Thread there's some ops to enable
        // wakeup() will force the SelectorThread to bail out
        // of select() to process your registered request
        getSelector().wakeup();
    }
}
