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
import com.gigaspaces.lrmi.nio.Pivot;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * selector thread that deals with server side writes that could not complete in one go.
 *
 * @author asy ronen
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class WriteSelectorThread extends AbstractSelectorThread {

    private final Pivot _pivot;
    private final Queue<ChannelEntry> _keysToCreate = new ConcurrentLinkedQueue<ChannelEntry>();
    private final AtomicInteger _keysToCreateCounter = new AtomicInteger();

    public WriteSelectorThread(Pivot pivot, String name) throws IOException {
        super();
        this._pivot = pivot;
        GSThread.daemon(this, name).start();
    }

    @Override
    protected void enableSelectionKeys() {
        int size = _keysToCreateCounter.get();
        for (int i = 0; i < size; i++) {
            ChannelEntry channelEntry = _keysToCreate.poll();
            _keysToCreateCounter.decrementAndGet();
            SocketChannel channel = channelEntry.getSocketChannel();
            try {
                channelEntry._writeSelectionKey = register(channel, SelectionKey.OP_WRITE);
            } catch (ClosedChannelException e) {
                // what can you do ?
            }
        }
    }

    @Override
    protected void handleConnection(SelectionKey key) throws IOException, InterruptedException {
        if (key.isWritable()) {
            handleWrite(key);
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));

        SocketChannel channel = (SocketChannel) key.channel();
        ChannelEntry channelEntry = _pivot.getChannelEntryFromChannel(channel);

        if (channelEntry != null) {
            channelEntry.onWriteEvent();
        }
    }

    public void setWriteInterest(ChannelEntry channelEntry) {
        if (channelEntry != null) {
            if (channelEntry._writeSelectionKey == null) {
                _keysToCreate.add(channelEntry);
                _keysToCreateCounter.incrementAndGet();
                getSelector().wakeup();
            } else {
                final SelectionKey key = channelEntry._writeSelectionKey;
                try {
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                } catch (CancelledKeyException e) {
                    if (_logger.isWarnEnabled())
                        _logger.warn("exception caught while setting write interest", e);
                    //In case we get cancelled key exception close this channel and let the upper layer recover
                    closeChannel(channelEntry.getSocketChannel());
                }
            }
        }
    }

    public void removeWriteInterest(SelectionKey key) {
        if (key != null) {
            key.cancel();
        }
    }
}
