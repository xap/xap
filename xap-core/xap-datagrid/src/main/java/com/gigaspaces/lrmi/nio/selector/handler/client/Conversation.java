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

package com.gigaspaces.lrmi.nio.selector.handler.client;

import com.gigaspaces.async.SettableFuture;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIUtilities;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by Barak Bar Orion 12/30/14.
 */
@com.gigaspaces.api.InternalApi
public class Conversation {
    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(Constants.LOGGER_LRMI);

    private final SocketChannel channel;
    private final SettableFuture<Conversation> future;
    private final List<AbstractChat> chats;
    private AbstractChat currentChat;

    public Conversation(InetSocketAddress address) throws IOException {
        future = new SettableFuture<Conversation>();
        channel = SocketChannel.open();
        chats = new ArrayList<AbstractChat>();
        try {
            channel.configureBlocking(false);
            LRMIUtilities.initNewSocketProperties(channel);
            channel.connect(address);
        } catch (Exception e) {
            channel.close();
            throw new IOException(e);
        }
    }

    public SocketChannel channel() {
        return channel;
    }

    public SettableFuture<Conversation> future() {
        return future;
    }

    public void close(Throwable cause) {
        if (!future.isDone()) {
            future.setResult(cause);
        }
        try {
            channel.close();
        } catch (Throwable ignored) {
        }
    }

    public void handleKey(SelectionKey key) {
        if (!key.channel().isOpen()) {
            key.cancel(); //ensure key is cancelled when channel is closed
        }
        if (!key.isValid()) {
            canceledKey();
            return;
        }
        if (key.isConnectable()) {
            int ops = key.interestOps();
            ops &= ~SelectionKey.OP_CONNECT;
            key.interestOps(ops);
            finishConnection(key);
        }
        processChats(key);
    }

    private void processChats(SelectionKey key) {
        while (true) {
            if (chats.isEmpty() && currentChat == null) {
                key.cancel();
                future.setResult(this);
                return;
            }
            boolean chatDone;
            if (currentChat == null) {
                currentChat = chats.remove(0);
                chatDone = currentChat.init(this, key);
            } else {
                chatDone = currentChat.process(key);
            }
            if (chatDone) {
                currentChat = null;
            } else {
                return;
            }

        }
    }

    private void canceledKey() {
        if (!future.isDone()) {
            future.setResult(new ClosedChannelException());
        }
    }

    private void finishConnection(SelectionKey key) {
        SocketChannel socketChannel = (SocketChannel) key.channel();

        // Finish the connection. If the connection operation failed
        // this will raise an IOException.
        try {
            socketChannel.finishConnect();
        } catch (Throwable t) {
            key.cancel();
            close(t);
        }
    }

    public void addChat(AbstractChat chat) {
        chats.add(chat);
    }
}
