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

import java.io.IOException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Barak Bar Orion 12/29/14.
 */
@com.gigaspaces.api.InternalApi
public class ClientConversationRunner implements Runnable {
    private static final Logger logger = Logger.getLogger(Constants.LOGGER_LRMI);
    private static final long SELECT_TIMEOUT = Long.getLong("com.gs.lrmi.nio.selector.select-timeout", 10000L);

    private final Selector selector;
    final private Queue<Conversation> registrationConversations = new ConcurrentLinkedQueue<Conversation>();

    public ClientConversationRunner() throws IOException {
        selector = SelectorProvider.provider().openSelector();
    }

    public SettableFuture<Conversation> addConversation(Conversation conversation) {
        registrationConversations.add(conversation);
        selector.wakeup();
        return conversation.future();
    }

    @Override
    public void run() {
        while (selector.isOpen()) {
            doSelect();
        }
    }

    private void doSelect() {
        SelectionKey key = null;
        try {
            addNewRegistrations();
            selector.select(SELECT_TIMEOUT);
            Set<SelectionKey> keys = selector.selectedKeys();
            if (keys == null || keys.isEmpty()) {
                return;
            }
            Iterator<SelectionKey> iterator = keys.iterator();
            while (iterator.hasNext()) {
                key = iterator.next();
                iterator.remove();
                Conversation conversation = (Conversation) key.attachment();
                conversation.handleKey(key);
            }
        } catch (ClosedSelectorException ex) {
            logger.log(Level.FINER, "Selector was closed.", ex);
            if (key != null) {
                key.cancel();
            }
        } catch (Throwable t) {
            logger.log(Level.SEVERE, "exception in main selection loop", t);
            if (key != null) {
                key.cancel();
            }
        }

    }

    private void addNewRegistrations() {
        if (registrationConversations.isEmpty()) {
            return;
        }
        Iterator<Conversation> iterator = registrationConversations.iterator();
        while (iterator.hasNext()) {
            Conversation conversation = iterator.next();
            try {
                conversation.channel().register(selector, SelectionKey.OP_CONNECT, conversation);
            } catch (Throwable t) {
                //find key and cancel it
                SelectionKey key = conversation.channel().keyFor(selector);
                if (key != null) {
                    key.cancel(); //ensure key is cancelled on the selector before socket.close()
                }
                conversation.close(t);
            } finally {
                iterator.remove();
            }
        }
    }

}
