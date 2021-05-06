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
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.lrmi.nio.selector.SelectorManager;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * Handle accept events from selector.
 *
 * @author Guy Korland
 * @since 6.0.4, 6.5
 */
@com.gigaspaces.api.InternalApi
public class AcceptSelectorThread extends AbstractSelectorThread {
    private final SelectorManager selectorManager;

    public AcceptSelectorThread(SelectorManager selectorManager, String name, ServerSocketChannel serverSocketChannel)
            throws IOException {
        super();
        this.selectorManager = selectorManager;
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(getSelector(), SelectionKey.OP_ACCEPT);
        GSThread.daemon(this, name).start();
    }

    @Override
    protected void handleConnection(SelectionKey key) throws IOException, InterruptedException {
        if (key.isAcceptable()) {
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel channel = server.accept();

            if (channel != null) {
                channel.configureBlocking(false);
                LRMIUtilities.initNewSocketProperties(channel);
                ReadSelectorThread handler = selectorManager.getReadHandler(channel);
                handler.createKey(channel);
            }
        }
    }

    @Override
    protected void enableSelectionKeys() {
        /* Empty implementation only one key should be registered on this selector. */
    }
}
