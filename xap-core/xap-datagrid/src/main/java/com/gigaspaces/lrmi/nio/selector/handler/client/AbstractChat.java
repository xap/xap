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

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/**
 * Created by Barak Bar Orion 12/30/14.
 */
public abstract class AbstractChat<T> {
    protected Conversation conversation;

    /**
     * @return true if done with this chat
     */
    public abstract boolean init(Conversation conversation, SelectionKey key);

    /**
     * @return true if done with this chat
     */
    public abstract boolean process(SelectionKey key);

    public abstract T result();

    protected void removeInterest(SelectionKey key, int op) {
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & op) != 0) {
            key.interestOps(interestOps & ~op);
        }
    }

    protected void addInterest(SelectionKey key, int op) {
        if (!key.isValid()) {
            return;
        }
        final int interestOps = key.interestOps();
        if ((interestOps & op) == 0) {
            key.interestOps(interestOps | op);
        }
    }

    public byte write(SelectionKey key, ByteBuffer msg) {
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            channel.write(msg);
        } catch (Throwable t) {
            key.cancel(); //ensure key is cancelled on the selector before socket.close()
            conversation.close(t);
            return -1; //close chat, write error
        }
        if (msg.remaining() == 0) {
            removeInterest(key, SelectionKey.OP_WRITE);
            return 1; //done with write
        } else {
            addInterest(key, SelectionKey.OP_WRITE);
            return 0; //keep chat open
        }
    }
}
