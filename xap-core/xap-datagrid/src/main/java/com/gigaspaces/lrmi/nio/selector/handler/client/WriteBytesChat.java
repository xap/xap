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

/**
 * Created by Barak Bar Orion 12/30/14.
 */
@com.gigaspaces.api.InternalApi
public class WriteBytesChat extends AbstractChat<Void> {

    private final ByteBuffer msg;

    public WriteBytesChat(byte[] msg) {
        this.msg = ByteBuffer.wrap(msg);
    }

    /**
     * @return true if done with this chat
     */
    public boolean init(Conversation conversation, SelectionKey key) {
        this.conversation = conversation;
        return process(key);
    }

    /**
     * @return true if done with this chat
     */
    public boolean process(SelectionKey key) {
        return (0 != write(key, msg));
    }

    @Override
    public Void result() {
        return null;
    }
}
