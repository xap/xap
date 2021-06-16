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
package com.gigaspaces.internal.space.transport.xnio.client;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.space.transport.xnio.XNioChannel;
import com.gigaspaces.internal.space.transport.xnio.XNioSettings;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public interface XNioConnectionPool extends Closeable {
    XNioChannel acquire() throws IOException;

    void release(XNioChannel channel);

    default SocketChannel createChannel(InetSocketAddress serverAddress, int connectionTimeout) {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            //LRMIUtilities.initNewSocketProperties(socketChannel);
            XNioSettings.initSocketChannel(socketChannel);
            socketChannel.socket().connect(serverAddress, connectionTimeout);
            return socketChannel;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
