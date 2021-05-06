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

package com.gigaspaces.lrmi.nio.selector;

import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.nio.Pivot;
import com.gigaspaces.lrmi.nio.selector.handler.AcceptSelectorThread;
import com.gigaspaces.lrmi.nio.selector.handler.ReadSelectorThread;
import com.gigaspaces.lrmi.nio.selector.handler.WriteSelectorThread;
import com.j_spaces.kernel.ManagedRunnable;
import com.j_spaces.kernel.SystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.StringTokenizer;

/**
 * Event queue for I/O events raised by a selector. This class receives the lower level events
 * raised by a Selector and dispatches them to the appropriate handler. The SelectorThread class
 * performs a similar task. In particular:
 *
 * - Listens for connection requests, accepts them and creates new connections in the Pivot Channels
 * Table.
 *
 * - Selects a set of channels available for read. If a channel is available for read, it creates a
 * bus packet with the channel info and dispatches it to a worker thread or handling.
 *
 * @author Guy Korland
 * @since 6.0.4, 6.5
 **/
@com.gigaspaces.api.InternalApi
public class SelectorManager extends ManagedRunnable {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);

    private final ServerSocket _serverSocket;
    private final String _host;
    private final int _port;
    private final ReadSelectorThread[] _readSelectorThread;
    private final WriteSelectorThread[] _writeSelectorThread;
    private final AcceptSelectorThread _acceptSelectorThread;

    public SelectorManager(Pivot pivot, String hostName, String port, int readSelectorThreads) throws IOException {
        _host = hostName;
        _readSelectorThread = new ReadSelectorThread[readSelectorThreads];
        _writeSelectorThread = new WriteSelectorThread[readSelectorThreads];
        try {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            _serverSocket = serverSocketChannel.socket();
            _logger.debug("Binding to {} at {}", port, hostName);
            _port = bind(_serverSocket, hostName, port);


            for (int i = 0; i < readSelectorThreads; ++i) {
                _readSelectorThread[i] = new ReadSelectorThread(pivot, "LRMI-Selector-Read-Thread-" + i);
                _writeSelectorThread[i] = new WriteSelectorThread(pivot, "LRMI-Selector-Write-Thread-" + i);
            }

            _acceptSelectorThread = new AcceptSelectorThread(this, "LRMI-Selector-Accept-Thread-" + _port,
                    serverSocketChannel);
            _logger.info("Listening to incoming connections on {} (reader threads: {}, writer threads: {})",
                    getBindInetSocketAddress(), readSelectorThreads, readSelectorThreads);
        } catch (IOException e) {
            waitWhileFinish();
            throw e;
        }
    }

    private static int bind(ServerSocket serverSocket, String host, String port) throws IOException {
        int backlog = Integer.getInteger(SystemProperties.LRMI_ACCEPT_BACKLOG, SystemProperties.LRMI_ACCEPT_BACKLOG_DEFUALT);

        StringTokenizer st = new StringTokenizer(port, ",");
        while (st.hasMoreTokens()) {
            String portToken = st.nextToken().trim();
            int rangeSeparatorPos = portToken.indexOf('-');
            if (rangeSeparatorPos == -1) {
                if (tryBind(serverSocket, host, Integer.parseInt(portToken.trim()), backlog))
                    return serverSocket.getLocalPort();
            } else {
                int startPort = Integer.parseInt(portToken.substring(0, rangeSeparatorPos).trim());
                int endPort = Integer.parseInt(portToken.substring(rangeSeparatorPos + 1).trim());
                if (endPort < startPort) {
                    throw new IllegalArgumentException("Start port [" + startPort + "] must be greater than end port [" + endPort + "]");
                }
                for (int i = startPort; i <= endPort; i++) {
                    if (tryBind(serverSocket, host, i, backlog))
                        return serverSocket.getLocalPort();
                }
            }
        }
        throw new IOException("Failed to bind to port [" + port + "] on host [" + host + "] - " +
                "a different port or ports range can be configured using the '" + GsEnv.keyOrDefault(NIOConfiguration.BIND_PORT_ENV_VAR) +
                "' environment variable or the '" + NIOConfiguration.BIND_PORT_SYS_PROP + "' system property.");
    }

    private static boolean tryBind(ServerSocket serverSocket, String host, int port, int backlog) {
        try {
            serverSocket.bind(new InetSocketAddress(host, port), backlog);
            return true;
        } catch (IOException e) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Failed to bind to port [" + port + "] on host [" + host + "]", e);
            }
            return false;
        }
    }

    @Override
    protected void waitWhileFinish() {
        if (_serverSocket != null) {
            try {
                _serverSocket.close();
            } catch (IOException ex) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Error while closing the server socket.", ex);
                }
            }
        }

        // accept handler
        if (_acceptSelectorThread != null)
            _acceptSelectorThread.requestShutdown();

        // close readers
        for (ReadSelectorThread selectorThread : _readSelectorThread) {
            if (selectorThread != null)
                selectorThread.requestShutdown();
        }

        // close writers
        for (WriteSelectorThread selectorThread : _writeSelectorThread) {
            if (selectorThread != null)
                selectorThread.requestShutdown();
        }
    }

    public int getPort() {
        return _port;
    }

    public String getHostName() {
        return _host;
    }

    public InetSocketAddress getBindInetSocketAddress() {
        return (InetSocketAddress) _serverSocket.getLocalSocketAddress();
    }

    public ReadSelectorThread getReadHandler(SelectableChannel channel) {
        return getHandler(_readSelectorThread, channel);
    }

    public WriteSelectorThread getWriteHandler(SelectableChannel channel) {
        return getHandler(_writeSelectorThread, channel);
    }

    private static <T> T getHandler(T[] array, SelectableChannel channel) {
        //Uses hasCode() as a way to get random value, uses identityHashCode to make sure the value won't be negative
        return array[Math.abs(System.identityHashCode(channel) % array.length)];
    }
}
