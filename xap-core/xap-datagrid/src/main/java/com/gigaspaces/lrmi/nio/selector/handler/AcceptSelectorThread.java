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

import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.lrmi.nio.Pivot;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.StringTokenizer;


/**
 * Handle accept events from selector.
 *
 * @author Guy Korland
 * @since 6.0.4, 6.5
 */
@com.gigaspaces.api.InternalApi
public class AcceptSelectorThread extends AbstractSelectorThread {
    final private ReadSelectorThread[] _readHandlers;
    final private ServerSocket _serverSocket;

    final private int port;
    final private String _hostName;

    public AcceptSelectorThread(Pivot pivot, ReadSelectorThread[] readHandlers, String hostName, String port) throws IOException {
        super(pivot);
        _readHandlers = readHandlers;
        _hostName = hostName;

        // Create the socket listener
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();

        _serverSocket = serverSocketChannel.socket();

		/*
         * If port number of <code>zero</code> will let the system pick up an
		 * ephemeral port in a <code>bind</code> operation.
		 */
        int backlog = Integer.getInteger(SystemProperties.LRMI_ACCEPT_BACKLOG,
                SystemProperties.LRMI_ACCEPT_BACKLOG_DEFUALT);


        int portNumber = -1;
        boolean bounded = false;
        StringTokenizer st = new StringTokenizer(port, ",");
        while (st.hasMoreTokens() && !bounded) {
            String portToken = st.nextToken().trim();
            int index = portToken.indexOf('-');
            if (index == -1) {
                portNumber = Integer.parseInt(portToken.trim());
                try {
                    _serverSocket.bind(new InetSocketAddress(hostName, portNumber), backlog);
                    bounded = true;
                } catch (IOException e) {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Failed to bind to port [" + portNumber + "] on host [" + hostName + "]", e);
                    }
                    continue;
                }
            } else {
                int startPort = Integer.parseInt(portToken.substring(0, index).trim());
                int endPort = Integer.parseInt(portToken.substring(index + 1).trim());
                if (endPort < startPort) {
                    throw new IllegalArgumentException("Start port [" + startPort + "] must be greater than end port [" + endPort + "]");
                }
                for (int i = startPort; i <= endPort; i++) {
                    portNumber = i;
                    try {
                        _serverSocket.bind(new InetSocketAddress(hostName, portNumber), backlog);
                        bounded = true;
                        break;
                    } catch (IOException ex) {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("Failed to bind to port [" + portNumber + "] on host [" + hostName + "]", ex);
                        }
                        continue;
                    }
                }
            }
        }
        if (!bounded) {
            throw new IOException("Failed to bind to port [" + port + "] on host [" + hostName + "]");
        }
        this.port = _serverSocket.getLocalPort();

        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(getSelector(), SelectionKey.OP_ACCEPT);
    }

    public int getPort() {
        return this.port;
    }

    public String getHostName() {
        return this._hostName;
    }

    @Override
    protected void handleConnection(SelectionKey key) throws IOException,
            InterruptedException {
        if (key.isAcceptable()) {
            handleAccept(key);
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel channel = server.accept();

        if (channel != null) {
            channel.configureBlocking(false);
            LRMIUtilities.initNewSocketProperties(channel);

            //Uses hasCode() as a way to get random value, uses identityHashCode to make sure the value won't be negative
            ReadSelectorThread handler = _readHandlers[Math.abs(System.identityHashCode(channel) % _readHandlers.length)];
            handler.createKey(channel);
        }
    }

    @Override
    protected void enableSelectionKeys() {
        /* Empty implementation only one key should be registered on this selector. */
    }


    @Override
    protected void waitWhileFinish() {
        super.waitWhileFinish();

        try {
            // close the connection of ServerSocket
            _serverSocket.close();
        } catch (IOException ex) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Error while closing the server socket.", ex);
            }
        }
    }

    public InetSocketAddress getBindInetSocketAddress() {
        return (InetSocketAddress) _serverSocket.getLocalSocketAddress();
    }
}
