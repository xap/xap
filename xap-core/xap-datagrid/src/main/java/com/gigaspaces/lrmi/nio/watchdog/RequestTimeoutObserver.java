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


package com.gigaspaces.lrmi.nio.watchdog;

import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.LogLevel;
import com.gigaspaces.lrmi.ConnectionResource;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.lrmi.nio.CPeer;
import com.gigaspaces.lrmi.nio.LrmiChannel;
import com.gigaspaces.lrmi.nio.async.AsyncContext;
import com.gigaspaces.lrmi.nio.watchdog.Watchdog.WatchedObject;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RequestTimeoutObserver handles NIO connection timeouts on the client side. If client doesn't
 * receive a reply, it checks the connection with the server and if it is not valid anymore, the
 * connection is closed.
 *
 * @author anna
 * @version 1.0
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class RequestTimeoutObserver
        implements TimeoutObserver {
    protected final static int _INSPECT_TIMEOUT = Integer.getInteger(SystemProperties.WATCHDOG_INSPECT_TIMEOUT, 10000).intValue();

    protected final static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_WATCHDOG);

    private final long _inspectResponseTimeout;

    public RequestTimeoutObserver(long requestTimeout) {
        _inspectResponseTimeout = Long.getLong(SystemProperties.WATCHDOG_INSPECT_RESPONSE_TIMEOUT,
                Math.max(requestTimeout, _INSPECT_TIMEOUT));
    }

    /**
     * bucket can never be empty
     *
     * @see nio.watchdog.Watchdog#add(Map<WatchedObject, Collection<WatchedObject>>, WatchedObject)
     * @see nio.watchdog.TimeoutObserver#timeoutOccured(nio.watchdog.Watchdog.WatchedObject)
     */
    public void timeoutOccured(Collection<WatchedObject> bucket) throws Exception {
        SocketAddress serverAddress = null;

        // Check if the server is still alive
        SocketChannel socketChannel = null;

        long startInvocationVersion = -1;

        try {
            socketChannel = SocketChannel.open();
            LRMIUtilities.initNewSocketProperties(socketChannel);
            Socket newSock = socketChannel.socket();
            WatchedObject watched = bucket.iterator().next();
            startInvocationVersion = watched.getVersion();

            // Test connection to server
            // Open a new socket
            serverAddress = watched.getChannel().getRemoteSocketAddress();
            if (serverAddress == null)
                throw new IOException("Watched socket was already closed: " + watched.getChannel().getSocketDesc());

            final int localPort = watched.getChannel().getLocalPort();

            if (_logger.isDebugEnabled())
                _logger.debug("Attempting to create a new socket to the ServerEndPoint [" + serverAddress + "], local port[" + localPort + "]");

            // for connect
            socketChannel.configureBlocking(true);

            // The RequesetTimeoutObserver and RequestResponseTimeoutObserver are already mixed together and should
            // be refactored. In the meantime, the _responseTimeout parameter only affects
            // handleOpenSocket which only applies to the RequestResponseTimeoutObserver (the RequestTimeoutObserver
            // has an empty implementation for it
            long absoluteTimeout = SystemTime.timeMillis() + _inspectResponseTimeout;

            newSock.connect(serverAddress, _INSPECT_TIMEOUT);
            handleOpenSocket(socketChannel, localPort, absoluteTimeout, watched.getClient());

            if (_logger.isDebugEnabled())
                _logger.debug(getValidConnectionMessage(serverAddress));

            // Reset client time
            watched.startWatch();
        } catch (IOException e) {
            // Connection is not longer valid - close it
            close(bucket, serverAddress, e, startInvocationVersion);
        } finally {
            if (socketChannel != null)
                socketChannel.close();
        }
    }

    /**
     * @param connectionResource the monitored cpeer
     */
    protected void handleOpenSocket(
            SocketChannel socketChannel,
            int watchedObjectLocalPort,
            long absoluteTimeout,
            ConnectionResource connectionResource)
            throws IOException {
    }

    protected String getValidConnectionMessage(SocketAddress serverAddress) {
        return "Established new connection with the ServerEndPoint [" + serverAddress + "], assuming connection is valid";
    }

    protected String getInvalidConnectionMessage(SocketAddress serverAddress, LrmiChannel watchedSocketChannel, Watchdog.WatchedObject watched) {
        return "The ServerEndPoint [" + serverAddress + "] is not reachable (timeout [" +
                _INSPECT_TIMEOUT + "]); closing invalid connection with local address ["
                + getLocalAddressString(watchedSocketChannel) + "]" + getWatchedObjectInvocationMessage(watched);
    }

    protected String getFailureToCloseInvalidConnectionMessage(SocketAddress serverAddress, LrmiChannel watchedSocketChannel) {
        return "A connection to the ServerEndPoint [" +
                watchedSocketChannel.getRemoteSocketAddress() +
                "] that is not reachable, could not be closed. ";
    }

    protected String getLocalAddressString(LrmiChannel channel) {
        //Avoid possible NPE if socket gets disconnected
        SocketAddress localSocketAddress = channel != null ? channel.getLocalSocketAddress() : null;
        String localAddress = localSocketAddress != null ? localSocketAddress.toString() : "not connected";
        return localAddress;
    }

    protected LogLevel getCloseConnectionLoggingLevel() {
        return LogLevel.DEBUG;
    }

    /**
     * Close client socket
     */
    private void close(Collection<WatchedObject> bucket, SocketAddress serverAddress, Exception e, long
            originalInvocationVersion) throws IOException {
        for (WatchedObject watched : bucket) {
            // We only advance the invocation version for response watched objects 
            // so this condition only applies for them.
            // When this condition holds, it means the this #close method has been 
            // called after the response watch has been closed at least once
            // after the initial invocation that started this monitoring.
            long currentWatchedInvocationVersion = watched.getVersion();
            if (!RequestResponseTimeoutObserver.DISABLE_RESPONSE_WATCH &&
                    currentWatchedInvocationVersion > originalInvocationVersion) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Not closing invalid connection as current invocation version does not match" +
                            " original invocation version. [original version=" + originalInvocationVersion +
                            ", current version=" + currentWatchedInvocationVersion +
                            "original message [" +
                            getInvalidConnectionMessage(serverAddress, watched.getChannel(), watched) + "[" + e + "]]");
                }
                continue;
            }

            try {
                // Stop watching this socket
                watched.stopWatch();

                // Set the exception
                watched.setException(e);

                // this call is not idempotent so we store the value.
                LogLevel closeConnectionLoggingLevel = getCloseConnectionLoggingLevel();

                if (closeConnectionLoggingLevel.isEnabled(_logger)) {
                    String invalidConnectionMessage = getInvalidConnectionMessage(serverAddress, watched.getChannel(), watched);
                    closeConnectionLoggingLevel.log(_logger, invalidConnectionMessage + "[" + e + "]", e);
                }

                // Close the socket
                if (watched.getChannel().isBlocking()) {
                    watched.getChannel().close();
                } else {
                    AsyncContext context = ((CPeer) watched.getClient()).getAsyncContext();
                    if (context != null) {
                        // we set the selection key to null
                        // so the client handler will not try unregistering the channel.
                        // this is ok, because the following invocation will also result
                        // in the socket channel being closed and in turn its selection keys
                        // will be cancelled, so we get the same wanted result.
                        context.setSelectionKey(null);
                        context.close(new ClosedChannelException());
                    }
                }
            } catch (Exception ex) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(getFailureToCloseInvalidConnectionMessage(serverAddress, watched.getChannel()), ex);
                }
            }
        }
    }

    protected static String getWatchedObjectInvocationMessage(WatchedObject watched) {
        return StringUtils.hasText(watched.getMonitoringId()) ? " [ remote invocation of: " + watched.getMonitoringId() + "] " : "";
    }

}
