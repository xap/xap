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

import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.LogLevel;
import com.gigaspaces.lrmi.ConnectionResource;
import com.gigaspaces.lrmi.nio.*;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;


/**
 * On top of {@link RequestTimeoutObserver} behaviour, The request response timeout observer also
 * sends a monitor request to the corresponding server and checks whether the corresponding channel
 * entry in currently in progress. If not the connection is considered dead and will be closed.
 *
 * @author Dan Kilman
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class RequestResponseTimeoutObserver extends RequestTimeoutObserver {
    static final boolean DISABLE_RESPONSE_WATCH = Boolean.parseBoolean(System.getProperty(SystemProperties.WATCHDOG_DISABLE_RESPONSE_WATCH, "false"));

    private final boolean _protocolValidationEnabled;

    private long _lastDisconnectionTimestamp = 0;

    public RequestResponseTimeoutObserver(long requestTimeout, boolean protocolValidationEnabled) {
        super(requestTimeout);
        _protocolValidationEnabled = protocolValidationEnabled;
    }

    @Override
    protected void handleOpenSocket(
            SocketChannel socketChannel,
            int watchedSocketLocalPort,
            long absoluteTimeout,
            ConnectionResource connectionResource) throws IOException {
        if (DISABLE_RESPONSE_WATCH) {
            super.handleOpenSocket(socketChannel, watchedSocketLocalPort, absoluteTimeout, connectionResource);
            return;
        }

        // don't do enhanced monitoring if server is old
        PlatformLogicalVersion serviceVersion = ((CPeer) connectionResource).getServiceVersion();
        if (serviceVersion.lessThan(PlatformLogicalVersion.v9_5_0))
            return;

        Socket socket = socketChannel.socket();
        if (socket == null)
            throw new IOException("Socket closed for: " + socketChannel + " [no socket]");

        SocketAddress remoteSocketAddress = socket.getRemoteSocketAddress();
        if (remoteSocketAddress == null)
            throw new IOException("Socket closed for: " + socketChannel + "[no remote socket address]");

        boolean previousIsBlocking = socketChannel.isBlocking();
        socketChannel.configureBlocking(false);
        try {
            if (_protocolValidationEnabled)
                writeProtocolValidationHeader(socketChannel, absoluteTimeout);

            writeWatchdogMonitorSystemRequest(socketChannel,
                    watchedSocketLocalPort,
                    absoluteTimeout,
                    remoteSocketAddress,
                    serviceVersion);

            readWatchdogMonitorSystemResponse(socketChannel,
                    absoluteTimeout,
                    remoteSocketAddress,
                    serviceVersion);
        } finally {
            socketChannel.configureBlocking(previousIsBlocking);
        }
    }

    @Override
    protected String getValidConnectionMessage(SocketAddress serverAddress) {
        if (DISABLE_RESPONSE_WATCH)
            return super.getValidConnectionMessage(serverAddress);

        return "Established new connection with the ServerEndPoint [" + serverAddress + "] and verified current invocation is " +
                "in progress. Assuming connection is valid.";
    }

    @Override
    protected String getInvalidConnectionMessage(SocketAddress serverAddress, LrmiChannel watchedSocketChannel, Watchdog.WatchedObject watched) {
        if (DISABLE_RESPONSE_WATCH)
            return super.getInvalidConnectionMessage(serverAddress, watchedSocketChannel, watched);

        String suffix = getWatchedObjectInvocationMessage(watched);

        return "The ServerEndPoint [" + serverAddress + "] is not reachable or is " +
                "reachable but with no matching invocation in progress at the server " +
                "peer, closing invalid connection with local address ["
                + getLocalAddressString(watchedSocketChannel) + "]" + suffix;
    }

    @Override
    protected String getFailureToCloseInvalidConnectionMessage(SocketAddress serverAddress, LrmiChannel watchedSocketChannel) {
        if (DISABLE_RESPONSE_WATCH)
            return super.getFailureToCloseInvalidConnectionMessage(serverAddress, watchedSocketChannel);

        return "A connection to the ServerEndPoint [" +
                watchedSocketChannel.getRemoteSocketAddress() +
                "] that has no invocation in progress at the server peer, could not be closed. ";
    }

    @Override
    protected LogLevel getCloseConnectionLoggingLevel() {
        if (DISABLE_RESPONSE_WATCH)
            return super.getCloseConnectionLoggingLevel();

        LogLevel result = LogLevel.DEBUG;
        long timeMillis = SystemTime.timeMillis();
        // not the first time and less than 1 minute since last time 
        if (_lastDisconnectionTimestamp > 0 &&
                timeMillis < _lastDisconnectionTimestamp + TimeUnit.MINUTES.toMillis(1)) {
            result = LogLevel.WARNING;
        }
        _lastDisconnectionTimestamp = timeMillis;
        return result;
    }

    private void writeWatchdogMonitorSystemRequest(
            SocketChannel socketChannel,
            int watchedSocketLocalPort,
            long absoluteTimeout,
            SocketAddress remoteSocketAddress,
            PlatformLogicalVersion serviceVersion)
            throws IOException {
        if (_logger.isDebugEnabled())
            _logger.debug("Writing watch monitor request to the ServerEndpoint at [" + remoteSocketAddress + "]");

        // header(int) + local watched socket port(int)
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4);
        int requestCode;
        if (serviceVersion.lessThan(PlatformLogicalVersion.v9_6_0))
            requestCode = SystemRequestHandler.RequestCodeHeader.WATCHDOG_MONITOR_HEADER_V95X;
        else
            requestCode = SystemRequestHandler.RequestCodeHeader.WATCHDOG_MONITOR_HEADER;

        byteBuffer.putInt(requestCode);
        byteBuffer.putInt(watchedSocketLocalPort);
        byteBuffer.flip();

        while (byteBuffer.hasRemaining() && SystemTime.timeMillis() < absoluteTimeout) {
            int writtenBytes = socketChannel.write(byteBuffer);
            if (writtenBytes == 0)
                Thread.yield();
        }

        if (byteBuffer.hasRemaining())
            throw new SocketTimeoutException("Timed out during monitoring connection [write system request]");
    }

    private void readWatchdogMonitorSystemResponse(
            SocketChannel socketChannel,
            long absoluteTimeout,
            SocketAddress remoteSocketAddress,
            PlatformLogicalVersion serviceVersion) throws IOException {
        if (_logger.isDebugEnabled())
            _logger.debug("Reading watch monitor response from the ServerEndpoint at [" + remoteSocketAddress + "]");

        // we set timeout on the socket for read opeations
        socketChannel.configureBlocking(true);

        // update timeout on socket
        long timeout = absoluteTimeout - SystemTime.timeMillis();
        if (timeout <= 0)
            throw new SocketTimeoutException("Timed out during monitoring connection [before read system response]");
        socketChannel.socket().setSoTimeout((int) timeout);

        // read length of reponse
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        readFromSocketChannel(socketChannel, lengthBuffer, "read length", absoluteTimeout);
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();

        // update timeout on socket before reading response
        timeout = absoluteTimeout - SystemTime.timeMillis();
        if (timeout <= 0)
            throw new SocketTimeoutException("Timed out during monitoring connection [during read system response]");
        socketChannel.socket().setSoTimeout((int) timeout);

        // read reply packet
        ByteBuffer packetBuffer = ByteBuffer.allocate(length);
        readFromSocketChannel(socketChannel, packetBuffer, "read packet", absoluteTimeout);
        GSByteArrayInputStream bis = new GSByteArrayInputStream(packetBuffer.array());
        MarshalInputStream mis = new MarshalInputStream(bis);
        ReplyPacket<?> replyPacket = new ReplyPacket();
        try {
            replyPacket.readExternal(mis);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unexpected reply for watchdog monitoring request. " + e);
        } finally {
            mis.closeContext();
        }

        if (serviceVersion.lessThan(PlatformLogicalVersion.v9_6_0)) {
            boolean found = (Boolean) replyPacket.getResult();
            if (!found)
                throw new IOException("No matching server channel invocation was found for this client at " +
                        socketChannel.socket().getRemoteSocketAddress());
        } else {
            ChannelEntry.State channelEntryState = ChannelEntry.State.fromCode((Integer) replyPacket.getResult());
            if (channelEntryState != ChannelEntry.State.PROGRESS) {
                throw new IOException("No matching server channel invocation was found for this client at " +
                        socketChannel.socket().getRemoteSocketAddress() +
                        ", server channel state: " + channelEntryState);
            }
        }
    }

    private void writeProtocolValidationHeader(
            SocketChannel socketChannel,
            long absoluteTimeout)
            throws IOException {
        ProtocolValidation.writeProtocolValidationHeader(socketChannel,
                absoluteTimeout - SystemTime.timeMillis());
    }

    private static void readFromSocketChannel(SocketChannel socketChannel, ByteBuffer buffer, String stage, long absoluteTimeout) throws IOException {
        try {
            while (0 < buffer.remaining()) {
                long timeout = absoluteTimeout - SystemTime.timeMillis();
                if (timeout <= 0) {
                    throw new SocketTimeoutException("Timed out during monitoring connection [at stage '" + stage + "']");
                }
                socketChannel.read(buffer);
            }
        } catch (SocketTimeoutException e) {
            throw e;
        } catch (IOException e) {
            throw new IOException("[during " + stage + "]:" + e, e);
        }
    }

}
