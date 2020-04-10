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

package com.gigaspaces.lrmi.nio;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dan Kilman
 * @since 9.5
 */
@com.gigaspaces.api.InternalApi
public class SystemRequestHandlerImpl
        implements SystemRequestHandler {
    @Override
    public boolean handles(int requestCode) {
        switch (requestCode) {
            case RequestCodeHeader.WATCHDOG_MONITOR_HEADER_V95X:
            case RequestCodeHeader.WATCHDOG_MONITOR_HEADER:
                return true;
            default:
                return false;
        }
    }

    @Override
    public SystemRequestContext getRequestContext(int requestCode) {
        switch (requestCode) {
            case RequestCodeHeader.WATCHDOG_MONITOR_HEADER_V95X:
                return new WatchdogMonitorSystemRequestContextV950();
            case RequestCodeHeader.WATCHDOG_MONITOR_HEADER:
                return new WatchdogMonitorSystemRequestContext();
            default:
                throw new IllegalStateException("No handler for request code: " + requestCode);
        }
    }

    public static abstract class AbstractWatchdogMonitorSystemRequestContext implements SystemRequestContext {
        final static private Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_WATCHDOG);

        private int _monitoredClientSocketPort;

        @Override
        public int getRequestDataLength() {
            // int for the client side port number of the watched socket
            return 4;
        }

        @Override
        public void prepare(byte[] result) {
            _monitoredClientSocketPort = ByteBuffer.wrap(result).order(ByteOrder.BIG_ENDIAN).getInt();
        }

        @Override
        public Runnable getResponseTask(final Pivot pivot,
                                        final ChannelEntry channelEntry,
                                        final long requestReadStartTimestamp) {
            final long taskCreationTimestamp = SystemTime.timeMillis();
            return new Runnable() {
                @Override
                public void run() {
                    boolean found = false;
                    ChannelEntry.State channelEntryState = null;

                    InetAddress clientAddress = null;
                    Socket socket = channelEntry.getSocketChannel().socket();
                    if (socket != null)
                        clientAddress = socket.getInetAddress();

                    InetSocketAddress monitoredClient = null;
                    if (clientAddress != null) {
                        monitoredClient = new InetSocketAddress(clientAddress, _monitoredClientSocketPort);
                        channelEntryState = pivot.getChannelEntryState(monitoredClient);
                        found = channelEntryState == ChannelEntry.State.PROGRESS;
                    }

                    boolean reuseBuffer = false;
                    Writer.Context ctx = new Writer.SystemResponseContext();
                    String monitoringId = null;
                    ReplyPacket<?> replyPacket = createReplyPacket(found, channelEntryState);

                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Writing watchdog monitor system response for client[" + monitoredClient +
                                "], channelEntryState[ " + channelEntryState + " ] " +
                                "watchdog request read at [ " + requestReadStartTimestamp + " ] " +
                                "watchdog response task creation at [ " + taskCreationTimestamp + " ] " +
                                "watchdog response task execution at [" + SystemTime.timeMillis() + " ]");
                    }

                    channelEntry.writeReply(replyPacket, reuseBuffer, ctx, monitoringId);
                }
            };
        }

        abstract protected ReplyPacket<?> createReplyPacket(boolean found, ChannelEntry.State channelEntryState);

    }

    public static class WatchdogMonitorSystemRequestContextV950 extends AbstractWatchdogMonitorSystemRequestContext {
        @Override
        protected ReplyPacket<?> createReplyPacket(boolean found, ChannelEntry.State channelEntryState) {
            return new ReplyPacket<Boolean>(found, null);
        }
    }

    public static class WatchdogMonitorSystemRequestContext extends AbstractWatchdogMonitorSystemRequestContext {
        @Override
        protected ReplyPacket<?> createReplyPacket(boolean found, ChannelEntry.State channelEntryState) {
            int channelEntryStateCode = channelEntryState != null ? channelEntryState.getCode() : 0;
            return new ReplyPacket<Integer>(channelEntryStateCode, null);
        }
    }

}
