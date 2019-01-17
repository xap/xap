package com.gigaspaces.lrmi.tcp;

import com.gigaspaces.async.SettableFuture;
import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIMethod;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.lrmi.ServerAddress;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.HandshakeRequest;
import com.gigaspaces.lrmi.nio.*;
import com.gigaspaces.lrmi.nio.selector.handler.client.ClientConversationRunner;
import com.gigaspaces.lrmi.nio.selector.handler.client.Conversation;
import com.gigaspaces.lrmi.nio.selector.handler.client.LRMIChat;
import com.gigaspaces.lrmi.nio.selector.handler.client.WriteBytesChat;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TcpChannel extends LrmiChannel {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_LRMI);
    private static final int SELECTOR_BUG_CONNECT_RETRY = Integer.getInteger(SystemProperties.LRMI_SELECTOR_BUG_CONNECT_RETRY, 5);

    private final SocketChannel socketChannel;
    private final String socketDisplayString;
    private final InetSocketAddress endpoint;

    public TcpChannel(SocketChannel socketChannel, ITransportConfig config, InetSocketAddress endpoint) {
        super(new TcpWriter(socketChannel, config), new TcpReader(socketChannel, config.getSlowConsumerRetries()));
        this.socketChannel = socketChannel;
        this.socketDisplayString = NIOUtils.getSocketDisplayString(socketChannel);
        this.endpoint = endpoint;
    }

    public static TcpChannel createSync(ServerAddress address, ITransportConfig config) throws IOException {
        final String host = address.getHost();
        final int port = address.getPort();
        if (_logger.isLoggable(Level.FINE))
            _logger.fine("connecting new socket channel to " + host + ":" + port + ", connect timeout=" + config.getSocketConnectTimeout() + " keepalive=" + LRMIUtilities.KEEP_ALIVE_MODE);

        SocketChannel socketChannel;
        InetSocketAddress endpoint;
        for (int i = 0; /* true */ ; ++i) {
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true); // blocking just for the connect
            LRMIUtilities.initNewSocketProperties(socketChannel);
            endpoint = new InetSocketAddress(host, port);

            try {
                socketChannel.socket().connect(endpoint, (int) config.getSocketConnectTimeout());
                break;
            } catch (ClosedSelectorException e) {
                //handles the error and might throw exception when we retried to much
                handleConnectError(i, address, socketChannel, config.getSocketConnectTimeout(), e);
            }
        }

        socketChannel.configureBlocking(config.isBlockingConnection()); //setting as nonblocking if needed

        /*
         * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6380091
         * The bug is that a non-existent thread is being signaled.
         * The issue is intermittent and the failure modes vary because the pthread_t of a terminated
         * thread is passed to pthread_kill.
         * The reason this is happening is because SocketChannelImpl's connect method isn't
         * resetting the readerThread so when the channel is closed it attempts to signal the reader.
         * The test case provokes this problem because it has a thread that terminates immediately after
         * establishing a connection.
         *
         * Workaround:
         * A simple workaround for this one is to call SocketChannel.read(ByteBuffer.allocate(0))
         * right after connecting the socket. That will reset the SocketChannelImpl.readerThread member
         * so that no interrupting is done when the channel is closed.
         **/
        socketChannel.read(ByteBuffer.allocate(0));
        return new TcpChannel(socketChannel, config, endpoint);
    }

    public static TcpChannel createAsync(ServerAddress address, ITransportConfig config, LRMIMethod lrmiMethod, ClientConversationRunner clientConversationRunner) throws IOException {
        final String host = address.getHost();
        final int port = address.getPort();

        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine("connecting new socket channel to " + host + ":" + port + ", connect timeout=" + config.getSocketConnectTimeout() + " keepalive=" + LRMIUtilities.KEEP_ALIVE_MODE);
        }
        Conversation conversation = new Conversation(new InetSocketAddress(host, port));
        boolean protocolValidationEnabled = ((NIOConfiguration) config).isProtocolValidationEnabled();
        if (protocolValidationEnabled) {
            conversation.addChat(new WriteBytesChat(ProtocolValidation.getProtocolHeaderBytes()));
        }

        RequestPacket requestPacket = new RequestPacket(new HandshakeRequest(PlatformLogicalVersion.getLogicalVersion()));
        requestPacket.operationPriority = CPeer.getOperationPriority(lrmiMethod, LRMIInvocationContext.getCurrentContext());
        conversation.addChat(new LRMIChat(requestPacket));

        try {
            SettableFuture<Conversation> future = clientConversationRunner.addConversation(conversation);
            if (config.getSocketConnectTimeout() == 0) { // socket zero timeout means wait indefinably.
                future.get();
            } else {
                future.get(config.getSocketConnectTimeout(), TimeUnit.MILLISECONDS);
            }
            conversation.channel().configureBlocking(true);
            SocketChannel socketChannel = conversation.channel();
            return new TcpChannel(socketChannel, config, null);
        } catch (Throwable t) {
            conversation.close(t);
            throw new IOException(t);
        }
    }

    /**
     * Handles the ClosedSelectorException error. This is a workaround for a bug in IBM1.4 JVM
     * (IZ19325)
     */
    private static void handleConnectError(int retry, ServerAddress address, SocketChannel sockChannel, long socketTimeout, ClosedSelectorException e) {
        // BugID GS-5873: retry to connect, this is a workaround for a bug in IBM1.4 JVM (IZ19325)
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "retrying connection due to closed selector exception: connecting to " +
                    address.getHost() + ":" + address.getPort() + ", connect timeout=" + socketTimeout +
                    " keepalive=" + LRMIUtilities.KEEP_ALIVE_MODE, e);
        try {
            sockChannel.close();
        } catch (Exception ex) {
            if (_logger.isLoggable(Level.FINE))
                _logger.log(Level.FINE, "Failed to close socket: connecting to " +
                        address.getHost() + ":" + address.getPort() + ", connect timeout=" + socketTimeout +
                        " keepalive=" + LRMIUtilities.KEEP_ALIVE_MODE, ex);
        }

        if (retry + 1 == SELECTOR_BUG_CONNECT_RETRY)
            throw e;
    }

    @Override
    public String getSocketDisplayString() {
        return socketDisplayString;
    }

    @Override
    public String getCurrSocketDisplayString() {
        return NIOUtils.getSocketDisplayString(socketChannel);
    }

    @Override
    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    @Override
    public boolean isBlocking() {
        return socketChannel.isBlocking();
    }

    @Override
    public String getSocketDesc() {
        return socketChannel.socket().toString();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        Socket socket = socketChannel.socket();
        return socket != null ? socket.getLocalSocketAddress() : null;
    }

    @Override
    public Integer getLocalPort() {
        Socket socket = socketChannel.socket();
        return socket != null ? socket.getLocalPort() : null;
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        Socket socket = socketChannel.socket();
        return socket != null ? socket.getRemoteSocketAddress() : null;
    }

    @Override
    public void close() throws IOException {
        socketChannel.close();
    }

    public InetSocketAddress getEndpoint() {
        return endpoint;
    }
}
