package com.gigaspaces.lrmi.tcp;

import com.gigaspaces.exception.lrmi.SlowConsumerException;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.SystemRequestHandler;
import com.gigaspaces.lrmi.nio.TemporarySelectorFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TcpReader extends Reader {

    private static final Logger _slowerConsumerLogger = Logger.getLogger(Constants.LOGGER_LRMI_SLOW_COMSUMER);

    private final SocketChannel _socketChannel;
    //TODO fix read slow consumer to work for all read operations like write operations
    private final int _slowConsumerRetries;

    public TcpReader(SocketChannel socketChannel, int slowConsumerRetries) {
        this(socketChannel, slowConsumerRetries, null);
    }

    public TcpReader(SocketChannel socketChannel, SystemRequestHandler systemRequestHandler) {
        this(socketChannel, Integer.MAX_VALUE, systemRequestHandler);
    }

    private TcpReader(SocketChannel socketChannel, int slowConsumerRetries, SystemRequestHandler systemRequestHandler) {
        super(systemRequestHandler);
        this._socketChannel = socketChannel;
        this._slowConsumerRetries = slowConsumerRetries;
    }

    @Override
    protected String getEndpointDesc() {
        return String.valueOf(_socketChannel);
    }

    @Override
    protected SocketAddress getEndPointAddress() {
        return _socketChannel.socket().getRemoteSocketAddress();
    }

    @Override
    protected String getEndPointAddressDesc() {
        return _socketChannel.socket() != null ? String.valueOf(_socketChannel.socket().getRemoteSocketAddress()) : "unknown";
    }

    @Override
    protected int directRead(ByteBuffer buffer) throws IOException {
        return _socketChannel.read(buffer);
    }

    @Override
    protected int readHeaderBlocking(ByteBuffer buffer, int slowConsumerLatency, AtomicInteger retries) throws IOException {
        TempSelectorHelper temp = null;
        int bytesRead = 0;

        buffer.clear();
        try {
            while (bytesRead < HEADER_SIZE) {
                int bRead = read(buffer);
                bytesRead += bRead;

                if (bRead == 0) {
                    temp = processEmptyOrNonBlocking(temp, slowConsumerLatency, retries);
                }
            }
        } finally {
            if (temp != null)
                temp.close();
        }
        incReceivedTraffic(HEADER_SIZE);
        buffer.flip();
        return buffer.getInt();
    }

    @Override
    protected void readPayloadBlocking(ByteBuffer buffer, int dataLength, int slowConsumerLatency, AtomicInteger retries) throws IOException {
        /*
         * Sliding window is used to read the data from the channel using limited size buffer instead of
         * reading using all the buffer, this is because Java SocketChannel allocate direct buffer that has the same size as
         * the user buffer when reading from the channel, this may cause our of memory if user buffer is too long.
         */
        boolean shouldUseSlidingWindow = dataLength >= BUFFER_LIMIT;

        int bytesRead = 0;
        int bRead;
        TempSelectorHelper temp = null;
        try {
            while (bytesRead < dataLength) {
                ByteBuffer workingBuffer = buffer;
                if (shouldUseSlidingWindow) {
                    buffer.position(bytesRead).limit(Math.min(dataLength, bytesRead + BUFFER_LIMIT));
                    workingBuffer = buffer.slice();
                }

                bRead = read(workingBuffer);
                bytesRead += bRead;

                if (bRead == 0) {
                    temp = processEmptyOrNonBlocking(temp, slowConsumerLatency, retries);
                }
            }
        } finally {
            if (temp != null)
                temp.close();
        }
        incReceivedTraffic(buffer.position());
        buffer.position(0);
        buffer.limit(dataLength);
    }

    private static class TempSelectorHelper {
        public SelectionKey key;
        Selector selector;

        public void close() {
            if (key != null)
                key .cancel();

            if (selector != null) {
                // releases and clears the key.
                try {
                    selector.selectNow();
                } catch (IOException ignored) {
                }

                TemporarySelectorFactory.returnSelector(selector);
            }
        }
    }

    private TempSelectorHelper processEmptyOrNonBlocking(TempSelectorHelper temp, int slowConsumerLatency, AtomicInteger retries) throws IOException {
        final int selectTimeout = (slowConsumerLatency / _slowConsumerRetries) + 1;
        final boolean channelIsBlocking = _socketChannel.isBlocking();

        if (slowConsumerLatency > 0 && (retries.incrementAndGet()) > _slowConsumerRetries) {
            String slowConsumerMsg = prepareSlowConsumerCloseMsg(getEndPointAddress(), slowConsumerLatency);
            if (_slowerConsumerLogger.isLoggable(Level.WARNING))
                _slowerConsumerLogger.warning(slowConsumerMsg);
            throw new SlowConsumerException(slowConsumerMsg);
        }
        // if bRead == 0 this channel is either none blocking, or it is in blocking mode
        // but there the socket buffer was empty while the read was called.
        // We should use selector to read from this channel without do busy loop.
        if (channelIsBlocking) {
            _socketChannel.configureBlocking(false);
        }

        if (temp == null) {
            temp = new TempSelectorHelper();
            temp.selector = TemporarySelectorFactory.getSelector();
            temp.key = _socketChannel.register(temp.selector, SelectionKey.OP_READ);
        }
        temp.key.interestOps(temp.key.interestOps() | SelectionKey.OP_READ);

        if (_slowerConsumerLogger.isLoggable(Level.FINE))
            _slowerConsumerLogger.fine(prepareSlowConsumerSleepMsg(getEndPointAddress(), retries.get(), slowConsumerLatency));

        temp.selector.select(slowConsumerLatency == 0 ? 0 : selectTimeout);
        temp.key.interestOps(temp.key.interestOps() & (~SelectionKey.OP_READ));
        if (channelIsBlocking) {
            _socketChannel.configureBlocking(true);
        }

        return temp;
    }

    private String prepareSlowConsumerCloseMsg(SocketAddress address, int slowConsumerLatency) {
        return "Closed slow consumer: " + address +
                " SlowConsumerRetries=" + _slowConsumerRetries +
                " SlowConsumerLatency=" + slowConsumerLatency;
    }

    private String prepareSlowConsumerSleepMsg(SocketAddress endPointAddress, int retries, int slowConsumerLatency) {
        return "Sleeping - waiting for slow consumer: " + endPointAddress +
                " Retry=" + retries +
                " SlowConsumerRetries=" + _slowConsumerRetries +
                " SlowConsumerLatency=" + slowConsumerLatency;
    }
}
