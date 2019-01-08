package com.gigaspaces.lrmi.tcp;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.exception.lrmi.SlowConsumerException;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.Transmitter;
import com.gigaspaces.lrmi.nio.TemporarySelectorFactory;
import com.gigaspaces.lrmi.nio.Writer;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TcpTransmitter extends Transmitter {
    private static final Logger _slowerConsumerLogger = Logger.getLogger(Constants.LOGGER_LRMI_SLOW_COMSUMER);
    private static final int BUFFER_LIMIT = Writer.BUFFER_LIMIT;

    private final SocketChannel _sockChannel;
    private final int _slowConsumerThroughput;
    private final boolean _slowConsumer;
    private final int _slowConsumerLatency;
    private final int _slowConsumerRetries;
    private final int _slowConsumerSleepTime;
    private final int _slowConsumerBytes;

    public TcpTransmitter(SocketChannel sockChannel, ITransportConfig config) {
        _sockChannel = sockChannel;
        _slowConsumerThroughput = config != null ? config.getSlowConsumerThroughput() : 0;
        _slowConsumerLatency = config != null ? config.getSlowConsumerLatency() : Integer.MAX_VALUE;
        _slowConsumerRetries = config != null ? config.getSlowConsumerRetries() : Integer.MAX_VALUE;
        _slowConsumer = _slowConsumerThroughput > 0;
        _slowConsumerSleepTime = _slowConsumerLatency / _slowConsumerRetries + 1;
        _slowConsumerBytes = (_slowConsumerThroughput * _slowConsumerLatency) / 1000;
    }

    @Override
    public boolean isOpen() {
        return _sockChannel.isOpen();
    }

    @Override
    public boolean isBlocking() {
        return _sockChannel.isBlocking();
    }

    @Override
    public void writeBytesToChannelBlocking(ByteBuffer dataBuffer) throws IOException {
        int totalBytesWritten = 0; // total bytes written from the all buffer
        int bytesRetries = 0; // total amount of bytes written since the
        int retries = _slowConsumerRetries;
        final int length = dataBuffer.remaining();
        boolean useSlidingWindow = length >= BUFFER_LIMIT;

        int currentPosision = 0;
        Selector tempSelector = null;
        SelectionKey tmpKey = null;

        try {
            while (true) {
                int bytes;
                if (useSlidingWindow) {
                    if (totalBytesWritten >= length) // finish writing all
                        break;

                    dataBuffer.position(currentPosision).limit(Math.min(length, currentPosision + BUFFER_LIMIT));
                    ByteBuffer window = dataBuffer.slice();
                    int windowSize = window.remaining();
                    bytes = _sockChannel.write(window);
                    currentPosision += bytes;

                    if (bytes == 0) {
                        if (tempSelector == null) {
                            tempSelector = TemporarySelectorFactory.getSelector();
                            tmpKey = _sockChannel.register(tempSelector, SelectionKey.OP_WRITE);
                        }

                        tmpKey.interestOps(tmpKey.interestOps() | SelectionKey.OP_WRITE);
                        int res = tempSelector.select(1000);
                        tmpKey.interestOps(tmpKey.interestOps() & (~SelectionKey.OP_WRITE));

                        if (res == 1) {
                            continue;
                        }
                    } else if (bytes == windowSize) {
                        totalBytesWritten += bytes;
                        continue;
                    }
                } else {
                    bytes = _sockChannel.write(dataBuffer);
                }

                totalBytesWritten += bytes;
                if (totalBytesWritten >= length) // finish writing all
                    break;

                bytesRetries += bytes;
                if (_slowConsumer && bytesRetries < _slowConsumerBytes) {
                    if (retries-- == 0) {
                        String slowConsumerCloseMsg = prepareSlowConsumerCloseMsg();
                        if (_slowerConsumerLogger.isLoggable(Level.WARNING)) {
                            _slowerConsumerLogger.warning(slowConsumerCloseMsg);
                        }
                        _sockChannel.close();
                        throw new SlowConsumerException(slowConsumerCloseMsg);
                    }
                    //else
                    try {
                        if (_slowerConsumerLogger.isLoggable(Level.FINE)) {
                            _slowerConsumerLogger.fine(prepareSlowConsumerSleepMsg(retries));
                        }
                        Thread.sleep(_slowConsumerSleepTime);
                    } catch (InterruptedException e) {
                        IOException ioe = new IOException("Interrupted while writing response.");
                        ioe.initCause(e);
                        throw ioe;
                    }
                } else {
                    bytesRetries = 0;
                    retries = _slowConsumerRetries;
                }

            }
        } finally {
            if (tmpKey != null)
                tmpKey.cancel();

            if (tempSelector != null) {
                // releases and clears the key.
                try {
                    tempSelector.selectNow();
                } catch (IOException ex) {
                }

                TemporarySelectorFactory.returnSelector(tempSelector);
            }
        }
        /*
         *  _dataBuffer isn't available after this point and should not be used!!
         */
    }

    private String prepareSlowConsumerSleepMsg(int retriesLeft) {
        return "Sleeping - waiting for slow consumer: " + getEndPointAddress() +
                " Retry=" + (_slowConsumerRetries - retriesLeft) +
                " SlowConsumerThroughput=" + _slowConsumerThroughput +
                " SlowConsumerRetries=" + _slowConsumerRetries +
                " SlowConsumerLatency=" + _slowConsumerLatency;
    }

    private String prepareSlowConsumerCloseMsg() {
        return "Closed slow consumer: " + getEndPointAddress() +
                " SlowConsumerThroughput=" + _slowConsumerThroughput +
                " SlowConsumerRetries=" + _slowConsumerRetries +
                " SlowConsumerLatency=" + _slowConsumerLatency;
    }

    private SocketAddress getEndPointAddress() {
        return _sockChannel.socket().getRemoteSocketAddress();
    }
}
