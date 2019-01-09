package com.gigaspaces.lrmi.tcp;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.exception.lrmi.SlowConsumerException;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIInvocationTrace;
import com.gigaspaces.lrmi.nio.ProtocolValidation;
import com.gigaspaces.lrmi.nio.TemporarySelectorFactory;
import com.gigaspaces.lrmi.nio.Writer;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TcpWriter extends Writer {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_LRMI);
    private static final Logger _slowerConsumerLogger = Logger.getLogger(Constants.LOGGER_LRMI_SLOW_COMSUMER);
    private static final int BUFFER_LIMIT = Writer.BUFFER_LIMIT;
    private static final int WRITE_DELAY_BEFORE_WARN = Integer.getInteger(SystemProperties.WRITE_DELAY_BEFORE_WARN, SystemProperties.WRITE_DELAY_BEFORE_WARN_DEFAULT);

    private final SocketChannel _sockChannel;
    private final Queue<Writer.Context> _contexts;
    private final int _slowConsumerThroughput;
    private final boolean _slowConsumer;
    private final int _slowConsumerLatency;
    private final int _slowConsumerRetries;
    private final int _slowConsumerSleepTime;
    private final int _slowConsumerBytes;

    public TcpWriter(SocketChannel socketChannel) {
        this(socketChannel, null);
    }

    public TcpWriter(SocketChannel sockChannel, ITransportConfig config) {
        _sockChannel = sockChannel;
        _contexts = new LinkedList<>();
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
    public SocketAddress getEndPointAddress() {
        return _sockChannel.socket().getRemoteSocketAddress();
    }

    @Override
    protected boolean hasQueuedContexts() {
        return !_contexts.isEmpty();
    }

    @Override
    public void writeProtocolValidationHeader() throws IOException {
        ProtocolValidation.writeProtocolValidationHeader(_sockChannel, Long.MAX_VALUE);
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

    @Override
    protected void writeNonBlocking(Writer.Context ctx, boolean restoreReadInterest) throws IOException {
        if (_contexts.isEmpty()) {
            noneBlockingWrite(ctx);
            if (ctx.getPhase() != Writer.Context.Phase.FINISH) {
                _contexts.offer(ctx);
                setWriteInterest();
                Writer.getPendingWritesCounter().increment();
            } else {
                // must call it because we might be here after a ClassProvider writing with a registered
                // write interest.
                removeWriteInterest(restoreReadInterest);
            }
        } else {
            _contexts.offer(ctx);
            setWriteInterest();
            Writer.getPendingWritesCounter().increment();
        }
    }

    @Override
    protected void onWriteEventImpl() throws IOException {
        LRMIInvocationTrace trace = null;
        try {
            while (!_contexts.isEmpty()) {
                Writer.Context current = _contexts.peek();
                trace = current.getTrace();
                if (trace != null)
                    LRMIInvocationContext.updateContext(trace, null, null, null, null, false, null, null);
                noneBlockingWrite(current);
                if (current.getPhase() != Writer.Context.Phase.FINISH) {
                    // channel write buffer is full, wait on selector.
                    setWriteInterest();
                    break;
                } else {
                    traceContextTotalWriteTime(current);
                    _contexts.poll();
                    Writer.getPendingWritesCounter().decrement();
                }
            }
            if (_contexts.isEmpty()) {
                removeWriteInterest(true);
            }
        } finally {
            if (trace != null)
                LRMIInvocationContext.resetContext();
        }
    }

    protected void noneBlockingWrite(Writer.Context ctx) throws IOException {
        if (ctx.getPhase() == Writer.Context.Phase.START) {
            int dataLength = ctx.getBuffer().remaining();
            ctx.setTotalLength(dataLength);
            ctx.setPhase(Writer.Context.Phase.WRITING);
        }
        if (ctx.getPhase() == Writer.Context.Phase.WRITING) {
            boolean useSlidingWindow = ctx.getTotalLength() >= BUFFER_LIMIT;

            int bytes;
            if (useSlidingWindow) {
                while (ctx.getTotalBytesWritten() < ctx.getTotalLength()) // finish writing all
                {
                    ctx.getBuffer().position(ctx.getCurrentPosition()).limit(Math.min(ctx.getTotalLength(), ctx.getCurrentPosition() + BUFFER_LIMIT));
                    ByteBuffer window = ctx.getBuffer().slice();
                    int windowSize = window.remaining();
                    bytes = _sockChannel.write(window);
                    ctx.setCurrentPosition(ctx.getCurrentPosition() + bytes);
                    ctx.setTotalBytesWritten(ctx.getTotalBytesWritten() + bytes);

                    if (bytes < windowSize) // socket channel buffer seems to be full, need to wait on the selector.
                    {
                        return;
                    }
                }
            } else {
                bytes = _sockChannel.write(ctx.getBuffer());
                ctx.setTotalBytesWritten(ctx.getTotalBytesWritten() + bytes);
            }

            if (ctx.getTotalBytesWritten() == ctx.getTotalLength()) // finish writing all
            {
                ctx.setPhase(Writer.Context.Phase.FINISH);
            }
        }
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

    private void removeWriteInterest(boolean restoreReadInterest) {
        if (_writeInterestManager != null) {
            _writeInterestManager.removeWriteInterest(restoreReadInterest);
        }
    }

    private void setWriteInterest() {
        if (_writeInterestManager != null) {
            _writeInterestManager.setWriteInterest();
        }
    }

    private void traceContextTotalWriteTime(Writer.Context context) {
        long writeTime = System.currentTimeMillis() - context.getCreationTime();
        if (WRITE_DELAY_BEFORE_WARN < writeTime) {
            String method = context.getTrace() != null ? context.getTrace().getTraceShortDisplayString() : "unknown";
            _logger.warning("write to " + getEndPointAddress() + " method " + method + " was fully performed only " + writeTime + " milliseconds after requested" +
                    ", the system may be overloaded or the network is bad.");
        }
    }
}
