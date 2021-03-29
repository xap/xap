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

import com.gigaspaces.exception.lrmi.SlowConsumerException;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.internal.io.MarshalContextClearedException;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIInvocationTrace;
import com.gigaspaces.lrmi.SmartByteBufferCache;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;
import com.gigaspaces.lrmi.nio.filters.IOFilterManager;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Writer is capable of writing Request Packets and Reply Packets to a Socket Channel. An NIO
 * Client Peer uses an instance of a Writer to write Request Packets while an NIO Server uses an
 * instance of a Writer to write Reply Packets.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public class Writer implements IChannelWriter {
    //logger
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);
    final private static Logger _slowerConsumerLogger = LoggerFactory.getLogger(Constants.LOGGER_LRMI_SLOW_COMSUMER);

    /**
     * writer socket channel.
     */
    final private SocketChannel _sockChannel;

    final static private int BUFFER_LIMIT = Integer.getInteger(SystemProperties.MAX_LRMI_BUFFER_SIZE, SystemProperties.MAX_LRMI_BUFFER_SIZE_DEFAULT);

    final static private int LENGTH_SIZE = 4; //4 bytes for length

    final private MarshalOutputStream _oos;
    final private GSByteArrayOutputStream _baos;

    final static private int WRITE_DELAY_BEFORE_WARN = Integer.getInteger(SystemProperties.WRITE_DELAY_BEFORE_WARN, SystemProperties.WRITE_DELAY_BEFORE_WARN_DEFAULT);

    /**
     * reuse buffer, growing on demand.
     */
    final private SmartByteBufferCache _bufferCache = SmartByteBufferCache.getDefaultSmartByteBufferCache();

    final private int _slowConsumerThroughput;
    final private boolean _slowConsumer;
    final private int _slowConsumerLatency;
    final private int _slowConsumerRetries;
    final private int _slowConsumerSleepTime;
    final private int _slowConsumerBytes;
    private static final LongAdder generatedTraffic = new LongAdder();
    private long _generatedTraffic;

    final private static byte[] DUMMY_BUFFER = new byte[0];

    IOFilterManager _filterManager;

    private final Queue<Context> _contexts;
    private static final LongAdder pendingWrites = new LongAdder();

    private IWriteInterestManager _writeInterestManager;

    public static LongAdder getGeneratedTrafficCounter() {
        return generatedTraffic;
    }

    public static LongAdder getPendingWritesCounter() {
        return pendingWrites;
    }

    public Writer(SocketChannel sockChannel, IWriteInterestManager writeInterestManager) {
        this(sockChannel, 0, Integer.MAX_VALUE, Integer.MAX_VALUE, writeInterestManager);
    }

    public Writer(SocketChannel sockChannel, int slowConsumerThroughput, int slowConsumerLatency, int slowConsumerRetries, IWriteInterestManager writeInterestManager) {
        _sockChannel = sockChannel;
        _slowConsumerThroughput = slowConsumerThroughput;
        _slowConsumerLatency = slowConsumerLatency;
        _slowConsumerRetries = slowConsumerRetries;

        _slowConsumer = _slowConsumerThroughput > 0;
        _slowConsumerSleepTime = _slowConsumerLatency / _slowConsumerRetries + 1;
        _slowConsumerBytes = (_slowConsumerThroughput * _slowConsumerLatency) / 1000;
        _contexts = new LinkedList<Context>();
        _writeInterestManager = writeInterestManager;

        try {
            _baos = new GSByteArrayOutputStream();
            _baos.setSize(LENGTH_SIZE); // mark the buffer to start writing only after the length place
            _oos = new MarshalOutputStream(_baos, true); // add a TC_RESET using the MarshalOutputStream.writeStreamHeader() 
            initBuffer(_baos);
        } catch (Exception e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.getMessage(), e);
            }

            throw new RuntimeException("Failed to initialize LRMI Writer stream: ", e);
        }
    }

    /**
     * Do not call this method unless the last write was completed. Otherwise you may have
     * concurrency issues, this is because _writeInterestManager is used from the selector thread as
     * well as from the user thread (you in this case) After the last write was completed the
     * selector thread will not use _writeInterestManager until write is performed.
     */
    public void setWriteInterestManager(IWriteInterestManager writeInterestManager) {
        _writeInterestManager = writeInterestManager;
    }

    /**
     * @return the endpoint of the connected SocketChannel.
     */
    public SocketAddress getEndPointAddress() {
        return _sockChannel != null ? _sockChannel.socket().getRemoteSocketAddress() : null;
    }

    public void writeRequest(RequestPacket packet, boolean reuseBuffer, Context ctx) throws IOException, IOFilterException {
        writePacket(packet, reuseBuffer, ctx);
    }


    public void writeRequest(RequestPacket packet, boolean reuseBuffer) throws IOException, IOFilterException {
        writePacket(packet, reuseBuffer, null);
    }

    public void writeRequest(RequestPacket packet) throws IOException, IOFilterException {
        writeRequest(packet, true);
    }

    public void writeReply(ReplyPacket packet, boolean reuseBuffer, Context ctx) throws IOException, IOFilterException {
        writePacket(packet, reuseBuffer, ctx);
    }

    public void writeReply(ReplyPacket packet, boolean reuseBuffer) throws IOException, IOFilterException {
        writePacket(packet, reuseBuffer, null);
    }

    public void writeReply(ReplyPacket packet) throws IOException, IOFilterException {
        writeReply(packet, true);
    }

    public boolean isOpen() {
        return _sockChannel.isOpen();
    }

    //Access to contexts should be synchronized
    private synchronized void writePacket(IPacket packet, boolean requestReuseBuffer, Context ctx) throws IOException, IOFilterException {
        if (_logger.isTraceEnabled()) {
            _logger.trace("--> Write Packet " + packet);
        }
        ByteBuffer byteBuffer;
        MarshalOutputStream mos;
        GSByteArrayOutputStream bos;

        final boolean reuseBuffer = requestReuseBuffer && _contexts.isEmpty();
        if (reuseBuffer) {
            mos = _oos;
            bos = _baos;
            byteBuffer = prepareStream();
        } else // build a temporal buffer and streams
        {
            bos = new GSByteArrayOutputStream();
            bos.setSize(LENGTH_SIZE); // for the stream size
            mos = new MarshalOutputStream(bos, false);
            byteBuffer = wrap(bos);
        }

        ByteBuffer buffer;
        try {
            packet.writeExternal(mos);
        } catch (MarshalContextClearedException e) {
            //Keep original exception for upper layer to handle properly
            throw e;
        } catch (Exception e) {
            throw new MarshallingException("Failed to marsh: " + packet, e);
        } finally // make sure we clean the buffers even if an exception was thrown
        {
            buffer = prepareBuffer(mos, bos, byteBuffer);

            if (reuseBuffer) {
                bos.setBuffer(DUMMY_BUFFER); // set DUMMY_BUFFER to release the strong reference to the byte[]
                bos.reset();
                mos.reset();
                if (buffer != byteBuffer) // replace the buffer in soft reference if needed
                    _bufferCache.set(buffer);
                else
                    _bufferCache.notifyUsedSize(buffer.limit());
            } else {
                //Clear context because this output stream is no longer used
                mos.closeContext();
            }
        }
        _generatedTraffic += buffer.limit();
        generatedTraffic.add(buffer.limit());

        if (ctx != null) {
            // non blocking mode.
            ctx.setBuffer(buffer);
            writeBytesNonBlocking(ctx);
        } else {
            // blocking mode.
            writeBytesBlocking(buffer);
        }
    }

    public static class Context {
        public static enum Phase {START, WRITING, FINISH}

        private Phase phase = Phase.START;
        private ByteBuffer buffer;
        private int totalBytesWritten = 0;
        private int currentPosition = 0;
        private int totalLength;
        private final LRMIInvocationTrace trace;
        private final long creationTime;

        public Context(LRMIInvocationTrace trace) {
            this.trace = trace;
            this.creationTime = System.currentTimeMillis();
        }

        public void setBuffer(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public void setTotalBytesWritten(int totalBytesWritten) {
            this.totalBytesWritten = totalBytesWritten;
        }

        public int getTotalBytesWritten() {
            return totalBytesWritten;
        }

        public void setCurrentPosition(int currentPosition) {
            this.currentPosition = currentPosition;
        }

        public int getCurrentPosition() {
            return currentPosition;
        }

        public void setTotalLength(int totalLength) {
            this.totalLength = totalLength;
        }

        public int getTotalLength() {
            return totalLength;
        }

        public void setPhase(Phase phase) {
            this.phase = phase;
        }

        public Phase getPhase() {
            return phase;
        }

        public LRMIInvocationTrace getTrace() {
            return trace;
        }

        public boolean isSystemResponse() {
            return false;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public Context duplicate() {
            Context res = createContextForDuplication();
            res.setPhase(phase);
            res.setTotalLength(totalLength);
            res.setCurrentPosition(currentPosition);
            res.setTotalBytesWritten(totalBytesWritten);
            res.setBuffer(buffer);
            return res;
        }

        protected Context createContextForDuplication() {
            return new Context(trace);
        }
    }

    public static class SystemResponseContext extends Context {
        public SystemResponseContext() {
            super(null);
        }

        @Override
        public boolean isSystemResponse() {
            return true;
        }

        @Override
        protected Context createContextForDuplication() {
            return new SystemResponseContext();
        }
    }

    public static class ChannelEntryContext extends Context {
        private final WriteExecutionPhaseListener executionPhaseListener;

        public ChannelEntryContext(LRMIInvocationTrace trace, WriteExecutionPhaseListener listener) {
            super(trace);
            executionPhaseListener = listener;
        }

        @Override
        public void setPhase(Phase phase) {
            super.setPhase(phase);
            executionPhaseListener.onPhase(phase);
        }

        @Override
        protected Context createContextForDuplication() {
            return new ChannelEntryContext(getTrace(), executionPhaseListener);
        }
    }

    public void setFilterManager(IOFilterManager filterManager) {
        this._filterManager = filterManager;
    }


    public boolean isBlocking() {
        return _sockChannel.isBlocking();
    }

    private void writeBytesNonBlocking(Context ctx) throws IOException, IOFilterException {
        if (_filterManager != null && !ctx.isSystemResponse()) {
            _filterManager.writeBytesNonBlocking(ctx);
        } else {
            //Regular write Bytes non blocking, restore read interest if finish writing
            //synchronously
            writeBytesToChannelNoneBlocking(ctx, true);
        }
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.lrmi.nio.IChannelWriter#writeBytesToChannelNoneBlocking(com.gigaspaces.lrmi.nio.Writer.Context)
     */
    public synchronized void writeBytesToChannelNoneBlocking(Context ctx, boolean restoreReadInterest) throws IOException {
        if (_contexts.isEmpty()) {
            noneBlockingWrite(ctx);
            if (ctx.getPhase() != Context.Phase.FINISH) {
                _contexts.offer(ctx);
                setWriteInterest();
                pendingWrites.increment();
            } else {
                // must call it because we might be here after a ClassProvider writing with a registered
                // write interest.
                removeWriteInterest(restoreReadInterest);
            }
        } else {
            _contexts.offer(ctx);
            setWriteInterest();
            pendingWrites.increment();
        }
    }

    protected void noneBlockingWrite(Context ctx) throws IOException {
        if (ctx.getPhase() == Context.Phase.START) {
            int dataLength = ctx.getBuffer().remaining();
            ctx.setTotalLength(dataLength);
            ctx.setPhase(Context.Phase.WRITING);
        }
        if (ctx.getPhase() == Context.Phase.WRITING) {
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
                ctx.setPhase(Context.Phase.FINISH);
            }
        }
    }


    private void writeBytesBlocking(ByteBuffer dataBuffer) throws IOException, IOFilterException {
        if (_filterManager != null) {
            _filterManager.writeBytesBlocking(dataBuffer);
        } else {
            writeBytesToChannelBlocking(dataBuffer);
        }
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.lrmi.nio.IChannelWriter#writeBytesToChannelBlocking(java.nio.ByteBuffer)
     */
    public void writeBytesToChannelBlocking(ByteBuffer dataBuffer)
            throws IOException, ClosedChannelException, SlowConsumerException {
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
                        String slowConsumerCloseMsg = prepareSlowConsumerCloseMsg(getEndPointAddress());
                        if (_slowerConsumerLogger.isWarnEnabled()) {
                            _slowerConsumerLogger.warn(slowConsumerCloseMsg);
                        }
                        _sockChannel.close();
                        throw new SlowConsumerException(slowConsumerCloseMsg);
                    }
                    //else
                    try {
                        if (_slowerConsumerLogger.isDebugEnabled()) {
                            _slowerConsumerLogger.debug(prepareSlowConsumerSleepMsg(getEndPointAddress(), retries));
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

    /**
     * Called from WriteSelectorThread to complete pending write requests.
     *
     * This is synchronized to ensure mutual exclusion with writeBytesToChannelNoneBlocking method
     *
     * @see #noneBlockingWrite
     */
    public synchronized void onWriteEvent() throws IOException {
        LRMIInvocationTrace trace = null;
        try {
            while (!_contexts.isEmpty()) {
                Context current = _contexts.peek();
                trace = current.getTrace();
                if (trace != null)
                    LRMIInvocationContext.updateContext(trace, null, null, null, null, false, null, null);
                noneBlockingWrite(current);
                if (current.getPhase() != Context.Phase.FINISH) {
                    // channel write buffer is full, wait on selector.
                    setWriteInterest();
                    break;
                } else {
                    traceContextTotalWriteTime(current);
                    _contexts.poll();
                    pendingWrites.decrement();
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

    private void traceContextTotalWriteTime(Context context) {
        long writeTime = System.currentTimeMillis() - context.getCreationTime();
        if (WRITE_DELAY_BEFORE_WARN < writeTime) {
            String method = context.getTrace() != null ? context.getTrace().getTraceShortDisplayString() : "unknown";
            _logger.warn("write to " + getEndPointAddress() + " method " + method + " was fully performed only " + writeTime + " milliseconds after requested" +
                    ", the system may be overloaded or the network is bad.");
        }
    }


    /**
     * @param byteBuffer buffer that might be used by the GSByteArrayOutputStream
     * @return prepared buffer.
     */
    private ByteBuffer prepareBuffer(MarshalOutputStream mos, GSByteArrayOutputStream bos,
                                     ByteBuffer byteBuffer) throws IOException {
        mos.flush();

        int length = bos.size();

        if (byteBuffer.array() != bos.getBuffer()) // the buffer was changed
        {
            byteBuffer = wrap(bos);
        } else {
            byteBuffer.clear();
        }

        byteBuffer.putInt(length - LENGTH_SIZE);
        byteBuffer.position(length);
        byteBuffer.flip();

        return byteBuffer;
    }

    /**
     * Wraps a GSByteArrayOutputStream inner buffer with a ByteBuffer
     *
     * @param bos stream to wrap
     * @return wrapping ByteBuffer
     */
    private ByteBuffer wrap(GSByteArrayOutputStream bos) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bos.getBuffer());
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
        return byteBuffer;
    }

    /**
     * Wraps a stream with a buffer and save it a soft reference local cache.
     *
     * @param bos stream to wrap
     * @return wrapping ByteBuffer
     */
    private ByteBuffer initBuffer(GSByteArrayOutputStream bos) {
        ByteBuffer byteBuffer = wrap(bos);
        _bufferCache.set(byteBuffer);
        return byteBuffer;
    }

    /**
     * @return buffer that might be used by the GSByteArrayOutputStream
     */
    private ByteBuffer prepareStream() throws IOException {
        ByteBuffer byteBuffer = _bufferCache.get();

        byte[] streamBuffer = byteBuffer.array();
        _baos.setBuffer(streamBuffer, LENGTH_SIZE); // 4 bytes for size 
        return byteBuffer;
    }


    /**
     * Prepares log massage.
     */
    private String prepareSlowConsumerSleepMsg(SocketAddress address, int retriesLeft) {
        return "Sleeping - waiting for slow consumer: " + address +
                " Retry=" + (_slowConsumerRetries - retriesLeft) +
                " SlowConsumerThroughput=" + _slowConsumerThroughput +
                " SlowConsumerRetries=" + _slowConsumerRetries +
                " SlowConsumerLatency=" + _slowConsumerLatency;
    }

    /**
     * Prepares log massage.
     */
    private String prepareSlowConsumerCloseMsg(SocketAddress address) {
        return "Closed slow consumer: " + address +
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

    public void closeContext() {
        _oos.closeContext();
    }

    public void resetContext() {
        _oos.resetContext();
    }

    public long getGeneratedTraffic() {
        return _generatedTraffic;
    }

    public void writeProtocolValidationHeader() throws IOException {
        ProtocolValidation.writeProtocolValidationHeader(_sockChannel, Long.MAX_VALUE);
    }

}
