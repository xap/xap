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

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.internal.backport.java.util.concurrent.atomic.LongAdder;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.internal.io.MarshalContextClearedException;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationTrace;
import com.gigaspaces.lrmi.SmartByteBufferCache;
import com.gigaspaces.lrmi.Transmitter;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;
import com.gigaspaces.lrmi.nio.filters.IOFilterManager;
import com.gigaspaces.lrmi.tcp.TcpTransmitter;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    final private static Logger _logger = Logger.getLogger(Constants.LOGGER_LRMI);

    final private Transmitter _transmitter;

    final static public int BUFFER_LIMIT = Integer.getInteger(SystemProperties.MAX_LRMI_BUFFER_SIZE, SystemProperties.MAX_LRMI_BUFFER_SIZE_DEFAULT);

    final static private int LENGTH_SIZE = 4; //4 bytes for length

    final private MarshalOutputStream _oos;
    final private GSByteArrayOutputStream _baos;

    /**
     * reuse buffer, growing on demand.
     */
    final private SmartByteBufferCache _bufferCache = SmartByteBufferCache.getDefaultSmartByteBufferCache();

    private static final LongAdder generatedTraffic = new LongAdder();
    private long _generatedTraffic;

    final private static byte[] DUMMY_BUFFER = new byte[0];

    IOFilterManager _filterManager;

    private static final LongAdder pendingWrites = new LongAdder();

    public static LongAdder getGeneratedTrafficCounter() {
        return generatedTraffic;
    }

    public static LongAdder getPendingWritesCounter() {
        return pendingWrites;
    }

    public Writer(SocketChannel socketChannel) {
        this(socketChannel, null);
    }

    public Writer(SocketChannel socketChannel, ITransportConfig config) {
        this(new TcpTransmitter(socketChannel, config));
    }

    public Writer(Transmitter transmitter) {
        _transmitter = transmitter;

        try {
            _baos = new GSByteArrayOutputStream();
            _baos.setSize(LENGTH_SIZE); // mark the buffer to start writing only after the length place
            _oos = new MarshalOutputStream(_baos, true); // add a TC_RESET using the MarshalOutputStream.writeStreamHeader() 
            initBuffer(_baos);
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, e.getMessage(), e);
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
        _transmitter.setWriteInterestManager(writeInterestManager);
    }

    /**
     * @return the endpoint of the connected SocketChannel.
     */
    public SocketAddress getEndPointAddress() {
        return _transmitter.getEndPointAddress();
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
        return _transmitter.isOpen();
    }

    //Access to contexts should be synchronized
    private synchronized void writePacket(IPacket packet, boolean requestReuseBuffer, Context ctx) throws IOException, IOFilterException {
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.finest("--> Write Packet " + packet);
        }
        ByteBuffer buffer = serialize(packet, requestReuseBuffer);

        if (ctx != null) {
            // non blocking mode.
            ctx.setBuffer(buffer);
            if (_filterManager != null && !ctx.isSystemResponse()) {
                _filterManager.writeBytesNonBlocking(ctx);
            } else {
                //Regular write Bytes non blocking, restore read interest if finish writing synchronously
                writeBytesToChannelNoneBlocking(ctx, true);
            }
        } else {
            // blocking mode.
            if (_filterManager != null) {
                _filterManager.writeBytesBlocking(buffer);
            } else {
                writeBytesToChannelBlocking(buffer);
            }
        }
    }

    private ByteBuffer serialize(IPacket packet, boolean requestReuseBuffer) throws IOException {
        ByteBuffer byteBuffer;
        MarshalOutputStream mos;
        GSByteArrayOutputStream bos;

        final boolean reuseBuffer = requestReuseBuffer && !_transmitter.hasQueuedContexts();
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
        return buffer;
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
        return _transmitter.isBlocking();
    }

    @Override
    public synchronized void writeBytesToChannelNoneBlocking(Context ctx, boolean restoreReadInterest) throws IOException {
        _transmitter.writeBytesToChannelNoneBlocking(ctx, restoreReadInterest);
    }

    @Override
    public void writeBytesToChannelBlocking(ByteBuffer dataBuffer) throws IOException {
        _transmitter.writeBytesToChannelBlocking(dataBuffer);
    }

    /**
     * Called from WriteSelectorThread to complete pending write requests.
     *
     * This is synchronized to ensure mutual exclusion with writeBytesToChannelNoneBlocking method
     *
     * @see TcpTransmitter#noneBlockingWrite
     */
    public synchronized void onWriteEvent() throws IOException {
        _transmitter.onWriteEvent();
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
        _transmitter.writeProtocolValidationHeader();
    }
}