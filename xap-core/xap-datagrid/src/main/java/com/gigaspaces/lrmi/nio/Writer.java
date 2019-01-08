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

import com.gigaspaces.internal.backport.java.util.concurrent.atomic.LongAdder;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationTrace;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;
import com.gigaspaces.lrmi.nio.filters.IOFilterManager;
import com.gigaspaces.lrmi.tcp.TcpWriter;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;


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
public abstract class Writer implements IChannelWriter {
    final private static Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);

    final static public int BUFFER_LIMIT = Integer.getInteger(SystemProperties.MAX_LRMI_BUFFER_SIZE, SystemProperties.MAX_LRMI_BUFFER_SIZE_DEFAULT);

    final private PacketSerializer serializer;

    protected IWriteInterestManager _writeInterestManager;

    private static final LongAdder generatedTraffic = new LongAdder();
    private long _generatedTraffic;

    IOFilterManager _filterManager;

    private static final LongAdder pendingWrites = new LongAdder();

    public static LongAdder getGeneratedTrafficCounter() {
        return generatedTraffic;
    }

    public static LongAdder getPendingWritesCounter() {
        return pendingWrites;
    }

    protected Writer() {
        serializer = new PacketSerializer();
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
    public abstract SocketAddress getEndPointAddress();

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

    public abstract boolean isOpen();

    //Access to contexts should be synchronized
    private synchronized void writePacket(IPacket packet, boolean requestReuseBuffer, Context ctx) throws IOException, IOFilterException {
        if (_logger.isTraceEnabled()) {
            _logger.trace("--> Write Packet " + packet);
        }
        ByteBuffer buffer = serializer.serialize(packet, requestReuseBuffer&& !hasQueuedContexts());
        _generatedTraffic += buffer.limit();
        generatedTraffic.add(buffer.limit());

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

    protected abstract boolean hasQueuedContexts();

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

    @Override
    public synchronized void writeBytesToChannelNoneBlocking(Context ctx, boolean restoreReadInterest) throws IOException {
        writeNonBlocking(ctx, restoreReadInterest);
    }

    protected abstract void writeNonBlocking(Context ctx, boolean restoreReadInterest) throws IOException;

    /**
     * Called from WriteSelectorThread to complete pending write requests.
     *
     * This is synchronized to ensure mutual exclusion with writeBytesToChannelNoneBlocking method
     *
     * @see TcpWriter#noneBlockingWrite
     */
    public synchronized void onWriteEvent() throws IOException {
        onWriteEventImpl();
    }

    protected abstract void onWriteEventImpl() throws IOException;

    public PacketSerializer getSerializer() {
        return serializer;
    }

    public long getGeneratedTraffic() {
        return _generatedTraffic;
    }

    public abstract void writeProtocolValidationHeader() throws IOException;
}
