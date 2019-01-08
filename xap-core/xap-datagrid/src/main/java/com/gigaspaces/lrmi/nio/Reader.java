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

import com.gigaspaces.exception.lrmi.LRMIUnhandledException;
import com.gigaspaces.internal.backport.java.util.concurrent.atomic.LongAdder;
import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.MarshalContextClearedException;
import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIUtilities;
import com.gigaspaces.lrmi.SmartByteBufferCache;
import com.gigaspaces.lrmi.nio.SystemRequestHandler.SystemRequestContext;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;
import com.gigaspaces.lrmi.nio.filters.IOFilterManager;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.kernel.SystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.io.ObjectStreamConstants;
import java.io.UnsupportedEncodingException;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.charset.Charset;
import java.rmi.NoSuchObjectException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * A Reader is capable of reading Request Packets and Reply Packets from a Socket Channel. An NIO
 * Client Peer uses an instance of a Reader to read Reply Packets while an NIO Server uses an
 * instance of a Reader to read Request Packets.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
@com.gigaspaces.api.InternalApi
public abstract class Reader {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);
    private static final Logger offendingMessageLogger = LoggerFactory.getLogger(Constants.LOGGER_LRMI + ".offending");
    public static final long SUSPICIOUS_THRESHOLD = Long.valueOf(System.getProperty("com.gs.lrmi.suspicious-threshold", "20000000"));
    private static final LongAdder receivedTraffic = new LongAdder();

    private static final byte[] DUMMY_BUFFER = new byte[0];

    // byte array that is used to clear the ObjectInputStream tables after each read
    // the byte array is written to be stream as if it was sent over the network
    // to simulate TC_RESET
    private static final byte[] _resetBuffer = new byte[]{ObjectStreamConstants.TC_RESET, ObjectStreamConstants.TC_NULL};

    protected static final int BUFFER_LIMIT = Integer.getInteger(SystemProperties.MAX_LRMI_BUFFER_SIZE, SystemProperties.MAX_LRMI_BUFFER_SIZE_DEFAULT);

    /* Object stream - initialized with null to simplify the code. */
    private MarshalInputStream _ois;
    final private GSByteArrayInputStream _bais = new GSByteArrayInputStream(DUMMY_BUFFER);

    /* cached data  buffer */
    final private SmartByteBufferCache _bufferCache = SmartByteBufferCache.getDefaultSmartByteBufferCache();

    final protected static int HEADER_SIZE = 4;
    /* data length buffer */
    final private ByteBuffer _headerBuffer = ByteBuffer.allocateDirect(HEADER_SIZE);

    private boolean _bufferIsOccupied = false;

    private IOFilterManager _filterManager;

    final private MarshalInputStream.Context _streamContext;

    private long _receivedTraffic;

    private final SystemRequestHandler _systemRequestHandler;

    public static LongAdder getReceivedTrafficCounter() {
        return receivedTraffic;
    }

    protected Reader(SystemRequestHandler systemRequestHandler) {
        this._systemRequestHandler = systemRequestHandler;
        _headerBuffer.order(ByteOrder.BIG_ENDIAN);
        _streamContext = MarshalInputStream.createContext();
        try {
            _ois = new MarshalInputStream(_bais, _streamContext);
        } catch (IOException e) {
            if (_logger.isErrorEnabled()) {
                _logger.error(e.getMessage(), e);
            }
            throw new RuntimeException("Failed to initialize LRMI Reader stream: ", e);
        }
    }

    public MarshalInputStream readRequest(Context ctx) throws IOException, IOFilterException {
        return bytesToStream(ctx);
    }

    public RequestPacket readRequest(boolean createNewBuffer) throws IOException, ClassNotFoundException, IOFilterException {
        return bytesToPacket(new RequestPacket(), createNewBuffer, 0, 0);
    }

    public RequestPacket readRequest() throws IOException, ClassNotFoundException, IOFilterException {
        return bytesToPacket(new RequestPacket(), false, 0, 0);
    }

    public MarshalInputStream readReply(Context ctx) throws IOException, IOFilterException {
        return bytesToStream(ctx);
    }

    public <T> ReplyPacket<T> readReply(boolean createNewBuffer) throws IOException, ClassNotFoundException, IOFilterException {
        return bytesToPacket(new ReplyPacket<T>(), createNewBuffer, 0, 0);
    }

    public <T> ReplyPacket<T> readReply() throws IOException, ClassNotFoundException, IOFilterException {
        return bytesToPacket(new ReplyPacket<T>(), false, 0, 0);
    }

    public <T> void readReply(ReplyPacket<T> packet) throws IOException, ClassNotFoundException, IOFilterException {
        bytesToPacket(packet, false, 0, 0);
    }

    public <T> ReplyPacket<T> readReply(int slowConsumerTimeout, int sizeLimit) throws IOException, ClassNotFoundException, IOFilterException {
        return bytesToPacket(new ReplyPacket<T>(), false, slowConsumerTimeout, sizeLimit);
    }

    public static class Context {
        public enum Phase {START, HEADER, BODY, FINISH}

        public final SelectionKey selectionKey;
        public Phase phase = Phase.START;
        public int bytesRead = 0;
        public ByteBuffer buffer = null;
        public int dataLength = 0;
        public boolean createNewBuffer = false;
        public byte[] bytes;
        public SystemRequestContext systemRequestContext;
        public long startTimestamp = SystemTime.timeMillis();
        public int messageSizeLimit = 0;

        public Context(SelectionKey selectionKey) {
            this.selectionKey = selectionKey;
        }

        public void reset() {
            phase = Phase.START;
            dataLength = 0;
            buffer = null;
            bytes = null;
            systemRequestContext = null;
            startTimestamp = SystemTime.timeMillis();
        }

        public boolean isSystemRequest() {
            return systemRequestContext != null;
        }

    }

    public static class ProtocolValidationContext {
        public final ByteBuffer buffer;
        public final SelectionKey selectionKey;

        public ProtocolValidationContext(SelectionKey selectionKey) {
            this.selectionKey = selectionKey;
            this.buffer = ByteBuffer.allocate(ProtocolValidation.getProtocolHeaderBytesLength());
        }
    }

    public void setFilterManager(IOFilterManager filterManager) {
        this._filterManager = filterManager;
    }

    public ByteBuffer readBytesFromChannelBlocking(boolean createNewBuffer, int slowConsumerLatency, int sizeLimit)
            throws IOException {
        final AtomicInteger retries = new AtomicInteger(0);
        final int dataLength = readHeaderBlocking(_headerBuffer, slowConsumerLatency, retries);
        if (0 < sizeLimit && sizeLimit < dataLength) {
            throw new IOException("Handshake failed expecting message of up to " + sizeLimit + " bytes, actual size is: " + dataLength + " bytes.");
        }
        if (dataLength > SUSPICIOUS_THRESHOLD) {
            _logger.warn("About to allocate " + dataLength + " bytes - from socket channel: " + getEndpointDesc());
        }

        /* allocate the buffer on demand, otherwise reuse the buffer */
        final ByteBuffer buffer = getByteBufferAllocated(createNewBuffer, dataLength);
        readPayloadBlocking(buffer, dataLength, slowConsumerLatency, retries);
        return buffer;
    }

    protected abstract int readHeaderBlocking(ByteBuffer buffer, int slowConsumerLatency, AtomicInteger retries) throws IOException;

    protected abstract void readPayloadBlocking(ByteBuffer buffer, int dataLength, int slowConsumerLatency, AtomicInteger retries) throws IOException;

    private ByteBuffer getByteBufferAllocated(boolean createNewBuffer, int dataLength) {
        try {
            if (createNewBuffer) {
                return ByteBuffer.allocate(dataLength);
            } else {
                return _bufferCache.get(dataLength);
            }
        } catch (OutOfMemoryError outOfMemoryError) {
            _logger.warn("Got out of memory error while trying to allocate byte buffer of size  " + dataLength, outOfMemoryError);
            throw outOfMemoryError;
        }
    }

    private ByteBuffer readBytesFromChannelNoneBlocking(Context ctx)
            throws IOException {

        if (ctx.phase == Context.Phase.START) {
            _headerBuffer.clear();
            ctx.phase = Context.Phase.HEADER;
        }
        if (ctx.phase == Context.Phase.HEADER) {
            if (!readHeaderNonBlocking(ctx))
                return null;
            /** allocate the buffer on demand, otherwise reuse the buffer */
            ctx.buffer = getByteBufferAllocated(ctx.createNewBuffer, ctx.dataLength);
            ctx.bytesRead = 0;
            ctx.phase = Context.Phase.BODY;
        }
        if (ctx.phase == Context.Phase.BODY) {
            return readPayloadNonBlocking(ctx);
        }
        throw new IllegalStateException(String.valueOf(ctx.phase));
    }

    private boolean readHeaderNonBlocking(Context ctx) throws IOException {
        int bRead = read(_headerBuffer);
        ctx.bytesRead += bRead;

        if (ctx.bytesRead < HEADER_SIZE) {
            return false;
        }
        incReceivedTraffic(HEADER_SIZE);
        _headerBuffer.flip();
        ctx.dataLength = _headerBuffer.getInt();

        // Process system request if needed:
        if (ctx.dataLength < 0 && _systemRequestHandler.handles(ctx.dataLength /* represents request header */)) {
            ctx.systemRequestContext = _systemRequestHandler.getRequestContext(ctx.dataLength);
            ctx.dataLength = ctx.systemRequestContext.getRequestDataLength();
        }

        if (ctx.messageSizeLimit != 0 && ctx.messageSizeLimit <= ctx.dataLength) {
            String msg = "Handshake failed, expecting message of up to " + ctx.messageSizeLimit + " bytes, actual size is: " + ctx.dataLength + " bytes, offending address is " + getEndPointAddressDesc();
            if (offendingMessageLogger.isTraceEnabled()) {
                try {
                    ByteBuffer buffer = getByteBufferAllocated(ctx.createNewBuffer, Math.min(ctx.dataLength, 5 * 1024));
                    directRead(buffer);
                    buffer.flip();
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    try {
                        String str = new String(bytes, "UTF-8");
                        offendingMessageLogger.trace(msg + ", received string is : " + str);
                    } catch (UnsupportedEncodingException e) {
                        offendingMessageLogger.trace(msg + ", base64 encoding of the received  buffer is : " + new BASE64Encoder().encode(bytes));
                    }
                } catch (Exception ignored) {
                }
            }
            throw new ConnectException(msg);
        }
        return true;
    }

    private ByteBuffer readPayloadNonBlocking(Context ctx) throws IOException {
        boolean shouldUseSlidingWindow = ctx.dataLength >= BUFFER_LIMIT;

        if (shouldUseSlidingWindow) {
            while (ctx.bytesRead < ctx.dataLength) {
                ctx.buffer.position(ctx.bytesRead).limit(Math.min(ctx.dataLength, ctx.bytesRead + BUFFER_LIMIT));
                ByteBuffer window = ctx.buffer.slice();
                int bRead = read(window);
                ctx.bytesRead += bRead;

                if (bRead < window.capacity()) {
                    return null;
                }
            }
        } else {
            int bRead = read(ctx.buffer);

            ctx.bytesRead += bRead;
            if (ctx.bytesRead < ctx.dataLength) {
                return null;
            }
        }

        ctx.phase = Context.Phase.FINISH;
        incReceivedTraffic(ctx.buffer.position());
        ctx.buffer.position(0);
        ctx.buffer.limit(ctx.dataLength);
        return ctx.buffer;
    }

    protected abstract String getEndpointDesc();

    protected abstract SocketAddress getEndPointAddress();

    protected abstract String getEndPointAddressDesc();

    public RequestPacket unmarshallRequest(MarshalInputStream stream) throws ClassNotFoundException, NoSuchObjectException {
        RequestPacket packet = new RequestPacket();
        unmarshall(packet, stream);

        return packet;
    }

    public <T> ReplyPacket<T> unmarshallReply(MarshalInputStream stream) throws ClassNotFoundException, NoSuchObjectException {
        ReplyPacket<T> packet = new ReplyPacket<T>();
        unmarshall(packet, stream);

        if (_logger.isTraceEnabled()) {
            _logger.trace("<-- Read Packet " + packet);
        }
        return packet;
    }

    private MarshalInputStream bytesToStream(Context ctx)
            throws IOException, IOFilterException {
        boolean startOfRequest = (ctx.phase == Context.Phase.START);
        if (_bufferIsOccupied && startOfRequest)
            ctx.createNewBuffer = true;
        else
            _bufferIsOccupied = true;

        byte[] res = readBytesNonBlocking(ctx);

        boolean endOfRequest = ctx.phase == Context.Phase.FINISH;
        if (endOfRequest) {
            if (ctx.isSystemRequest()) {
                ctx.systemRequestContext.prepare(res);
            } else {
                ctx.bytes = res;
                if (ctx.createNewBuffer) {
                    ctx.createNewBuffer = false;
                    return new MarshalInputStream(new GSByteArrayInputStream(res), _streamContext);
                }

                _bais.setBuffer(res);
                return _ois;
            }
        }
        return null;
    }

    private <T extends IPacket> T bytesToPacket(T packet, boolean createNewBuffer, int slowConsumerTimeout, int sizeLimit)
            throws IOException, ClassNotFoundException, IOFilterException {
        if (_bufferIsOccupied || createNewBuffer) {
            GSByteArrayInputStream bis = new GSByteArrayInputStream(readBytesBlocking(true, slowConsumerTimeout, sizeLimit));
            MarshalInputStream mis = new MarshalInputStream(bis, _streamContext);
            unmarshall(packet, mis);
            if (_logger.isTraceEnabled()) {
                _logger.trace("<-- Read Packet " + packet);
            }
            return packet;
        }

        _bufferIsOccupied = true;
        _bais.setBuffer(readBytesBlocking(false, slowConsumerTimeout, sizeLimit));
        unmarshall(packet, _ois);
        if (_logger.isTraceEnabled()) {
            _logger.trace("<-- Read packet " + packet);
        }
        return packet;
    }

    private void unmarshall(IPacket packet, MarshalInputStream mis) throws ClassNotFoundException, NoSuchObjectException {
        try {
            packet.readExternal(mis);

            resetStreamState(mis);
        } catch (MarshalContextClearedException e) {
            // Doesn't wrap it as UnMarshallingException, but rethrow so the server will close the connection.
            throw e;
        } catch (NoSuchObjectException e) {
            // Doesn't wrap it as UnMarshallingException, but rethrow so the server will close the connection.
            throw e;
        } catch (ClassNotFoundException e) {
            throw e;
        } catch (LRMIUnhandledException e) {
            //Special exception that should be thrown as is without side effects
            boolean isReusedBuffer = mis == _ois;
            try {
                //We need to create a new buffer because this buffer is unusable anymore because it was not read fully
                if (isReusedBuffer)
                    _ois = new MarshalInputStream(_bais, _streamContext);
            } catch (IOException ioe) {
                throw new UnMarshallingException("Failed to unmarsh :" + packet, ioe);
            } finally {
                if (isReusedBuffer && _bufferIsOccupied) {
                    _bais.setBuffer(DUMMY_BUFFER); // release the internal reference for the byte array
                    _bufferIsOccupied = false;
                }
            }

            throw e;
        } catch (Exception e) {
            throw new UnMarshallingException("Failed to unmarsh :" + packet, e);
        }
    }

    private void resetStreamState(MarshalInputStream mis) throws IOException,
            ClassNotFoundException {
        if (mis == _ois) {
            try {
                //this is the only way to do reset on ObjetInputStream:
                // add reset flag and let the ObjectInputStream to read it 
                // so all the handles in the ObjectInputStream will be cleared
                _bais.setBuffer(_resetBuffer);
                mis.readObject();
            } finally {
                if (_bufferIsOccupied) {
                    _bais.setBuffer(DUMMY_BUFFER); // release the internal reference for the byte array
                    _bufferIsOccupied = false;
                }
            }
        }
    }

    private byte[] readBytesBlocking(boolean createNewBuffer, int slowConsumerTimeout, int sizeLimit) throws IOException, IOFilterException {
        final ByteBuffer bytes = readBytesFromChannelBlocking(createNewBuffer, slowConsumerTimeout, sizeLimit);
        if (_filterManager != null) {
            return _filterManager.handleBlockingContant(toByteArray(bytes), slowConsumerTimeout);
        }

        return bytes.array();
    }

    private byte[] toByteArray(ByteBuffer bytes) {
        byte[] res = new byte[bytes.remaining()];
        bytes.get(res);
        return res;
    }

    /**
     * Reads from socket to _bytes.
     *
     * @return the bytes that was read.
     */
    private byte[] readBytesNonBlocking(Context ctx) throws IOException, IOFilterException {
        ByteBuffer bytes = readBytesFromChannelNoneBlocking(ctx);
        if (bytes == null) {
            return null;
        }
        if (ctx.phase == Context.Phase.FINISH) {
            if (_filterManager == null || ctx.isSystemRequest())
                return bytes.array();

            return _filterManager.handleNoneBlockingContant(ctx, toByteArray(bytes));
        }

        return null;
    }

    public void closeContext() {
        _ois.closeContext();
    }

    public void resetContext() {
        _ois.resetContext();
    }

    public long getReceivedTraffic() {
        return _receivedTraffic;
    }

    public String readProtocolValidationHeader(ProtocolValidationContext context) throws IOException {
        read(context.buffer);
        byte[] contentBuffer = Arrays.copyOf(context.buffer.array(), context.buffer.position());
        return new String(contentBuffer, Charset.forName("UTF-8"));
    }

    protected int read(ByteBuffer buffer) throws IOException {
        int bytesRead = directRead(buffer);
        if (bytesRead != -1)
            return bytesRead;
        ClosedChannelException e = new ClosedChannelException();
        e.initCause(new IOException("Connection has been closed by peer"));
        throw e;
    }

    protected abstract int directRead(ByteBuffer buffer) throws IOException;

    protected void incReceivedTraffic(int val) {
        _receivedTraffic += val;
        receivedTraffic.add(val);
    }
}
