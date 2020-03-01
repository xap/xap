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

package com.gigaspaces.lrmi.nio.filters;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIInvocationTrace;
import com.gigaspaces.lrmi.nio.IChannelWriter;
import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.Writer;
import com.gigaspaces.lrmi.nio.Writer.Context.Phase;

import net.jini.space.InternalSpaceException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.j_spaces.kernel.SystemProperties.LRMI_NETWORK_FILTER_UNWRAP_BUFFER_ALLOCATION_STRATEGY;

@com.gigaspaces.api.InternalApi
public class IOBlockFilterContainer {
    private static final Logger logger = Logger.getLogger(IOBlockFilterContainer.class.getName());
    private static final Logger _contextLogger = Logger.getLogger(Constants.LOGGER_LRMI_CONTEXT);

    public static final byte MESSAGE_SUFFIX = 0;
    public static final byte PREFIX_PART = 1;
    private static final int HUNDRED_M = 100 * 1024 * 1024;
    private final IChannelWriter writer;
    private final Reader reader;

    private boolean newContent;
    private final static ByteBuffer empty = ByteBuffer.wrap(new byte[]{});

    private static final UnwrapAllocationStrategy UNWRAP_ALLOCATION_STRATEGY
            = UnwrapAllocationStrategy.fromString(System.getProperty(LRMI_NETWORK_FILTER_UNWRAP_BUFFER_ALLOCATION_STRATEGY, "CONSTANT"));

    public IOBlockFilterContainer(Reader reader, IChannelWriter writer) {
        this.reader = reader;
        this.writer = writer;
    }

    public void wrap(IOFilterContext context) throws IOFilterException {
        context.getDst().putInt(0); // message size
        context.getDst().put(MESSAGE_SUFFIX);
        context.filter.wrap(context.getSrc(), context.getDst());
        if (context.getSrc().hasRemaining()) {
            context.getDst().put(4, PREFIX_PART);
            context.result.setStatus(IOFilterResult.Status.BUFFER_OVERFLOW);
        } else {
            context.result.setStatus(IOFilterResult.Status.OK);
        }
        context.getDst().flip();
        context.getDst().putInt(0, context.getDst().remaining() - 4); // message size
    }

    public void unwrap(IOFilterContext context) throws IOFilterException {
        if (context.getSrc().remaining() < context.applicationBufferSize)
            enlargeSourceBuffer(context);
        boolean isLastMessage = (context.getDst().get() == MESSAGE_SUFFIX);
        context.filter.unwrap(context.getDst(), context.getSrc());
        if (context.result.getStatus() == IOFilterResult.Status.CLOSED) {
            throw new InternalSpaceException("IOFilter closed");
        }
        while (context.getDst().hasRemaining()) {
            if (context.getSrc().remaining() < context.applicationBufferSize)
                enlargeSourceBuffer(context);
            context.filter.unwrap(context.getDst(), context.getSrc());
            if (context.result.getStatus() == IOFilterResult.Status.CLOSED) {
                throw new InternalSpaceException("IOFilter closed");
            }
        }
        if (isLastMessage) {
            // while source still not empty / enlarge dst and
            context.result.setStatus(IOFilterResult.Status.OK);
            context.getSrc().flip();
        } else {
            context.result.setStatus(IOFilterResult.Status.BUFFER_UNDERFLOW);
        }
    }

    /**
     * This method called from unrap where the context src buffer is actually the target buffer.
     * Allocate and return a new src (target buffer) according to the remaining space
     * in the src buffer the application buffer size and the allocation policy.
     * @param context the unwrap context.
     */
    private void enlargeSourceBuffer(IOFilterContext context) {
        switch (UNWRAP_ALLOCATION_STRATEGY){
            case CONSTANT:
                context.setSrc(enlargeBuffer(context.getSrc(),
                        context.applicationBufferSize - context.getSrc().remaining()));
                break;
            case EXPONENTIAL:
                int additionalSize = Math.min(context.getSrc().capacity(), HUNDRED_M);
                context.setSrc(enlargeBuffer(context.getSrc(), additionalSize));
                break;
        }
    }

    private ByteBuffer enlargeBuffer(ByteBuffer src, int additionalSize) {
        ByteBuffer res = ByteBuffer.allocate(src.limit() + additionalSize);
        src.flip();
        res.put(src);
        res.position(src.position());
        return res;
    }

    public IOFilterContext createContext(IOBlockFilter filter) {
        IOFilterContext ctx = new IOFilterContext();
        ctx.filter = filter;
        ctx.setSrc(ByteBuffer.allocate(filter.getApplicationBufferSize()));
        ctx.setDst(ByteBuffer.allocate(filter.getPacketBufferSize() + 5));
        ctx.applicationBufferSize = filter.getApplicationBufferSize();
        ctx.packetBufferSize = filter.getPacketBufferSize();
        ctx.result = new IOFilterResult(IOFilterResult.Status.OK,
                IOFilterResult.HandshakeStatus.NOT_HANDSHAKING, 0, 0);
        return ctx;
    }

    public synchronized void writeBytesNonBlocking(Writer.Context ctx,
                                                   IOFilterContext filterContext) throws IOFilterException, IOException {
        if (filterContext.result.getHandshakeStatus() != IOFilterResult.HandshakeStatus.NOT_HANDSHAKING) {
            throw new IOFilterException("Request to write bytes while handshake " + filterContext.result.getHandshakeStatus());
        }
        ctx.getBuffer().getInt(); // Swallow the message header.
        Writer.Context context = ctx.duplicate();

        ByteBuffer orig = filterContext.getSrc();
        try {
            filterContext.setSrc(context.getBuffer());
            wrap(filterContext);
            context = createWriteContext(ctx, filterContext);
            context.setBuffer(copy(filterContext.getDst()));
            filterContext.getDst().clear();
            context.setCurrentPosition(0);
            context.setPhase(Phase.START);

            boolean finished = filterContext.result.getStatus() == IOFilterResult.Status.OK;
            writer.writeBytesToChannelNoneBlocking(context, finished);
            while (!finished) {
                wrap(filterContext);
                // with last message reuse original cxt let AsyncContext know message write is done.
                context = createWriteContext(ctx, filterContext);
                context.setBuffer(copy(filterContext.getDst()));
                filterContext.getDst().clear();
                context.setCurrentPosition(0);
                context.setPhase(Phase.START);

                finished = filterContext.result.getStatus() == IOFilterResult.Status.OK;
                writer.writeBytesToChannelNoneBlocking(context, finished);
            }
        } finally {
            filterContext.setSrc(orig);
        }
        if (filterContext.result.getHandshakeStatus() != IOFilterResult.HandshakeStatus.NOT_HANDSHAKING) {
            throw new IOFilterException("Request to write bytes while handshake " + filterContext.result.getHandshakeStatus());
        }
    }

    private Writer.Context createWriteContext(Writer.Context ctx,
                                              IOFilterContext filterContext) {
        Writer.Context context;
        if (filterContext.result.getStatus() == IOFilterResult.Status.OK) {
            context = ctx;
        } else {
            LRMIInvocationTrace trace = _contextLogger.isLoggable(Level.FINE) ? LRMIInvocationContext.getCurrentContext().getTrace() : null;
            context = new Writer.Context(trace);
        }
        return context;
    }

    public synchronized byte[] handleNoneBlockingContent(Reader.Context ctx, byte[] bytes,
                                                         IOFilterContext filterContext) throws IOFilterException {
        try {
            if (filterContext.result.getHandshakeStatus() != IOFilterResult.HandshakeStatus.NOT_HANDSHAKING) {
                newContent = true;
//				logger.info("Copy " + bytes.length + " to " + filterContext.getDst() + " for filter " + filterContext.filter);
                copy(filterContext.getDst(), bytes);
                processHandshake(filterContext);
                ctx.reset();
                return null;
            }
        } catch (Exception e) {
            throw new IOFilterException(e);
        }
        ByteBuffer old = filterContext.getDst();
        filterContext.setDst(ByteBuffer.wrap(bytes));
        try {
            unwrap(filterContext);
            if (filterContext.result.getStatus() == IOFilterResult.Status.OK) {
                filterContext.getDst().clear();
                return toBytes(filterContext.getSrc());
            } else {
                ctx.phase = Reader.Context.Phase.START;
                ctx.bytesRead = 0;
            }
        } finally {
            filterContext.setDst(old);
        }
        return null;
    }

    private void copy(ByteBuffer buf, byte[] bytes) {
        buf.clear();
        buf.put(bytes);
        buf.flip();
    }

    public synchronized byte[] handleBlockingContant(byte[] bytes,
                                                     IOFilterContext filterContext, int slowConsumerTimeout) throws ClosedChannelException,
            IOFilterException {
        try {
            if (filterContext.result.getHandshakeStatus() != IOFilterResult.HandshakeStatus.NOT_HANDSHAKING) {
                newContent = true;
                copy(filterContext.getDst(), bytes);
                processHandshake(filterContext);
                return null;
            }
        } catch (Exception e) {
            throw new IOFilterException(e);
        }
        ByteBuffer source = ByteBuffer.wrap(bytes);
        ByteBuffer old = filterContext.getDst();
        try {
            filterContext.result.setStatus(IOFilterResult.Status.BUFFER_UNDERFLOW);
            while (filterContext.result.getStatus() != IOFilterResult.Status.OK) {
                filterContext.setDst(source);
                unwrap(filterContext);
                if (filterContext.result.getStatus() == IOFilterResult.Status.BUFFER_UNDERFLOW) {
                    source = reader.readBytesFromChannelBlocking(true, slowConsumerTimeout, 0);
                }
            }
            filterContext.getDst().flip();
            return toBytes(filterContext.getSrc());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to handle blocking content", e);
            return bytes;
        } finally {
            filterContext.setDst(old);
        }
    }

    public synchronized void writeBytesBlocking(ByteBuffer dataBuffer,
                                                IOFilterContext filterContext) throws IOFilterException, IOException {
        if (filterContext.result.getHandshakeStatus() != IOFilterResult.HandshakeStatus.NOT_HANDSHAKING) {
            throw new IOFilterException("Request to write bytes while handshake " + filterContext.result.getHandshakeStatus());
        }
        dataBuffer.getInt(); // Swallow the message header.
        ByteBuffer originalSrcBuf = filterContext.getSrc();
        try {

            filterContext.setSrc(dataBuffer);
            wrap(filterContext);
            writer.writeBytesToChannelBlocking(filterContext.getDst());
            filterContext.getDst().clear();
            while (filterContext.result.getStatus() != IOFilterResult.Status.OK) {
                wrap(filterContext);
                writer.writeBytesToChannelBlocking(filterContext.getDst());
                filterContext.getDst().clear();
            }
        } finally {
            filterContext.setSrc(originalSrcBuf);
        }
        if (filterContext.result.getHandshakeStatus() != IOFilterResult.HandshakeStatus.NOT_HANDSHAKING) {
            throw new IOFilterException("Request to write bytes while handshake " + filterContext.result.getHandshakeStatus());
        }
    }

    public void beginHandshake(IOFilterContext filterContext) throws IOFilterException, IOException {
        filterContext.filter.beginHandshake();
        filterContext.result.setHandshakeStatus(filterContext.filter
                .getHandshakeStatus());
        processHandshake(filterContext);
    }

    private void processHandshake(IOFilterContext filterContext)
            throws IOFilterException, IOException {
        boolean isBlocking = writer.isBlocking();
        while (filterContext.result.getHandshakeStatus() != IOFilterResult.HandshakeStatus.NOT_HANDSHAKING) {
            handshakeOneStep(filterContext, isBlocking);
            if (needToWaitForMoreBytes(filterContext, isBlocking)) {
                return;
            }
        }
    }

    /**
     * @return true iff handshake process need more info from other side and we are in none blocking
     * mode.
     */
    private boolean needToWaitForMoreBytes(IOFilterContext filterContext,
                                           boolean isBlocking) {
        return !isBlocking
                && filterContext.result.getHandshakeStatus() == IOFilterResult.HandshakeStatus.NEED_UNWRAP;
    }

    // do one handsake action and update the hs status.
    private void handshakeOneStep(IOFilterContext filterContext,
                                  boolean isBlocking) throws IOFilterException, IOException {
        if (filterContext.result.getStatus() == IOFilterResult.Status.CLOSED) {
            throw new InternalSpaceException("IOFilter closed");
        }
        switch (filterContext.result.getHandshakeStatus()) {
            case FINISHED:
                filterContext.result.setHandshakeStatus(filterContext.filter
                        .getHandshakeStatus());
                filterContext.getDst().clear();
                filterContext.getSrc().clear();
                return;
            case NEED_TASK:
                handleNeedTask(filterContext);
                filterContext.result.setHandshakeStatus(filterContext.filter
                        .getHandshakeStatus());
                return;
            case NEED_UNWRAP:
                handleNeedUnwrap(filterContext, isBlocking);
                filterContext.result.setHandshakeStatus(filterContext.filter
                        .getHandshakeStatus());
                return;
            case NEED_WRAP:
                handleNeedWrap(filterContext, isBlocking);
                filterContext.result.setHandshakeStatus(filterContext.filter
                        .getHandshakeStatus());
                return;
            case NOT_HANDSHAKING:
                filterContext.result.setHandshakeStatus(filterContext.filter
                        .getHandshakeStatus());
                return;
            default:
                throw new IllegalStateException(String.valueOf(filterContext.result
                        .getHandshakeStatus()));
        }

    }

    private void handleNeedWrap(IOFilterContext filterContext,
                                boolean isBlocking) throws IOFilterException, IOException {
        ByteBuffer buf = ByteBuffer
                .allocate(filterContext.packetBufferSize + 4);
        buf.order(ByteOrder.BIG_ENDIAN);

        buf.putInt(0); // message size

        filterContext.filter.wrap(empty, buf);
        buf.flip();
        buf.putInt(0, buf.remaining() - 4);
        // add the size in the beginning;
        if (isBlocking) {
            writer.writeBytesToChannelBlocking(buf);
        } else {
            LRMIInvocationTrace trace = _contextLogger.isLoggable(Level.FINE) ? LRMIInvocationContext.getCurrentContext().getTrace() : null;
            Writer.Context ctx = new Writer.Context(trace);
            ctx.setTotalLength(buf.remaining());
            ctx.setBuffer(buf);
            //Does not get here from write path, read interest was not explicitly
            //restored after this so we should not restore it out selves.
            writer.writeBytesToChannelNoneBlocking(ctx, false);
        }
    }

    private void handleNeedUnwrap(IOFilterContext filterContext,
                                  boolean isBlocking) throws IOFilterException, IOException {
        if (isBlocking) {
            handleBlokingNeedUnwrap(filterContext);
        } else {
            handleNoneBlokingNeedUnwrap(filterContext);
        }
    }

    private void handleNoneBlokingNeedUnwrap(IOFilterContext filterContext)
            throws IOFilterException {
        if (newContent) {
            filterContext.filter.unwrap(filterContext.getDst(), filterContext.getSrc());
            filterContext.getDst().clear();
            newContent = false;
        }
    }

    private void handleBlokingNeedUnwrap(IOFilterContext filterContext)
            throws IOFilterException, IOException {
        ByteBuffer bytes = reader.readBytesFromChannelBlocking(true, 0, 0);
        filterContext.filter.unwrap(bytes, filterContext.getSrc());
    }

    private void handleNeedTask(IOFilterContext filterContext) {
        filterContext.filter.getDelegatedTask().run();
    }

    private ByteBuffer copy(ByteBuffer buffer) {
        ByteBuffer res = ByteBuffer.allocate(buffer.remaining());
        for (int i = buffer.position(); i < buffer.limit(); ++i) {
            res.put(i, buffer.get(i));
        }
        return res;

    }

    public static byte[] toBytes(ByteBuffer buf) {
        byte[] res = new byte[buf.remaining()];
        buf.get(res);
        buf.clear();
        return res;
    }

    public static List<Byte> toList(byte[] array) {
        if (array == null) {
            return Collections.emptyList();
        }
        List<Byte> res = new ArrayList<Byte>(array.length);
        for (int i = 0; i < array.length; ++i) {
            res.add(i, array[i]);
        }
        return res;
    }

}

class WriteBuffer {
    private final ByteBuffer all;
    private final ByteBuffer messagePart;

    public WriteBuffer(int size) {
        all = ByteBuffer.allocate(size + 4 + 1);
        all.order(ByteOrder.BIG_ENDIAN);
        all.position(4);
        messagePart = all.slice();
        messagePart.order(ByteOrder.BIG_ENDIAN);
    }

    public ByteBuffer getMessagePart() {
        return messagePart;
    }

    public ByteBuffer getAll() {
        return all;
    }

}

enum UnwrapAllocationStrategy{
    CONSTANT,
    EXPONENTIAL;

    public static UnwrapAllocationStrategy fromString(String text) {
        if("EXPONENTIAL".equalsIgnoreCase(text)){
            return EXPONENTIAL;
        }
        return CONSTANT;
    }
}
