package com.gigaspaces.internal.space.transport.xnio;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.internal.space.transport.xnio.server.ReadProcessor;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIUtilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Niv Ingberg
 * @since 16.0
 */
@ExperimentalApi
public class XNioChannel {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);
    private static final int LENGTH_SIZE = 4;
    private static final long SUSPICIOUS_THRESHOLD = Long.parseLong(System.getProperty("com.gs.lrmi.suspicious-threshold", "20000000"));
    private static final boolean DIRECT_BUFFERS = XNioSettings.DIRECT_BUFFERS;

    private final SocketChannel socketChannel;
    private final ByteBuffer headerBuffer = ByteBuffer.allocateDirect(LENGTH_SIZE);
    private ByteBuffer nbrDataBuffer;
    private int nbrIterations = 1;

    private final ByteBufferBackedOutputStream bos = new ByteBufferBackedOutputStream();
    private final ByteBufferBackedInputStream bis = new ByteBufferBackedInputStream();
    private MarshalOutputStream oos;
    private MarshalInputStream ois;

    private final AtomicInteger fragmentedBlockingWrites = new AtomicInteger();
    private final AtomicInteger fragmentedBlockingReads = new AtomicInteger();
    private final AtomicInteger fragmentedNonBlockingReads = new AtomicInteger();

    private final AtomicBoolean acquisitionLock = new AtomicBoolean();

    private ReadProcessor readProcessor;

    public XNioChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        try {
            LRMIUtilities.initNewSocketProperties(socketChannel);
        } catch (SocketException e) {
            throw new UncheckedIOException(e);
        }
    }

    public boolean tryAcquire() {
        return acquisitionLock.compareAndSet(false, true);
    }

    public void release() {
        acquisitionLock.set(false);
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public ByteBuffer serialize(Object obj) throws IOException {
        bos.setBuffer(null);
        // Hack - placeholder for header.
        bos.getBuffer().putInt(0);
        // Serialize to dynamic buffer (reference might change):
        if (oos == null)
            oos = new MarshalOutputStream(bos);
        else
            oos.reset();
        IOUtils.writeObject(oos, obj);
        oos.flush();
        // Get ref to buffer with serialized data:
        ByteBuffer result = bos.getBuffer();
        // Prepend length:
        result.putInt(0, result.position() - 4);
        result.flip();
        return result;
    }

    public Object deserialize(ByteBuffer buffer) throws IOException, ClassNotFoundException {
        bis.setBuffer(buffer);
        if (ois == null)
            ois = new MarshalInputStream(bis);

        try {
            return IOUtils.readObject(ois);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    public void writeBlocking(ByteBuffer buffer) throws IOException {
        final int length = buffer.remaining();
        int written = 0;
        int iterations = 0;
        while (written < length) {
            int bytes = socketChannel.write(buffer);
            written += bytes;
            iterations++;
        }

        if (iterations != 1)
            warnFragmented(buffer, iterations, fragmentedBlockingWrites, "writeBlocking");
    }

    public ByteBuffer readBlocking() throws IOException {
        headerBuffer.position(0);
        int iterations;
        iterations = readBlocking(headerBuffer);
        ByteBuffer buffer = allocate(headerBuffer.getInt());
        iterations += readBlocking(buffer);
        if (iterations != 2)
            warnFragmented(buffer, iterations - 1, fragmentedBlockingReads, "readBlocking");
        return buffer;
    }

    public ByteBuffer readNonBlocking() throws IOException {
        // Non-blocking read may read partial data, which needs to be preserved and resumed when called again.
        // If the data buffer has not been initialized, we're in the header:
        if (nbrDataBuffer == null) {
            // Try to read the header - if read is not completed abort:
            if (!readNonBlocking(headerBuffer))
                return null;
            // Header read successfully - init data buffer:
            nbrDataBuffer = allocate(headerBuffer.getInt());
        }
        // Try to read the data - if read is not completed abort:
        if (!readNonBlocking(nbrDataBuffer))
            return null;
        if (nbrIterations != 1) {
            warnFragmented(nbrDataBuffer, nbrIterations, fragmentedNonBlockingReads, "readNonBlocking");
        }
        // Read operation completed successfully - reset context for next read operation and return result:
        ByteBuffer result = nbrDataBuffer;
        nbrDataBuffer = null;
        headerBuffer.position(0);
        nbrIterations = 1;
        return result;
    }

    private boolean readNonBlocking(ByteBuffer buffer) throws IOException {
        int bRead = socketChannel.read(buffer);
        if (bRead == -1) // EOF
            throw new ClosedChannelException();

        if (buffer.hasRemaining()) {
            nbrIterations++;
            return false;
        }

        buffer.flip();
        return true;
    }

    private ByteBuffer allocate(int length) {
        if (length > SUSPICIOUS_THRESHOLD)
            _logger.warn("About to allocate " + length + " bytes - from socket channel: " + socketChannel);
        return DIRECT_BUFFERS ? ByteBuffer.allocateDirect(length) : ByteBuffer.allocate(length);
    }

    private void warnFragmented(ByteBuffer buffer, int iterations, AtomicInteger counter, String operation) {
        int total = counter.incrementAndGet();
        _logger.warn("{} for buffer with length {} was fragmented {} times (total fragmented operations: {})",
                operation, buffer.limit(), iterations, total);
    }

    private int readBlocking(ByteBuffer buffer) throws IOException {
        int iterations = 0;
        while (buffer.hasRemaining()) {
            int bRead = socketChannel.read(buffer);
            if (bRead == -1) // EOF
                throw new ClosedChannelException();
            iterations++;
        }
        buffer.flip();
        return iterations;
    }

    public void close() throws IOException {
        if (oos != null)
            oos.close();
        if (ois != null)
            ois.close();
        socketChannel.close();
    }

    public ReadProcessor getReadProcessor() {
        return readProcessor;
    }

    public void setReadProcessor(ReadProcessor readProcessor) {
        this.readProcessor = readProcessor;
    }
}
