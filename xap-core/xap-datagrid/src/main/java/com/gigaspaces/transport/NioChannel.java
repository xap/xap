package com.gigaspaces.transport;

import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.logger.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;

public class NioChannel {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_LRMI);
    private static final int LENGTH_SIZE = 4;
    private static final long SUSPICIOUS_THRESHOLD = Long.parseLong(System.getProperty("com.gs.lrmi.suspicious-threshold", "20000000"));
    private static final boolean CUSTOM_MARSHAL = PocSettings.customMarshal;

    private final SocketChannel socketChannel;
    private final LightMarshalOutputStream.Context mosContext;
    private final LightMarshalInputStream.Context misContext;
    private final ByteBuffer headerBuffer = ByteBuffer.allocateDirect(LENGTH_SIZE);
    private NonBlockingReadContext nbrContext;

    public NioChannel(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
        mosContext = CUSTOM_MARSHAL ? new LightMarshalOutputStream.Context() : null;
        misContext = CUSTOM_MARSHAL ? new LightMarshalInputStream.Context() : null;
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public ByteBuffer serialize(Object obj) throws IOException {
        try (GSByteArrayOutputStream bos = new GSByteArrayOutputStream()) {
            bos.setSize(LENGTH_SIZE); // for the stream size
            try (ObjectOutputStream mos = newObjectOutputStream(bos)) {
                mos.writeObject(obj);
            }

            int length = bos.size();
            ByteBuffer buffer = ByteBuffer.wrap(bos.getBuffer());
            buffer.putInt(length - LENGTH_SIZE);
            buffer.position(length);
            buffer.flip();
            return buffer;
        }
    }

    public Object deserialize(ByteBuffer buffer) throws IOException, ClassNotFoundException {
        try (GSByteArrayInputStream bis = new GSByteArrayInputStream(buffer.array())) {
            try (ObjectInputStream ois = newObjectInputStream(bis)) {
                return ois.readObject();
            }
        }
    }

    public void writeBlocking(ByteBuffer buffer) throws IOException {
        final int length = buffer.remaining();
        int written = 0;
        while (written < length) {
            int bytes = socketChannel.write(buffer);
            written += bytes;
        }
    }

    public ByteBuffer readBlocking() throws IOException {
        headerBuffer.position(0);
        readBlocking(headerBuffer);
        ByteBuffer buffer = allocate(headerBuffer.getInt());
        readBlocking(buffer);
        return buffer;
    }

    public ByteBuffer readNonBlocking() throws IOException {
        // Non-blocking read may read partial data, which needs to be preserved and resumed when called again.
        // If no context this is a new read - create new context:
        if (nbrContext == null)
            nbrContext = new NonBlockingReadContext();
        // If the data buffer has not been initialized, we're in the header:
        if (nbrContext.dataBuffer == null) {
            // Try to read the header - if read is not completed abort:
            if (!readNonBlocking(nbrContext.headerBuffer))
                return null;
            // Header read successfully - init data buffer:
            nbrContext.dataBuffer = allocate(nbrContext.headerBuffer.getInt());
        }
        // Try to read the data - if read is not completed abort:
        if (!readNonBlocking(nbrContext.dataBuffer))
            return null;
        // Read operation completed successfully - reset context for next read operation and return result:
        ByteBuffer result = nbrContext.dataBuffer;
        nbrContext = null;
        return result;
    }

    private ByteBuffer allocate(int length) {
        if (length > SUSPICIOUS_THRESHOLD)
            _logger.warn("About to allocate " + length + " bytes - from socket channel: " + socketChannel);
        return ByteBuffer.allocate(length);
    }

    private void readBlocking(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int bRead = socketChannel.read(buffer);
            if (bRead == -1) // EOF
                throw new ClosedChannelException();
        }
        buffer.flip();
    }

    private boolean readNonBlocking(ByteBuffer buffer) throws IOException {
        int bRead = socketChannel.read(buffer);
        if (bRead == -1) // EOF
            throw new ClosedChannelException();

        if (buffer.hasRemaining())
            return false;

        buffer.flip();
        return true;
    }

    public void close() throws IOException {
        if (mosContext != null)
            mosContext.close();
        if (misContext != null)
            misContext.close();
        socketChannel.close();
    }

    private ObjectOutputStream newObjectOutputStream(GSByteArrayOutputStream bos) throws IOException {
        return CUSTOM_MARSHAL ? new LightMarshalOutputStream(bos, mosContext) : new ObjectOutputStream(bos);
    }

    private ObjectInputStream newObjectInputStream(GSByteArrayInputStream bis) throws IOException {
        return CUSTOM_MARSHAL ? new LightMarshalInputStream(bis, misContext) : new ObjectInputStream(bis);
    }

    private static class NonBlockingReadContext {
        private final ByteBuffer headerBuffer = ByteBuffer.allocateDirect(LENGTH_SIZE);
        private ByteBuffer dataBuffer;
    }
}
