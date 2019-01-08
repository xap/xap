package com.gigaspaces.lrmi.nio;

import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.internal.io.MarshalContextClearedException;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.SmartByteBufferCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PacketSerializer {
    private static final Logger _logger = Logger.getLogger(Constants.LOGGER_LRMI);
    private static final int LENGTH_SIZE = 4; //4 bytes for length
    private static final byte[] DUMMY_BUFFER = new byte[0];

    private final ByteBufferProvider bbProvider;

    public PacketSerializer() {
        try {
            bbProvider = new DefaultByteBufferProvider();
        } catch (Exception e) {
            if (_logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, e.getMessage(), e);
            }

            throw new RuntimeException("Failed to initialize LRMI Writer stream: ", e);
        }
    }

    public ByteBuffer serialize(IPacket packet, boolean reuseBuffer) throws IOException {
        return serialize(packet, reuseBuffer ? this.bbProvider : new DynamicByteBufferProvider());
    }

    public static ByteBuffer serialize(IPacket packet, ByteBufferProvider bbProvider) throws IOException {
        final ByteBuffer byteBuffer = bbProvider.getByteBuffer();

        ByteBuffer buffer;
        try {
            packet.writeExternal(bbProvider.getMarshalOutputStream());
        } catch (MarshalContextClearedException e) {
            //Keep original exception for upper layer to handle properly
            throw e;
        } catch (Exception e) {
            throw new MarshallingException("Failed to marsh: " + packet, e);
        } finally // make sure we clean the buffers even if an exception was thrown
        {
            buffer = bbProvider.postSerialize(byteBuffer);
        }
        return buffer;
    }

    /**
     * @param byteBuffer buffer that might be used by the GSByteArrayOutputStream
     * @return prepared buffer.
     */
    private static ByteBuffer prepareBuffer(MarshalOutputStream mos, GSByteArrayOutputStream bos,
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
    private static ByteBuffer wrap(GSByteArrayOutputStream bos) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bos.getBuffer());
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
        return byteBuffer;
    }

    public void closeContext() {
        bbProvider.getMarshalOutputStream().closeContext();
    }

    public void resetContext() {
        bbProvider.getMarshalOutputStream().resetContext();
    }

    public static abstract class ByteBufferProvider {
        private final MarshalOutputStream _oos;
        protected final GSByteArrayOutputStream _baos;
        private final boolean reusable;

        protected ByteBufferProvider(boolean reusable) throws IOException {
            this.reusable = reusable;
            _baos = new GSByteArrayOutputStream();
            _baos.setSize(LENGTH_SIZE); // mark the buffer to start writing only after the length place
            _oos = new MarshalOutputStream(_baos, reusable); // add a TC_RESET using the MarshalOutputStream.writeStreamHeader()
        }

        public abstract ByteBuffer getByteBuffer();

        public MarshalOutputStream getMarshalOutputStream() {
            return _oos;
        }

        public GSByteArrayOutputStream getBinaryOutputStream() {
            return _baos;
        }

        protected abstract void updateByteBuffer(ByteBuffer buffer);

        protected abstract void notifyUsedSize(int limit);

        public ByteBuffer postSerialize(ByteBuffer original) throws IOException {
            ByteBuffer result = prepareBuffer(_oos, _baos, original);
            if (reusable) {
                _baos.setBuffer(DUMMY_BUFFER); // set DUMMY_BUFFER to release the strong reference to the byte[]
                _baos.reset();
                _oos.reset();
                if (result != original) // replace the buffer in soft reference if needed
                    updateByteBuffer(result);
                else
                    notifyUsedSize(result.limit());
            } else {
                //Clear context because this output stream is no longer used
                _oos.closeContext();
            }

            return result;
        }
    }

    public static class DefaultByteBufferProvider extends ByteBufferProvider {
        /**
         * reuse buffer, growing on demand.
         */
        private final SmartByteBufferCache _bufferCache = SmartByteBufferCache.getDefaultSmartByteBufferCache();

        public DefaultByteBufferProvider() throws IOException {
            super(true);
            _bufferCache.set(wrap((_baos)));
        }

        @Override
        public ByteBuffer getByteBuffer() {
            ByteBuffer byteBuffer = _bufferCache.get();
            _baos.setBuffer(byteBuffer.array(), LENGTH_SIZE); // 4 bytes for size
            return byteBuffer;
        }

        @Override
        public void updateByteBuffer(ByteBuffer buffer) {
            _bufferCache.set(buffer);
        }

        @Override
        protected void notifyUsedSize(int limit) {
            _bufferCache.notifyUsedSize(limit);
        }
    }

    public static class DynamicByteBufferProvider extends ByteBufferProvider {

        public DynamicByteBufferProvider() throws IOException {
            super(false);
        }

        @Override
        public ByteBuffer getByteBuffer() {
            return wrap(getBinaryOutputStream());
        }

        @Override
        public void updateByteBuffer(ByteBuffer buffer) {

        }

        @Override
        protected void notifyUsedSize(int limit) { }
    }

    public static class FixedByteBufferProvider extends ByteBufferProvider {

        private final ByteBuffer byteBuffer;

        public FixedByteBufferProvider(ByteBuffer byteBuffer) throws IOException {
            super(true);
            this.byteBuffer = byteBuffer;
            _baos.setBufferWithMaxCapacity(byteBuffer.array());
        }

        @Override
        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        @Override
        public void updateByteBuffer(ByteBuffer buffer) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void notifyUsedSize(int limit) {}
    }
}
