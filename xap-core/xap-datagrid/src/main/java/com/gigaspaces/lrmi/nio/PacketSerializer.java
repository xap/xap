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

    private final MarshalOutputStream _oos;
    private final GSByteArrayOutputStream _baos;
    /**
     * reuse buffer, growing on demand.
     */
    private final SmartByteBufferCache _bufferCache = SmartByteBufferCache.getDefaultSmartByteBufferCache();

    public PacketSerializer() {
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

    public ByteBuffer serialize(IPacket packet, boolean reuseBuffer) throws IOException {
        ByteBuffer byteBuffer;
        MarshalOutputStream mos;
        GSByteArrayOutputStream bos;

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
        return buffer;
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
     * @return buffer that might be used by the GSByteArrayOutputStream
     */
    private ByteBuffer prepareStream() {
        ByteBuffer byteBuffer = _bufferCache.get();

        byte[] streamBuffer = byteBuffer.array();
        _baos.setBuffer(streamBuffer, LENGTH_SIZE); // 4 bytes for size
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

    public void closeContext() {
        _oos.closeContext();
    }

    public void resetContext() {
        _oos.resetContext();
    }
}
