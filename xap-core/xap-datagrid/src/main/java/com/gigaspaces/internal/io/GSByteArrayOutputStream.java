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

package com.gigaspaces.internal.io;

import com.gigaspaces.api.InternalApi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * This class is an optimized porting of java.io.ByteArrayOutputStream: a) All methods are not
 * synchronized. b) Most safety checks have been removed. c) ensureCapacity method have been added,
 * for low level optimizations. Naturally, this class and all of its methods are not thread safe.
 *
 * @author niv
 * @since 6.6
 */
@InternalApi
public class GSByteArrayOutputStream extends OutputStream {
    /**
     * The buffer where data is stored.
     */
    protected byte[] _buffer;

    /**
     * The number of valid bytes in the buffer.
     */
    protected int _count;

    private int _compactionNeededTimes;
    private int maxCapacity;

    /**
     * Creates a new byte array output stream. The buffer capacity is initially 32 bytes, though its
     * size increases if necessary.
     */
    public GSByteArrayOutputStream() {
        this(32);
    }

    /**
     * Creates a new byte array output stream, with a buffer capacity of the specified size, in
     * bytes.
     *
     * @param capacity the initial capacity.
     * @throws IllegalArgumentException if size is negative.
     */
    public GSByteArrayOutputStream(int capacity) {
        if (capacity < 0)
            throw new IllegalArgumentException("Negative initial size: " + capacity);
        _buffer = new byte[capacity];
    }

    /**
     * Copy internal buffer
     *
     * @param arr    target buffer
     * @param srcPos start position
     */
    final public void copyToBuffer(ByteBuffer buffer) {
        buffer.put(_buffer, 0, _count);
    }

    /**
     * Set stream buffer, and reset the counter.
     *
     * @param buf new buffer
     * @return old buffer
     */
    public void setBuffer(byte[] buf) {
        setBuffer(buf, 0);
    }

    /**
     * Set stream buffer and set the counter.
     *
     * @param buf   new buffer
     * @param count amount of valid bytes
     * @return old buffer
     */
    public void setBuffer(byte[] buffer, int count) {
        this._buffer = buffer;
        this._count = count;
    }

    public void setBufferWithMaxCapacity(byte[] buf) {
        setBuffer(buf);
        maxCapacity = buf.length;
    }


    /**
     * Gets internal buffers
     *
     * @return internal buffer
     */
    public byte[] getBuffer() {
        return this._buffer;
    }

    /**
     * get buffer count
     *
     * @return count
     */
    public int getCount() {
        return _count;
    }

    /**
     * Set the buffer size.
     *
     * @param size amount of valid bytes
     */
    public void setSize(int size) {
        this._count = size;
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number of valid bytes in this
     * output stream.
     * @see ByteArrayOutputStream#count
     */
    public int size() {
        return _count;
    }

    /**
     * The current buffer capacity.
     *
     * @return buffer current capacity
     */
    public int getCapacity() {
        return _buffer.length;
    }

    /**
     * Writes the specified byte to this byte array output stream.
     *
     * @param b the byte to be written.
     */
    @Override
    public void write(int b) {
        ensureCapacity(1);
        _buffer[_count++] = (byte) b;
    }


    public void writeByte(byte b) {
        ensureCapacity(1);
        _buffer[_count++] = b;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array starting at offset
     * <code>off</code> to this byte array output stream.
     *
     * @param b   the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     */
    @Override
    public void write(byte b[], int off, int len) {
        if (len == 0)
            return;
        ensureCapacity(len);
        System.arraycopy(b, off, _buffer, _count, len);
        _count += len;
    }

    public boolean ensureCapacity(int delta) {
        int newcount = _count + delta;
        if (newcount > _buffer.length) {
            if (newcount > maxCapacity)
                throw new IllegalStateException("Max capacity breached: " + maxCapacity+", current: " + newcount);
            byte newbuf[] = new byte[Math.max(_buffer.length << 1, newcount)];
            System.arraycopy(_buffer, 0, newbuf, 0, _count);
            _buffer = newbuf;
            return true;
        }
        return false;
    }

    /**
     * Writes the complete contents of this byte array output stream to the specified output stream
     * argument, as if by calling the output stream's write method using <code>out.write(buf, 0,
     * count)</code>.
     *
     * @param out the output stream to which to write the data.
     * @throws IOException if an I/O error occurs.
     */
    public void writeTo(OutputStream out) throws IOException {
        out.write(_buffer, 0, _count);
    }

    /**
     * Resets the <code>count</code> field of this byte array output stream to zero, so that all
     * currently accumulated output in the output stream is discarded. The output stream can be used
     * again, reusing the already allocated buffer space.
     *
     * @see ByteArrayInputStream#count
     */

    public void reset() {
        _count = 0;
    }

    /**
     * Creates a newly allocated byte array. Its size is the current size of this output stream and
     * the valid contents of the buffer have been copied into it.
     *
     * @return the current contents of this output stream, as a byte array.
     * @see ByteArrayOutputStream#size()
     */
    public byte[] toByteArray() {
        byte newbuf[] = new byte[_count];
        System.arraycopy(_buffer, 0, newbuf, 0, _count);
        return newbuf;
    }

    /**
     * Gets the underlying buffer if its size is at most two times more than the actual data written
     * on the buffer.
     *
     * If this is not the case, creates a newly allocated byte array. Its size is the current size
     * of the actual content of this output stream and the valid contents of the buffer have been
     * copied into it. If the last 3 consecutive calls have resulted in new buffer creation, the
     * underlying buffer is replaced with the last created buffer while keeping the same count
     * position.
     */
    public byte[] getCompactBuffer() {
        //If count is less then half of the capacity we return a compacted buffer
        if (_count <= getCapacity() >> 1) {
            byte newbuf[] = new byte[_count];
            System.arraycopy(_buffer, 0, newbuf, 0, _count);

            if (++_compactionNeededTimes >= 3) {
                _compactionNeededTimes = 0;
                _buffer = newbuf;
            }

            return newbuf;
        }
        _compactionNeededTimes = 0;
        return _buffer;
    }
}
