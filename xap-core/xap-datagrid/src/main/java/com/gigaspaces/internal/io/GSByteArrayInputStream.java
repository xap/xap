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

import java.io.InputStream;

/**
 * This class is an optimized porting of java.io.ByteArrayInputStream: a) All methods are not
 * synchronized. b) Most safety checks have been removed.
 *
 * @author niv
 * @since 6.6
 */
@com.gigaspaces.api.InternalApi
public class GSByteArrayInputStream extends InputStream {
    /**
     * An array of bytes that was provided by the creator of the stream. Elements
     * <code>buf[0]</code> through <code>buffer[count-1]</code> are the only bytes that can ever be
     * read from the stream;  element <code>buffer[pos]</code> is the next byte to be read.
     */
    protected byte _buffer[];

    /**
     * The index of the next character to read from the input stream buffer. This value should
     * always be nonnegative and not larger than the value of <code>count</code>. The next byte to
     * be read from the input stream buffer will be <code>buffer[position]</code>.
     */
    protected int _position;

    /**
     * The index one greater than the last valid character in the input stream buffer. This value
     * should always be nonnegative and not larger than the length of <code>buffer</code>. It  is
     * one greater than the position of the last byte within <code>buffer</code> that can ever be
     * read  from the input stream buffer.
     */
    protected int _count;

    /**
     * Creates a <code>ByteArrayInputStream</code> so that it  uses <code>buffer</code> as its
     * buffer array. The buffer array is not copied. The initial value of <code>pos</code> is
     * <code>0</code> and the initial value of  <code>count</code> is the length of
     * <code>buffer</code>.
     *
     * @param buffer the input buffer.
     */
    public GSByteArrayInputStream(byte buffer[]) {
        setBuffer(buffer);
    }

    /**
     * Sets a <code>GSByteArrayInputStream</code> so that it  uses <code>buffer</code> as its buffer
     * array. The buffer array is not copied. The initial value of <code>pos</code> is
     * <code>0</code> and the initial value of  <code>count</code> is the length of
     * <code>buffer</code>.
     *
     * @param buffer the input buffer.
     */
    public void setBuffer(byte[] buffer) {
        setBuffer(buffer, buffer.length);
    }

    /**
     * Sets a <code>GSByteArrayInputStream</code> so that it  uses <code>buffer</code> as its buffer
     * array. The buffer array is not copied. The initial value of <code>pos</code> is
     * <code>0</code> and the initial value
     *
     * @param buffer the input buffer.
     * @param count  the actual count.
     */
    public void setBuffer(byte[] buffer, int count) {
        this._buffer = buffer;
        this._position = 0;
        this._count = count;
    }


    public void reset() {
        _position = 0;
    }

    public byte[] getBuffer() {
        return _buffer;
    }

    /**
     * Reads the next byte of data from this input stream. The value byte is returned as an
     * <code>int</code> in the range <code>0</code> to <code>255</code>. If no byte is available
     * because the end of the stream has been reached, the value <code>-1</code> is returned. <p>
     * This <code>read</code> method cannot block.
     *
     * @return the next byte of data, or <code>-1</code> if the end of the stream has been reached.
     */
    @Override
    public int read() {
        return (_position < _count) ? readUnsignedByte() : -1;
    }

    public byte readByte() {
        return _buffer[_position++];
    }

    public int readUnsignedByte() {
        return _buffer[_position++] & 0xff;
    }

    /**
     * Reads up to <code>len</code> bytes of data into an array of bytes from this input stream. If
     * <code>pos</code> equals <code>count</code>, then <code>-1</code> is returned to indicate end
     * of file. Otherwise, the  number <code>k</code> of bytes read is equal to the smaller of
     * <code>len</code> and <code>count-pos</code>. If <code>k</code> is positive, then bytes
     * <code>buffer[pos]</code> through <code>buffer[pos+k-1]</code> are copied into
     * <code>b[off]</code>  through <code>b[off+k-1]</code> in the manner performed by
     * <code>System.arraycopy</code>. The value <code>k</code> is added into <code>pos</code> and
     * <code>k</code> is returned. <p> This <code>read</code> method cannot block.
     *
     * @param b   the buffer into which the data is read.
     * @param off the start offset of the data.
     * @param len the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or <code>-1</code> if there is no
     * more data because the end of the stream has been reached.
     */
    public int read(byte b[], int off, int len) {
        if (_position >= _count)
            return -1;
        if (_position + len > _count)
            len = _count - _position;
        if (len <= 0)
            return 0;

        System.arraycopy(_buffer, _position, b, off, len);
        _position += len;
        return len;
    }

    /**
     * Skips <code>n</code> bytes of input from this input stream. Fewer bytes might be skipped if
     * the end of the input stream is reached. The actual number <code>k</code> of bytes to be
     * skipped is equal to the smaller of <code>n</code> and  <code>count-pos</code>. The value
     * <code>k</code> is added into <code>pos</code> and <code>k</code> is returned.
     *
     * @param n the number of bytes to be skipped.
     * @return the actual number of bytes skipped.
     */
    public long skip(long n) {
        if (_position + n > _count)
            n = _count - _position;

        if (n < 0)
            return 0;

        _position += n;
        return n;
    }

    /**
     * Returns the number of bytes that can be read from this input stream without blocking. The
     * value returned is <code>count&nbsp;- pos</code>, which is the number of bytes remaining to be
     * read from the input buffer.
     *
     * @return the number of bytes that can be read from the input stream without blocking.
     */
    public int available() {
        return _count - _position;
    }

    public int getPosition() {
        return _position;
    }

    /**
     * Sets <code>position</code> on the input stream. If the requested position is larger or equals to buffer length,
     * position will be set to the final byte of the input stream.
     *
     * @param position the input position.
     * @return the actual position.
     */

    public long setPosition(int position) {
        if (position >= _count)
            position = _count - 1;

        if (position < 0)
            return 0;

        _position = position;
        return _position;
    }
}
