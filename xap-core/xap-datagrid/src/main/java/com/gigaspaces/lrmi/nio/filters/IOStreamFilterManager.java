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

import com.gigaspaces.lrmi.nio.Reader;
import com.gigaspaces.lrmi.nio.Reader.Context;
import com.gigaspaces.lrmi.nio.Writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@com.gigaspaces.api.InternalApi
public class IOStreamFilterManager implements IOFilterManager {

    @SuppressWarnings("UnusedDeclaration")
    private final static Logger logger = LoggerFactory.getLogger(IOStreamFilterManager.class.getName());

    private final Writer writer;
    private final IOStreamFilter filter;

    public IOStreamFilterManager(Reader reader, Writer writer, IOStreamFilter filter) {
        this.writer = writer;
        this.filter = filter;
        writer.setFilterManager(this);
        reader.setFilterManager(this);

    }


    public byte[] handleBlockingContant(byte[] bytes, int slowConsumerTimeout)
            throws IOException, IOFilterException {
        try {
            return filter.unrwap(ByteBuffer.wrap(bytes));
        } catch (Exception e) {
            throw new IOFilterException(e.getMessage(), e);
        }
    }


    public byte[] handleNoneBlockingContant(Context ctx, byte[] bytes)
            throws IOException, IOFilterException {
        try {
            return filter.unrwap(ByteBuffer.wrap(bytes));
        } catch (Exception e) {
            throw new IOFilterException(e.getMessage(), e);
        }
    }


    public void writeBytesBlocking(ByteBuffer dataBuffer)
            throws IOException, IOFilterException {
        dataBuffer.getInt();
        try {
            byte[] bytes = filter.wrap(dataBuffer);
            writer.writeBytesToChannelBlocking(toByteBuffer(bytes));
        } catch (Exception e) {
            throw new IOFilterException(e.getMessage(), e);
        }

    }


    public void writeBytesNonBlocking(com.gigaspaces.lrmi.nio.Writer.Context ctx)
            throws IOException {
        ctx.getBuffer().getInt();
        try {
            byte[] bytes = filter.wrap(ctx.getBuffer());
            ctx.setBuffer(toByteBuffer(bytes));
        } catch (Exception e) {
            throw new IOException(String.valueOf(e));
        }
        writer.writeBytesToChannelNoneBlocking(ctx, true);

    }

    public void beginHandshake() {
    }

    public void setUseClientMode(@SuppressWarnings("UnusedParameters") boolean mode) {
    }

    private ByteBuffer toByteBuffer(byte[] bytes) {
        ByteBuffer res = ByteBuffer.allocate(bytes.length + 4);
        res.order(ByteOrder.BIG_ENDIAN);
        res.putInt(bytes.length);
        res.put(bytes);
        res.flip();
//		logger.info("Sending to socket header of " + bytes.length + " and byte buffer with remaining " + res.remaining());
        return res;
    }

}
