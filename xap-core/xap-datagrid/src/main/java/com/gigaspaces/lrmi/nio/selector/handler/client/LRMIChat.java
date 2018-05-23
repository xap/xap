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

package com.gigaspaces.lrmi.nio.selector.handler.client;

import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.nio.RequestPacket;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

/**
 * Created by Barak Bar Orion 12/30/14.
 */
@com.gigaspaces.api.InternalApi
public class LRMIChat extends AbstractChat<ByteBuffer> {
    @SuppressWarnings("UnusedDeclaration")
    private static final Logger logger = Logger.getLogger(Constants.LOGGER_LRMI);

    private ByteBuffer msg;
    final private ByteBuffer headerBuffer;
    private ByteBuffer readBuf;

    private enum Mode {
        WRITE, READ, HEADER,
    }

    private Mode mode;

    public LRMIChat(RequestPacket packet) throws IOException {
        MarshalOutputStream mos;
        GSByteArrayOutputStream bos = new GSByteArrayOutputStream();
        bos.setSize(4);
        mos = new MarshalOutputStream(bos, false);
        ByteBuffer buffer = wrap(bos);
        packet.writeExternal(mos);
        msg = prepareBuffer(mos, bos, buffer);
        mode = Mode.WRITE;
        headerBuffer = ByteBuffer.allocate(4);
        headerBuffer.order(ByteOrder.BIG_ENDIAN);
    }

    @Override
    public boolean init(Conversation conversation, SelectionKey key) {
        this.conversation = conversation;
        return process(key);
    }

    @Override
    public boolean process(SelectionKey key) {
        if (!key.isValid()) {
            return false;
        }
        if (mode == Mode.WRITE) {
            final byte writeResult = write(key, msg);
            if (-1 == writeResult) {
                return true; //write error, end chat
            } else if (1 == writeResult) {
                //done with write, switch to read
                mode = Mode.HEADER;
                addInterest(key, SelectionKey.OP_READ);
            }
            return false; //don't end chat
        } else {
            return read(key);
        }
    }

    @Override
    public ByteBuffer result() {
        return readBuf;
    }

    private boolean read(SelectionKey key) {
        if (!key.isReadable()) {
            return false;
        }
        SocketChannel channel = (SocketChannel) key.channel();
        if (mode == Mode.HEADER) {
            try {
                if (0 > channel.read(headerBuffer)) {
                    key.cancel(); //ensure key is cancelled on the selector before socket.close()
                    conversation.close(new EOFException("end of stream has been reached unexpectedly during read of header"));
                    return true; //done with chat
                }
            } catch (Throwable t) {
                key.cancel(); //ensure key is cancelled on the selector before socket.close()
                conversation.close(t);
                return true;
            }
            if (headerBuffer.remaining() == 0) {
                headerBuffer.flip();
                int size = headerBuffer.getInt();
                readBuf = ByteBuffer.allocate(size);
                mode = Mode.READ;
            } else {
                return false;
            }
        }
        if (mode == Mode.READ) {
            try {
                if (0 > channel.read(readBuf)) {
                    key.cancel(); //ensure key is cancelled on the selector before socket.close()
                    conversation.close(new EOFException("end of stream has been reached unexpectedly during input"));
                    return true; //done with chat
                }
            } catch (Throwable t) {
                key.cancel(); //ensure key is cancelled on the selector before socket.close()
                conversation.close(t);
                return true;
            }
            if (readBuf.remaining() == 0) {
                readBuf.flip();
                removeInterest(key, SelectionKey.OP_READ);
                return true;
            } else {
                return false;
            }
        }

        return false;
    }


    private ByteBuffer wrap(GSByteArrayOutputStream bos) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bos.getBuffer());
        byteBuffer.order(ByteOrder.BIG_ENDIAN);
        return byteBuffer;
    }

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

        byteBuffer.putInt(length - 4);
        byteBuffer.position(length);
        byteBuffer.flip();

        return byteBuffer;
    }
}
