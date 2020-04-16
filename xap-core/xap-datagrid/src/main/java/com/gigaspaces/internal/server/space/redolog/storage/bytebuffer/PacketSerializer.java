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

package com.gigaspaces.internal.server.space.redolog.storage.bytebuffer;

import com.gigaspaces.internal.io.GSByteArrayInputStream;
import com.gigaspaces.internal.io.GSByteArrayOutputStream;
import com.gigaspaces.logger.Constants;
import com.j_spaces.kernel.pool.IResourceFactory;
import com.j_spaces.kernel.pool.IResourcePool;
import com.j_spaces.kernel.pool.Resource;
import com.j_spaces.kernel.pool.ResourcePool;

import java.io.IOException;
import java.io.ObjectStreamConstants;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Supports single serializer multi concurrent deserializers
 *
 * @author eitany
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class PacketSerializer<T> {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_BACKLOG);

    private final ByteBufferObjectOutputStream _oss;
    private final GSByteArrayOutputStream _gsByteOutputStream;
    private final IPacketStreamSerializer<T> _packetStreamSerializer;
    private byte[] _buffer;
    private ByteBuffer _byteBuffer;

    private IResourcePool<InputStreamsResource> _inputStreamsPool =
            new ResourcePool<InputStreamsResource>(new IResourceFactory<InputStreamsResource>() {

                public InputStreamsResource allocate() {
                    return new InputStreamsResource();
                }

            }, 0, 16);


    PacketSerializer(int initBufferSize, IPacketStreamSerializer<T> packetStreamSerializer) {
        _packetStreamSerializer = packetStreamSerializer;
        _buffer = new byte[initBufferSize];
        _gsByteOutputStream = new GSByteArrayOutputStream();
        _gsByteOutputStream.setBuffer(_buffer);
        _byteBuffer = ByteBuffer.wrap(_buffer);

        try {
            _oss = new ByteBufferObjectOutputStream(_gsByteOutputStream);
        } catch (IOException e) {
            throw new RuntimeException("Internal error creating PacketSerializer", e);
        }
    }

    public PacketSerializer(IPacketStreamSerializer<T> packetStreamSerializer) {
        this(64 * 1024, packetStreamSerializer);
    }

    public ByteBuffer serializePacket(T packet) throws IOException {
        _packetStreamSerializer.writePacketToStream(_oss, packet);
        _oss.flush();
        _oss.reset();
        byte[] actualBuffer = _gsByteOutputStream.getBuffer();
        //Need to rewrap the buffer because it was changed
        if (actualBuffer != _buffer) {
            _buffer = actualBuffer;
            _byteBuffer = ByteBuffer.wrap(_buffer);
        }
        //Remove reset indicator
        _byteBuffer.limit(_gsByteOutputStream.size() - 1);
        _byteBuffer.position(0);

        _gsByteOutputStream.setSize(0);
        return _byteBuffer;
    }

    public T deserializePacket(byte[] serializedPacket) throws IOException, ClassNotFoundException {
        InputStreamsResource resource = _inputStreamsPool.getResource();
        try {
            resource.setBuffer(serializedPacket);
            return resource.readPacket();
        } catch (IOException e) {
            if (_logger.isErrorEnabled())
                _logger.error("Unexpected error when deserializing replication packet from the swap space", e);
            resource.markCorrupted();
            throw e;
        } catch (ClassNotFoundException e) {
            if (_logger.isErrorEnabled())
                _logger.error("Unexpected error when deserializing replication packet from the swap space", e);
            resource.markCorrupted();
            throw e;
        } catch (RuntimeException re) {
            if (_logger.isErrorEnabled())
                _logger.error("Unexpected error when deserializing replication packet from the swap space", re);
            resource.markCorrupted();
            throw re;
        } finally {
            resource.release();
        }
    }

    final private static byte[] _resetBuffer = new byte[]{ObjectStreamConstants.TC_RESET, ObjectStreamConstants.TC_NULL};
    final private static byte[] _dummyBuffer = new byte[0];

    public class InputStreamsResource extends Resource {
        private GSByteArrayInputStream gsByteArrayInputStream = new GSByteArrayInputStream(_dummyBuffer);
        private ByteBufferObjectInputStream objectInputStream;
        private boolean _corrupted;

        public T readPacket() throws IOException, ClassNotFoundException {
            if (objectInputStream == null)
                objectInputStream = new ByteBufferObjectInputStream(gsByteArrayInputStream);
            return _packetStreamSerializer.readPacketFromStream(objectInputStream);
        }

        public void markCorrupted() {
            _corrupted = true;
        }

        public void setBuffer(byte[] serializedPacket) {
            gsByteArrayInputStream.setBuffer(serializedPacket);
        }

        @Override
        public void clear() {
            if (_corrupted) {
                recreateStream();
                return;
            }
            gsByteArrayInputStream.setBuffer(_resetBuffer);
            try {
                objectInputStream.readObject();
                gsByteArrayInputStream.setBuffer(_dummyBuffer);
            } catch (IOException e) {
                recreateStream();
                throw new RuntimeException("Unexpected error when clearing packet serializer resource", e);
            } catch (ClassNotFoundException e) {
                recreateStream();
                throw new RuntimeException("Unexpected error when clearing packet serializer resource", e);
            }
        }

        private void recreateStream() {
            gsByteArrayInputStream = new GSByteArrayInputStream(_dummyBuffer);
            try {
                objectInputStream = new ByteBufferObjectInputStream(gsByteArrayInputStream);
                _corrupted = false;
            } catch (IOException e) {
                throw new RuntimeException("Unexpected error when resetting packet serializer resource streams", e);
            }
        }

    }


}
