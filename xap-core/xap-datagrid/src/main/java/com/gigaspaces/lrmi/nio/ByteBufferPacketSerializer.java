package com.gigaspaces.lrmi.nio;

import com.gigaspaces.internal.io.*;

import java.io.IOException;
import java.io.ObjectStreamConstants;
import java.nio.ByteBuffer;

public class ByteBufferPacketSerializer {
    private final ByteBuffer byteBuffer;
    private final GSByteBufferOutputStream bos;
    private final MarshalOutputStream mos;
    private final GSByteBufferInputStream bis;
    private final MarshalInputStream mis;
    private static final byte[] RESET_BUFFER = new byte[]{ObjectStreamConstants.TC_RESET, ObjectStreamConstants.TC_NULL};

    public ByteBufferPacketSerializer(ByteBuffer byteBuffer) throws IOException {
        this.byteBuffer = byteBuffer;
        this.bos = new GSByteBufferOutputStream(byteBuffer);
        this.mos = new MarshalOutputStream(bos, true);
        this.bis = new GSByteBufferInputStream(byteBuffer);
        this.mis = new MarshalInputStream(bis);
    }

    public void serialize(IPacket packet) throws IOException {

        try {
            packet.writeExternal(mos);
        } catch (MarshalContextClearedException e) {
            //Keep original exception for upper layer to handle properly
            throw e;
        } catch (Exception e) {
            throw new MarshallingException("Failed to marsh: " + packet, e);
        } finally {
            // make sure we clean the buffers even if an exception was thrown
            mos.flush();
            mos.reset();
        }
    }

    public IPacket deserialize(IPacket packet) throws IOException, ClassNotFoundException {
        try {
            packet.readExternal(mis);
            return packet;
        } finally {
            //this is the only way to do reset on ObjetInputStream:
            // add reset flag and let the ObjectInputStream to read it
            // so all the handles in the ObjectInputStream will be cleared
            byteBuffer.clear();
            byteBuffer.put(RESET_BUFFER);
            mis.readObject();
            byteBuffer.clear();
        }
    }
}
