package com.gigaspaces.lrmi.nio;

import com.gigaspaces.internal.io.*;

import java.io.IOException;
import java.io.ObjectStreamConstants;
import java.nio.ByteBuffer;

public class ByteBufferPacketSerializer {
    private final ByteBuffer byteBuffer;

    private static final byte[] RESET_BUFFER = new byte[]{ObjectStreamConstants.TC_RESET, ObjectStreamConstants.TC_NULL};

    public ByteBufferPacketSerializer(ByteBuffer byteBuffer) throws IOException {
        this.byteBuffer = byteBuffer;


    }

    public void serialize(IPacket packet) throws IOException {
        GSByteBufferOutputStream bos = new GSByteBufferOutputStream(byteBuffer);
        MarshalOutputStream mos = new MarshalOutputStream(bos, true);
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
            GSByteBufferInputStream bis = new GSByteBufferInputStream(byteBuffer);
            MarshalInputStream mis = new MarshalInputStream(bis);
            packet.readExternal(mis);
            return packet;
        } finally {
            //this is the only way to do reset on ObjetInputStream:
            // add reset flag and let the ObjectInputStream to read it
            // so all the handles in the ObjectInputStream will be cleared
//            byteBuffer.clear();
//            byteBuffer.put(RESET_BUFFER);
//            mis.readObject();
//            byteBuffer.clear();
        }
    }
}
