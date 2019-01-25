package com.gigaspaces.lrmi.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.epoll.EpollEventLoopGroup;

import java.io.*;

public class NettyRMI {
    private static volatile NettyRMI ourInstance = new NettyRMI();

    public static NettyRMI getInstance() {
        return ourInstance;
    }
    private EpollEventLoopGroup eventLoopGroup;

    private NettyRMI() {
        eventLoopGroup = new EpollEventLoopGroup();
    }

    public EpollEventLoopGroup getEventLoopGroup() {
        return eventLoopGroup;
    }

    public static Object simpleDeserialize(ByteBuf frame){
        byte[] arr = new byte[frame.readableBytes()];
        frame.readBytes(arr);
        try (ByteArrayInputStream ba = new ByteArrayInputStream(arr); ObjectInputStream inn = new ObjectInputStream(ba)) {
            return inn.readObject();
        } catch (Exception e) {
            return e;
        }
    }
    public static IOException simpleSerialize(Object payload, ByteBuf buf){
        try {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
            oos.writeObject(payload);
            oos.flush();
            byte[] bytes = bytesOut.toByteArray();
            bytesOut.close();
            oos.close();
            buf.writeBytes(bytes);
        } catch (IOException e) {
            return e;
        }
        return null;
    }
}
