package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.lrmi.ServerAddress;
import com.gigaspaces.lrmi.nio.*;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;

import static com.gigaspaces.lrmi.rdma.RdmaConstants.RDMA_CONNECT_TIMEOUT;

public class RdmaChannel extends LrmiChannel {

    private GSRdmaEndpointFactory factory;
    private GSRdmaClientEndpoint endpoint;
    private com.gigaspaces.lrmi.netty.Client nettyClient;

    private RdmaChannel(ServerAddress address) throws IOException {
        super(new RdmaWriter(), new RdmaReader());
        //connect to the server
        InetAddress ipAddress = InetAddress.getByName(address.getHost());
        InetSocketAddress socketAddress = new InetSocketAddress(ipAddress, address.getPort());

        if (RdmaConstants.NETTY_ENABLED) {
            try {
                this.nettyClient = new com.gigaspaces.lrmi.netty.Client(socketAddress,
                        this::nettyDeserializeReply,
                        this::nettySerialize);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
        } else {
            this.factory = new GSRdmaEndpointFactory(new LrmiRdmaResourceFactory(), RdmaChannel::deserializeReplyPacket);
            this.endpoint = (GSRdmaClientEndpoint) factory.create();
            try {
                endpoint.connect(socketAddress, RDMA_CONNECT_TIMEOUT);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    public static RdmaChannel create(ServerAddress address) throws IOException {
        return new RdmaChannel(address);
    }

    @Override
    public SocketChannel getSocketChannel() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBlocking() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSocketDesc() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getLocalPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getSocketDisplayString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCurrSocketDisplayString() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        try {
            factory.close();
            endpoint.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static Object deserializeReplyPacket(ByteBuffer buffer) {
        IPacket packet = new ReplyPacket<>();
        return deserializePacket(packet, buffer);
    }

    public static Object deserializeRequestPacket(ByteBuffer buffer) {
        IPacket packet = new RequestPacket();
        return deserializePacket(packet, buffer);
    }

    private static Object deserializePacket(IPacket packet, ByteBuffer buffer) {
        try {
            ByteBufferPacketSerializer serializer = new ByteBufferPacketSerializer(buffer);
            return serializer.deserialize(packet);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return e;
        }
    }

    public CompletableFuture<ReplyPacket> submit(RequestPacket requestPacket) {
        if (nettyClient != null) {
            return nettyClient.send(requestPacket);
        } else {
            return endpoint.getTransport().send(requestPacket);
        }
    }

    private Object nettyDeserializeReply(ByteBuf frame) {
        byte[] arr = new byte[frame.readableBytes()];
        frame.readBytes(arr);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(arr); MarshalInputStream mis = new MarshalInputStream(bis)) {
            IPacket packet = new ReplyPacket();
            packet.readExternal(mis);
            return packet;
        } catch (Exception e) {
            return e;
        }
    }

    private IOException nettySerialize(Object payload, ByteBuf buf) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); MarshalOutputStream mos = new MarshalOutputStream(bos, true)) {
            IPacket packet = (IPacket) payload;
            packet.writeExternal(mos);
            mos.flush();
            buf.writeBytes(bos.toByteArray());
        } catch (IOException e) {
            return e;
        }
        return null;
    }
}
