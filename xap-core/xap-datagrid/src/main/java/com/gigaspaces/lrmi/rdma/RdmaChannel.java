package com.gigaspaces.lrmi.rdma;

import com.gigaspaces.lrmi.ServerAddress;
import com.gigaspaces.lrmi.nio.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;

import static com.gigaspaces.lrmi.rdma.RdmaConstants.RDMA_CONNECT_TIMEOUT;

public class RdmaChannel extends LrmiChannel {

    private final GSRdmaEndpointFactory factory;
    private GSRdmaClientEndpoint endpoint;

    private RdmaChannel(RdmaWriter writer, RdmaReader reader, GSRdmaEndpointFactory factory, GSRdmaClientEndpoint endpoint) {
        super(writer, reader);
        this.factory = factory;
        this.endpoint = endpoint;
    }

    public static RdmaChannel create(ServerAddress address) throws IOException {
        GSRdmaEndpointFactory factory = new GSRdmaEndpointFactory(new LrmiRdmaResourceFactory(), RdmaChannel::deserializeReplyPacket);
        GSRdmaClientEndpoint endpoint = (GSRdmaClientEndpoint) factory.create();
        //connect to the server
        InetAddress ipAddress = InetAddress.getByName(address.getHost());
        InetSocketAddress socketAddress = new InetSocketAddress(ipAddress, address.getPort());
        try {
            endpoint.connect(socketAddress, RDMA_CONNECT_TIMEOUT);
        } catch (Exception e) {
            throw new IOException(e);
        }

        RdmaWriter writer = new RdmaWriter();
        RdmaReader reader = new RdmaReader();
        return new RdmaChannel(writer, reader, factory, endpoint);
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
        return endpoint.getTransport().send(requestPacket);
    }
}
