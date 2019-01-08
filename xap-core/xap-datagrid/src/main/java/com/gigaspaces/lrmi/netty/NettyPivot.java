package com.gigaspaces.lrmi.netty;

import com.gigaspaces.config.lrmi.nio.NIOConfiguration;
import com.gigaspaces.internal.io.MarshalInputStream;
import com.gigaspaces.internal.io.MarshalOutputStream;
import com.gigaspaces.internal.lrmi.LRMIInboundMonitoringDetailsImpl;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.AbstractPivot;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.classloading.ClassProviderRequest;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.HandshakeRequest;
import com.gigaspaces.lrmi.nio.IPacket;
import com.gigaspaces.lrmi.nio.PAdapter;
import com.gigaspaces.lrmi.nio.ReplyPacket;
import com.gigaspaces.lrmi.nio.RequestPacket;
import com.gigaspaces.lrmi.rdma.*;
import com.gigaspaces.management.transport.ITransportConnection;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.rmi.NoSuchObjectException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NettyPivot extends AbstractPivot {
    final private static Logger _logger = Logger.getLogger(Constants.LOGGER_LRMI);

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final NIOConfiguration nioConfig;

    public NettyPivot(NIOConfiguration nioConfig, PAdapter protocol) {
        super();
        this.nioConfig = nioConfig;

        try {
            InetAddress ipAddress = InetAddress.getByName(nioConfig.getBindHostName());
            InetSocketAddress address = new InetSocketAddress(ipAddress, Integer.valueOf(nioConfig.getBindPort()));
            com.gigaspaces.lrmi.netty.Server nettyServer = new com.gigaspaces.lrmi.netty.Server(address.getPort(),
                    NettyPivot::process, this::nettyDeserializeRequest, this::nettySerialize);
            executorService.submit(nettyServer);
        } catch (Exception e) {
            e.printStackTrace();
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

    private Object nettyDeserializeRequest(ByteBuf frame) {
        byte[] arr = new byte[frame.readableBytes()];
        frame.readBytes(arr);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(arr); MarshalInputStream mis = new MarshalInputStream(bis)) {
            IPacket packet = new RequestPacket();
            packet.readExternal(mis);
            return packet;
        } catch (Exception e) {
            return e;
        }
    }

    public static Object process(Object req) {

        LRMIInvocationContext.updateContext(null, LRMIInvocationContext.ProxyWriteType.UNCACHED, LRMIInvocationContext.InvocationStage.SERVER_UNMARSHAL_REQUEST, PlatformLogicalVersion.getLogicalVersion(), null, false, null, null);

        RequestPacket requestPacket = (RequestPacket) req;
        /* link channelEntry with remoteObjID, this gives us info which channelEntries open vs. remoteObjID */

        if (requestPacket.isCallBack) {
            throw new UnsupportedOperationException("callback is not supported in RDMA!");
        }

        ReplyPacket replyPacket = consumeAndHandleRequest(requestPacket);
        return replyPacket;
    }


    private static ReplyPacket consumeAndHandleRequest(RequestPacket requestPacket) {
        Object reqObject = requestPacket.getRequestObject();
        if (reqObject != null) {
            if (reqObject instanceof ClassProviderRequest) { //TODO remote class loading
                throw new UnsupportedOperationException("Remote classloading is not supported in RDMA");
//                return new ReplyPacket<IClassProvider>(_classProvider, null);
            }
            if (reqObject instanceof HandshakeRequest) {
                return new ReplyPacket<Object>(null, null);
            }
        }
        Exception resultEx = null;
        Object result = null;
        try {

            // Check for dummy packets - ignore them
            if (requestPacket.getObjectId() != LRMIRuntime.DUMMY_OBJECT_ID) {
                if (requestPacket.getInvokeMethod() == null) {
                    _logger.log(Level.WARNING, "canceling invocation of request packet without invokeMethod : " + requestPacket);
                } else {
                    result = LRMIRuntime.getRuntime().invoked(requestPacket.getObjectId(),
                            requestPacket.getInvokeMethod().realMethod,
                            requestPacket.getArgs());
                }
            }
        } catch (NoSuchObjectException ex) {
            /** the remote object died, no reason to print-out the message in non debug mode,  */
            if (requestPacket.isOneWay()) {
                if (_logger.isLoggable(Level.SEVERE)) {
                    _logger.log(Level.SEVERE, "Failed to invoke one way method : " + requestPacket.getInvokeMethod() +
                            "\nReason: This remoteObject: " + requestPacket.getObjectId() +
                            " has been already unexported.", ex);
                }
            }

            resultEx = ex;
        } catch (Exception ex) {
            resultEx = ex;
        }


        if (requestPacket.isOneWay()) {
            if (resultEx != null && _logger.isLoggable(Level.SEVERE)) {
                _logger.log(Level.SEVERE, "Failed to invoke one-way method: " + requestPacket.getInvokeMethod(), resultEx);
            }

            return null;
        }


        /** return response */
        //noinspection unchecked
        return new ReplyPacket(result, resultEx);
    }

    @Override
    public int getPort() {
        return Integer.valueOf(nioConfig.getBindPort());
    }

    @Override
    public String getHostName() {
        return nioConfig.getBindHostName();
    }

    @Override
    public InetSocketAddress getServerBindInetSocketAddress() {
        return null;
    }

    @Override
    public void shutdown() {
        executorService.shutdown();
    }

    @Override
    public List<ITransportConnection> getRemoteObjectConnectionsList(long objectId) {
        return null;
    }

    @Override
    public int countRemoteObjectConnections(long objectId) {
        return 0;
    }

    @Override
    public LRMIInboundMonitoringDetailsImpl getMonitoringDetails() {
        return null;
    }

    @Override
    public void unexport(long objectId) {

    }
}
