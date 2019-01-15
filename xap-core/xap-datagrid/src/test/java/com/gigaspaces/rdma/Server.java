package com.gigaspaces.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.*;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class Server implements RdmaEndpointFactory<Server.CustomServerEndpoint> {

    RdmaActiveEndpointGroup<CustomServerEndpoint> endpointGroup;
    private String host = "192.168.33.137";
    private int port = 8888;

    public void launch(String[] args) throws Exception {
        this.run();
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Server simpleServer = new Server();
        simpleServer.launch(args);
    }

    public Server.CustomServerEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
        return new Server.CustomServerEndpoint(endpointGroup, idPriv, serverSide, 100);
    }

    public void run() throws Exception {
        //create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
        endpointGroup = new RdmaActiveEndpointGroup<CustomServerEndpoint>(1000, false, 128, 4, 128);
        endpointGroup.init(this);
        //create a server endpoint
        RdmaServerEndpoint<CustomServerEndpoint> serverEndpoint = endpointGroup.createServerEndpoint();


        //we can call bind on a server endpoint, just like we do with sockets
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);
        serverEndpoint.bind(address, 10);
        DiSNILogger.getLogger().info("SimpleServer::servers bound to address " + address.toString());

        //we can accept new connections
        Server.CustomServerEndpoint clientEndpoint = serverEndpoint.accept();
        //we have previously passed our own endpoint factory to the group, therefore new endpoints will be of type CustomServerEndpoint
        DiSNILogger.getLogger().info("SimpleServer::client connection accepted");


        ByteBuffer sendBuf = clientEndpoint.getSendBuf();
        ByteBuffer recvBuf = clientEndpoint.getRecvBuf();
        SVCPostSend svcPostSend = clientEndpoint.postSend(clientEndpoint.getWrList_send());
        SVCPostRecv svcPostRecv = clientEndpoint.postRecv(clientEndpoint.getWrList_recv());

        //in our custom endpoints we have prepared (memory registration and work request creation) some memory buffers beforehand.
        sendBuf.asCharBuffer().put("Hello from the server");
        DiSNILogger.getLogger().info("sending msg");
        svcPostSend.execute();
        DiSNILogger.getLogger().info("waiting for send completion");
        clientEndpoint.getWcEvents().take();
        sendBuf.clear();

        DiSNILogger.getLogger().info("try receiving msg");
        svcPostRecv.execute();
        DiSNILogger.getLogger().info("waiting for receive completion");
        clientEndpoint.getWcEvents().take();
        recvBuf.clear();
        String msgFromClient = recvBuf.asCharBuffer().toString();

        DiSNILogger.getLogger().info("msg from client is : " + msgFromClient);


        sendBuf.asCharBuffer().put("Hello again from the server");
        DiSNILogger.getLogger().info("sending msg");
        svcPostSend.execute();
        DiSNILogger.getLogger().info("waiting for send completion");
        clientEndpoint.getWcEvents().take();
        sendBuf.clear();

        DiSNILogger.getLogger().info("try receiving msg");
        svcPostRecv.execute();
        DiSNILogger.getLogger().info("waiting for receive completion");
        clientEndpoint.getWcEvents().take();
        recvBuf.clear();
        msgFromClient = recvBuf.asCharBuffer().toString();

        DiSNILogger.getLogger().info("msg from client is : " + msgFromClient);

        //close everything
        clientEndpoint.close();
        DiSNILogger.getLogger().info("client endpoint closed");
        serverEndpoint.close();
        DiSNILogger.getLogger().info("server endpoint closed");
        endpointGroup.close();
        DiSNILogger.getLogger().info("group closed");
        System.exit(0);
    }

    public static class CustomServerEndpoint extends RdmaActiveEndpoint {
        private ByteBuffer buffers[];
        private IbvMr mrlist[];
        private int buffercount = 2;

        private IbvMr dataMr;
        private ByteBuffer sendBuf;
        private IbvMr sendMr;
        private ByteBuffer recvBuf;
        private IbvMr recvMr;

        private LinkedList<IbvSendWR> wrList_send;
        private IbvSge sgeSend;
        private LinkedList<IbvSge> sgeList;
        private IbvSendWR sendWR;

        private LinkedList<IbvRecvWR> wrList_recv;
        private IbvSge sgeRecv;
        private LinkedList<IbvSge> sgeListRecv;
        private IbvRecvWR recvWR;

        private ArrayBlockingQueue<IbvWC> wcEvents;

        public CustomServerEndpoint(RdmaActiveEndpointGroup<CustomServerEndpoint> endpointGroup,
                                    RdmaCmId idPriv, boolean serverSide, int buffersize) throws IOException {
            super(endpointGroup, idPriv, serverSide);
            this.buffercount = 3;
            buffers = new ByteBuffer[buffercount];
            this.mrlist = new IbvMr[buffercount];

            for (int i = 0; i < buffercount; i++) {
                buffers[i] = ByteBuffer.allocateDirect(buffersize);
            }

            this.wrList_send = new LinkedList<IbvSendWR>();
            this.sgeSend = new IbvSge();
            this.sgeList = new LinkedList<IbvSge>();
            this.sendWR = new IbvSendWR();

            this.wrList_recv = new LinkedList<IbvRecvWR>();
            this.sgeRecv = new IbvSge();
            this.sgeListRecv = new LinkedList<IbvSge>();
            this.recvWR = new IbvRecvWR();

            this.wcEvents = new ArrayBlockingQueue<IbvWC>(10);
        }

        //important: we override the init method to prepare some buffers (memory registration, post recv, etc).
        //This guarantees that at least one recv operation will be posted at the moment this endpoint is connected.
        public void init() throws IOException {
            super.init();

            for (int i = 0; i < buffercount; i++) {
                mrlist[i] = registerMemory(buffers[i]).execute().free().getMr();
            }


            this.sendBuf = buffers[0];
            this.sendMr = mrlist[0];
            this.recvBuf = buffers[1];
            this.recvMr = mrlist[1];

            sgeSend.setAddr(sendMr.getAddr());
            sgeSend.setLength(sendMr.getLength());
            sgeSend.setLkey(sendMr.getLkey());
            sgeList.add(sgeSend);
            sendWR.setWr_id(2000);
            sendWR.setSg_list(sgeList);
            sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
            sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
            wrList_send.add(sendWR);

            sgeRecv.setAddr(recvMr.getAddr());
            sgeRecv.setLength(recvMr.getLength());
            int lkey = recvMr.getLkey();
            sgeRecv.setLkey(lkey);
            sgeListRecv.add(sgeRecv);
            recvWR.setSg_list(sgeListRecv);
            recvWR.setWr_id(2001);
            wrList_recv.add(recvWR);

            DiSNILogger.getLogger().info("SimpleServer::initiated recv");
            this.postRecv(wrList_recv).execute().free();
        }

        public void dispatchCqEvent(IbvWC wc) throws IOException {
            DiSNILogger.getLogger().info("SERVER: op code = " + IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) + ", id = " + wc.getWr_id());
            wcEvents.add(wc);
        }

        public ArrayBlockingQueue<IbvWC> getWcEvents() {
            return wcEvents;
        }

        public LinkedList<IbvSendWR> getWrList_send() {
            return wrList_send;
        }

        public LinkedList<IbvRecvWR> getWrList_recv() {
            return wrList_recv;
        }

        public ByteBuffer getSendBuf() {
            return sendBuf;
        }

        public ByteBuffer getRecvBuf() {
            return recvBuf;
        }

        public IbvSendWR getSendWR() {
            return sendWR;
        }

        public IbvRecvWR getRecvWR() {
            return recvWR;
        }

        public IbvMr getDataMr() {
            return dataMr;
        }

        public IbvMr getSendMr() {
            return sendMr;
        }

        public IbvMr getRecvMr() {
            return recvMr;
        }
    }
}
