package com.gigaspaces.rdma;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.util.DiSNILogger;
import com.ibm.disni.verbs.*;
import org.apache.log4j.BasicConfigurator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        return new Server.CustomServerEndpoint(endpointGroup, idPriv, serverSide, 1000);
    }

    public void run() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
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
        //we have previously passed our own endpoint factory to the group, therefore new endpoints will be of type CustomServerEndpoint
        DiSNILogger.getLogger().info("SimpleServer::client connection accepted");

        while (true) {
            Server.CustomServerEndpoint rdmaEndpoint = serverEndpoint.accept();
            executorService.submit(() -> {
                try {
                    while (true){
                        chatWithClient(rdmaEndpoint);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }


//        serverEndpoint.close();
//        DiSNILogger.getLogger().info("server endpoint closed");
//        endpointGroup.close();
//        DiSNILogger.getLogger().info("group closed");
//        System.exit(0);
    }

    private void chatWithClient(CustomServerEndpoint rdmaEndpoint) throws IOException, InterruptedException {

        ByteBuffer recvBuf = rdmaEndpoint.getRecvBuf();

        recvBuf.clear();
        DiSNILogger.getLogger().info("try receiving msg");
        rdmaEndpoint.postRecv(rdmaEndpoint.wrList_recv).execute().free();
        DiSNILogger.getLogger().info("waiting for receive completion");
        rdmaEndpoint.getWcEvents().take();
        recvBuf.clear();
        String msgFromClient = "";
        RdmaMsg msg = null;
        long reqId = recvBuf.getLong();
        DiSNILogger.getLogger().info("SERVER: reqId = "+reqId);
        try {
            msg = ClientTransport.readResponse(recvBuf);
            msgFromClient = msg.getPayload().toString();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        DiSNILogger.getLogger().info("msg from client is : " + msgFromClient);

        String response = msgFromClient.toUpperCase();

        RdmaMsg rdmaMsg = new RdmaMsg(response);

        ByteBuffer buf = rdmaEndpoint.getSendBuf();
        buf.putLong(reqId);
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
        oos.writeObject(rdmaMsg);
        oos.flush();
        byte[] bytes = bytesOut.toByteArray();
        buf.put(bytes);
        oos.close();
        bytesOut.close();

        rdmaEndpoint.postSend(rdmaEndpoint.wrList_send).execute().free();
        rdmaEndpoint.getWcEvents().take();
        buf.clear();
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
            DiSNILogger.getLogger().info("SERVER: op code = " + IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()) + ", id = " + wc.getWr_id() + ", err = " + wc.getErr());
            if(IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_SEND) || IbvWC.IbvWcOpcode.valueOf(wc.getOpcode()).equals(IbvWC.IbvWcOpcode.IBV_WC_RECV)){
                wcEvents.add(wc);
            }
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
