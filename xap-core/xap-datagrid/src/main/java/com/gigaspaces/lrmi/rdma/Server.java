package com.gigaspaces.lrmi.rdma;

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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.gigaspaces.lrmi.rdma.RdmaConstants.BUFFER_SIZE;

public class Server {

    RdmaActiveEndpointGroup<GSRdmaServerEndpoint> endpointGroup;
    ArrayBlockingQueue<GSRdmaServerEndpoint> pendingRequests;
    private static String host = "192.168.33.137";
    private static int port = 8888;

    public static void main(String[] args) throws Exception {


        BasicConfigurator.configure();

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        ArrayBlockingQueue<GSRdmaServerEndpoint> pendingRequests = new ArrayBlockingQueue<GSRdmaServerEndpoint>(RdmaConstants.MAX_INCOMMING_REQUESTS);


        GSRdmaEndpointFactory factory = new GSRdmaEndpointFactory();
        RdmaServerEndpoint<GSRdmaAbstractEndpoint> serverEndpoint = factory.getEndpointGroup().createServerEndpoint();


        //we can call bind on a server endpoint, just like we do with sockets
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);
        serverEndpoint.bind(address, 10);
        DiSNILogger.getLogger().info("SimpleServer::servers bound to address " + address.toString());

        //we can accept new connections
        //we have previously passed our own endpoint factory to the group, therefore new endpoints will be of type GSRdmaServerEndpoint
        DiSNILogger.getLogger().info("SimpleServer::client connection accepted");

        executorService.submit(new RdmaServerReceiver(pendingRequests, rdmaMsg -> new RdmaMsg(rdmaMsg.getPayload().toString().toUpperCase())));

        while (true) {
            GSRdmaServerEndpoint rdmaEndpoint = (GSRdmaServerEndpoint) serverEndpoint.accept();
            rdmaEndpoint.setPendingRequests(pendingRequests);
            DiSNILogger.getLogger().info("destination: "+rdmaEndpoint.getDstAddr()+", source: "+rdmaEndpoint.getSrcAddr());
        }


//        serverEndpoint.close();
//        DiSNILogger.getLogger().info("server endpoint closed");
//        endpointGroup.close();
//        DiSNILogger.getLogger().info("group closed");
//        System.exit(0);
    }

}
