package com.gigaspaces.lrmi.rdma;

import org.apache.log4j.BasicConfigurator;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class Server {

    private static String host = "192.168.33.137";
    private static int port = 8888;

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);
        RdmaServerTransport transport = new RdmaServerTransport(address, rdmaMsg -> new RdmaMsg(rdmaMsg.getPayload().toString().toUpperCase()));
        transport.run();
    }

}
