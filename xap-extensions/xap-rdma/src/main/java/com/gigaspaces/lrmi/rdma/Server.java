package com.gigaspaces.lrmi.rdma;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class Server {

    private static String host = "192.168.72.60";
    private static int port = 8888;

    static {
        if (host.length() == 0) {
            try {
                host = InetAddress.getLocalHost().getHostAddress();
                System.out.println("Binding to " + host);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);
        RdmaServerTransport transport = new RdmaServerTransport(address,
                request -> request.toString().toUpperCase(), 1,
                ClientTransport::readResponse, new RdmaResourceFactory());
        transport.run();
    }

}
