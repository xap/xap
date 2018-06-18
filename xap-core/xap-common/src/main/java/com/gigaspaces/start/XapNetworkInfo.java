package com.gigaspaces.start;

import org.jini.rio.boot.BootUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.logging.Logger;

/**
 * @author Niv Ingberg
 * @since 12.1
 */
public class XapNetworkInfo {
    private final String hostId;
    private final InetAddress host;
    private final InetAddress publicHost;
    private String publicHostId;
    private boolean isPublicIpConfigure;

    public XapNetworkInfo() {
        try {
            this.hostId = BootUtil.getHostAddress();
            this.host = InetAddress.getByName(hostId);


            /*
            NEW CODE
             */
            publicHostId = System.getenv("XAP_NIC_ADDRESS_PUBLIC");
            if(publicHostId !=null){
                isPublicIpConfigure=true;
            }
            else{
                isPublicIpConfigure=false;
            }
            if(publicHostId == null ){
                publicHostId=hostId;
            }
            if(publicHostId.equals(hostId)){
                publicHost=host;
            }
            else{
                publicHost = InetAddress.getByName(publicHostId);

            }

            //CAN'T USE LOGGER HERE - NOT INITIALIZED YET
            System.out.println("----> XapNetworkInfo.ctr() NEW[publicHostId="+publicHostId + " publicHost="+publicHost+"] PREV[hostId="+hostId + " host="+host+"]");

        } catch (UnknownHostException e) {
            throw new IllegalStateException("Failed to get network information", e);
        }
    }

    public String getHostId() {
        return hostId;
    }

    public InetAddress getHost() {
        return host;
    }


    public String getPublicHostId() {
        return publicHostId;
    }

    public InetAddress getPublicHost() {
        return publicHost;
    }


    public boolean isPublicIpConfigure(){
        return isPublicIpConfigure;
    }
}
