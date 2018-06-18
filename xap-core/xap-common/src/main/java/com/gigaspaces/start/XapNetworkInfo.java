package com.gigaspaces.start;

import org.jini.rio.boot.BootUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Niv Ingberg
 * @since 12.1
 */
public class XapNetworkInfo {
    private final String hostId;
    private final InetAddress host;
    private final InetAddress publicHost;
    private String publicHostId;
    private boolean publicHostConfigured;

    public XapNetworkInfo() {
        try {
            this.hostId = BootUtil.getHostAddress();
            this.host = InetAddress.getByName(hostId);

            //apply public host if configured, otherwise same as bind address
            publicHostId = System.getenv("XAP_PUBLIC_HOST");
            if (publicHostId == null) {
                publicHostId = hostId;
                publicHost = host;
            } else {
                publicHostConfigured = true;
                publicHost = InetAddress.getByName(publicHostId);
            }

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


    public boolean isPublicHostConfigured(){
        return publicHostConfigured;
    }
}
