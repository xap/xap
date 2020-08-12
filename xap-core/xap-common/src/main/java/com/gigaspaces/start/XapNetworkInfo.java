package com.gigaspaces.start;

import com.gigaspaces.internal.utils.GsEnv;
import org.jini.rio.boot.BootUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Niv Ingberg
 * @since 12.1
 */
public class XapNetworkInfo {
    private static volatile XapNetworkInfo instance;

    private final String hostId;
    private final InetAddress host;
    private final InetAddress publicHost;
    private String publicHostId;
    private boolean publicHostConfigured;

    public static XapNetworkInfo getInstance() {
        XapNetworkInfo snapshot = instance;
        if (snapshot != null)
            return snapshot;
        synchronized (XapNetworkInfo.class) {
            if (instance == null)
                instance = new XapNetworkInfo();
            return instance;
        }
    }

    private XapNetworkInfo() {
        try {
            this.hostId = BootUtil.getHostAddress();
            this.host = InetAddress.getByName(hostId);

            //apply public host if configured, otherwise same as bind address
            publicHostId = GsEnv.get("PUBLIC_HOST");
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
