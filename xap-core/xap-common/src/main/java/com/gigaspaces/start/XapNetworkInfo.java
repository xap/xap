package com.gigaspaces.start;

import com.gigaspaces.logger.Constants;
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
    final private static Logger _logger = Logger.getLogger(Constants.LOGGER_LRMI);

    public XapNetworkInfo() {
        try {
            this.hostId = BootUtil.getHostAddress();
            this.host = InetAddress.getByName(hostId);
            _logger.warning("before  inititlize hostId=" + hostId + ", host="+host);

            publicHostId = System.getenv("XAP_NIC_ADDRESS_PUBLIC");
            if(publicHostId !=null){
                _logger.warning("got XAP_NIC_ADDRESS_PUBLIC " + publicHostId);
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
            _logger.warning("after inititlize publicHostId=" + publicHostId + ", publicHost="+publicHost);


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
}
