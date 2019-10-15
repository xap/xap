/*
 * 
 * Copyright 2005 Sun Microsystems, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 	http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package net.jini.discovery;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.utils.GsEnv;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * A holder class for constants that pertain to the unicast and multicast discovery protocols.
 *
 * @author Sun Microsystems, Inc.
 */
@com.gigaspaces.api.InternalApi
public class Constants {

    /**
     * Is multicast enabled or not
     */
    private static final boolean multicastEnabled = Boolean.valueOf(System.getProperty(CommonSystemProperties.MULTICAST_ENABLED_PROPERTY, "true"));
    /**
     * The port for both unicast and multicast boot requests.
     */
    private static final int multicastDiscoveryPort = GsEnv.keyOrSystemProperty("MULTICAST_LOOKUP_PORT", "com.gs.multicast.discoveryPort", 4174);
    /**
     * The port for both unicast and multicast boot requests.
     */
    private static final int multicastTtl = Integer.getInteger("com.gs.multicast.ttl", 3);
    private static final String requestAddressProperty = System.getProperty("com.gs.multicast.request", "224.0.1.187");
    private static final String announcementAddressProperty = System.getProperty("com.gs.multicast.announcement", "224.0.1.188");

    /**
     * If {@link java.net.Socket#setKeepAlive(boolean)} should be set to true.
     */
    private static final boolean useSocketKeepAlive = Boolean.parseBoolean(System.getProperty("com.gs.discovery.useSocketKeepAlive", "true"));
    /**
     * If {@link java.net.Socket#setTcpNoDelay(boolean)} should be set to true.
     */
    private static final boolean useTcpNoDelay = Boolean.parseBoolean(System.getProperty("com.gs.discovery.useSocketTcpNoDelay", "true"));

    /**
     * The address of the multicast group over which the multicast request protocol takes place.
     */
    private static InetAddress requestAddress = null;
    /**
     * The address of the multicast group over which the multicast announcement protocol takes
     * place.
     */
    private static InetAddress announcementAddress = null;

    /**
     * This class cannot be instantiated.
     */
    private Constants() {
    }

    /**
     * Return the address of the multicast group over which the multicast request protocol takes
     * place.
     *
     * @return the address of the multicast group over which the multicast request protocol takes
     * place
     */
    public static InetAddress getRequestAddress() throws UnknownHostException {
        if (requestAddress == null)
            requestAddress = InetAddress.getByName(requestAddressProperty);
        return requestAddress;
    }

    /**
     * Return the address of the multicast group over which the multicast announcement protocol
     * takes place.
     *
     * @return the address of the multicast group over which the multicast announcement protocol
     * takes place.
     */
    public static InetAddress getAnnouncementAddress() throws UnknownHostException {
        if (announcementAddress == null)
            announcementAddress = InetAddress.getByName(announcementAddressProperty);
        return announcementAddress;
    }

    public static int getDiscoveryPort() {
        return multicastDiscoveryPort;
    }

    /**
     * Default time to live value to use for sending multicast packets
     */
    public static int getTtl() {
        return multicastTtl;
    }

    public static boolean isMulticastEnabled() {
        return multicastEnabled;
    }

    public static boolean useSocketKeepAlive() {
        return useSocketKeepAlive;
    }

    public static boolean useSocketTcpNoDelay() {
        return useTcpNoDelay;
    }
}
