/*
 * Copyright 2005 Sun Microsystems, Inc.
 * Copyright 2005 GigaSpaces, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jini.rio.boot;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.start.ClasspathBuilder;
import com.gigaspaces.start.SystemInfo;

import net.jini.core.discovery.LookupLocator;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides static convenience methods for use in configuration files. This class cannot be
 * instantiated.
 */
@com.gigaspaces.api.InternalApi
public class BootUtil {
    private static final Logger LOGGER = Logger.getLogger(BootUtil.class.getName());

    private static final AtomicBoolean isLocalHostLoaded = new AtomicBoolean(false);
    private static NetworkInterface[] networkInterfaces;

    /**
     * This class cannot be instantiated.
     */
    private BootUtil() {
        throw new AssertionError("org.jini.rio.boot.BootUtil cannot be instantiated");
    }

    /**
     * Return the codebase for the provided JAR names and port. This method will first get the IP
     * Address of the machine using <code>java.net.InetAddress.getLocalHost().getHostAddress()</code>,
     * then construct the codebase using the host address and the port for each jar in the array of
     * jars.
     *
     * @param jars The JAR names to use
     * @param port The port to use when constructing the codebase
     * @return The codebase as a space-delimited String for the provided JARs
     * @throws java.net.UnknownHostException if no IP address for the local host could be found.
     */
    public static String getCodebase(String[] jars, String protocol, String port) throws UnknownHostException {
        return (getCodebase(jars, protocol, SystemInfo.singleton().network().getHostId(), port));
    }

    /**
     * Return the codebase for the provided JAR names, port and address
     *
     * @param jars    Array of JAR names
     * @param address The address to use when constructing the codebase
     * @param port    The port to use when constructing the codebase
     * @return the codebase for the JAR
     */
    public static String getCodebase(String[] jars, String protocol, String address, String port) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < jars.length; i++) {
            if (i > 0)
                buffer.append(" ");
            buffer.append(protocol + "://" + address + ":" + port + "/" + jars[i]);
        }
        return (buffer.toString());
    }

    /**
     * Return the local host address using <code>java.net.InetAddress.getLocalHost().getHostAddress()</code>
     *
     * @return The local host address
     * @throws java.net.UnknownHostException if no IP address for the local host could be found.
     */
    public static String getHostAddress() throws UnknownHostException {
        //Fix GS-11738, important! Don't change
        warmup();

        String value = System.getProperty(CommonSystemProperties.HOST_ADDRESS);
        //if (value == null)
        //    value = GsEnv.get("NIC_ADDRESS");
        if (value == null) {
            InetAddress inetAddress = java.net.InetAddress.getLocalHost();
            value = BootIOUtils.wrapIpv6HostAddressIfNeeded(inetAddress);
        } else if (NetworkTuple.isNetworkTuple(value)) {
            NetworkTuple tuple = getNetworkSnapshot().parse(value);
            if (tuple.address == null)
                throw new RuntimeException("Failed to find network interface for [" + value + "]");
            value = tuple.host;
        } else {
            InetAddress inetAddress = java.net.InetAddress.getByName(value);
            value = toHostAddress(inetAddress, value);
        }
        //Set the translation back in the properties.
        System.setProperty(CommonSystemProperties.HOST_ADDRESS, value);
        return value;
    }

    private static NetworkSnapshot getNetworkSnapshot() {
        try {
            return new NetworkSnapshot();
        } catch (SocketException e) {
            throw new IllegalStateException("Failed to get network information", e);
        }
    }

    private static String toHostAddress(InetAddress inetAddress, String value) {
        if (inetAddress instanceof Inet6Address && value.startsWith("["))
            return value;

        final String hostAddress = inetAddress.getHostAddress();
        if (value.equalsIgnoreCase(hostAddress))
            return hostAddress;

        final String canonicalHostName = inetAddress.getCanonicalHostName();
        if (value.equalsIgnoreCase(canonicalHostName))
            return canonicalHostName;

        return inetAddress.getHostName();
    }

    /**
     * Get an anonymous port
     *
     * @return An anonymous port created by instantiating a <code>java.net.ServerSocket</code> with
     * a port of 0
     */
    public static String getAnonymousPort() throws java.io.IOException {
        return String.valueOf(getPort(0));
    }

    /**
     * This method checks whether supplied port is free.
     *
     * If port 0 returns an anonymous port.
     */
    private static int getPort(int port) throws IOException {
        ServerSocket socket = null;

        try {
            socket = new ServerSocket(port);
            return socket.getLocalPort();
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException ex) { /* don't care */ }
            }
        }
    }

    /**
     * Get a free port in supplied port range. minPort <= freePort <=maxPort The max port should be
     * up to 65536 value.
     *
     * @param minPort the minimum port number of the port range to scan.
     * @param maxPort the maximum port number of the port range to scan.
     * @return a free socket port.
     */
    public static String getPortInRange(int minPort, int maxPort) throws java.io.IOException {
        final int MIN_PORT_LIMIT = 1;
        final int MAX_PORT_LIMIT = 65536;

        if (minPort < 0)
            minPort = MIN_PORT_LIMIT;

        if (maxPort > MAX_PORT_LIMIT)
            maxPort = MAX_PORT_LIMIT;

        IOException ioEx = null;
        for (int i = minPort; i <= maxPort; i++) {
            try {
                return String.valueOf(getPort(i));
            } catch (IOException ex) {
                ioEx = ex;
            }
        }

        throw new IOException("Failed to get free port from supplied port range. minPort: " + minPort + " maxPort: " + maxPort + "Reason:" + ioEx);
    }

    public static int getAvailablePort(String hostname, String portRange) throws IOException {
        StringTokenizer st = new StringTokenizer(portRange, ",");
        while (st.hasMoreTokens()) {
            int index = portRange.indexOf('-');
            if (index == -1) {
                int port = Integer.parseInt(st.nextToken().trim());
                if (port == 0) {
                    return 0;
                }
                try {
                    return getPort(port);
                } catch (IOException e) {
                    // can't create one
                }
            } else {
                int startPort = Integer.parseInt(portRange.substring(0, index));
                int endPort = Integer.parseInt(portRange.substring(index + 1));
                if (endPort < startPort) {
                    throw new IllegalArgumentException("Start port [" + startPort + "] must be greater than end port [" + endPort + "]");
                }
                for (int i = startPort; i <= endPort; i++) {
                    try {
                        return getPort(i);
                    } catch (IOException ex) {
                        // can't create ont
                    }
                }
            }
        }
        throw new IOException("Failed to get free port from [" + portRange + "]");
    }

    public static List<URL> toURLs(String element) throws MalformedURLException {
        return new ClasspathBuilder().appendJars(Paths.get(element)).toURLs();
    }

    /**
     * Convenience method to return a String array as a CSV String. E.g. useful for toString()
     * implementations.
     *
     * @param arr array to display. Elements may be of any type (toString will be called on each
     *            element).
     */
    public static String arrayToCommaDelimitedString(Object[] arr) {
        return arrayToDelimitedString(arr, ",");
    }

    /**
     * Convenience method to return a String array as a delimited (e.g. CSV) String. E.g. useful for
     * toString() implementations.
     *
     * @param arr   array to display. Elements may be of any type (toString will be called on each
     *              element).
     * @param delim delimiter to use (probably a ",")
     */
    public static String arrayToDelimitedString(Object[] arr, String delim) {
        if (arr == null) {
            return "";
        }

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) {
                sb.append(delim);
            }
            sb.append(arr[i]);
        }
        return sb.toString();
    }

    /**
     * Convert comma delimited and/or space String to array of Strings
     */
    public static String[] toArray(String arg) {
        if (arg == null) {
            return new String[0];
        }
        StringTokenizer tok = new StringTokenizer(arg, " ,");
        String[] array = new String[tok.countTokens()];
        int i = 0;
        while (tok.hasMoreTokens()) {
            array[i] = tok.nextToken();
            i++;
        }
        return (array);
    }

    /**
     * Parse a comma delimited LookupLocator URLs and build array of <code>LookupLocator</code> out
     * of it. For example: "host1:4180,host2:4180,host3:4180"
     *
     * @param lookupLocatorURLs List of LookupLocators urls, separates by ",".
     * @return LookupLocator[] Array of initilized <code>LookupLocator</code>.
     */
    static public LookupLocator[] toLookupLocators(String lookupLocatorURLs) {
        String locatorURL = null;
        ArrayList<LookupLocator> locatorList = new ArrayList<LookupLocator>();
        if (lookupLocatorURLs != null && lookupLocatorURLs.length() > 0) {
            StringTokenizer st = new StringTokenizer(lookupLocatorURLs, ",");
            while (st.hasMoreTokens()) {
                try {
                    locatorURL = st.nextToken().trim();
                    if (locatorURL.length() == 0 || locatorURL.equals("\"\"")) {
                        // don't create empty lookup locator
                        continue;
                    }
                    if (!locatorURL.startsWith("jini://")) {
                        locatorURL = "jini://" + locatorURL;
                    }
                    LookupLocator lookupLocator = new LookupLocator(locatorURL);
                    locatorList.add(lookupLocator);
                } catch (MalformedURLException ex) {
                    if (LOGGER.isLoggable(Level.WARNING)) {
                        LOGGER.log(
                                Level.WARNING,
                                "Failed to parse list of LookupLocator URLs: " + locatorURL + " - " + ex.toString(), ex
                        );
                    }
                }
            }//end of while()
        }
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.log(Level.FINE, locatorList.toString());
        }
        return locatorList.toArray(new LookupLocator[0]);
    }

    /**
     * Takes an array of LookupLocator builds a comma seperated string of urls</code> out of it.
     * Return example (removeProtocolPrefix==true): "host1:4180,host2:4180,host3:4180" Return
     * example (removeProtocolPrefix==false): "jini://host1:4180/,jini://host2:4180/,jini://host3:4180/"
     *
     * @param lookupLocators       The locators
     * @param removeProtocolPrefix whether the jini:// prefix should be removed
     * @return URL to lookup locator
     */
    public static String toLookupLocatorURLs(LookupLocator[] lookupLocators, boolean removeProtocolPrefix) {
        if (removeProtocolPrefix) {
            StringBuilder sb = new StringBuilder();
            for (LookupLocator locator : lookupLocators) {
                sb.append(locator.getHost()).append(":").append(locator.getPort()).append(",");
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - 1);
            }
            return sb.toString();
        } else {
            return arrayToCommaDelimitedString(lookupLocators);
        }
    }

    public static String formatDuration(long duration) {
        return duration / 1000d + "s";
    }

    public static String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    /**
     * @return Available network interfaces
     * @since 11.0
     */
    public synchronized static NetworkInterface[] getNetworkInterfaces() throws SocketException {

        if (networkInterfaces == null) {
            final NetworkSnapshot networkSnapshot = getNetworkSnapshot();
            final NetworkInterface defaultNic = networkSnapshot.getDefaultNic();
            networkInterfaces = defaultNic != null ? new NetworkInterface[]{defaultNic} : networkSnapshot.getNetworkInterfaces(true);
        }

        return networkInterfaces;
    }

    private static void warmup() {
        if (!isLocalHostLoaded.getAndSet(true)) {
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.fine("---Before local host initialization---");
                try {
                    InetAddress.getLocalHost();
                } catch (UnknownHostException e) {
                    if (LOGGER.isLoggable(Level.WARNING))
                        LOGGER.log(Level.WARNING, e.toString(), e);
                }
                if (LOGGER.isLoggable(Level.FINE))
                    LOGGER.fine("---After local host initialization---");
            }
        }
    }

    public static class NetworkInterfaceWrapper {

        private final NetworkInterface networkInterface;
        private final List<InetAddress> addresses;

        public NetworkInterfaceWrapper(NetworkInterface networkInterface) {
            this.networkInterface = networkInterface;
            this.addresses = new ArrayList<InetAddress>();
            Enumeration<InetAddress> addressIterator = networkInterface.getInetAddresses();
            while (addressIterator.hasMoreElements())
                addresses.add(addressIterator.nextElement());
        }

        public InetAddress findAddress(boolean excludeLoopback, boolean excludeIPv4, boolean excludeIPv6) {
            for (InetAddress address : addresses) {
                if (excludeLoopback && address.isLoopbackAddress())
                    continue;
                if (excludeIPv4 && address instanceof Inet4Address)
                    continue;
                if (excludeIPv6 && address instanceof Inet6Address)
                    continue;
                return address;
            }
            return null;
        }
    }

    public static class NetworkSnapshot {
        private final List<NetworkInterface> allNics = new ArrayList<NetworkInterface>();
        private final List<NetworkInterfaceWrapper> activeNics = new ArrayList<NetworkInterfaceWrapper>();

        public NetworkSnapshot() throws SocketException {
            Enumeration<NetworkInterface> nicsIterator = NetworkInterface.getNetworkInterfaces();
            if (nicsIterator != null) {
                while (nicsIterator.hasMoreElements()) {
                    NetworkInterface nic = nicsIterator.nextElement();
                    allNics.add(nic);
                    NetworkInterfaceWrapper wrapper = new NetworkInterfaceWrapper(nic);
                    if (!wrapper.addresses.isEmpty())
                        activeNics.add(wrapper);
                }
            }
        }

        public List<NetworkInterfaceWrapper> findByName(String name) {
            List<NetworkInterfaceWrapper> result = new ArrayList<NetworkInterfaceWrapper>();
            for (NetworkInterfaceWrapper wrapper : activeNics)
                if (name.equals(wrapper.networkInterface.getName()) || name.equals(wrapper.networkInterface.getDisplayName()))
                    result.add(wrapper);
            return result;
        }

        public NetworkTuple parse(String value) throws UnknownHostException {
            if (NetworkTuple.isNetworkTuple(value))
                value = value.substring(NetworkTuple.WRAPPER.length(), value.length() - NetworkTuple.WRAPPER.length());
            final int pos = value.indexOf(':');
            final String name = pos != -1 ? value.substring(0, pos) : value;
            final String type = pos != -1 ? value.substring(pos + 1) : null;
            HostTranslationType hostType = type != null ? HostTranslationType.valueOf(type) : null;
            return new NetworkTuple(name, hostType, this);
        }

        public NetworkInterface[] getNetworkInterfaces(boolean activeOnly) {
            NetworkInterface[] result;
            if (activeOnly) {
                result = new NetworkInterface[activeNics.size()];
                for (int i = 0; i < result.length; i++)
                    result[i] = activeNics.get(i).networkInterface;
            } else {
                result = allNics.toArray(new NetworkInterface[allNics.size()]);
            }
            return result;
        }

        public NetworkInterface getDefaultNic() {
            NetworkInterface result = null;
            String hostName = System.getProperty(CommonSystemProperties.HOST_ADDRESS);
            if (hostName == null) {
                // Disabled pending dns cleanup in lab
                //hostName = getHostName(null);
                //hostName = "#local:ip#";
            }

            if (hostName != null) {
                try {
                    if (NetworkTuple.isNetworkTuple(hostName)) {
                        result = parse(hostName).networkInterface;
                    } else {
                        result = NetworkInterface.getByInetAddress(InetAddress.getByName(hostName));
                    }
                } catch (Exception e) {
                    if (LOGGER.isLoggable(Level.WARNING))
                        LOGGER.log(Level.WARNING, e.toString(), e);
                }
            }

            return result;
        }
    }

    private static enum HostTranslationType {ip, ip6, host, canonicalhost}

    public static class NetworkTuple {

        public static final String WRAPPER = "#";
        private final String name;
        private final HostTranslationType hostType;
        private final InetAddress address;
        private final NetworkInterface networkInterface;
        private final String host;

        public NetworkTuple(String name, HostTranslationType hostType, NetworkSnapshot networkSnapshot) throws UnknownHostException {
            this.name = name;
            this.hostType = hostType;
            if (name.equals("local")) {
                address = InetAddress.getLocalHost();
                // TODO: Can we init networkInterface in this case?
                networkInterface = null;
            } else {
                final boolean excludeLoopback = true;
                final boolean excludeIPv4 = hostType == HostTranslationType.ip6;
                final boolean excludeIPv6 = hostType != HostTranslationType.ip6;
                List<NetworkInterfaceWrapper> nics = networkSnapshot.findByName(name);
                NetworkInterface resultNic = null;
                InetAddress resultAddress = null;
                for (NetworkInterfaceWrapper nic : nics) {
                    InetAddress address = nic.findAddress(excludeLoopback, excludeIPv4, excludeIPv6);
                    if (address != null) {
                        resultNic = nic.networkInterface;
                        resultAddress = address;
                        break;
                    }
                }
                this.networkInterface = resultNic;
                this.address = resultAddress;
            }
            host = translateHost(address, hostType);
        }

        public static boolean isNetworkTuple(String s) {
            return s.startsWith(WRAPPER) && s.endsWith(WRAPPER);
        }

        private static String translateHost(InetAddress address, HostTranslationType hostType) {
            if (address == null)
                return null;

            if (hostType == null)
                return BootIOUtils.wrapIpv6HostAddressIfNeeded(address);

            switch (hostType) {
                case canonicalhost:
                    return address.getCanonicalHostName();
                case host:
                    return address.getHostName();
                case ip:
                case ip6:
                    return BootIOUtils.wrapIpv6HostAddressIfNeeded(address);
                default:
                    throw new IllegalStateException("Unsupported host type: " + hostType);
            }
        }
    }

    /**
     * Loads the requested resource and returns its URL. Several attempts to search for the
     * requested resource:
     *
     * 1. It calls the (current thread) contextClassLoader.getResource(config/schemas/yyy.xml). if
     * not found --> 1.1 It calls the (current thread) contextClassLoader.getResource(yyy.xml). if
     * not found --> 2. It calls this classLoader.getResource(config/schemas/yyy.xml) (e.g. if
     * running in AppServer) if not found --> 2.1 It calls this classLoader.getResource(yyy.xml)
     * (e.g. if running in AppServer) if not found AND useOnlyClasspath == false (expensive
     * operation!) --> 2.2 if the resource was not found, instead of returning null, we attempt to
     * load it as a File IF it was passed as file format e.g. D:/gigaspaces/GenericJDBCProperties/HSQLProperties/jdbc.properties
     * 3. Uses the com.gigaspaces.start.Locator.locate(yyy.xml) that searches recursively under
     * the GS, using root file system. Not the default behavior
     *
     * Notes: In some scenarios, the ResourceLoader does not use the right class loader and returns
     * no resource. E.g. when running within application server the config and jar files loaded
     * using the Thread.currentThread().getContextClassLoader().getResource() but since the lower
     * level CL is used (of the APPServer) we should try also and use the getClass().getResource()
     * in case the first attempt failed to find the resource. A third attempt, if the resource still
     * was not found, it will be using the Locator facility that searches for the resource (using
     * only the resource name) under a base directory in the file system.
     *
     * @param name           name of resource to load
     * @param locatorBaseDir if not null we will try to search for the resource in the file system
     *                       locatorBaseDir, we will try to recursively search under the
     *                       locatorBaseDir as the base search path. By default, false.
     * @return URL containing the resource
     */
    static public URL getResourceURL(String name, String locatorBaseDir) {
        URL result = null;
        try {
            //do not allow search using / prefix which does not work with classLoader.getResource()
            if (name.startsWith("/")) {
                name = name.substring(1);
            }
            final int lastChar = name.lastIndexOf('/');
            final String onlyResourceName = lastChar > -1 ? name.substring(lastChar + 1) : null;
            final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            if (contextClassLoader != null) {
                result = getResourceURL(contextClassLoader, name, onlyResourceName);
            }
            /* GS-12786 - Removed by Niv to break circular dependency between SystemJars.CORE_COMMON_ARTIFACT and SystemJars.DATA_GRID_ARTIFACT. This seems like dead code.
            if(result == null)
            {
                final ClassLoader resourceClassLoader = com.j_spaces.kernel.ResourceLoader.class.getClassLoader();
                if(resourceClassLoader != null && !resourceClassLoader.equals(contextClassLoader))
                    result = getResourceURL(resourceClassLoader, name, onlyResourceName);
            }
            */
            if (result == null) {
                File file = new File(name);
                if (file.isFile() && file.exists()) {
                    result = file.toURI().toURL();
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.log(Level.FINE, "Going to load the resource <" + name + "> from the path: " + result);
                    }
                    if (result != null) {
                        if (LOGGER.isLoggable(Level.FINE)) {
                            LOGGER.log(Level.FINE, "Load resource: [" + name + "] \n"
                                    + " [ Returning result: " + result + " ] \n"
                                    + (LOGGER.isLoggable(Level.FINEST) ? BootUtil.getStackTrace(new Exception("Debugging stack trace only (can be ignored): ")) : ""));
                        }
                        return result;
                    }
                }
            }

            // if we still did not find the resource using classpath, if requested,
            // we just try to locate it somewhere under gs home dir using its resource name only!
            if (result == null && locatorBaseDir != null) {
                // before scanning the whole file system - check if an exact path was given.
                String path = null;
                if (!locatorBaseDir.endsWith("/") && !name.startsWith("/")) {
                    // missing a '/'
                    path = locatorBaseDir + "/" + name;
                } else {
                    path = locatorBaseDir + name;
                }

                File resource = new File(path);

                if (resource.exists()) {
                    result = resource.toURI().toURL();
                }
            }
        } catch (Exception e) {
            //wrap ConfigurationException and throw relevant exception
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE, "Failed to load resource: [" + name + "] ", e);
            }
        }
        return result;
    }

    private static URL getResourceURL(ClassLoader classLoader, String name, String onlyResourceName) {
        if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.log(Level.FINE, "Going to load resource <" + name + "> from ClassLoader: " + classLoader.getClass().getName());
        }

        boolean searchedOnlyResourceName = false;
        URL result = classLoader.getResource(name);

        if (result == null && onlyResourceName != null) {
            //try search only with resource name
            searchedOnlyResourceName = true;
            if (LOGGER.isLoggable(Level.FINE)) {
                LOGGER.log(Level.FINE, "Going to load resource <" + onlyResourceName + "> from ClassLoader: " + classLoader.getClass().getName());
            }
            result = classLoader.getResource(onlyResourceName);
        }

        if (LOGGER.isLoggable(Level.FINE)) {
            StringBuilder classLoaderHierarchy = new StringBuilder("ClassLoader Hierarchy: ");
            ClassLoader tmpCL = classLoader;
            while (tmpCL != null) {
                classLoaderHierarchy.append(tmpCL.getClass().toString()).append(" <-- ");
                tmpCL = tmpCL.getParent();
            }
            LOGGER.log(Level.FINE, "Load resource: [" + (searchedOnlyResourceName ? onlyResourceName : name) + "] Thread: [" + Thread.currentThread().getName()
                    + "] using ClassLoader: [" + classLoader + "] \n"
                    + " [ " + classLoaderHierarchy.toString() + " ] \n"
                    + " [ Returning result: " + result + " ] \n"
                    + (LOGGER.isLoggable(Level.FINEST) ? BootUtil.getStackTrace(new Exception("Debugging stack trace only (can be ignored): ")) : ""));
        }

        return result;
    }
}
