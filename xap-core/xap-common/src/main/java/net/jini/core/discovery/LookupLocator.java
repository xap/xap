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
package net.jini.core.discovery;

import com.gigaspaces.start.SystemInfo;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.discovery.Constants;
import net.jini.io.MarshalInputStream;
import net.jini.io.OptimizedByteArrayInputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class that performs unicast discovery, using version 1 of the unicast discovery
 * protocol.
 *
 * @author Sun Microsystems, Inc.
 * @since 1.0
 */
@com.gigaspaces.api.InternalApi
public class LookupLocator implements Serializable {
    private static final long serialVersionUID = 1448769379829432795L;

    /**
     * The port for both unicast and multicast boot requests.
     */
    private static final int discoveryPort = Constants.getDiscoveryPort();
    /**
     * The current version of the unicast discovery protocol.
     */
    private static final int protoVersion = 1;

    /**
     * The name of the host at which to perform discovery.
     *
     * @serial
     */
    protected String host;
    /**
     * The port number on the host at which to perform discovery.
     *
     * @serial
     */
    protected int port;

    /**
     * The timeout after which we give up waiting for a response from the lookup service.
     */
    static final int defaultTimeout = Integer.getInteger("net.jini.discovery.timeout", new Integer(60 * 1000)).intValue();

    /**
     * Construct a new <code>LookupLocator</code> object, set up to perform discovery to the given
     * URL.  The <code>host</code> and <code>port</code> fields will be populated with the
     * <i>host</i> and <i>port</i> components of the input URL.  No host name resolution is
     * attempted. <p> The syntax of the URL must be that of a <i>hierarchical</i> {@link
     * java.net.URI} with a <i>server-based naming authority</i>. Requirements for the components
     * are as follows: <ul> <li> A <i>scheme</i> of <code>"jini"</code> must be present. <li> A
     * <i>server-based naming authority</i> must be present; <i>user-info</i> must not be present.
     * The <i>port</i>, if present, must be between 1 and 65535 (both included). If no port is
     * specified, a default value of <code>4162</code> is used. <li> A <i>path</i> if present, must
     * be <code>"/"</code>; <i>path segment</i>s must not be present. <li> There must not be any
     * other components present. </ul> <p> The four allowed forms of the URL are thus: <ul> <li>
     * <code>jini://</code><i>host</i> <li> <code>jini://</code><i>host</i><code>/</code> <li>
     * <code>jini://</code><i>host</i><code>:</code><i>port</i> <li> <code>jini://</code><i>host</i><code>:</code><i>port</i><code>/</code>
     *
     * @param url the URL to use
     * @throws MalformedURLException <code>url</code> could not be parsed
     * @throws NullPointerException  if <code>url</code> is <code>null</code>
     */
    public LookupLocator(String url) throws MalformedURLException {
        if (url == null) {
            throw new NullPointerException("url is null");
        }
        URI uri = null;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            MalformedURLException mue =
                    new MalformedURLException("URI parsing failure: " + url);
            mue.initCause(e);
            throw mue;
        }
        if (!uri.isAbsolute()) {
            throw new MalformedURLException("no scheme specified: " + url);
        }
        if (uri.isOpaque()) {
            throw new MalformedURLException("not a hierarchical url: " + url);
        }
        if (!uri.getScheme().toLowerCase().equals("jini")) {
            throw new MalformedURLException(
                    "Invalid URL scheme: " + url);
        }
        String uriPath = uri.getPath();
        if ((uriPath.length() != 0) && (!uriPath.equals("/"))) {
            throw new MalformedURLException(
                    "URL path contains path segments: " + url);
        }
        if (uri.getQuery() != null) {
            throw new MalformedURLException(
                    "invalid character, '?', in URL: " + url);
        }
        if (uri.getFragment() != null) {
            throw new MalformedURLException(
                    "invalid character, '#', in URL: " + url);
        }
        // Make sure that it is a server-based authority, if the authority
        // component exists
        try {
            uri = uri.parseServerAuthority();
            if (uri.getUserInfo() != null) {
                throw new MalformedURLException(
                        "invalid character, '@', in URL host: " + url);
            }
            if ((host = uri.getHost()) == null) {
                // authority component does not exist - not a hierarchical URL
                throw new MalformedURLException(
                        "Not a hierarchical URL: " + url);
            }
            port = uri.getPort();
            if (port == -1) {
                port = discoveryPort;
            }
        } catch (URISyntaxException e) {
            handle3986Authority(uri);
        }
        if ((port <= 0) || (port >= 65536)) {
            throw new MalformedURLException("port number out of range: " + url);
        }
    }

    /**
     * Construct a new <code>LookupLocator</code> object, set to perform unicast discovery to the
     * input <code>host</code> and <code>port</code>.  The <code>host</code> and <code>port</code>
     * fields will be populated with the <code>host</code> and <code>port</code> arguments.  No host
     * name resolution is attempted. <p>The <code>host</code> argument must meet any one of the
     * following syntactical requirements: <ul> <li>A host as required by a <i>server-based naming
     * authority</i> in section 3.2.2 of <a href="http://www.ietf.org/rfc/rfc2396.txt"> <i>RFC 2396:
     * Uniform Resource Identifiers (URI): Generic Syntax</i></a> <li>A literal IPv6 address as
     * defined by <a href="http://www.ietf.org/rfc/rfc2732.txt"> <i>RFC 2732: Format for Literal
     * IPv6 Addresses in URL's</i></a> <li>A literal IPv6 address as defined by <a
     * href="http://www.ietf.org/rfc/rfc3513.txt"> <i>RFC 3513: Internet Protocol Version 6 (IPv6)
     * Addressing Architecture </i></a> </ul>
     *
     * @param host the name of the host to contact
     * @param port the number of the port to connect to
     * @throws IllegalArgumentException if <code>port</code> is not between 1 and 65535 (both
     *                                  included) or if <code>host</code> cannot be parsed.
     * @throws NullPointerException     if <code>host</code> is <code>null</code>
     */
    public LookupLocator(String host, int port) {
        if (host == null)
            throw new NullPointerException("null host");
        if (port <= 0 || port >= 65536)
            throw new IllegalArgumentException("port number out of range");
        URI uri;
        try {
            // Use URI to validate the host.
            // We pass in the port to handle the case where the host is in the
            // form of a valid IPv6 address with a port appended to it.
            uri = new URI(null, null, host, port, null, null, null);
            if (uri.getUserInfo() != null) {
                throw new IllegalArgumentException(
                        "invalid character, '@', in host: " + host);
            }
            this.host = host;
            this.port = port;
        } catch (URISyntaxException e) {
            uri = try3986Authority(host, port);
            assert ((this.port > 0) && (this.port < 65536));
        }
        String uriPath = uri.getPath();
        if (uriPath.length() != 0) {
            throw new IllegalArgumentException(
                    "invalid character, '/', in host: " + host);
        }
        if (uri.getQuery() != null) {
            throw new IllegalArgumentException(
                    "invalid character, '?', in host: " + host);
        }
        if (uri.getFragment() != null) {
            throw new IllegalArgumentException(
                    "invalid character, '#', in host: " + host);
        }
    }

    /**
     * Returns the name of the host that this instance should contact. <code>LookupLocator</code>
     * implements this method to return the <code>host</code> field.
     *
     * @return a String representing the host value
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the number of the port to which this instance should connect.
     * <code>LookupLocator</code> implements this method to return the <code>port</code> field.
     *
     * @return an int representing the port value
     */
    public int getPort() {
        return port;
    }

    /**
     * Perform unicast discovery and return the ServiceRegistrar object for the given lookup
     * service.  Unicast discovery is performed anew each time this method is called.
     * <code>LookupLocator</code> implements this method to simply invoke {@link #getRegistrar(int)}
     * with a timeout value, which is determined by the value of the <code>net.jini.discovery.timeout</code>
     * system property.  If the property is set, is not negative, and can be parsed as an
     * <code>Integer</code>, the value of the property is used as the timeout value. Otherwise, a
     * default value of <code>60</code> seconds is assumed.
     *
     * @return the ServiceRegistrar for the lookup service denoted by this LookupLocator object
     * @throws IOException            an error occurred during discovery
     * @throws ClassNotFoundException if a class required to unmarshal the <code>ServiceRegistrar</code>
     *                                proxy cannot be found
     */
    public ServiceRegistrar getRegistrar()
            throws IOException, ClassNotFoundException {
        return getRegistrar(defaultTimeout);
    }

    /**
     * Perform unicast discovery and return the ServiceRegistrar object for the given lookup
     * service, with the given discovery timeout. Unicast discovery is performed anew each time this
     * method is called. <code>LookupLocator</code> implements this method to use the values of the
     * <code>host</code> and <code>port</code> field in determining the host and port to connect
     * to.
     *
     * <p> If a connection can be established to start unicast discovery but the remote end fails to
     * respond within the given time limit, an exception is thrown.
     *
     * @param timeout the maximum time to wait for a response, in milliseconds.  A value of
     *                <code>0</code> specifies an infinite timeout.
     * @return the ServiceRegistrar for the lookup service denoted by this LookupLocator object
     * @throws IOException              an error occurred during discovery
     * @throws ClassNotFoundException   if a class required to unmarshal the <code>ServiceRegistrar</code>
     *                                  proxy cannot be found
     * @throws IllegalArgumentException if <code>timeout</code> is negative
     */
    public ServiceRegistrar getRegistrar(int timeout)
            throws IOException, ClassNotFoundException {
        InetAddress[] addrs = null;
        try {
            addrs = InetAddress.getAllByName(host);
        } catch (UnknownHostException uhe) {
            // Cannot resolve the host name, maybe the socket implementation
            // can do it for us.
            Socket sock = new Socket(host, port);
            return getRegistrarFromSocket(sock, timeout);
        }
        IOException ioEx = null;
        SecurityException secEx = null;
        ClassNotFoundException cnfEx = null;
        for (int i = 0; i < addrs.length; i++) {
            try {
                Socket sock = new Socket(addrs[i], port);
                return getRegistrarFromSocket(sock, timeout);
            } catch (ClassNotFoundException ex) {
                cnfEx = ex;
            } catch (IOException ex) {
                ioEx = ex;
            } catch (SecurityException ex) {
                secEx = ex;
            }
        }
        // All our attempts failed. Throw ClassNotFoundException, IOException,
        // SecurityException in that order of preference.
        if (cnfEx != null) {
            throw cnfEx;
        }
        if (ioEx != null) {
            throw ioEx;
        }
        assert (secEx != null);
        throw secEx;
    }

    // Convenience method to do unicast discovery on a socket
    private static ServiceRegistrar getRegistrarFromSocket(Socket sock,
                                                           int timeout)
            throws IOException, ClassNotFoundException {
        try {
            sock.setSoTimeout(timeout);
            if (Constants.useSocketTcpNoDelay()) {
                try {
                    sock.setTcpNoDelay(true);
                } catch (SocketException e) {
                    // ignore possible failures and proceed anyway
                }
            }
            if (Constants.useSocketKeepAlive()) {
                try {
                    sock.setKeepAlive(true);
                } catch (SocketException e) {
                    // ignore possible failures and proceed anyway
                }
            }
            DataOutputStream dstr =
                    new DataOutputStream(sock.getOutputStream());
            dstr.writeInt(protoVersion);
            dstr.flush();
            ObjectInputStream istr =
                    new ObjectInputStream(sock.getInputStream());
            byte[] registrarBytes = new byte[istr.readInt()];
            istr.readFully(registrarBytes);
//        MarshalledObject mo = (MarshalledObject)istr.readObject();
            for (int grpCount = istr.readInt(); --grpCount >= 0; ) {
                istr.readUTF(); // ensure proper format, then discard
            }
            // close the socket early since unmarshalling takes time and we
            // want to release the socket as fast as possible
            sock.close();
            ObjectInputStream os = new MarshalInputStream(new OptimizedByteArrayInputStream(registrarBytes), Thread.currentThread().getContextClassLoader(), false, null, Collections.EMPTY_SET);
            return (ServiceRegistrar) os.readObject();
        } finally {
            try {
                if (!sock.isClosed()) sock.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Return the string form of this LookupLocator, as a URL of scheme <code>"jini"</code>.
     */
    public String toString() {
        return "jini://" + getHost0(host) + ":" + port + "/";
    }

    /**
     * Two locators are equal if they have the same <code>host</code> and <code>port</code> fields.
     * The case of the <code>host</code> is ignored. Alternative forms of the same IPv6 addresses
     * for the <code>host</code> value are treated as being unequal.
     */
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof LookupLocator) {
            LookupLocator oo = (LookupLocator) o;
            return port == oo.port && host.equalsIgnoreCase(oo.host);
        }
        return false;
    }

    /**
     * Returns a hash code value calculated from the <code>host</code> and <code>port</code> field
     * values.
     */
    public int hashCode() {
        return host.toLowerCase().hashCode() ^ port;
    }

    // Checks if the host is an RFC 3513 IPv6 literal and converts it into
    // RFC 2732 form.
    private static String getHost0(String host) {
        if ((host.indexOf(':') >= 0) && (host.charAt(0) != '[')) {
            // This is a 3513 form IPv6 literal
            return '[' + host + ']';
        } else {
            return host;
        }
    }

    private URI try3986Authority(String host, int port) {
        try {
            URI u = new URI("jini://" + host + ":" + port);
            handle3986Authority(u);
            return u;
        } catch (URISyntaxException use) {
            // has to be an invalid host
            IllegalArgumentException iae =
                    new IllegalArgumentException("syntax error in host: " +
                            host);
            iae.initCause(use);
            throw iae;
        } catch (MalformedURLException mue) {
            IllegalArgumentException iae =
                    new IllegalArgumentException("syntax error in host: " +
                            host);
            iae.initCause(mue);
            throw iae;
        }
    }

    private void handle3986Authority(URI uri) throws MalformedURLException {
        assert (!uri.isOpaque());
        String authority;
        if ((authority = uri.getAuthority()) == null) {
            throw new MalformedURLException("Missing authority: " + uri);
        }
        if (authority.indexOf('@') != -1) {
            throw new MalformedURLException("invalid character, '@', in host: "
                    + uri);
        }
        parseHostPort(authority, uri);
    }

    private void parseHostPort(String authority, URI uri)
            throws MalformedURLException {
        int index = authority.lastIndexOf(':');
        if (index == -1) {
            port = discoveryPort;
            host = authority;
            return;
        }
        // Check for any other colons
        if (authority.indexOf(':') != index) {
            throw new MalformedURLException(": not allowed in host name: "
                    + uri);
        }
        String portString = authority.substring(index + 1);
        int portInt;
        if (portString.length() == 0) {
            throw new MalformedURLException("invalid port in authority: " +
                    uri);
        } else {
            try {
                portInt = Integer.parseInt(portString);
            } catch (NumberFormatException ne) {
                MalformedURLException mue = new MalformedURLException(
                        "invalid port in authority: " + uri);
                mue.initCause(ne);
                throw mue;
            }
        }
        port = portInt;
        host = authority.substring(0, index);
        if (host.length() == 0) {
            throw new MalformedURLException("zero length host name: " + uri);
        }
    }
}
