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

import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.time.SystemTime;
import com.sun.jini.config.Config;
import com.sun.jini.discovery.Discovery;
import com.sun.jini.discovery.DiscoveryConstraints;
import com.sun.jini.discovery.DiscoveryProtocolException;
import com.sun.jini.discovery.EncodeIterator;
import com.sun.jini.discovery.MulticastAnnouncement;
import com.sun.jini.discovery.MulticastRequest;
import com.sun.jini.discovery.UnicastResponse;
import com.sun.jini.discovery.internal.MultiIPDiscovery;
import com.sun.jini.logging.LogUtil;
import com.sun.jini.thread.TaskManager;
import com.sun.jini.thread.WakeupManager;
import com.sun.jini.thread.WakeupManager.Ticket;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.EmptyConfiguration;
import net.jini.config.NoSuchEntryException;
import net.jini.constraint.BasicMethodConstraints;
import net.jini.core.constraint.InvocationConstraints;
import net.jini.core.constraint.MethodConstraints;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.io.UnsupportedConstraintException;
import net.jini.security.BasicProxyPreparer;
import net.jini.security.ProxyPreparer;
import net.jini.security.Security;

import org.jini.rio.boot.BootUtil;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * This class is a helper utility class that encapsulates the functionality required of an entity
 * that wishes to employ multicast discovery to find lookup services located within the entity's
 * "multicast radius" (roughly, the number of hops beyond which neither the multicast requests from
 * the entity, nor the multicast announcements from the lookup service, will propagate). This class
 * helps make the process of acquiring references to lookup services - based on no information other
 * than lookup service group membership - much simpler for both services and clients.
 *
 * @author Sun Microsystems, Inc.
 * @com.sun.jini.impl <!-- Implementation Specifics -->
 *
 * The following implementation-specific items are discussed below: <ul><li> <a
 * href="#ldConfigEntries">Configuring LookupDiscovery</a> <li> <a href="#ldLogging">Logging</a>
 * </ul>
 *
 * <a name="ldConfigEntries"> <p> <b><font size="+1">Configuring LookupDiscovery</font></b> <p>
 * </a>
 *
 * This implementation of <code>LookupDiscovery</code> supports the following configuration entries;
 * where each configuration entry name is associated with the component name
 * <code>net.jini.discovery.LookupDiscovery</code>. Note that the configuration entries specified
 * here are specific to this implementation of <code>LookupDiscovery</code>. Unless otherwise
 * stated, each entry is retrieved from the configuration only once per instance of this utility,
 * where each such retrieval is performed in the constructor.
 *
 * <a name="discoveryConstraints"> <table summary="Describes the discoveryConstraints configuration
 * entry" border="0" cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1">
 * <code>discoveryConstraints</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> {@link
 * net.jini.core.constraint.MethodConstraints}
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>null</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> Constraints to
 * apply to the multicast request, multicast announcement and unicast discovery protocols. Multicast
 * request constraints are derived by calling {@link MethodConstraints#getConstraints
 * getConstraints} on the obtained <code>MethodConstraints</code> instance with a
 * <code>Method</code> object for the {@link com.sun.jini.discovery.DiscoveryConstraints#multicastRequest
 * multicastRequest} method; multicast announcement and unicast discovery constraints are similarly
 * obtained by passing <code>Method</code> objects for the {@link com.sun.jini.discovery.DiscoveryConstraints#multicastAnnouncement
 * multicastAnnouncement} and {@link com.sun.jini.discovery.DiscoveryConstraints#unicastDiscovery
 * unicastDiscovery} methods, respectively.  A <code>null</code> value is interpreted as	mapping all
 * methods to empty constraints. <p> This class supports the use of the following constraint types
 * to control discovery behavior: <ul> <li> {@link com.sun.jini.discovery.DiscoveryProtocolVersion}:
 * this constraint can be used to control which version(s) of the multicast request, multicast
 * announcement and unicast discovery protocols are used. <li> {@link
 * com.sun.jini.discovery.MulticastMaxPacketSize}: this constraint can be used to control the
 * maximum size of multicast request packets to send; it can also be used to specify the size of the
 * buffer used to receive incoming multicast announcement packets. <li> {@link
 * com.sun.jini.discovery.MulticastTimeToLive}: this constraint can be used to control the time to
 * live (TTL) value set on outgoing multicast request packets. <li> {@link
 * com.sun.jini.discovery.UnicastSocketTimeout}: this constraint can be used to control the read
 * timeout set on sockets over which unicast discovery is performed. <li> {@link
 * net.jini.core.constraint.ConnectionRelativeTime}: this constraint can be used to control the
 * relative connection timeout set on sockets over which unicast discovery is performed. <li> {@link
 * net.jini.core.constraint.ConnectionAbsoluteTime}: this constraint can be used to control the
 * absolute connection timeout set on sockets over which unicast discovery is performed. </ul>
 * Constraints other than those listed above are passed on to the underlying implementations of
 * versions 1 and 2 of the discovery protocols. </table>
 *
 * <a name="finalMulticastRequestInterval"> <table summary="Describes the
 * finalMulticastRequestInterval configuration entry" border="0" cellpadding="2"> <tr valign="top">
 * <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th scope="col" align="left"
 * colspan="2"> <font size="+1"> <code>finalMulticastRequestInterval</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> <code>long</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>2*60*1000 (2
 * minutes)</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> With respect to
 * when this utility is started, as well as when the set of groups to discover is changed, this
 * entry represents the number of milliseconds to wait after sending the n-th multicast request
 * where n is equal to the value of the <a href="#multicastRequestMax">
 * <code>multicastRequestMax</code></a> entry of this component. </table> <a
 * name="initialMulticastRequestDelayRange"> <table summary="Describes the
 * initialMulticastRequestDelayRange configuration entry" border="0" cellpadding="2"> <tr
 * valign="top"> <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th scope="col"
 * align="left" colspan="2"> <font size="+1"> <code>initialMulticastRequestDelayRange</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> <code>long</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>0
 * milliseconds</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> With respect to
 * when this utility is started, this entry controls how long to wait before sending out the first
 * multicast request. If the value is positive, the first request will be delayed by a random value
 * between <code>0</code> and <code>initialMulticastRequestDelayRange</code> milliseconds.
 * Subsequent request intervals are controlled by the <a href="#multicastRequestInterval">
 * <code>multicastRequestInterval</code></a> entry. Note that this entry only has effect when this
 * utility is initialized. The first multicast request is not delayed if the groups to discover are
 * subsequently changed. </table> <a name="multicastAnnouncementInterval"> <table summary="Describes
 * the multicastAnnouncementInterval configuration entry" border="0" cellpadding="2"> <tr
 * valign="top"> <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th scope="col"
 * align="left" colspan="2"> <font size="+1"> <code>multicastAnnouncementInterval</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> <code>long</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>2*60*1000 (2
 * minutes)</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> A lookup service
 * will send out multicast packets announcing its existence every N milliseconds; for some value of
 * N. The value of this entry controls how often this utility examines the multicast announcements
 * from previously discovered lookup services for <i>liveness</i>. </table>
 *
 * <a name="multicastInterfaceRetryInterval"> <table summary="Describes the
 * multicastInterfaceRetryInterval configuration entry" border="0" cellpadding="2"> <tr
 * valign="top"> <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th scope="col"
 * align="left" colspan="2"> <font size="+1"> <code>multicastInterfaceRetryInterval</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> <code>int</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>5*60*1000 (5
 * minutes)</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> With respect to any
 * network interface this utility is configured to use to send and receive multicast packets (see
 * entry <a href="#multicastInterfaces"> <code>multicastInterfaces</code></a>), if failure is
 * encountered upon the initial attempt to set the interface or join the desired multicast group,
 * this utility will retry the failed interface every <a href="#multicastInterfaceRetryInterval">
 * <code>multicastInterfaceRetryInterval</code></a> milliseconds until success is encountered.
 * </table>
 *
 * <a name="multicastInterfaces"> <table summary="Describes the multicastInterfaces configuration
 * entry" border="0" cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1">
 * <code>multicastInterfaces</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> {@link
 * java.net.NetworkInterface NetworkInterface[]}
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>new </code>
 * {@link java.net.NetworkInterface NetworkInterface[]} &nbsp <code>{all currently supported
 * interfaces}</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> Each network
 * interface that is represented by an element in the array corresponding to this configuration item
 * will be used to send and receive multicast packets when this utility is participating in the
 * multicast discovery process. When not set, this utility will use all of the network interfaces in
 * the system. When this entry is set to a zero length array, multicast discovery is effectively
 * <b><i>disabled</i></b>. And when set to <code>null</code>, the interface to which the operating
 * system defaults will be used. </table>
 *
 * <a name="multicastRequestHost"> <table summary="Describes the multicastRequestHost configuration
 * entry" border="0" cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1">
 * <code>multicastRequestHost</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> <code>String</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>{@link
 * java.net.InetAddress}.getLocalHost().getHostAddress()</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> This entry
 * specifies the host name to include in multicast requests if participating in <b><i>version
 * 2</i></b> of the multicast request protocol. The name cannot be <code>null</code>. </table>
 *
 * <a name="multicastRequestInterval"> <table summary="Describes the multicastRequestInterval
 * configuration entry" border="0" cellpadding="2"> <tr valign="top"> <th scope="col"
 * summary="layout"> <font size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font
 * size="+1"> <code>multicastRequestInterval</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> <code>long</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>5000</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> With respect to
 * when this utility is started, as well as when the set of groups to discover is changed, this
 * entry represents the number of milliseconds to wait after sending the n-th multicast request, and
 * before sending the (n+1)-st request, where n is less than the value of the <a
 * href="#multicastRequestMax"><code>multicastRequestMax</code></a> entry of this component.
 * </table>
 *
 * <a name="multicastRequestMax"> <table summary="Describes the multicastRequestMax configuration
 * entry" border="0" cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1">
 * <code>multicastRequestMax</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> <code>int</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>7</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> The maximum number
 * multicast requests to send when this utility is started for the first time, and whenever the
 * groups to discover is changed. </table>
 *
 * <a name="registrarPreparer"> <table summary="Describes the registrarPreparer configuration entry"
 * border="0" cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1">
 * <code>registrarPreparer</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> {@link
 * net.jini.security.ProxyPreparer}
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>new {@link
 * net.jini.security.BasicProxyPreparer}() </code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> Preparer for the
 * proxies to the lookup services that are discovered and used by this utility. <p> This preparer
 * should perform all operations required to use a newly received proxy to a lookup service, which
 * may including verifying trust in the proxy, granting permissions, and setting constraints. <p>
 * The following methods of the {@link net.jini.core.lookup.ServiceRegistrar ServiceRegistrar}
 * returned by this preparer are invoked by this implementation of <code>LookupDiscovery</code>:
 * <ul> <li>{@link net.jini.core.lookup.ServiceRegistrar#getServiceID getServiceID} <li>{@link
 * net.jini.core.lookup.ServiceRegistrar#getGroups getGroups} <li>{@link
 * net.jini.core.lookup.ServiceRegistrar#getLocator getLocator} </ul> </table>
 *
 * <a name="taskManager"> <table summary="Describes the taskManager configuration entry" border="0"
 * cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1">
 * <code>taskManager</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> {@link
 * com.sun.jini.thread.TaskManager}
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>new {@link
 * com.sun.jini.thread.TaskManager#TaskManager() TaskManager}(15, (15*1000), 1.0f)</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> The object that
 * pools and manages the various threads executed by this utility. The default manager creates a
 * maximum of 15 threads, waits 15 seconds before removing idle threads, and uses a load factor of
 * 1.0 when determining whether to create a new thread. This object should not be shared with other
 * components in the application that employs this utility. </table> <a name="unicastDelayRange">
 * <table summary="Describes the unicastDelayRange configuration entry" border="0" cellpadding="2">
 * <tr valign="top"> <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th
 * scope="col" align="left" colspan="2"> <font size="+1"> <code>unicastDelayRange</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> <code>long</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>0
 * milliseconds</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> Controls how long
 * this utility will wait before sending out unicast discovery requests. If the value is positive,
 * any unicast discovery request that it initiates will be delayed by a random value between
 * <code>0</code> and <code>unicastDelayRange</code> milliseconds. A typical use of this entry would
 * be to achieve a more uniform distribution of unicast discovery requests to a lookup service, when
 * a large number of <code>LookupDiscovery</code> instances simultaneously receive multicast
 * announcements from the lookup service. </table> <a name="wakeupManager"> <table
 * summary="Describes the wakeupManager configuration entry" border="0" cellpadding="2"> <tr
 * valign="top"> <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th scope="col"
 * align="left" colspan="2"> <font size="+1"> <code>wakeupManager</code></font>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> {@link
 * com.sun.jini.thread.WakeupManager}
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>new {@link
 * com.sun.jini.thread.WakeupManager#WakeupManager(com.sun.jini.thread.WakeupManager.ThreadDesc)
 * WakeupManager}(new {@link com.sun.jini.thread.WakeupManager.ThreadDesc}(null,true))</code>
 *
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> Object used to
 * schedule unicast discovery requests that are delayed using the <a
 * href="#unicastDelayRange"><code>unicastDelayRange</code></a> configuration entry of this utility.
 * This entry is processed only if <code>unicastDelayRange</code> has a positive value. </table> <a
 * name="ldLogging"> <p> <b><font size="+1">Logging</font></b> <p> </a>
 *
 * This implementation of <code>LookupDiscovery</code> uses the {@link Logger} named
 * <code>net.jini.discovery.LookupDiscovery</code> to log information at the following logging
 * levels: <p>
 *
 * <table border="1" cellpadding="5" summary="Describes the information logged by LookupDiscovery,
 * and the levels at which that information is logged">
 *
 * <caption halign="center" valign="top"> <b><code>net.jini.discovery.LookupDiscovery</code></b>
 * </caption>
 *
 * <tr> <th scope="col"> Level</th> <th scope="col"> Description</th> </tr>
 *
 * <tr> <td>{@link java.util.logging.Level#SEVERE SEVERE}</td> <td> when this utility is configured
 * to use either the default network interface assigned by the system, or a specific list of network
 * interfaces, if one of those interfaces is bad or not configured for multicast, or if a runtime
 * exception occurs while either sending multicast requests, or while configuring one of the
 * interfaces to receive multicast announcements, that fact will be logged at this level </td> </tr>
 * <tr> <td>{@link java.util.logging.Level#INFO INFO}</td> <td> when any exception other than an
 * <code>InterruptedIOException</code>, <code>InterruptedException</code> or
 * <code>UnsupportedConstraintException</code> occurs in a thread or task while attempting to
 * marshal an outgoing multicast request </td> </tr> <tr> <td>{@link java.util.logging.Level#INFO
 * INFO}</td> <td> when any exception other than an <code>InterruptedIOException</code> or
 * <code>SocketTimeoutException</code> occurs in a non-interrupted thread while attempting to
 * receive an incoming multicast packet </td> </tr> <tr> <td>{@link java.util.logging.Level#INFO
 * INFO}</td> <td> when any exception other than an <code>InterruptedIOException</code> occurs while
 * attempting unicast discovery </td> </tr> <tr> <td>{@link java.util.logging.Level#INFO INFO}</td>
 * <td> when this utility is configured to use either the default network interface assigned by the
 * system, or a specific list of network interfaces, with respect to any such interface, if failure
 * is encountered upon the initial attempt to set the interface or join the desired multicast group,
 * the interface will be periodically retried, and successful recovery will be logged at this level
 * </td> </tr> <tr> <td>{@link java.util.logging.Level#INFO INFO}</td> <td>when any exception occurs
 * while attempting to prepare a proxy</td> </tr> <tr> <td>{@link java.util.logging.Level#CONFIG
 * CONFIG}</td> <td> when the <code>multicastInterfaces</code> entry is configured to be
 * <code>null</code>, multicast packets will be sent and received through the default network
 * interface assigned by the system, and that fact will be logged at this level </td> </tr> <tr>
 * <td>{@link java.util.logging.Level#CONFIG CONFIG}</td> <td> when the
 * <code>multicastInterfaces</code> entry is configured to be a zero length array, multicast
 * discovery will be disabled, and and that fact will be logged at this level </td> </tr> <tr>
 * <td>{@link java.util.logging.Level#CONFIG CONFIG}</td> <td> when the
 * <code>multicastInterfaces</code> entry contains a specific list of network interfaces, multicast
 * packets will be sent and received through only the network interfaces contained in that list, and
 * those interfaces will be logged at this level </td> </tr> <tr> <td>{@link
 * java.util.logging.Level#CONFIG CONFIG}</td> <td> when the <code>multicastInterfaces</code> entry
 * is excluded from the configuration, multicast packets will be sent and received through all
 * interfaces in the system, and those interfaces will be logged at this level </td> </tr> <tr>
 * <td>{@link com.sun.jini.logging.Level#FINE FINE}</td> <td> when an
 * <code>UnknownHostException</code> occurs while determining the <code>multicastRequestHost</code>,
 * but the caller does not have permissions to retrieve the local host name. The original
 * <code>UnknownHostException</code> with the host name information is logged </td> <tr> <td>{@link
 * com.sun.jini.logging.Level#FINE FINE}</td> <td> when this utility is configured to use all
 * network interfaces enabled in the system, if one of those interfaces is bad or not configured for
 * multicast, or if a runtime exception occurs while either sending multicast requests, or while
 * configuring one of the interfaces to receive multicast announcements, that fact will be logged at
 * this level </td> </tr> <tr> <td>{@link com.sun.jini.logging.Level#FINE FINE}</td> <td>
 * when any exception occurs while attempting to unmarshal an incoming multicast announcement </td>
 * </tr> <tr> <td>{@link com.sun.jini.logging.Level#FINE FINE}</td> <td> when an
 * <code>UnsupportedConstraintException</code> occurs while marshalling an outgoing multicast
 * request, indicating that the provider that threw the exception will not be used for encoding that
 * request </td> </tr> <tr> <td>{@link com.sun.jini.logging.Level#FINE FINE}</td> <td> when
 * an <code>IOException</code> occurs upon attempting to close the socket after the thread that
 * listens for multicast responses is asked to terminate </td> </tr> <tr> <td>{@link
 * com.sun.jini.logging.Level#FINE FINE}</td> <td> when an exception is handled during
 * unicast discovery </td> </tr> <tr> <td>{@link java.util.logging.Level#FINE FINE}</td> <td> when
 * this utility is configured to use all network interfaces enabled in the system, with respect to
 * any such interface, if failure is encountered upon the initial attempt to set the interface or
 * join the desired multicast group, the interface will be periodically retried, and successful
 * recovery will be logged at this level </td> </tr> <tr> <td>{@link java.util.logging.Level#FINEST
 * FINEST}</td> <td>whenever any thread or task is started</td> </tr> <tr> <td>{@link
 * java.util.logging.Level#FINEST FINEST}</td> <td> whenever any thread (except the
 * <code>Notifier</code> thread) or task completes successfully </td> </tr> <tr> <td>{@link
 * java.util.logging.Level#FINEST FINEST}</td> <td>whenever a discovered, discarded, or changed
 * event is sent</td> </tr> <tr> <td>{@link java.util.logging.Level#FINEST FINEST}</td> <td>whenever
 * a proxy is prepared</td> </tr> </table> <p>
 * @see net.jini.core.lookup.ServiceRegistrar
 * @see DiscoveryChangeListener
 * @see DiscoveryManagement
 * @see DiscoveryGroupManagement
 * @see DiscoveryListener
 * @see DiscoveryEvent
 * @see DiscoveryPermission
 */
@com.gigaspaces.api.InternalApi
public class LookupDiscovery implements DiscoveryManagement,
        DiscoveryGroupManagement {
    /* Name of this component; used in config entry retrieval and the logger.*/
    private static final String COMPONENT_NAME = "net.jini.discovery.LookupDiscovery";
    /* Logger used by this utility. */
    private static final Logger logger = Logger.getLogger(COMPONENT_NAME);
    /**
     * Maximum number of concurrent tasks that can be run in any task manager created by this
     * class.
     */
    private static final int MAX_N_TASKS = 15;
    /**
     * Default maximum size of multicast packets to send and receive.
     */
    private static final int DEFAULT_MAX_PACKET_SIZE = 512;
    /**
     * Default timeout to set on sockets used for unicast discovery.
     */
    private static final int DEFAULT_SOCKET_TIMEOUT = 1 * 60 * 1000;

    /**
     * Flag indicating whether or not this class is still functional.
     */
    private boolean terminated = false;
    /**
     * Set of listeners to be sent discovered/discarded/changed events.
     */
    private final ArrayList listeners = new ArrayList(1);
    /**
     * The groups to discover. Empty set -- NO_GROUPS, null -- ALL_GROUPS
     */
    private Set groups = null;
    /**
     * Map from ServiceID to UnicastResponse.
     */
    private final Map registrars = new HashMap(11);
    /**
     * Set that takes one of the following: <p><ul> <li> Socket (discovery from multicast
     * request/response exchange) <li> LookupLocator (discovery from multicast announcement) <li>
     * CheckGroupsMarker (discarded/changed from announcement) <li> CheckReachabilityMarker
     * (announcements stopped, tests reachability) </ul><p> Each element of this set represents a
     * potential (or pending) discovered, discarded, or changed event. Instances of
     * UnicastDiscoveryTask retrieve the next available element from this set and, based on the
     * object type of the element, determines the processing to perform and what event type to send
     * to the registered listeners.
     */
    private final Set pendingDiscoveries = new HashSet(11);
    /**
     * Thread that handles pending notifications.
     */
    private Notifier notifierThread;
    /**
     * Notifications to be sent to listeners.
     */
    private final LinkedList pendingNotifies = new LinkedList();
    /**
     * Task manager for running UnicastDiscoveryTasks and DecodeAnnouncementTasks.
     */
    private TaskManager taskManager;
    /* WakeupManager to delay tasks. */
    private WakeupManager discoveryWakeupMgr = null;
    private boolean isDefaultWakeupMgr = false;
    /* Outstanding tickets - Access synchronized on pendingDiscoveries */
    private List tickets;
    /**
     * Thread that handles incoming multicast announcements.
     */
    private AnnouncementListener announceeThread;
    /**
     * Collection that contains instances of the Requestor Thread class, each of which participate
     * in multicast discovery by periodically sending out multicast discovery requests for a finite
     * period of time.
     */
    private final Collection requestors = new LinkedList();
    /**
     * Thread that manages incoming multicast responses. Runs only when there are Requestor threads
     * running.
     */
    private ResponseListener respondeeThread = null;
    /**
     * Map from ServiceID to multicast announcement time stamps; used by the process that monitors
     * multicast announcements from already-discovered lookup services, and determines when those
     * announcements have stopped.
     */
    private final HashMap regInfo = new HashMap(11);
    /**
     * Thread that monitors multicast announcements from already-discovered lookup services and,
     * upon determining that those announcements have stopped, queues a reachability test with the
     * UnicastDiscoveryTask which will ultimately result in the lookup service being discarded if
     * the reachability test indicates that the lookup service is actually down.
     */
    private AnnouncementTimerThread announcementTimerThread;
    /* Preparer for the proxies to the lookup services that are discovered
     * and used by this utility.
     */
    private ProxyPreparer registrarPreparer;
    /* Utility for participating in version 2 of discovery protocols. */
    private Discovery protocol2;
    /* Maximum number multicast requests to send when this utility is started
     * for the first time, and whenever the groups to discover are changed.
     */
    private int multicastRequestMax = 7;
    /* With respect to when this utility is started, as well as when the set
     * of groups to discover is changed, the value of this field represents
     * the number of milliseconds to wait after sending the n-th multicast
     * request, and before sending the (n+1)-st request, where n is less than
     * the value of <code)multicastRequestMax</code>.
     */
    private long multicastRequestInterval = 5000L;
    /* With respect to when this utility is started, as well as when the set
     * of groups to discover is changed, the value of this field represents
     * the number of milliseconds to wait after sending the n-th multicast
     * request, where n is equal to the value of
     * <code)multicastRequestMax</code>.
     */
    private long finalMulticastRequestInterval = 2 * 60 * 1000L;
    /* Name of requesting host to include in multicast request if
     * participating in version 2 of multicast request protocol.
     */
    private String multicastRequestHost;
    /* Constraints specified for outgoing multicast requests. */
    private DiscoveryConstraints multicastRequestConstraints;
    /* The network interfaces (NICs) through which multicast packets will
     * be sent.
     */
    private NetworkInterface[] nics;
    /* NICs that initially failed are retried after this number of millisecs.*/
    private int nicRetryInterval = 5 * 60 * 1000;
    /* Controls how often (in milliseconds) this utility examines the
     * multicast announcements from previously discovered lookup services
     * for "liveness".
     */
    private long multicastAnnouncementInterval = Long.getLong("net.jini.discovery.LookupDiscovery.multicastAnnouncementInterval", 5000).longValue();
    /*
     * Controls how long to wait before responding to multicast
     * announcements
     */
    private long unicastDelayRange = 0;
    /* Controls how long to wait before sending out multicast requests */
    private long initialMulticastRequestDelayRange = 0;
    /*
     * Flag which indicates that initial multicast request thread has been
     * started.
     */
    private boolean initialRequestorStarted = false;
    /* Constraints specified for incoming multicast announcements. */
    private DiscoveryConstraints multicastAnnouncementConstraints;
    /* Unprocessed constraints specified for unicast discovery. */
    private InvocationConstraints rawUnicastDiscoveryConstraints;

    /**
     * Constants used to tell the notifierThread the type of event to send
     */
    private static final int DISCOVERED = 0;
    private static final int DISCARDED = 1;
    private static final int CHANGED = 2;

    /**
     * Constants used to indicate the set of network interfaces being used
     */
    private static final int NICS_USE_ALL = 0;//use all NICs in the system
    private static final int NICS_USE_SYS = 1;//use NIC assigned by the system
    private static final int NICS_USE_LIST = 2;//use list of NICs from config
    private static final int NICS_USE_NONE = 3;//multicast disabled
    /**
     * Flag that indicates how the set of network interfaces was configured
     */
    private int nicsToUse = NICS_USE_ALL;

    /**
     * Data structure containing task data processed by the Notifier Thread
     */
    private static class NotifyTask {
        /**
         * The set of listeners to notify
         */
        public final ArrayList listeners;
        /**
         * Map of discovered registrars-to-groups in which each is a member
         */
        public final Map groupsMap;
        /**
         * The type of event to send: DISCOVERED, DISCARDED, CHANGED
         */
        public final int eventType;

        public NotifyTask(ArrayList listeners, Map groupsMap, int eventType) {
            this.listeners = listeners;
            this.groupsMap = groupsMap;
            this.eventType = eventType;
        }
    }//end class NotifyTask

    /**
     * Thread that retrieves data structures of type NotifyTask from a queue and, based on the
     * contents of the data structure, sends the appropriate event (discovered/discarded/changed) to
     * each registered listener. <p> Only 1 instance of this thread is run.
     */
    private class Notifier extends GSThread {
        /**
         * Create a daemon thread
         */
        public Notifier() {
            super("event-listener-notification");
            setDaemon(true);
        }//end constructor

        @Override
        public void run() {
            logger.finest("LookupDiscovery - Notifier thread started");
            while (true) {
                final NotifyTask task;
                synchronized (pendingNotifies) {
                    if (pendingNotifies.isEmpty()) {
                        notifierThread = null;
                        return;
                    }//endif
                    task = (NotifyTask) pendingNotifies.removeFirst();
                }//end sync
                /* The call to notify() on the registered listeners is
                 * performed inside a doPrivileged block that restores the
				 * access control context that was in place when this utility
				 * was created.
				 *
				 * This is done because the notify() method called below
				 * executes in the client. But the listener object that
				 * defines that notify() method may have been obtained by
				 * the client from some (possibly untrusted) 3rd party. With
				 * respect to 3rd party code executing in the client, it is
				 * not desirable to allow such code to execute with more
				 * priviledges than the client that created this utility.
				 * Therefore, before executing notify() on any of the
				 * registered listeners, the client's Subject should be
				 * restored, and the listner code should be restricted to
				 * doing nothing more than the client itself is allowed to do.
				 */
                boolean firstListener = true;
                for (Iterator iter = task.listeners.iterator();
                     iter.hasNext(); ) {
                    DiscoveryListener l =
                            (DiscoveryListener) iter.next();
                    /* Always send discovered and discarded events */
                    if ((task.eventType == CHANGED)
                            && !(l instanceof DiscoveryChangeListener)) {
                        continue;
                    }//endif
                    DiscoveryEvent e =
                            new DiscoveryEvent
                                    (LookupDiscovery.this,
                                            deepCopy((HashMap) task.groupsMap));
                    /* Log the event info about the lookup(s) */
                    if (firstListener
                            && (logger.isLoggable(Level.FINEST))) {
                        String eType =
                                new String[]{"discovered",
                                        "discarded",
                                        "changed"}[task.eventType];
                        ServiceRegistrar[] regs = e.getRegistrars();
                        logger.finest(eType + " event  -- " + regs.length
                                + " lookup(s)");
                        Map groupsMap = e.getGroups();
                        for (int i = 0; i < regs.length; i++) {
                            LookupLocator loc = null;
                            try {
                                loc = regs[i].getLocator();
                            } catch (Throwable ex) { /* ignore */ }
                            String[] groups =
                                    (String[]) groupsMap.get(regs[i]);
                            logger.finest("    " + eType + " locator  = "
                                    + loc);
                            if (groups.length == 0) {
                                logger.finest("    " + eType + " group    "
                                        + "= NO_GROUPS");
                            } else {
                                for (int j = 0; j < groups.length; j++) {
                                    logger.finest("    " + eType
                                            + " group[" + j + "] = "
                                            + groups[j]);
                                }//end loop
                            }//endif(groups.length)
                        }//end loop
                    }//endif(firstListener && isLoggable(Level.FINEST)
                    switch (task.eventType) {
                        case DISCOVERED:
                            l.discovered(e);
                            break;
                        case DISCARDED:
                            l.discarded(e);
                            break;
                        case CHANGED:
                            ((DiscoveryChangeListener) l).changed(e);
                            break;
                    }//end switch(eventType)
                    firstListener = false;
                }//end loop
            }//end loop
        }//end run
    }//end class Notifier

    /**
     * Thread that listens for multicast announcements from lookup services. <p> If the
     * announcements are from a lookup service that has not already been discovered, and if it is
     * determined that the lookup service belongs to at least one group of interest, a
     * "pendingDiscovery" is queued for the UnicastDiscoveryTask to process asynchronously,
     * completing the discovery process by performing unicast discovery. <p> If the announcements
     * are from a lookup service that has already been discovered, the lookup service's member
     * groups - as indicated in the announcements - are analyzed for changes that may result in
     * either the lookup service being discarded, or in a changed event being sent. <p> Only 1
     * instance of this thread is run.
     */
    private class AnnouncementListener extends Thread {
        /**
         * Multicast socket for receiving packets
         */
        private MulticastSocket sock;
        /* Set of interfaces whose elements also belong to the nics[] array,
         * which encountered failure when setting the interface or joining
         * the desired multicast group, and which will be retried periodically.
         */
        private ArrayList retryNics = null;

        /**
         * Create a daemon thread
         */
        public AnnouncementListener() throws IOException {
            super("GS-multicast-discovery-announcement-listener");
            setDaemon(true);
            try {
                sock = new MulticastSocket(Constants.getDiscoveryPort());
            } catch (IOException e) {
                IOException e1 = new IOException("Failed to create multicast socket on [" + Constants.getDiscoveryPort() + "], host address:" + SystemInfo.singleton().network().getHostId());
                e1.initCause(e);
                throw e1;
            }
            switch (nicsToUse) {
                case NICS_USE_ALL:
				/* Using all interfaces. Skip (but report) any interfaces
				 * that are "bad" or not configured for multicast.
				 */
                    for (int i = 0; i < nics.length; i++) {
                        try {
                            sock.setNetworkInterface(nics[i]);
                            sock.joinGroup(Constants.getAnnouncementAddress());
                        } catch (IOException e) {
                            if (retryNics == null) {
                                retryNics = new ArrayList(nics.length);
                            }//endif
                            retryNics.add(nics[i]);
                            if (logger.isLoggable(Level.FINE)) {
                                LogRecord logRec =
                                        new LogRecord(Level.FINE,
                                                "network interface "
                                                        + "is bad or not configured "
                                                        + "for multicast: {0}");
                                logRec.setParameters(new Object[]{nics[i]});
                                logRec.setThrown(e);
                                logger.log(logRec);
                            }//endif
                        }
                    }//end loop
                    break;
                case NICS_USE_LIST:
				/* Using a configured list of specific interfaces. Skip
				 * (but report) any interfaces that are "bad" or not
				 * configured for multicast.
				 */
                    for (int i = 0; i < nics.length; i++) {
                        try {
                            sock.setNetworkInterface(nics[i]);
                            sock.joinGroup(Constants.getAnnouncementAddress());
                        } catch (IOException e) {
                            if (retryNics == null) {
                                retryNics = new ArrayList(nics.length);
                            }//endif
                            retryNics.add(nics[i]);
                            if (logger.isLoggable(Level.SEVERE)) {
                                LogRecord logRec =
                                        new LogRecord(Level.SEVERE,
                                                "network interface is bad or "
                                                        + "not configured for "
                                                        + "multicast: {0}");
                                logRec.setParameters(new Object[]{nics[i]});
                                logRec.setThrown(e);
                                logger.log(logRec);
                            }//endif
                        }
                    }//end loop
                    break;
                case NICS_USE_SYS:
				/* Using the system-dependent default interface. Don't
				 * need to specifically set the interface. If that
				 * interface is "bad" or not configured for multicast,
				 * log it and try again later.
				 */
                    try {
                        sock.joinGroup(Constants.getAnnouncementAddress());
                    } catch (IOException e) {
                        retryNics = new ArrayList(0);
                        if (logger.isLoggable(Level.SEVERE)) {
                            logger.log(Level.SEVERE, "system default network "
                                    + "interface is bad or not configured "
                                    + "for multicast", e);
                        }//endif
                    }
                    break;
                case NICS_USE_NONE:
                    break;//multicast disabled, do nothing
                default:
                    throw new AssertionError("nicsToUse flag out of range "
                            + "(0-3): " + nicsToUse);
            }//end switch(nicsToUse)
        }//end constructor

        /**
         * True if thread has been interrupted
         */
        private volatile boolean interrupted = false;

        /* This is a workaround for Thread.interrupt not working on
         * MulticastSocket.receive on all platforms.
         */
        @Override
        public void interrupt() {
            interrupted = true;
            sock.close();
        }//end interrupt

        /**
         * Accessor method that returns the <code>interrupted</code> flag.
         */
        @Override
        public boolean isInterrupted() {
            return interrupted;
        }//end isInterrupted

        /**
         * Convenience method that retries any previously failed interfaces.
         */
        private void retryBadNics() {
            if (retryNics == null) return;//no failed NICs to retry
            if (!retryNics.isEmpty()) {
                String recoveredStr = "network interface has recovered "
                        + "from previous failure: {0}";
                ArrayList tmpList = (ArrayList) retryNics.clone();
                retryNics.clear();
                for (int i = 0; i < tmpList.size(); i++) {
                    NetworkInterface nic = (NetworkInterface) tmpList.get(i);
                    try {
                        sock.setNetworkInterface(nic);
                        sock.joinGroup(Constants.getAnnouncementAddress());
                        if (nicsToUse == NICS_USE_LIST) {
                            logger.log(Level.INFO, recoveredStr, nic);
                        } else {
                            logger.log(Level.FINE, recoveredStr, nic);
                        }//endif
                    } catch (IOException e1) {
                        retryNics.add(nic);//put back for another retry later
                    }
                }//end loop
                if (retryNics.isEmpty()) retryNics = null;//future retries off
            } else {//(retryNics.size() == 0) ==> sys default interface
                try {
                    sock.joinGroup(Constants.getAnnouncementAddress());
                    retryNics = null;
                    logger.log(Level.INFO, "system default network "
                            + "interface has recovered from "
                            + "previous failure");
                } catch (IOException e1) {
                }
            }//endif(!retryNics.isEmpty())
        }//end retryBadNics

        @Override
        public void run() {
            logger.finest("LookupDiscovery - AnnouncementListener thread "
                    + "started");
            byte[] buf = new byte[
                    multicastAnnouncementConstraints.getMulticastMaxPacketSize(
                            DEFAULT_MAX_PACKET_SIZE)];
            DatagramPacket pkt = new DatagramPacket(buf, buf.length);
            long endTime = SystemTime.timeMillis() + nicRetryInterval;
            while (!isInterrupted()) {
                try {
                    int delta_t = 0;
                    if (retryNics != null) {//bad NICs, retry when time's up
                        delta_t = (int) (endTime - SystemTime.timeMillis());
                        if (delta_t <= 0) {
                            retryBadNics();
                            if (retryNics != null) {//still bad, reset timer
                                delta_t = nicRetryInterval;
                                endTime = SystemTime.timeMillis() + delta_t;
                            } else {//all NICs recovered, turn off timer
                                delta_t = 0;
                            }//endif
                        }//endif
                    }//endif
                    sock.setSoTimeout(delta_t);
                    pkt.setLength(buf.length);
                    try {
                        sock.receive(pkt);
                    } catch (NullPointerException e) {
                        break; // workaround for bug 4190513
                    }
                    taskManager.add(new DecodeAnnouncementTask(pkt));

                    buf = new byte[buf.length];
                    pkt = new DatagramPacket(buf, buf.length);

                } catch (SocketTimeoutException e) {//continue/retry bad NICs
                } catch (InterruptedIOException e) {
                    break;
                } catch (Exception e) {//ignore
                    if (isInterrupted()) break;
                    logger.log(Level.INFO,
                            "exception while listening for multicast "
                                    + "announcements",
                            e);
                }
            }//end loop(!interrupted)
            sock.close();
            sock = null;
            logger.finest("LookupDiscovery - AnnouncementListener thread "
                    + "completed");
        }//end run
    }//end class AnnouncementListener

    /**
     * Thread that listens for multicast responses to the multicast requests sent out by the
     * Requestor Thread class. Upon receiving a multicast response, the socket that accepted the
     * connection request associated with the the multicast response is added to the set of
     * pendingDiscoveries so that it (the socket) will be used by the UnicastDiscoveryTask to
     * complete the discovery process asynchronously. <p> Only 1 instance of this thread is run.
     */
    private class ResponseListener extends Thread {
        /**
         * Server socket for accepting connections
         */
        public ServerSocket serv;

        /**
         * Create a daemon thread
         */
        public ResponseListener() throws IOException {
            super("GS-multicast-discovery-response-listener");
            setDaemon(true);


            serv = new ServerSocket(0, 100, InetAddress.getByName(SystemInfo.singleton().network().getHostId()));
        }//end constructor

        /**
         * True if thread has been interrupted
         */
        private volatile boolean interrupted = false;

        /* This is a workaround for Thread.interrupt not working on
         * ServerSocket.accept on all platforms.  ServerSocket.close
         * can't be used as a workaround, because it also doesn't work
         * on all platforms.
         */
        @Override
        public void interrupt() {
            interrupted = true;
            try {
                (new Socket(SystemInfo.singleton().network().getHostId(), getPort())).close();
            } catch (IOException e) { /* ignore */ }
        }//end interrupt

        /**
         * Accessor method that returns the <code>interrupted</code> flag.
         */
        @Override
        public boolean isInterrupted() {
            return interrupted;
        }//end isInterrupt

        @Override
        public void run() {
            logger.finest("LookupDiscovery - ResponseListener thread started");
            while (!isInterrupted()) {
                try {
                    Socket sock = serv.accept();
                    if (isInterrupted()) {
                        try {
                            sock.close();
                        } catch (IOException e) {
                        }
                        break;
                    }//end if
                    synchronized (pendingDiscoveries) {
                        pendingDiscoveries.add(sock);
                        taskManager.add(new UnicastDiscoveryTask(sock));
                    }//end sync
                } catch (InterruptedIOException e) {
                    break;
                } catch (Exception e) {//ignore
                    logger.log(Level.INFO,
                            "exception while listening for multicast "
                                    + "response",
                            e);
                }
            }//end loop(!isInterrupted)
            try {
                serv.close();
            } catch (IOException e) {//ignore
                logger.log(Level.FINE,
                        "IOException while attempting a socket close",
                        e);
            }
            logger.finest("LookupDiscovery - ResponseListener thread "
                    + "completed");
        }//end run

        /**
         * Return the local port of the socket
         */
        public int getPort() {
            return serv.getLocalPort();
        }//end getPort
    }//end class ResponseListener

    /**
     * Thread that periodically sends out multicast requests for a limited period of time, and then
     * exits. <p> An instance of this thread is run at startup, and each time the set of groups to
     * discover is changed.
     */
    private class Requestor extends Thread {
        /**
         * Multicast socket for sending packets
         */
        private final MulticastSocket sock;
        /**
         * Unicast response port
         */
        private final int responsePort;
        /**
         * Groups to request
         */
        private final String[] groups;
        private final boolean delayFlag;

        /**
         * Create a daemon thread
         */
        public Requestor(String[] groups, int port, boolean delayFlag)
                throws IOException {
            super("GS-multicast-discovery-request");
            setDaemon(true);
            sock = new MulticastSocket(Constants.getDiscoveryPort());
            sock.setTimeToLive(
                    multicastRequestConstraints.getMulticastTimeToLive(
                            Constants.getTtl()));
            responsePort = port;
            this.groups = groups == null ? new String[0] : groups;
            this.delayFlag = delayFlag;
        }//end constructor

        /**
         * This method sends out N (for small N) multicast requests. Until the last request is sent
         * out, this method sleeps for 5 seconds after each request is sent. After the last request
         * is sent, this method sleeps for 2 minutes to allow the ResponseListener time to receive
         * and process any multicast responses sent in reply to the multicast requests. Before
         * sending a request, a new multicast request is constructed so that updates can be made to
         * the set of service IDs of the lookup services discovered due to previous requests. <p>
         * After all requests have been sent, and the ResponseListener has been given the
         * appropriate time to receive and process any multicast responses, if there are no more
         * active instances of this thread, this method terminates (interrupts) the
         * ResponseListener. Note that although it is more desirable to have the ResponseListener
         * set a timeout on the server socket (using setSoTimeout) and then simply exit after a
         * period of time in which both the ResponseListener has been idle, and there have been no
         * active Requestor threads, using setSoTimeout in this way can cause random hangs on the
         * Solaris(TM) operating system.
         */
        @Override
        public void run() {
            logger.finest("LookupDiscovery - Requestor thread started");
            int count; // bug 4084783/4187594
            try {
                if (delayFlag
                        && (initialMulticastRequestDelayRange > 0)
                        && (multicastRequestMax >= 0)) {
                    Thread.sleep((long) (Math.random() *
                            initialMulticastRequestDelayRange));
                }
                for (count = multicastRequestMax;
                     --count >= 0 && !isInterrupted(); ) {
                    DatagramPacket[] reqs = encodeMulticastRequest
                            (new MulticastRequest(multicastRequestHost,
                                    responsePort,
                                    groups,
                                    getServiceIDs()));
                    sendPacketByNIC(sock, reqs);
                    Thread.sleep(count > 0 ?
                            multicastRequestInterval : finalMulticastRequestInterval);
                }//end loop
            } catch (InterruptedException e) {//terminate gracefully
            } catch (InterruptedIOException e) {//terminate gracefully
            } catch (Exception e) {
                logger.log(Level.INFO, "exception while marshalling outgoing "
                        + "multicast request", e);
            } finally {
                synchronized (requestors) {
                    requestors.remove(Thread.currentThread());
                    if (respondeeThread != null && requestors.isEmpty()) {
                        respondeeThread.interrupt();
                        respondeeThread = null;
                    }
                }//end sync
                sock.close();
                logger.finest("LookupDiscovery - Requestor thread completed");
            }//end try/catch/finally
        }//end run
    }//end class Requestor

    /**
     * This thread monitors the multicast announcements sent from the lookup service(s) that have
     * already been discovered by this class, looking for indications that those announcements have
     * terminated.
     *
     * The data structure used to map the discovered lookup services to the time of arrival of the
     * most recent multicast announcement from each such lookup service is examined at regular
     * intervals; dependent on the system property <code>net.jini.discovery.announce</code>.
     *
     * If the difference between the current time and the last time of arrival for any announcement
     * exceeds a predetermined threshold, the corresponding lookup is polled for its current set of
     * member groups. If that lookup service is unreachable, or if it is reachable but its member
     * groups have been replaced, the lookup service is discarded.
     */
    private class AnnouncementTimerThread extends Thread {
        /* Number of interval to exceed for declaring announcements stopped */
        private static final long N_INTERVALS = 3;

        /**
         * Create a daemon thread
         */
        public AnnouncementTimerThread() {
            super("GS-multicast-announcement-timer");
            setDaemon(true);
        }

        public synchronized void run() {
            long timeThreshold = N_INTERVALS * multicastAnnouncementInterval;
            try {
                while (!isInterrupted()) {
                    wait(multicastAnnouncementInterval);
                    long curTime = SystemTime.timeMillis();
                    synchronized (registrars) {
						/* can't modify regInfo while iterating over it,
						 * so clone it
						 */
                        HashMap regInfoClone = (HashMap) (regInfo.clone());
                        Set eSet = regInfoClone.entrySet();
                        for (Iterator itr = eSet.iterator(); itr.hasNext(); ) {
                            Map.Entry pair = (Map.Entry) itr.next();
                            ServiceID srvcID = (ServiceID) pair.getKey();
                            long tStamp =
                                    ((AnnouncementInfo) pair.getValue()).tStamp;
                            long deltaT = curTime - tStamp;
                            if (deltaT > timeThreshold) {
								/* announcements stopped, queue reachability
								 * test and potential discarded event
								 */
                                UnicastResponse resp =
                                        (UnicastResponse) registrars.get(srvcID);
                                Object req = new CheckReachabilityMarker(resp);
                                synchronized (pendingDiscoveries) {
                                    if (pendingDiscoveries.add(req)) {
                                        taskManager.add(
                                                new UnicastDiscoveryTask(req));
                                    }//endif
                                }//end sync
                            }//end if
                        }//end loop (itr)
                    }//end sync
                }//end loop (!isInterrupted)
            } catch (InterruptedException e) {
            }
        }//end run
    }//end class AnnouncementTimerThread

    /**
     * Marker object placed in pendingDiscoveries set to indicate to UnicastDiscoveryTask that the
     * groups of the lookup service which sent the contained announcement need to be verified.
     */
    private static class CheckGroupsMarker {
        /**
         * Announcement sent by lookup service to check groups of
         */
        final MulticastAnnouncement announcement;

        CheckGroupsMarker(MulticastAnnouncement announcement) {
            this.announcement = announcement;
        }

        public int hashCode() {
            return announcement.getServiceID().hashCode();
        }

        public boolean equals(Object obj) {
            return obj instanceof CheckGroupsMarker &&
                    announcement.getServiceID().equals(
                            ((CheckGroupsMarker) obj).announcement.getServiceID());
        }
    }

    /**
     * Marker object placed in pendingDiscoveries set to indicate to UnicastDiscoveryTask that
     * reachability of the lookup service which sent the contained unicast response needs to be
     * verified.
     */
    private static class CheckReachabilityMarker {
        /**
         * Response sent by lookup service to check reachability of
         */
        final UnicastResponse response;

        CheckReachabilityMarker(UnicastResponse response) {
            this.response = response;
        }

        public int hashCode() {
            return response.getRegistrar().hashCode();
        }

        public boolean equals(Object obj) {
            return obj instanceof CheckReachabilityMarker &&
                    response.getRegistrar().equals(
                            ((CheckReachabilityMarker) obj).response.getRegistrar());
        }
    }

    /**
     * Task which decodes received multicast announcement packets.  This is separated into a task to
     * allow the AnnouncementListener thread to quickly loop and receive new announcement packets;
     * the act of decoding packets may involve relatively slow cryptographic operations such as
     * signature verification, and would impede the packet receiving loop if it were performed
     * inline.
     */
    private class DecodeAnnouncementTask implements TaskManager.Task {

        private final DatagramPacket datagram;

        /**
         * Creates a task for decoding the given multicast announcement packet.
         */
        public DecodeAnnouncementTask(DatagramPacket datagram) {
            this.datagram = datagram;
        }

        /**
         * Decodes this task's multicast announcement packet.  If the constraints for decoding
         * multicast announcements are satisfied and the announcement merits further processing, an
         * appropriate object is added to the pendingDiscoveries set, and control is transferred to
         * a UnicastDiscoveryTask.
         */
        public void run() {
            MulticastAnnouncement ann;
            try {
                ann = decodeMulticastAnnouncement(datagram);
            } catch (Exception e) {
                if (!(e instanceof InterruptedIOException)) {
                    logger.log(Level.FINE,
                            "exception decoding multicast announcement", e);
                }
                return;
            }

			/* If the registrars map contains the service ID of the registrar
			 * that sent the current announcement then that registrar has
			 * already been discovered.
			 *
			 * Determine if the member groups of the already-discovered
			 * registrar have been replaced by a set containing none of the
			 * desired groups. If yes, then discard the registrar.
			 *
			 * If the registrar that sent the current announcement has not
			 * already been discovered, then check to see if any of the
			 * group(s) in which the registrar is a member are in the set of
			 * desired groups to discover. If yes, then queue the registrar for
			 * unicast discovery.
			 */
            Object pending = null;
            ServiceID srvcID = ann.getServiceID();
            synchronized (registrars) {
                UnicastResponse resp =
                        (UnicastResponse) registrars.get(srvcID);
                if (resp != null) {
                    // already in discovered set, timestamp announcement
                    AnnouncementInfo aInfo =
                            (AnnouncementInfo) regInfo.get(srvcID);
                    aInfo.tStamp = SystemTime.timeMillis();
                    long currNum = ann.getSequenceNumber();
                    if ((newSeqNum(currNum, aInfo.seqNum)) &&
                            (!groupSetsEqual(resp.getGroups(), ann.getGroups()))) {
						/* Check if the groups have changed. In the case of
						 * split announcement messages, eventually, group difference
						 * will be seen for the given sequence number. This
						 * check ignores other differences, such as port numbers,
						 * but for the purposes of LookupDiscovery, this is not
						 * important.
						 */
                        pending = new CheckGroupsMarker(ann);
                    }
                } else if (groupsOverlap(ann.getGroups())) {
                    // newly discovered
                    pending = new LookupLocator(ann.getHost(), ann.getPort());
                }
            }
            if (pending != null) {
                try {
                    ann.checkConstraints();
                } catch (Exception e) {
                    if (!(e instanceof InterruptedIOException)) {
                        logger.log(Level.FINE,
                                "exception decoding multicast announcement", e);
                    }
                    return;
                }
                if (pending instanceof CheckGroupsMarker) {
                    synchronized (registrars) {
                        // Since this is a valid announcement, update the
                        // sequence number.
                        AnnouncementInfo aInfo = (AnnouncementInfo) regInfo.get(srvcID);
                        if (aInfo == null)
                            return;

                        aInfo.seqNum = ann.getSequenceNumber();
                    }
                }
                boolean added;
                // enqueue and handle pending action, if not already enqueued
                synchronized (pendingDiscoveries) {
                    added = pendingDiscoveries.add(pending);
                }
                if (added) {
                    if (unicastDelayRange <= 0) {
                        new UnicastDiscoveryTask(pending).run();
                    } else {
                        final UnicastDiscoveryTask ud =
                                new UnicastDiscoveryTask(pending, true);
                        final Ticket t = discoveryWakeupMgr.schedule(
                                SystemTime.timeMillis() +
                                        (long) (Math.random() * unicastDelayRange),
                                new Runnable() {
                                    public void run() {
                                        taskManager.add(ud);
                                    }
                                }
                        );
                        synchronized (ud) {
                            ud.ticket = t;
                            ud.delayRun = false;
                            synchronized (pendingDiscoveries) {
                                tickets.add(t);
                            }
                            ud.notifyAll();
                        }
                    }
                }
            }
        }

        /**
         * Returns <code>true</code> if currentNum is a new sequence number that needs to be
         * inspected. A -1 occurs if the announcement had no sequence number (for e.g. DiscoveryV1)
         * or the service had been discovered through unicast discovery. REMIND: Ideally the message
         * should have a flag which indicates no sequence number instead of overloading the -1
         * value
         */
        private boolean newSeqNum(long currentNum, long oldNum) {
            if (oldNum == -1) {
                // No sequence number information, so we guess that this is
                // a new announcement of interest.
                return true;
            } else if (currentNum > oldNum) {
                return true;
            } else {
                return false;
            }
        }

        /**
         * No ordering
         */
        public boolean runAfter(List tasks, int size) {
            return false;
        }
    }

    /**
     * Task which retrieves elements from the set of pendingDiscoveries and performs the appropriate
     * processing based on the object type of the element. <p> Each element of the set of
     * pendingDiscoveries is one of the following object types: Socket, LookupLocator,
     * CheckGroupsMarker, or CheckReachabilityMarker. <p> When the element to process is a Socket,
     * the element was a result of a multicast request/response exchange (see the Requestor and
     * ResponseListener Thread classes). In this case, this task completes the discovery of the
     * associated lookup service by performing the final stage of unicast discovery, ultimately
     * resulting in a discovered event being sent to all registered listeners. <p> When the element
     * to process is a LookupLocator, the element was a result of a multicast announcement received
     * from a lookup service - belonging to at least one group of interest - which has not already
     * been discovered. In this case, this task also completes the discovery of the lookup service
     * referenced in the announcement by performing the final stage of unicast discovery, ultimately
     * resulting in a discovered event being sent to all registered listeners. <p> When the element
     * to process is a CheckGroupsMarker, the element was a result of a multicast announcement
     * received from an already-discovered lookup service whose member groups have changed in some
     * way. In this case, this task determines how those member groups have changed and, based on
     * how they have changed, whether (or not) to send a discarded event or a changed event to the
     * appropriate registered listeners. <p> When the element to process is a
     * CheckReachabilityMarker, the element was a result of a determination that the multicast
     * announcements from an already-discovered lookup service have stopped being received (see the
     * AnnouncementTimerThread class). In this case, this task determines if the affected lookup
     * service is still available ("reachable"). If this task cannot communicate with the lookup
     * service, a discarded event is queued to be sent to all registered listeners. <p> Rather than
     * performing unicast discovery synchronously, after multicast discovery has occurred in either
     * the AnnouncementListener thread (the multicast announcement protocol) or the ResponseListener
     * thread (the multicast request protocol), the unicast discovery processing that is required to
     * complete the discovery process is queued for asynchronous execution in this task. Unicast
     * discovery is performed asynchronously because unicast discovery can take quite a while to
     * fail if a lookup service "disappears" (because the network or the lookup service itself has
     * crashed) between the time a multicast announcement or response indicates the existence of a
     * lookup service eligible for unicast discovery, and the time unicast discovery actually
     * starts. If unicast discovery is performed synchronously in the threads that implement the
     * multicast announcement and multicast request protocols, other multicast announcements (as
     * well as other unicast discoveries) will be missed whenever a lookup service disappears prior
     * to the commencement of the unicast discovery stage.
     */
    private class UnicastDiscoveryTask implements TaskManager.Task {
        private final Object req;
        private Ticket ticket = null;
        private boolean delayRun = false;

        UnicastDiscoveryTask(Object req) {
            this(req, false);
        }

        UnicastDiscoveryTask(Object req, boolean delayRun) {
            this.req = req;
            this.delayRun = delayRun;
        }

        public void run() {
            logger.finest("LookupDiscovery - UnicastDiscoveryTask started");
            try {
                synchronized (this) {
                    while (delayRun) {
                        this.wait();
                    }
                    synchronized (pendingDiscoveries) {
                        // If this was run by a WakeupManager, remove its
                        // ticket from the list of outstanding tickets.
                        if (ticket != null) {
                            tickets.remove(ticket);
                        }
                    }
                }
                Socket sock = null;
                MulticastAnnouncement announcement = null;
                UnicastResponse response = null;
                if (req instanceof Socket) {
                    // Perform unicast discovery on the connected socket.
                    DiscoveryConstraints unicastDiscoveryConstraints =
                            DiscoveryConstraints.process(
                                    rawUnicastDiscoveryConstraints);
                    sock = (Socket) req;
                    UnicastResponse resp;
                    try {
                        prepareSocket(sock, unicastDiscoveryConstraints);
                        resp = doUnicastDiscovery(sock,
                                unicastDiscoveryConstraints);
                    } finally {
                        try {
                            if (!sock.isClosed()) sock.close();
                        } catch (IOException e) { /* ignore */ }
                    }
                    maybeAddNewRegistrar(resp);
                } else if (req instanceof LookupLocator) {
                    // Perform unicast discovery using the LookupLocator
                    // host and port.
                    LookupLocator loc = (LookupLocator) req;
                    UnicastResponse resp = new MultiIPDiscovery() {
                        protected UnicastResponse performDiscovery(
                                Discovery disco,
                                DiscoveryConstraints dc,
                                Socket s)
                                throws IOException, ClassNotFoundException {
                            return doUnicastDiscovery(s, dc, disco);
                        }

                        protected void singleResponseException(Exception e,
                                                               InetAddress addr,
                                                               int port) {
                            logger.log(Level.FINE,
                                    "Exception occurred during unicast discovery " +
                                            addr + ":" + port, e);
                        }
                    }.getResponse(loc.getHost(),
                            loc.getPort(),
                            rawUnicastDiscoveryConstraints);
                    maybeAddNewRegistrar(resp);
                } else if (req instanceof CheckGroupsMarker) {
                    // handle group changes
                    announcement = ((CheckGroupsMarker) req).announcement;
                    ServiceID srvcID = announcement.getServiceID();
                    UnicastResponse resp = null;
                    synchronized (registrars) {
                        resp = (UnicastResponse) registrars.get(srvcID);
                    }
                    if (resp != null) {
                        maybeSendEvent(resp, announcement.getGroups());
                    }//endif
                } else if (req instanceof CheckReachabilityMarker) {
                    // test reachability
                    response = ((CheckReachabilityMarker) req).response;
                    maybeSendEvent(response, null);
                }//endif
            } catch (InterruptedIOException e) {
                logger.log(Level.FINE,
                        "exception occurred during unicast discovery",
                        e);
            } catch (Throwable e) {
                if (((req instanceof Socket) ||
                        (req instanceof LookupLocator)) &&
                        logger.isLoggable(Level.FINE)) {
                    String logmsg =
                            "exception occurred during unicast discovery to " +
                                    "{0}:{1,number,#} with constraints {2}";
                    String methodName = "run";
                    if (req instanceof Socket) {
                        Socket sock = (Socket) req;
                        LogUtil.logThrow(logger,
                                Level.INFO,
                                this.getClass(),
                                methodName,
                                logmsg,
                                new Object[]{
                                        sock.getInetAddress().getHostName(),
                                        new Integer(sock.getPort()),
                                        rawUnicastDiscoveryConstraints
                                },
                                e);
                    } else {
                        LookupLocator loc = (LookupLocator) req;
                        LogUtil.logThrow(logger,
                                Level.INFO,
                                this.getClass(),
                                methodName,
                                logmsg,
                                new Object[]{
                                        loc.getHost(),
                                        new Integer(loc.getPort()),
                                        rawUnicastDiscoveryConstraints
                                },
                                e);
                    }
                } else {
                    logger.log(Level.INFO, "exception occurred during unicast discovery - request: " + req, e);
                }
            } finally {
                // Done with the request. Remove it regardless of
                // if we succeeded or failed.
                synchronized (pendingDiscoveries) {
                    pendingDiscoveries.remove(req);
                }
            }//end try/catch
            logger.finest("LookupDiscovery - UnicastDiscoveryTask completed");
        }//end run

        /**
         * Returns true if current instance must be run after task(s) in task manager queue.
         *
         * @param tasks the tasks to consider.
         * @param size  elements with index less than size are considered.
         */
        public boolean runAfter(List tasks, int size) {
            return false;
        }//end runAfter
    }//end class UnicastDiscoveryTask

    /**
     * Construct a new lookup discovery object, set to discover the given set of groups.  The set is
     * represented as an array of strings.  This array may be empty, which is taken as the empty
     * set, and discovery is not performed.  The reference passed in may be null, which is taken as
     * no set, and in which case discovery of all reachable lookup services is performed. Otherwise,
     * the array contains the names of groups to discover. The caller must have DiscoveryPermission
     * for each group (or for all groups, if the array is null).
     *
     * @param groups the set of group names to discover (null for no set, empty for no discovery)
     * @throws java.lang.NullPointerException input array contains at least one null element
     * @throws java.io.IOException            an exception occurred in starting discovery
     * @see #LookupGroups.none()
     * @see #LookupGroups.all()
     * @see #setGroups
     * @see DiscoveryPermission
     */
    public LookupDiscovery(String[] groups) throws IOException {
        try {
            beginDiscovery(groups, EmptyConfiguration.INSTANCE);
        } catch (ConfigurationException e) { /* swallow this exception */ }
    }//end constructor

    /**
     * Constructs a new lookup discovery object, set to discover the given set of groups, and having
     * the given <code>Configuration</code>. <p> The set of groups to discover is represented as an
     * array of strings.  This array may be empty, which is taken as the empty set, and discovery is
     * not performed.  The reference passed in may be <code>null</code>, which is taken as no set,
     * and in which case discovery of all reachable lookup services is performed. Otherwise, the
     * array contains the names of groups to discover. The caller must have
     * <code>DiscoveryPermission</code> for each group (or for all groups, if the array is
     * <code>null</code>).
     *
     * @param groups the set of group names to discover (null for no set, empty for no discovery)
     * @param config an instance of <code>Configuration</code>, used to obtain the objects needed to
     *               configure the current instance of this class
     * @throws java.lang.NullPointerException         input array contains at least one
     *                                                <code>null</code> element or <code>null</code>
     *                                                is input for the configuration
     * @throws java.io.IOException                    an exception occurred in starting discovery
     * @throws net.jini.config.ConfigurationException indicates an exception occurred while
     *                                                retrieving an item from the given
     *                                                <code>Configuration</code>
     * @see #LookupGroups.none()
     * @see #LookupGroups.all()
     * @see #setGroups
     * @see DiscoveryPermission
     * @see net.jini.config.Configuration
     * @see net.jini.config.ConfigurationException
     */
    public LookupDiscovery(String[] groups, Configuration config)
            throws IOException, ConfigurationException {
        beginDiscovery(groups, config);
    }//end constructor

    /**
     * Register a listener as interested in receiving DiscoveryEvent notifications.
     *
     * @param l the listener to register
     * @throws java.lang.NullPointerException  this exception occurs when <code>null</code> is input
     *                                         to the listener parameter <code>l</code>.
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see DiscoveryEvent
     * @see #removeDiscoveryListener
     */
    public void addDiscoveryListener(DiscoveryListener l) {
        if (l == null) {
            throw new NullPointerException("can't add null listener");
        }
        synchronized (registrars) {
            if (terminated) {
                throw new IllegalStateException("discovery terminated");
            }
            if (listeners.indexOf(l) >= 0) return; //already have this listener
            listeners.add(l);
            if (registrars.isEmpty()) return;//nothing to send the new listener
            HashMap groupsMap = new HashMap(registrars.size());
            Iterator iter = registrars.values().iterator();
            while (iter.hasNext()) {
                UnicastResponse resp = (UnicastResponse) iter.next();
                groupsMap.put(resp.getRegistrar(), resp.getGroups());
            }
            ArrayList list = new ArrayList(1);
            list.add(l);
            addNotify(list, groupsMap, DISCOVERED);
        }
    }//end addDiscoveryListener

    /**
     * Indicate that a listener is no longer interested in receiving DiscoveryEvent notifications.
     *
     * @param l the listener to unregister
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see #addDiscoveryListener
     */
    public void removeDiscoveryListener(DiscoveryListener l) {
        synchronized (registrars) {
            if (terminated) {
                throw new IllegalStateException("discovery terminated");
            }
            listeners.remove(l);
        }
    }//end removeDiscoveryListener

    /**
     * Returns an array of instances of <code>ServiceRegistrar</code>, each corresponding to a proxy
     * to one of the currently discovered lookup services. For each invocation of this method, a new
     * array is returned.
     *
     * @return array of instances of <code>ServiceRegistrar</code>, each corresponding to a proxy to
     * one of the currently discovered lookup services
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see net.jini.core.lookup.ServiceRegistrar
     * @see net.jini.discovery.DiscoveryManagement#removeDiscoveryListener
     */
    public ServiceRegistrar[] getRegistrars() {
        synchronized (registrars) {
            if (terminated) {
                throw new IllegalStateException("discovery terminated");
            }
            if (registrars.isEmpty()) {
                return new ServiceRegistrar[0];
            }
            Iterator iter = registrars.values().iterator();
            ServiceRegistrar[] regs = new ServiceRegistrar[registrars.size()];
            for (int i = 0; iter.hasNext(); i++) {
                regs[i] = ((UnicastResponse) iter.next()).getRegistrar();
            }
            return regs;
        }
    }//end getRegistrars

    /**
     * Discard a registrar from the set of registrars already discovered.  This does not prevent
     * that registrar from being rediscovered; it is intended to be used to clear unreachable
     * entries from the set. <p>
     *
     * If the registrar has been discovered using this LookupDiscovery object, each listener
     * registered with this object will have its discarded method called with the given registrar as
     * parameter.
     *
     * @param reg the registrar to discard
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see DiscoveryListener#discarded
     */
    public void discard(ServiceRegistrar reg) {
        synchronized (registrars) {
            if (terminated) {
                throw new IllegalStateException("discovery terminated");
            }
            if (reg == null) return;
            sendDiscarded(reg, null);
        }//end sync
    }//end discard

    /**
     * Terminate the discovery process.
     */
    public void terminate() {
        synchronized (registrars) {
            if (terminated) return;
            terminated = true;
        }
        nukeThreads();
    }//end terminate

    /**
     * Return the set of group names this LookupDiscovery instance is trying to discover.  If this
     * method returns the empty array, that value is guaranteed to be referentially equal to
     * LookupDiscovery.NO_GROUPS.
     *
     * @return the set of groups to be discovered (null for all, empty for no discovery)
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see #LookupGroups.none()
     * @see #LookupGroups.all()
     * @see #setGroups
     */
    public String[] getGroups() {
        synchronized (registrars) {
            if (terminated) {
                throw new IllegalStateException("discovery terminated");
            }
            if (groups == null)
                return LookupGroups.all();
            if (groups.isEmpty())
                return LookupGroups.none();
            return collectionToStrings(groups);
        }
    }//end getGroups

    /**
     * Add a set of groups to the set to be discovered. The caller must have DiscoveryPermission for
     * each group.
     *
     * @param newGroups the groups to add
     * @throws java.io.IOException                     the multicast request protocol failed to
     *                                                 start
     * @throws java.lang.IllegalStateException         this exception occurs when this method is
     *                                                 called after the <code>terminate</code>
     *                                                 method has been called.
     * @throws java.lang.UnsupportedOperationException there is no set of groups to add to
     * @see DiscoveryPermission
     */
    public void addGroups(String[] newGroups) throws IOException {
        testArrayForNullElement(newGroups);
        checkGroups(newGroups);
        synchronized (registrars) {
            if (terminated)
                throw new IllegalStateException("discovery terminated");
            if (groups == null)
                throw new UnsupportedOperationException(
                        "can't add to \"any groups\"");
            Collection req = new ArrayList(newGroups.length);
            for (int i = 0; i < newGroups.length; i++) {
                if (groups.add(newGroups[i]))
                    req.add(newGroups[i]);
            }
            if (!req.isEmpty())
                requestGroups(req);
        }
    }//end addGroups

    /**
     * Change the set of groups to be discovered to correspond to the given set.  The set is
     * represented as an array of strings. This array may be empty, which is taken as the empty set,
     * and discovery is not performed.  The reference passed in may be null, which is taken as no
     * set, and in which case discovery of all reachable lookup services is performed.  Otherwise,
     * the array contains the names of groups to discover. The caller must have DiscoveryPermission
     * for each group (or for all groups, if the array is null).
     *
     * @param newGroups the new set of groups to discover (null for all, empty array for no
     *                  discovery)
     * @throws java.io.IOException             an exception occurred when starting multicast
     *                                         discovery
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see #LookupDiscovery
     * @see #LookupGroups.all()
     * @see #LookupGroups.none()
     * @see DiscoveryPermission
     * @see #getGroups
     */
    public void setGroups(String[] newGroups) throws IOException {
        testArrayForNullElement(newGroups);
        checkGroups(newGroups);
        boolean maybeDiscard = false;
        Set newGrps = null;
        if (newGroups != null) {
            newGrps = new HashSet(newGroups.length * 2);
            for (int i = 0; i < newGroups.length; i++) {
                newGrps.add(newGroups[i]);
            }
        }
        synchronized (registrars) {
            if (terminated)
                throw new IllegalStateException("discovery terminated");
            if (newGroups == null) {
                if (groups != null) {
                    groups = null;
                    requestGroups(null);
                }
                return;
            }
            if (groups == null) {
                groups = new HashSet(11);
                maybeDiscard = true;
            }
            Set toAdd = new HashSet(newGrps);
            toAdd.removeAll(groups);
            // Figure out which groups to get rid of.  We start off
            // with the full set for which we are already listening,
            // and eliminate any that are in both the new set and the
            // current set.
            Collection toRemove = new HashSet(groups);
            toRemove.removeAll(newGrps);
            // Add new groups before we remove any old groups, because
            // removeGroups will start a new round of multicast requests
            // if the set of groups becomes empty, and we don't want it
            // to do so without reason.
            groups.addAll(toAdd);
            if (!toRemove.isEmpty())
                maybeDiscard |= removeGroupsInt(collectionToStrings(toRemove));
            if (!toAdd.isEmpty())
                requestGroups(toAdd);
        }
        if (maybeDiscard)
            maybeDiscardRegistrars();
    }//end setGroups

    /**
     * Remove a set of groups from the set to be discovered.
     *
     * @param oldGroups groups to remove
     * @throws java.lang.IllegalStateException         this exception occurs when this method is
     *                                                 called after the <code>terminate</code>
     *                                                 method has been called.
     * @throws java.lang.UnsupportedOperationException there is no set of groups from which to
     *                                                 remove
     */
    public void removeGroups(String[] oldGroups) {
        testArrayForNullElement(oldGroups);
        boolean maybeDiscard;
        synchronized (registrars) {
            if (terminated)
                throw new IllegalStateException("discovery terminated");
            if (groups == null)
                throw new UnsupportedOperationException(
                        "can't remove from \"any groups\"");
            maybeDiscard = removeGroupsInt(oldGroups);
        }
        if (maybeDiscard)
            maybeDiscardRegistrars();
    }//end removeGroups

    /**
     * Sends the given packet data on the given <code>MulticastSocket</code> through each of the
     * network interfaces corresponding to elements of the array configured when this utility was
     * constructed.
     *
     * @param mcSocket the <code>MulticastSocket</code> on which the data will be sent
     * @param packet   <code>DatagramPacket</code> array whose elements are the data to send
     */
    private void sendPacketByNIC(MulticastSocket mcSocket,
                                 DatagramPacket[] packet)
            throws InterruptedIOException {
        switch (nicsToUse) {
            case NICS_USE_ALL:
			/* Using all interfaces. Skip (but report) any interfaces
			 * that are "bad" or not configured for multicast.
			 */
                for (int i = 0; i < nics.length; i++) {
                    try {
                        mcSocket.setNetworkInterface(nics[i]);
                        sendPacket(mcSocket, packet);
                    } catch (InterruptedIOException e) {
                        throw e;//to signal a graceful exit
                    } catch (IOException e) {
                        if (logger.isLoggable(Level.FINE)) {
                            LogRecord logRec =
                                    new LogRecord(Level.FINE,
                                            "network interface is "
                                                    + "bad or not configured for "
                                                    + "multicast: {0}");
                            logRec.setParameters(new Object[]{nics[i]});
                            logRec.setThrown(e);
                            logger.log(logRec);
                        }//endif
                    } catch (Exception e) {
                        if (logger.isLoggable(Level.FINE)) {
                            LogRecord logRec =
                                    new LogRecord(Level.FINE, "exception while "
                                            + "sending packet through network "
                                            + "interface: {0}");
                            logRec.setParameters(new Object[]{nics[i]});
                            logRec.setThrown(e);
                            logger.log(logRec);
                        }//endif
                    }
                }//end loop
                break;
            case NICS_USE_LIST:
			/* Using a configured list of specific interfaces. Skip (but
			 * always report) any interfaces that are "bad" or not
			 * configured for multicast.
			 */
                for (int i = 0; i < nics.length; i++) {
                    try {
                        mcSocket.setNetworkInterface(nics[i]);
                        sendPacket(mcSocket, packet);
                    } catch (InterruptedIOException e) {
                        throw e;//to signal a graceful exit
                    } catch (IOException e) {
                        if (logger.isLoggable(Level.SEVERE)) {
                            LogRecord logRec =
                                    new LogRecord(Level.SEVERE, "network interface "
                                            + "is bad or not configured for "
                                            + "multicast: {0}");
                            logRec.setParameters(new Object[]{nics[i]});
                            logRec.setThrown(e);
                            logger.log(logRec);
                        }//endif
                    } catch (Exception e) {
                        if (logger.isLoggable(Level.SEVERE)) {
                            LogRecord logRec =
                                    new LogRecord(Level.SEVERE, "exception while "
                                            + "sending packet through network "
                                            + "interface: {0}");
                            logRec.setParameters(new Object[]{nics[i]});
                            logRec.setThrown(e);
                            logger.log(logRec);
                        }//endif
                    }
                }//end loop
                break;
            case NICS_USE_SYS:
			/* Using the system-dependent default interface. Don't need
			 * to specifically set the interface. If that interface is
			 * "bad" or not configured for multicast, always report it.
			 */
                try {
                    sendPacket(mcSocket, packet);
                } catch (InterruptedIOException e) {
                    throw e;//to signal a graceful exit
                } catch (IOException e) {
                    if (logger.isLoggable(Level.SEVERE)) {
                        logger.log(Level.SEVERE, "system default network "
                                + "interface is bad or not configured "
                                + "for multicast", e);
                    }//endif
                } catch (Exception e) {
                    if (logger.isLoggable(Level.SEVERE)) {
                        logger.log(Level.SEVERE, "exception while sending "
                                + "packet through system default network "
                                + "interface", e);
                    }//endif
                }
                break;
            case NICS_USE_NONE:
                break;//multicast disabled, do nothing
            default:
                throw new AssertionError("nicsToUse flag out of range (0-3): "
                        + nicsToUse);
        }//end switch(nicsToUse)
    }//end sendPacketByNIC

    /**
     * Sends the given packet data on the given <code>MulticastSocket</code> through the network
     * interface that is currently set.
     *
     * @param mcSocket the <code>MulticastSocket</code> on which the data will be sent
     * @param packet   <code>DatagramPacket</code> array whose elements are the data to send
     */
    private static void sendPacket(MulticastSocket mcSocket,
                                   DatagramPacket[] packet) throws IOException {
        for (int i = 0; i < packet.length; i++) {
            mcSocket.send(packet[i]);
        }//end loop
    }//end sendPacket

    /**
     * Determines if the caller has discovery permission for each group.
     */
    private static void checkGroups(String[] groups) {
        SecurityManager sm = System.getSecurityManager();
        if (sm == null) return;
        if (groups != null) {
            for (int i = 0; i < groups.length; i++) {
                sm.checkPermission(new DiscoveryPermission(groups[i]));
            }//end loop
        } else {
            sm.checkPermission(new DiscoveryPermission("*"));
        }//endif
    }//end checkGroups

    /**
     * Converts a collection to an array of strings.
     */
    private static final String[] collectionToStrings(Collection c) {
        return c == null ? null : (String[]) c.toArray(new String[c.size()]);
    }//end collectionToStrings

    /**
     * Determines if two sets of registrar member groups have identical contents. Assumes there are
     * no duplicates, and the sets can never be null.
     *
     * @param groupSet0 <code>String</code> array containing the group names from the first set used
     *                  in the comparison
     * @param groupSet1 <code>String</code> array containing the group names from the second set
     *                  used in the comparison
     * @return <code>true</code> if the contents of each set is identical; <code>false</code>
     * otherwise
     */
    private static boolean groupSetsEqual(String[] groupSet0,
                                          String[] groupSet1) {
        if (groupSet0.length != groupSet1.length) return false;
		/* is every element of one set contained in the other set? */
        iLoop:
        for (int i = 0; i < groupSet0.length; i++) {
            for (int j = 0; j < groupSet1.length; j++) {
                if (groupSet0[i].equals(groupSet1[j])) {
                    continue iLoop;
                }
            }//end loop(j)
            return false;
        }//end loop(i)
        return true;
    }//end groupSetsEqual

    /**
     * Returns true if the registrars contained in the given (possibly null) UnicastResponse
     * instances are equals() to one another.
     */
    private static boolean registrarsEqual(UnicastResponse resp1,
                                           UnicastResponse resp2) {
        return resp1 != null && resp2 != null &&
                resp2.getRegistrar().equals(resp1.getRegistrar());
    }//end registrarsEqual

    /**
     * Remove the specified groups from the set of groups to discover, and return true if any were
     * actually removed.
     */
    private boolean removeGroupsInt(String[] oldGroups) {
        boolean removed = false;
        for (int i = 0; i < oldGroups.length; i++) {
            removed |= groups.remove(oldGroups[i]);
        }
        return removed;
    }//end removeGroupsInt

    /**
     * Returns the service IDs of the lookup service(s) discovered to date.
     */
    private ServiceID[] getServiceIDs() {
        synchronized (registrars) {
            return (ServiceID[])
                    registrars.keySet().toArray(new ServiceID[registrars.size()]);
        }//end sync
    }//end getServiceIDs

    /**
     * Indicate whether any of the group names in the given array match any of the groups of
     * interest.
     *
     * @param possibilities the set of group names to compare to the set of groups to discover (must
     *                      not be null)
     */
    private boolean groupsOverlap(String[] possibilities) {
		/* Match if we're interested in any group, or if we're
		 * interested in none and there are no possibilities.
		 */
        if (groups == null) return true;
        for (int i = 0; i < possibilities.length; i++) {
            if (groups.contains(possibilities[i])) return true;
        }//end loop
        return false;
    }//end groupsOverlap

    /**
     * Called at startup and whenever the set of groups to discover is changed. This method executes
     * the multicast request protocol by starting the ResponseListener thread to listen for
     * multicast responses; and starting a Requestor thread to send out multicast requests for the
     * set of groups contained in the given Collection.
     */
    private void requestGroups(final Collection req) throws IOException {
        try {
            if (!Constants.isMulticastEnabled()) {
                return;
            }
            Security.doPrivileged(new PrivilegedExceptionAction() {
                public Object run() throws Exception {
                    Thread t;
                    synchronized (requestors) {
                        if (respondeeThread == null) {
                            respondeeThread = new ResponseListener();
                            respondeeThread.start();
                        }//endif
                        boolean delayFlag = false;
                        if (!initialRequestorStarted) {
                            // only delay the first time
                            delayFlag = true;
                            initialRequestorStarted = true;
                        }
                        t = new Requestor(collectionToStrings(req),
                                respondeeThread.getPort(),
                                delayFlag);
                        requestors.add(t);
                        t.start();
                    }//end sync
                    return null;
                }//run
            });//end doPrivileged
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getException();
        }//end try/catch
    }//end requestGroups

    private static void prepareSocket(Socket s, DiscoveryConstraints dc)
            throws SocketException {
        if (Constants.useSocketTcpNoDelay()) {
            try {
                s.setTcpNoDelay(true);
            } catch (SocketException e) {
                // ignore possible failures and proceed anyway
            }
        }
        if (Constants.useSocketKeepAlive()) {
            try {
                s.setKeepAlive(true);
            } catch (SocketException e) {
                // ignore possible failures and proceed anyway
            }
        }
        s.setSoTimeout(dc.getUnicastSocketTimeout(DEFAULT_SOCKET_TIMEOUT));
    }

    /**
     * If the lookup service associated with the given UnicastResponse is not in the set of
     * already-discovered lookup services, this method adds it to that set, and each registered
     * listener is notified.
     *
     * @param resp the UnicastResponse associated with the lookup service to add
     */
    private void maybeAddNewRegistrar(UnicastResponse resp) {
		/* If the group names contained in the given incoming unicast response
		 * don't match any of the groups of interest, then don't waste time
		 * performing an unnecessary proxy preparation; simply return.
		 */
        synchronized (registrars) {
            if (!groupsOverlap(resp.getGroups())) return;
        }//end sync(registrars)

		/* Proxy preparation -
		 *
		 * The given incoming unicast response contains a proxy to a lookup
		 * service that belongs to at least one of the groups of interest.
		 * Before adding that proxy to the managed set of discovered lookup
		 * services, and before notifying any of the registered listeners,
		 * that proxy should be prepared. This is necessary in this utility
		 * because that lookup service may be tested for reachability at
		 * some point. Since that test involves a remote call (to getGroups())
		 * through the proxy, the proxy should be prepared.
		 *
		 * The preparation of that proxy is performed inside a doPrivileged
		 * block that restores the access control context that was in place
		 * when this utility was created. In this way, any code that is
		 * executed as a part of preparing the proxy will be executed with
		 * no additional permissions beyond the permissions that were granted
		 * to the client that created this utility. This is done because the
		 * proxy preparer executed below is provided by the deployer and thus
		 * can be viewed as an artifact of the client. Therefore, before
		 * executing the preparer's code in this utility, the client's
		 * Subject should be restored, and the preparer code should be
		 * restricted to doing nothing more than the client itself is
		 * allowed to do.
		 *
		 * Note that it's okay to modify the state of the given incoming
		 * unicast response here because, prior to modification and storage
		 * in the managed set of registrars in this method, it is assumed that
		 * that object is not accessed by any other thread.
		 */
        try {
            final ServiceRegistrar srcReg = resp.getRegistrar();
            ServiceRegistrar prepReg = (ServiceRegistrar) registrarPreparer.prepareProxy(srcReg);
            if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "LookupDiscovery - "
                                + "prepared lookup service proxy: {0}",
                        prepReg);
            }
            if (prepReg != srcReg) {
                resp = new UnicastResponse(resp.getHost(),
                        resp.getPort(),
                        resp.getGroups(),
                        prepReg);
            }
        } catch (Exception e) {
            Exception e1 = ((e instanceof PrivilegedActionException) ?
                    ((PrivilegedActionException) e).getException() : e);
            logger.log(Level.INFO,
                    "exception while preparing lookup service proxy",
                    e1);
            return;
        }
		/* Add any newly discovered registrars to the managed set and notify
		 * all listeners.
		 */
        synchronized (registrars) {
            if (groupsOverlap(resp.getGroups()) &&
                    !registrarsEqual(resp,
                            (UnicastResponse) registrars.put
                                    (resp.getRegistrar().getServiceID(), resp))) {
				/* Time stamp the service ID and store its current sequence
				 * number. The first time stamp associated
				 * with the current service ID occurs here. All other time
				 * stamps for that service ID will occur when multicast
				 * announcements for that service ID arrive (in the
				 * AnnouncementListener thread).
				 *
				 * Note that if the time stamp for the service ID were
				 * initialized upon the arrival of the first announcement,
				 * rather than here when it is first discovered, the
				 * AnnouncementTimerThread would not be able to detect the
				 * termination of announcements for the case where the
				 * termination happens to occur between the time the lookup
				 * is first discovered here, and the time the first
				 * announcement was supposed to have arrived. This can
				 * happen because a multicast request from the client can
				 * cause the lookup to be discovered before the first
				 * announcement arrives.
				 */
                regInfo.put(resp.getRegistrar().getServiceID(),
                        new AnnouncementInfo(SystemTime.timeMillis(), -1));
                if (!listeners.isEmpty()) {
                    addNotify((ArrayList) listeners.clone(),
                            mapRegToGroups(resp.getRegistrar(),
                                    resp.getGroups()),
                            DISCOVERED);
                }//endif
            }//endif
        }//end sync(registrars)
    }//end maybeAddNewRegistrar

    /**
     * Determine if any of the already-discovered registrars are no longer members of any of the
     * groups to discover, and discard those registrars that are no longer members of any of those
     * groups.
     */
    private void maybeDiscardRegistrars() {
        synchronized (registrars) {
            HashMap groupsMap = new HashMap(registrars.size());
            for (Iterator iter = registrars.values().iterator(); iter.hasNext(); ) {
                UnicastResponse ent = (UnicastResponse) iter.next();
                if (!groupsOverlap(ent.getGroups())) { // not interested anymore
                    groupsMap.put(ent.getRegistrar(), ent.getGroups());
                    regInfo.remove(ent.getRegistrar().getServiceID());
                    iter.remove(); // remove (srvcID,response) mapping
                }//endif
            }//end loop
            if (!groupsMap.isEmpty() && !listeners.isEmpty()) {
                addNotify((ArrayList) listeners.clone(), groupsMap, DISCARDED);
            }//endif
        }//end sync
    }//end maybeDiscardRegistrars

    /**
     * Add a notification task to the pending queue, and start an instance of the Notifier thread if
     * one isn't already running.
     */
    private void addNotify(ArrayList notifies, Map groupsMap, int eventType) {
        synchronized (pendingNotifies) {
            pendingNotifies.addLast(new NotifyTask(notifies,
                    groupsMap,
                    eventType));
            if (notifierThread == null) {
                Security.doPrivileged(new PrivilegedAction() {
                    public Object run() {
                        notifierThread = new Notifier();
                        notifierThread.start();
                        return null;
                    }//end run
                });//end doPrivileged
            }//endif
        }//end sync
    }//end addNotify

    /**
     * Terminates (interrupts) all currently-running threads.
     */
    private void nukeThreads() {
        Security.doPrivileged(new PrivilegedAction() {
            public Object run() {
                if (announcementTimerThread != null) {
                    announcementTimerThread.interrupt();
                }//endif
                synchronized (requestors) {
                    for (Iterator iter = requestors.iterator();
                         iter.hasNext(); ) {
                        Thread t = (Thread) iter.next();
                        t.interrupt();
                    }
                    if (respondeeThread != null)
                        respondeeThread.interrupt();
                }
                if (announceeThread != null) {
                    announceeThread.interrupt();
                }//endif
                synchronized (pendingDiscoveries) {
                    terminateTaskMgr();
                    Iterator i = tickets.iterator();
                    while (i.hasNext()) {
                        Ticket t = (Ticket) i.next();
                        i.remove();
                        discoveryWakeupMgr.cancel(t);
                    }
                    if (isDefaultWakeupMgr) {
                        // cancelAll should be a no-op in this case,
                        // but just be sure.
                        discoveryWakeupMgr.cancelAll();
                        discoveryWakeupMgr.stop();
                    }
                }//end sync
                return null;
            }//end run
        });//end doPrivileged
    }//end nukeThreads

    /**
     * This method removes all pending and active tasks from the TaskManager for this instance. It
     * also clears the set of pendingDiscoveries, and closes all associated sockets.
     */
    private void terminateTaskMgr() {
        synchronized (taskManager) {
			/* Remove all pending tasks */
            List pendingTasks = taskManager.getPending();
            for (int i = 0; i < pendingTasks.size(); i++) {
                taskManager.remove((TaskManager.Task) pendingTasks.get(i));
            }//end loop
			/* Clear pendingDiscoveries and close all associated sockets */
            synchronized (pendingDiscoveries) {
                for (Iterator iter = pendingDiscoveries.iterator();
                     iter.hasNext(); ) {
                    Object req = iter.next();
                    iter.remove();
                    if (req instanceof Socket) {
                        try {
                            ((Socket) req).close();
                        } catch (IOException e) { /* ignore */ }
                    }//endif
                }//end loop
            }//end sync
			/* Interrupt active TaskThreads, prepare the taskManager for GC. */
            taskManager.terminate();
        }//end sync(taskManager)
        synchronized (pendingNotifies) {
            pendingNotifies.clear();
        }//end sync
    }//end terminateTaskMgr

    /**
     * After a possible change in the member groups of the <code>ServiceRegistrar</code>
     * corresponding to the given <code>UnicastResponse</code> parameter, this method determines
     * whether or not the registrar's member groups have changed in such a way that either a changed
     * event or a discarded event is warranted. <p> Note that even if the contents of the new set of
     * groups initially indicate that the corresponding registrar is a candidate for a discarded or
     * a changed event, further analysis must be performed. This is because there is no guarantee
     * that the new set of member groups have not been "split" across the multicast announcements
     * sent by the lookup service; and so there is no guarantee that the contents of the new group
     * set actually reflect a change that warrants an event. To guarantee that the new group set
     * accurately reflects the registrar's member groups, this method makes a remote call to the
     * registrar to retrieve its actual member groups. <p> There is one situation where it is not
     * necessary to query the registrar for its current member groups. That situation is when the
     * set of groups input to the <code>newGroups</code> parameter is equivalent to NO_GROUPS. If
     * that new group set is equivalent to NO_GROUPS, it is guaranteed that the registrar's member
     * groups have not been split across the multicast announcements.
     *
     * @param response  instance of <code>UnicastResponse</code> corresponding to the registrar
     *                  whose current and previous member groups are to be compared
     * @param newGroups <code>String</code> array containing the new member groups of the registrar
     *                  corresponding to the <code>response</code> parameter (just after a possible
     *                  change)
     */
    private void maybeSendEvent(UnicastResponse response, String[] newGroups) {
        ServiceRegistrar reg = response.getRegistrar();
        boolean getActual = true;
        if (newGroups == null) { // newGroups null means get actual groups now
            newGroups = getActualGroups(reg);
            if (newGroups == null) return; // if null, then it was discarded
            getActual = false;
        }//endif

        if (groupSetsEqual(response.getGroups(), newGroups)) return;

        String[] actualGroups = newGroups;
        if (getActual && (newGroups.length > 0)) {
            actualGroups = getActualGroups(reg);
            if (actualGroups == null) return; // null ==> was already discarded
        }//endif

        synchronized (registrars) {
            // Other events may have occurred to registrars while we were
            // making our remote call.
            UnicastResponse resp =
                    (UnicastResponse) registrars.get(reg.getServiceID());
            if (resp == null) {
                // The registrar was discarded in the meantime. Oh well.
                return;
            }
            notifyOnGroupChange(reg, resp.getGroups(), actualGroups);
        }
    }//end maybeSendEvent

    /**
     * After a possible change in the member groups of the given <code>ServiceRegistrar</code>
     * parameter, this method compares the registrar's original set of member groups to its new set
     * of member groups. <p> If the criteria shown below is satisfied, either a discarded event or a
     * changed event will be sent to any registered listeners. The criteria is based on whether the
     * old and new groups are equal, and whether one or more elements of the new group set also
     * belong to the set of groups to discover (the new groups are "still of interest"). The
     * criteria is as follows: <p> if (old groups and new groups) <p><ul> <li> (not equal but
     * stillInterested) --> send a changed event <li> (!stillInterested)              --> send a
     * discarded event </ul> <p>
     *
     * @param reg       instance of <code>ServiceRegistrar</code> corresponding to the registrar
     *                  whose current and previous member groups are to be compared; and whose
     *                  corresponding service ID is used as the key into the various data structures
     *                  that contain pertinent information about that registrar
     * @param oldGroups <code>String</code> array containing the member groups of the
     *                  <code>reg</code> parameter prior to being changed
     * @param oldGroups <code>String</code> array containing the current member groups of the
     *                  <code>reg</code> parameter (just after a possible change)
     */
    private void notifyOnGroupChange(ServiceRegistrar reg,
                                     String[] oldGroups,
                                     String[] newGroups) {
        boolean equal = groupSetsEqual(oldGroups, newGroups);
        boolean stillInterested = groupsOverlap(newGroups);
        if (!equal && stillInterested) {
            sendChanged(reg, newGroups);
        } else if (!stillInterested) {
            sendDiscarded(reg, newGroups);
        }//endif
    }//end notifyOnGroupChange

    /**
     * Convenience method that sends a discarded event containing only one registrar to all
     * registered listeners. This method must be called from within a block that is synchronized on
     * the registrars map.
     *
     * @param reg       instance of <code>ServiceRegistrar</code> corresponding to the registrar to
     *                  include in the event
     * @param curGroups <code>String</code> array containing the current member groups of the
     *                  registrar referenced by the <code>reg</code> parameter
     */
    private void sendDiscarded(ServiceRegistrar reg, String[] curGroups) {
        ServiceID srvcID = reg.getServiceID();
        if (curGroups == null) { // discard request is from external source
            UnicastResponse resp = (UnicastResponse) registrars.get(srvcID);
            if (resp == null) return;
            curGroups = resp.getGroups();
        }//endif
        if (registrars.remove(srvcID) != null) {
            regInfo.remove(srvcID);
            if (!listeners.isEmpty()) {
                addNotify((ArrayList) listeners.clone(),
                        mapRegToGroups(reg, curGroups), DISCARDED);
            }//endif
        }//endif
    }//end sendDiscarded

    /**
     * Convenience method that sends a changed event containing only one registrar to all registered
     * listeners that are interested in such events. This method must be called from within a block
     * that is synchronized on the registrars map.
     *
     * @param reg       instance of <code>ServiceRegistrar</code> corresponding to the registrar to
     *                  include in the event
     * @param curGroups <code>String</code> array containing the current member groups of the
     *                  registrar referenced by the <code>reg</code> parameter
     */
    private void sendChanged(ServiceRegistrar reg, String[] curGroups) {
		/* replace old groups with new; prevents repeated changed events */
        UnicastResponse resp =
                (UnicastResponse) registrars.get(reg.getServiceID());
        registrars.put(reg.getServiceID(),
                new UnicastResponse(resp.getHost(),
                        resp.getPort(),
                        curGroups,
                        resp.getRegistrar()));
        if (!listeners.isEmpty()) {
            addNotify((ArrayList) listeners.clone(),
                    mapRegToGroups(reg, curGroups), CHANGED);
        }//endif
    }//end sendChanged

    /**
     * Creates and returns a deep copy of the input parameter. This method assumes the input map is
     * a HashMap of the registrar-to-groups mapping; and returns a clone not only of the map, but of
     * each key-value pair contained in the mapping.
     *
     * @param groupsMap mapping from a set of registrars to the member groups of each registrar
     * @return clone of the input map, and of each key-value pair contained in the input map
     */
    private Map deepCopy(HashMap groupsMap) {
		/* clone the input HashMap */
        HashMap newMap = (HashMap) (groupsMap.clone());
		/* clone the values of each mapping in place */
        Set eSet = newMap.entrySet();
        for (Iterator itr = eSet.iterator(); itr.hasNext(); ) {
            Map.Entry pair = (Map.Entry) itr.next();
			/* only need to clone the value of the order pair */
            pair.setValue(((String[]) pair.getValue()).clone());
        }
        return newMap;
    }//end deepCopy

    /**
     * This method retrieves from the given <code>ServiceRegistrar</code>, the current groups in
     * which that registrar is a member. If the registrar is un-reachable, then this method will
     * discard the registrar.
     *
     * @param reg instance of <code>ServiceRegistrar</code> referencing the registrar whose member
     *            groups are to be retrieved and returned
     * @return <code>String</code> array containing the current member groups of the registrar
     * referenced by the <code>reg</code> parameter
     */
    private String[] getActualGroups(final ServiceRegistrar reg) {

        // NOTE, security removed for this for perfomance (we don't use it anyways)
        /* The retrieval of the member groups of the given ServiceRegistrar
		 * is performed inside a doPrivileged block that restores the access
		 * control context that was in place when this utility was created.
		 *
		 * This is done because the call to getGroups() below is executed
		 * on a proxy to the given ServiceRegistrar; which may be downloaded
		 * code supplied by a 3rd party. With respect to downloaded, 3rd party
		 * code, it is not desirable to allow such code to execute with more
		 * priviledges than the client that created this utility. Therefore,
		 * before executing getGroups() on the given proxy, the client's
		 * Subject should be restored, and the proxy code should be
		 * restricted to doing nothing more than the client itself is
		 * allowed to do.
		 */
        try {
            return reg.getGroups();
        } catch (Throwable e) {
			/* A RemoteException, wrapped in a PriviligedActionException,
			 * occurred. This means that the reg is unreachable; discard it.
			 */
            discard(reg);
            return null;
        }//end try

    }//end getActualGroups

    /**
     * Convenience method that creates and returns a mapping of a single
     * <code>ServiceRegistrar</code> instance to a set of groups.
     *
     * @param reg       instance of <code>ServiceRegistrar</code> corresponding to the registrar to
     *                  use as the key to the mapping
     * @param curGroups <code>String</code> array containing the current member groups of the
     *                  registrar referenced by the <code>reg</code> parameter; and which is used as
     *                  the value of the mapping
     * @return <code>Map</code> instance containing a single mapping from a given registrar to its
     * current member groups
     */
    private Map mapRegToGroups(ServiceRegistrar reg, String[] curGroups) {
        HashMap groupsMap = new HashMap(1);
        groupsMap.put(reg, curGroups);
        return groupsMap;
    }//end mapRegToGroups

    /**
     * This method is used by the public methods of this class that are specified to throw a
     * <code>NullPointerException</code> when the given array of group names contains one or more
     * <code>null</code> elements; in which case, this method throws a
     * <code>NullPointerException</code> which should be allowed to propagate outward.
     *
     * @throws java.lang.NullPointerException this exception occurs when one or more of the elements
     *                                        of the <code>groupArray</code> parameter is
     *                                        <code>null</code>.
     */
    private void testArrayForNullElement(String[] groupArray) {
        if (groupArray == null) return;
        for (int i = 0; i < groupArray.length; i++) {
            if (groupArray[i] == null) {
                throw new NullPointerException("null element in group array");
            }//endif
        }//end loop
    }//end testArrayForNullElement

    /**
     * Using the given <code>Configuration</code>, initializes the current instance of this utility,
     * and initiates the discovery process for the given set of groups.
     *
     * @param groups the set of group names to discover
     * @param config an instance of <code>Configuration</code>, used to obtain the objects needed to
     *               configure this utility
     * @throws java.lang.NullPointerException         input array contains at least one
     *                                                <code>null</code> element or <code>null</code>
     *                                                is input for the configuration
     * @throws java.io.IOException                    an exception occurred when initiating
     *                                                discovery processing
     * @throws net.jini.config.ConfigurationException indicates an exception occurred while
     *                                                retrieving an item from the given
     *                                                <code>Configuration</code>
     */
    private void beginDiscovery(String[] groups, Configuration config)
            throws IOException, ConfigurationException {
        testArrayForNullElement(groups);
        checkGroups(groups);
        if (groups != null) {
            this.groups = new HashSet(groups.length * 2);
            for (int i = 0; i < groups.length; i++) {
                this.groups.add(groups[i]);
            }//end loop
        }//endif
        init(config);
        if (nicsToUse == NICS_USE_NONE) return;//disable discovery
        if (!Constants.isMulticastEnabled()) return;
        try {
            Security.doPrivileged(new PrivilegedExceptionAction() {
                public Object run() throws Exception {
                    announceeThread = new AnnouncementListener();
                    announcementTimerThread = new AnnouncementTimerThread();
                    return null;
                }//end run
            });//end doPrivileged
        } catch (PrivilegedActionException e) {
            throw (IOException) e.getException();
        }
        if (this.groups == null || !this.groups.isEmpty()) {
            requestGroups(this.groups);
        }//endif
        announceeThread.start();
        announcementTimerThread.start();
    }//end beginDiscovery

    /* Convenience method that encapsulates the retrieval of the configurable
     * items from the given <code>Configuration</code> object.
     */
    private void init(Configuration config) throws IOException,
            ConfigurationException {
        if (config == null) throw new NullPointerException("config is null");
		/* Lookup service proxy preparer */
        registrarPreparer = (ProxyPreparer) config.getEntry
                (COMPONENT_NAME,
                        "registrarPreparer",
                        ProxyPreparer.class,
                        new BasicProxyPreparer());
		/* constraints */
        MethodConstraints constraints = (MethodConstraints) config.getEntry
                (COMPONENT_NAME,
                        "discoveryConstraints",
                        MethodConstraints.class,
                        null);
        if (constraints == null) {
            constraints =
                    new BasicMethodConstraints(InvocationConstraints.EMPTY);
        }
        multicastRequestConstraints = DiscoveryConstraints.process(
                constraints.getConstraints(
                        DiscoveryConstraints.multicastRequestMethod));
        multicastAnnouncementConstraints = DiscoveryConstraints.process(
                constraints.getConstraints(
                        DiscoveryConstraints.multicastAnnouncementMethod));
        rawUnicastDiscoveryConstraints =
                constraints.getConstraints(
                        DiscoveryConstraints.unicastDiscoveryMethod);

		/* Task manager */
        try {
            taskManager = (TaskManager) config.getEntry(COMPONENT_NAME,
                    "taskManager",
                    TaskManager.class);
        } catch (NoSuchEntryException e) { /* use default */
            taskManager = new TaskManager(15, 30000, 3.0F, "LookupDiscovery Task", 10);
        }

		/* Multicast request-related configuration items */
        multicastRequestMax
                = ((Integer) config.getEntry
                (COMPONENT_NAME,
                        "multicastRequestMax",
                        int.class,
                        new Integer(multicastRequestMax))).intValue();
        multicastRequestInterval
                = ((Long) config.getEntry
                (COMPONENT_NAME,
                        "multicastRequestInterval",
                        long.class,
                        new Long(multicastRequestInterval))).longValue();
        finalMulticastRequestInterval
                = ((Long) config.getEntry
                (COMPONENT_NAME,
                        "finalMulticastRequestInterval",
                        long.class,
                        new Long(finalMulticastRequestInterval))).longValue();
        multicastRequestHost = (String) config.getEntry(COMPONENT_NAME, "multicastRequestHost", String.class, SystemInfo.singleton().network().getHostId());
		/* Configuration items related to the network interface(s) */
        try {
            nics = (NetworkInterface[]) config.getEntry
                    (COMPONENT_NAME,
                            "multicastInterfaces",
                            NetworkInterface[].class);
            if (!Constants.isMulticastEnabled()) {
                nicsToUse = NICS_USE_NONE;
                logger.config("LookupDiscovery - MULTICAST DISABLED");
            } else if (nics == null) {
                nicsToUse = NICS_USE_SYS;
                logger.config("LookupDiscovery - using system default network "
                        + "interface for multicast");
            } else {//(nics != null)
                if (nics.length == 0) {
                    nicsToUse = NICS_USE_NONE;
                    logger.config("LookupDiscovery - MULTICAST DISABLED");
                } else {//(nics.length > 0), use the given specific list
                    nicsToUse = NICS_USE_LIST;
                    if (logger.isLoggable(Level.CONFIG)) {
                        logger.log(Level.CONFIG,
                                "LookupDiscovery - multicast network "
                                        + "interface(s): {0}", Arrays.asList(nics));
                    }//endif
                }//endif
            }//endif
        } catch (NoSuchEntryException e) {// no config item, use default - all or none if disabled
            if (!Constants.isMulticastEnabled()) {
                nicsToUse = NICS_USE_NONE;
                logger.config("LookupDiscovery - MULTICAST DISABLED");
            } else {
                String multicastInterface = System.getProperty("net.jini.discovery.LookupDiscovery.multicastInterface");
                if (multicastInterface != null) {
                    // in case this is a system property
                    multicastInterface = System.getProperty(multicastInterface, multicastInterface);
                    if (multicastInterface.equalsIgnoreCase("system")) {
                        nics = null;
                        nicsToUse = NICS_USE_SYS;
                    } else {
                        nicsToUse = NICS_USE_LIST;
                        nics = new NetworkInterface[]{NetworkInterface.getByName(multicastInterface)};
                    }
                } else {
                    nics = BootUtil.getNetworkInterfaces();
                    nicsToUse = NICS_USE_ALL;
                    if (logger.isLoggable(Level.CONFIG)) {
                        logger.log(Level.CONFIG, "LookupDiscovery - multicast network interface(s): {0}", Arrays.toString(nics));
                    }//endif
                }
            }
        } //catch no config

        nicRetryInterval
                = ((Integer) config.getEntry
                (COMPONENT_NAME,
                        "multicastInterfaceRetryInterval",
                        int.class,
                        new Integer(nicRetryInterval))).intValue();
		/* Multicast announcement-related configuration items */
        multicastAnnouncementInterval
                = ((Long) config.getEntry
                (COMPONENT_NAME,
                        "multicastAnnouncementInterval",
                        long.class,
                        new Long(multicastAnnouncementInterval))).longValue();

        unicastDelayRange = Config.getLongEntry(config,
                COMPONENT_NAME,
                "unicastDelayRange",
                0,
                0,
                Long.MAX_VALUE);
        tickets = new ArrayList();
        if (unicastDelayRange > 0) {
			/* Wakeup manager */
            try {
                discoveryWakeupMgr =
                        (WakeupManager) config.getEntry(COMPONENT_NAME,
                                "wakeupManager",
                                WakeupManager.class);
            } catch (NoSuchEntryException e) { /* use default */
                discoveryWakeupMgr = new WakeupManager(
                        new WakeupManager.ThreadDesc(null, true));
                isDefaultWakeupMgr = true;
            }
        }

        initialMulticastRequestDelayRange = Config.getLongEntry(config,
                COMPONENT_NAME,
                "initialMulticastRequestDelayRange",
                0,
                0,
                Long.MAX_VALUE);

        if (logger.isLoggable(Level.CONFIG)) {
            logger.config(
                    "\n\nConfiguration settings for [com.gs.multicast] :" +
                            "\n .enabled= " + Constants.isMulticastEnabled() +
                            "\n .ttl= " + Constants.getTtl() +
                            "\n .discoveryPort= " + Constants.getDiscoveryPort() +
                            "\n .request= " + Constants.getRequestAddress() +
                            "\n .announcement= " + Constants.getAnnouncementAddress() +
                            "\n\nConfiguration settings for [" + COMPONENT_NAME + "] :" +
                            "\n .multicastRequestMax= " + multicastRequestMax +
                            "\n .multicastRequestInterval= " + multicastRequestInterval +
                            "\n .finalMulticastRequestInterval= " + finalMulticastRequestInterval +
                            "\n .multicastRequestHost= " + multicastRequestHost +
                            "\n .nics= " + Arrays.toString(nics) +
                            "\n .nicsToUse=" + nicsToUse +
                            "\n .nicRetryInterval=" + nicRetryInterval +
                            "\n .multicastAnnouncementInterval=" + multicastAnnouncementInterval +
                            "\n .unicastDelayRange=" + unicastDelayRange +
                            "\n .initialMulticastRequestDelayRange=" + initialMulticastRequestDelayRange
            );
        }

    }//end init

    /**
     * Decodes received multicast announcement packet. Constraint checking is delayed.
     */
    private MulticastAnnouncement decodeMulticastAnnouncement(
            final DatagramPacket pkt)
            throws IOException {
        // REMIND: cache recently received announcements to skip re-decoding?
        int pv;
        try {
            pv = ByteBuffer.wrap(
                    pkt.getData(), pkt.getOffset(), pkt.getLength()).getInt();
        } catch (BufferUnderflowException e) {
            throw new DiscoveryProtocolException(null, e);
        }
        multicastAnnouncementConstraints.checkProtocolVersion(pv);
        final Discovery disco = getDiscovery(pv);

        return disco.decodeMulticastAnnouncement(
                pkt,
                multicastAnnouncementConstraints.
                        getUnfulfilledConstraints(),
                true);
    }

    /**
     * Encodes outgoing multicast requests based on protocol in use, applying configured security
     * constraints (if any).
     */
    private DatagramPacket[] encodeMulticastRequest(final MulticastRequest req)
            throws IOException {
        // REMIND: cache latest request to skip re-encoding
        final Discovery disco = getDiscovery(
                multicastRequestConstraints.chooseProtocolVersion());
        final List packets = new ArrayList();
        EncodeIterator ei = disco.encodeMulticastRequest(
                req,
                multicastRequestConstraints.getMulticastMaxPacketSize(
                        DEFAULT_MAX_PACKET_SIZE),
                multicastRequestConstraints.getUnfulfilledConstraints()
        );
        while (ei.hasNext()) {
            try {
                packets.addAll(Arrays.asList(ei.next()));
            } catch (Exception e) {
                logger.log(
                        (e instanceof UnsupportedConstraintException) ?
                                Level.FINE : Level.INFO,
                        "exception encoding multicast request", e);
            }
        }
        if (packets.isEmpty()) {
            throw new DiscoveryProtocolException("no encoded requests");
        }
        return (DatagramPacket[]) packets.toArray(
                new DatagramPacket[packets.size()]);
    }

    /**
     * Performs unicast discovery over given socket based on protocol in use, applying configured
     * security constraints (if any).
     */
    private UnicastResponse
    doUnicastDiscovery(
            final Socket socket,
            final DiscoveryConstraints unicastDiscoveryConstraints,
            final Discovery disco)
            throws IOException, ClassNotFoundException {
        return disco.doUnicastDiscovery(
                socket,
                unicastDiscoveryConstraints.
                        getUnfulfilledConstraints(),
                null,
                null,
                null);
    }

    private UnicastResponse doUnicastDiscovery(
            final Socket socket,
            final DiscoveryConstraints unicastDiscoveryConstraints)
            throws IOException, ClassNotFoundException {
        Discovery disco =
                getDiscovery(unicastDiscoveryConstraints.chooseProtocolVersion());
        return doUnicastDiscovery(socket, unicastDiscoveryConstraints, disco);
    }

    /**
     * Returns Discovery instance for the given version, or throws DiscoveryProtocolException if the
     * version is unsupported.
     */
    private Discovery getDiscovery(int version)
            throws DiscoveryProtocolException {
        switch (version) {
            case Discovery.PROTOCOL_VERSION_1:
                return Discovery.getProtocol1();
            case Discovery.PROTOCOL_VERSION_2:
                if (protocol2 == null) {
                    protocol2 = Discovery.getProtocol2(null);
                }
                return protocol2;
            default:
                throw new DiscoveryProtocolException(
                        "unsupported protocol version: " + version);
        }
    }

    /**
     * Holder class for the time and sequence number of the last received announcement. The regInfo
     * map contains instances of this class as values.
     */
    private static class AnnouncementInfo {
        private long tStamp;
        private long seqNum;

        private AnnouncementInfo(long tStamp, long seqNum) {
            this.tStamp = tStamp;
            this.seqNum = seqNum;
        }
    }
}//end class LookupDiscovery
