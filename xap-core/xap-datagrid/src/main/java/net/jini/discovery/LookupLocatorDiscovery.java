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

import com.gigaspaces.exception.lrmi.ProxyClosedException;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.time.SystemTime;
import com.sun.jini.config.Config;
import com.sun.jini.constants.ThrowableConstants;
import com.sun.jini.discovery.Discovery;
import com.sun.jini.discovery.DiscoveryConstraints;
import com.sun.jini.discovery.UnicastResponse;
import com.sun.jini.discovery.internal.MultiIPDiscovery;
import com.sun.jini.logging.Levels;
import com.sun.jini.thread.RetryTask;
import com.sun.jini.thread.TaskManager;
import com.sun.jini.thread.WakeupManager;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.EmptyConfiguration;
import net.jini.config.NoSuchEntryException;
import net.jini.core.constraint.ConnectionAbsoluteTime;
import net.jini.core.constraint.InvocationConstraints;
import net.jini.core.constraint.MethodConstraints;
import net.jini.core.constraint.RemoteMethodControl;
import net.jini.core.discovery.LookupLocator;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.security.BasicProxyPreparer;
import net.jini.security.ProxyPreparer;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class encapsulates the functionality required of an entity that wishes to employ the unicast
 * discovery protocol to discover a lookup service. This utility provides an implementation that
 * makes the process of finding specific lookup services much simpler for both services and clients.
 * <p/> Because this class participates in only the unicast discovery protocol, and because the
 * unicast discovery protocol imposes no restriction on the physical location of the entity relative
 * to a lookup service, this utility can be used to discover lookup services running on hosts that
 * are located far from, or near to, the host on which the entity is running. This lack of a
 * restriction on location brings with it a requirement that the discovering entity supply this
 * class with specific information about the desired lookup services; namely, the location of the
 * device(s) hosting each lookup service. This information is supplied through an instance of the
 * {@link net.jini.core.discovery.LookupLocator LookupLocator} class.
 *
 * @author Sun Microsystems, Inc.
 * @com.sun.jini.impl <!-- Implementation Specifics --> <p/> The following implementation-specific
 * items are discussed below: <ul><li> <a href="#lldConfigEntries">Configuring
 * LookupLocatorDiscovery</a> <li> <a href="#lldLogging">Logging</a> </ul> <p/> <a
 * name="lldConfigEntries"> <p/> <b><font size="+1">Configuring LookupLocatorDiscovery</font></b>
 * <p/> </a> <p/> This implementation of <code>LookupLocatorDiscovery</code> supports the following
 * configuration entries; where each configuration entry name is associated with the component name
 * <code>net.jini.discovery.LookupLocatorDiscovery</code>. Note that the configuration entries
 * specified here are specific to this implementation of <codeLookupLocatorDiscovery</code>. Unless
 * otherwise stated, each entry is retrieved from the configuration only once per instance of this
 * utility, where each such retrieval is performed in the constructor. <p/> <a
 * name="initialUnicastDelayRange"> <table summary="Describes the initialUnicastDelayRange
 * configuration entry" border="0" cellpadding="2"> <tr valign="top"> <th scope="col"
 * summary="layout"> <font size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font
 * size="+1"> <code>initialUnicastDelayRange</code></font> <p/> <tr valign="top"> <td> &nbsp <th
 * scope="row" align="right"> Type: <td> <code>long</code> <p/> <tr valign="top"> <td> &nbsp <th
 * scope="row" align="right"> Default: <td> <code>0 milliseconds</code> <p/> <tr valign="top"> <td>
 * &nbsp <th scope="row" align="right"> Description: <td> With respect to when this utility is
 * started, this entry controls how long to wait before attempting unicast discovery. If the value
 * is positive, initial unicast discovery requests will be delayed by a random value between
 * <code>0</code> and <code>initialUnicastDelayRange</code> milliseconds. Once the wait period is
 * up, the <code>LookupLocator</code>s specified at construction time are used for initiating
 * unicast discovery requests, unless the managed <code>LookupLocator</code>s have been changed in
 * the interim; in which case, no delayed unicast discovery requests are performed. Note that this
 * entry only has effect when this utility is initialized. It does not delay discovery requests that
 * are initiated if the managed <code>LookupLocator</code>s are subsequently changed. </table> <a
 * name="registrarPreparer"> <table summary="Describes the registrarPreparer configuration entry"
 * border="0" cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1">
 * <code>registrarPreparer</code></font> <p/> <tr valign="top"> <td> &nbsp <th scope="row"
 * align="right"> Type: <td> {@link net.jini.security.ProxyPreparer} <p/> <tr valign="top"> <td>
 * &nbsp <th scope="row" align="right"> Default: <td> <code>new {@link
 * net.jini.security.BasicProxyPreparer}() </code> <p/> <tr valign="top"> <td> &nbsp <th scope="row"
 * align="right"> Description: <td> Preparer for the proxies to the lookup services that are
 * discovered and used by this utility. <p/> This preparer should perform all operations required to
 * use a newly received proxy to a lookup service, which may including verifying trust in the proxy,
 * granting permissions, and setting constraints. <p/> Currently, none of the methods on the {@link
 * net.jini.core.lookup.ServiceRegistrar ServiceRegistrar} returned by this preparer are invoked by
 * this implementation of <code>LookupLocatorDiscovery</code>. </table> <p/> <a name="taskManager">
 * <table summary="Describes the taskManager configuration entry" border="0" cellpadding="2"> <tr
 * valign="top"> <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th scope="col"
 * align="left" colspan="2"> <font size="+1"> <code>taskManager</code></font> <p/> <tr valign="top">
 * <td> &nbsp <th scope="row" align="right"> Type: <td> {@link com.sun.jini.thread.TaskManager} <p/>
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Default: <td> <code>new {@link
 * com.sun.jini.thread.TaskManager#TaskManager() TaskManager}(15, (15*1000), 1.0f)</code> <p/> <tr
 * valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> The object that pools
 * and manages the various threads executed by this utility. The default manager creates a maximum
 * of 15 threads, waits 15 seconds before removing idle threads, and uses a load factor of 1.0 when
 * determining whether to create a new thread. This object should not be shared with other
 * components in the application that employs this utility. </table> <p/> <a name="wakeupManager">
 * <table summary="Describes the wakeupManager configuration entry" border="0" cellpadding="2"> <tr
 * valign="top"> <th scope="col" summary="layout"> <font size="+1">&#X2022;</font> <th scope="col"
 * align="left" colspan="2"> <font size="+1"> <code>wakeupManager</code></font> <p/> <tr
 * valign="top"> <td> &nbsp <th scope="row" align="right"> Type: <td> {@link
 * com.sun.jini.thread.WakeupManager} <p/> <tr valign="top"> <td> &nbsp <th scope="row"
 * align="right"> Default: <td> <code>new {@link com.sun.jini.thread.WakeupManager#WakeupManager(
 *com.sun.jini.thread.WakeupManager.ThreadDesc) WakeupManager}(new {@link
 * com.sun.jini.thread.WakeupManager.ThreadDesc}(null,true))</code> <p/> <tr valign="top"> <td>
 * &nbsp <th scope="row" align="right"> Description: <td> Object that pools and manages the various
 * tasks that are initially executed by the object corresponding to the <a
 * href="#taskManager"><code>taskManager</code></a> entry of this component, but which fail during
 * that initial execution. This object schedules the re-execution of such a failed task - in the <a
 * href="#taskManager"><code>taskManager</code></a> object - at various times in the future,
 * (employing a "backoff strategy"). The re-execution of the failed task will continue to be
 * scheduled by this object until the task finally succeeds. This object should not be shared with
 * other components in the application that employs this utility. </table> <p/> <a
 * name="lldLogging"> <p/> <b><font size="+1">Logging</font></b> <p/> </a> <p/> This implementation
 * of <code>LookupLocatorDiscovery</code> uses the {@link Logger} named
 * <code>net.jini.discovery.LookupLocatorDiscovery</code> to log information at the following
 * logging levels: <p> <p/> <table border="1" cellpadding="5" summary="Describes the information
 * logged by LookupLocatorDiscovery, and the levels at which that information is logged"> <p/>
 * <caption halign="center" valign="top"> <b><code>net.jini.discovery.LookupLocatorDiscovery</code></b>
 * </caption> <p/> <tr> <th scope="col"> Level</th> <th scope="col"> Description</th> </tr> <p/>
 * <tr> <td>{@link java.util.logging.Level#INFO INFO}</td> <td> when any exception occurs in a task
 * or thread, while attempting unicast discovery of a given locator </td> </tr> <tr> <td>{@link
 * java.util.logging.Level#INFO INFO}</td> <td>when any exception occurs while attempting to prepare
 * a proxy</td> </tr> <tr> <td>{@link com.sun.jini.logging.Levels#HANDLED HANDLED}</td> <td> when an
 * exception is handled during unicast discovery. </td> </tr> <tr> <td>{@link
 * java.util.logging.Level#FINEST FINEST}</td> <td>whenever any thread or task is started</td> </tr>
 * <p/> <tr> <td>{@link java.util.logging.Level#FINEST FINEST}</td> <td> whenever any thread (except
 * the <code>Notifier</code> thread) or task completes successfully </td> </tr> <p/> <tr> <td>{@link
 * java.util.logging.Level#FINEST FINEST}</td> <td>whenever a discovered or discarded event is
 * sent</td> </tr> <p/> <tr> <td>{@link java.util.logging.Level#FINEST FINEST}</td> <td>whenever a
 * proxy is prepared</td> </tr> <p/> <tr> <td>{@link java.util.logging.Level#FINEST FINEST}</td>
 * <td> when an <code>IOException</code> occurs upon attempting to close the socket after a unicast
 * discovery attempt has either completed successfully or failed </td> </tr> </table> <p/> <p/> This
 * implementation of <code>LookupLocatorDiscovery</code> determines the constraints (if any) to
 * apply to unicast discovery for a given {@link net.jini.core.discovery.LookupLocator
 * LookupLocator} instance by calling the {@link net.jini.core.constraint.RemoteMethodControl#getConstraints
 * getConstraints} method of that instance, if it implements the {@link
 * net.jini.core.constraint.RemoteMethodControl RemoteMethodControl} interface. If the {@link
 * net.jini.core.discovery.LookupLocator LookupLocator} instance does not implement {@link
 * net.jini.core.constraint.RemoteMethodControl RemoteMethodControl}, then no constraints are
 * applied to unicast discovery for that instance. <p/> For more information on constraining unicast
 * discovery, refer to the documentation for the {@link net.jini.discovery.ConstrainableLookupLocator
 * ConstrainableLookupLocator} class.
 * @see net.jini.core.discovery.LookupLocator
 *
 * --------------------------------------- Fix 20.7.2007 GigaSpaces issue GS-2216:
 * ---------------------------------------
 *
 * The LookupLocatorDiscovery fix provides the ability to periodically test the reachability of
 * discovered lookup service instances,as well as providing the option to declare retry intervals
 * used to re-establish communication with a failed lookup service.
 *
 * This approach included in this project provides a more responsive unicast discovery approach by
 * declaring the interval(s) the utility will sleep betwen unicast discovery attempts (or retries),
 * and also provides a means to verify lookup service availability by periodically obtaining
 * infomration from the backend implementation.
 *
 * The default behavior of the LookupLocatorDiscovery utility has been modified to create an
 * internal DiscoveryListener that will create a LookAliveTask per discovered LookupService
 * instance. Each LookupAliveTask will periodically invoke the ServiceRegistrar.getLocator() method
 * (resulting in a round trip to the backend implementation). If a non-recoverable exception occurs
 * (ConnectException or otherwise) the LookAliveTask will discard the ServiceRegistrar in question.
 * The periodicity of the LookupAliveTask can be controlled by the com.gigaspaces.unicast.interval
 * system property. The default interval is 1 minute.
 *
 * Example :
 *
 * -Dcom.gigaspaces.unicast.ping=5000
 *
 * Will cause the LookupLocatorDiscovery utility to wait 5 seconds between invocations of the
 * getLocator() method.
 *
 * The LookupLocatorDiscovery unicast retry (sleep) intervals have been untouched. If the user
 * wishes to tune the unicast retry intervals, the com.gigaspaces.unicast.interval system property
 * is used to control the behavior of this LookupLocatorDiscovery utility. A comma separated list of
 * values defining the intervals to wait between subsequent retries. Values are declared in
 * milliseconds.
 *
 * Example :
 *
 * -Dcom.gigaspaces.unicast.interval=5000
 *
 * Will cause the LookupLocatorDiscovery utility to wait 5 seconds between retries
 *
 * -Dcom.gigaspaces.unicast.interval=5000,10000
 *
 * Will cause the LookupLocatorDiscovery utility to first wait 5 seconds, then 10 seconds between
 * retries. This declaration provides a graduating approach (similar in approach to the default
 * settings), increasing the amount of time to wait before the next discovery attempt should be made
 * - upon each invocation, eventually reaching a maximum time interval over which discovery is
 * re-tried. In this way, the network is not flooded with unicast discovery requests referencing a
 * lookup service that may not be available for quite some time (if ever).
 */
@com.gigaspaces.api.InternalApi
public class LookupLocatorDiscovery implements DiscoveryManagement,
        DiscoveryLocatorManagement {
    /* Name of this component; used in config entry retrieval and the logger.*/
    private static final String COMPONENT_NAME
            = "net.jini.discovery.LookupLocatorDiscovery";
    /* Logger used by this utility. */
    private static final Logger logger = Logger.getLogger(COMPONENT_NAME);

    private static final Logger loggerStats = Logger.getLogger(COMPONENT_NAME + ".stats");

    /**
     * Maximum number of concurrent tasks that can be run in any task manager created by this
     * class.
     */
    private static final int MAX_N_TASKS = 15;
    /**
     * Default timeout to set on sockets used for unicast discovery.
     */
    private static final int DEFAULT_SOCKET_TIMEOUT = 1 * 60 * 1000;
    /**
     * LookupLocator.getRegistrar method, used for looking up client constraints of contained lookup
     * locators.
     */
    private static final Method getRegistrarMethod;

    static {
        try {
            getRegistrarMethod = LookupLocator.class.getDeclaredMethod(
                    "getRegistrar", new Class[0]);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Task manager for the discovery tasks. On the first attempt to discover each locator, the
     * tasks used to perform those discoveries are managed by this <code>TaskManager</code> so that
     * the number of concurrent threads can be bounded. If one or more of those attempts fails, a
     * <code>WakeupManager</code> is used (through the use of a <code>RetryTask</code>) to schedule
     * - at a later time (employing a "backoff strategy") - the re-execution of each failed task in
     * this <code>TaskManager</code>.
     */
    private TaskManager discoveryTaskMgr;
    /**
     * Wakeup manager for the discovery tasks. For any locator, after an initial failure to discover
     * the locator, the task used to perform all future discovery attempts is managed by this
     * <code>WakeupManager</code>; which schedules the re-execution of the failed task - in the task
     * manager - at various times in the future until the locator is successfully discovered. This
     * wakeup manager is supplied to the <code>RetryTask</code>) that performs the actual discovery
     * attempt(s) so that when termination of this lookup locator discovery utility is requested,
     * all tasks scheduled for retry by this wakeup manager can be cancelled.
     */
    private WakeupManager discoveryWakeupMgr;
    /**
     * Stores LookupLocators that have not been discovered yet.
     */
    private final HashSet undiscoveredLocators = new HashSet(11);
    /**
     * Stores LookupLocators that have been discovered
     */
    private final ArrayList discoveredLocators = new ArrayList(11);
    /**
     * Thread that handles pending notifications.
     */
    private Notifier notifierThread;
    /**
     * Notifications to be sent to listeners.
     */
    private final LinkedList pendingNotifies = new LinkedList();
    /**
     * Stores DiscoveryListeners *
     */
    private final ArrayList listeners = new ArrayList(1);
    /**
     * Flag indicating whether or not this class is still functional.
     */
    private boolean terminated = false;
    /* Preparer for the proxies to the lookup services that are discovered
     * and used by this utility.
     */
    private ProxyPreparer registrarPreparer;
    /* Utility for participating in version 2 of the unicast discovery
     * protocol.
     */
    private final Discovery protocol2 = Discovery.getProtocol2(null);
    /* 
     * Controls how long to wait before attempting unicast discovery, on
     * startup.
     */
    private long initialUnicastDelayRange = 0;
    /*
     * Flag which indicates if discoverLocators was called during
     * initialUnicastDelayRange delay.
     */
    private boolean discoverLocatorsCalled = false;

    /**
     * Collection of LookupAliveTask instances
     */
    private final List lookupAliveTasks = new ArrayList();

    /**
     * TODO
     */
    private final int MAX_LOOKUP_ALIVE_TASKS_PER_LOCATOR = 5;

    /**
     * TODO
     */
    private final Map<String, Set<LookupAliveTask>> lookupAliveTasksMap = new HashMap<String, Set<LookupAliveTask>>();

    /**
     * Wrapper class in which each instance corresponds to a lookup service to discover via unicast
     * discovery.
     */
    private class LocatorReg {
        public ServiceRegistrar proxy = null;
        public final LookupLocator l;
        public String[] memberGroups = null;

        /* No need to sync on cnt since it's modified only in constructor */
        private int cnt = 0;
        private static final long MIN_RETRY = 15000;
        private long[] sleepTime = {1000, 5 * 1000, 10 * 1000, 15 * 1000,
                20 * 1000, 30 * 1000, 60 * 1000};

        private int tryIndx = 0;
        private long nextTryTime;
        private final int id;
        private long time = 0;

        public LocatorReg(LookupLocator l) {
            id = cnt++;
            this.l = l;
            nextTryTime = SystemTime.timeMillis();

            String s = System.getProperty("com.gigaspaces.unicast.interval");
            if (s != null) {
                StringBuffer buff = new StringBuffer();
                StringTokenizer st = new StringTokenizer(s, " \t\n\r\f,");
                ArrayList list = new ArrayList();
                for (int i = 0; st.hasMoreTokens(); i++) {
                    String value = st.nextToken();
                    try {
                        if (list.size() > 0)
                            buff.append(", ");
                        list.add(Long.valueOf(value));
                        buff.append(value);
                    } catch (NumberFormatException e) {
                        logger.log(Level.INFO, "Value [" + value + "] " +
                                "is illegal for property " +
                                "com.gigaspaces.unicast.interval, " +
                                "ignoring");
                    }
                }
                sleepTime = new long[list.size()];
                for (int i = 0; i < sleepTime.length; i++) {
                    sleepTime[i] = ((Long) list.get(i)).longValue();
                }
                if (logger.isLoggable(Level.INFO))
                    logger.log(Level.INFO,
                            "Set unicast interval to [" + buff.toString() +
                                    "]");
            }
        }//end constructor

        /**
         * Accessor that returns the absolute time at which the next discovery attempt should be
         * made after the previous attempt has failed to find the desired lookup service.
         */
        public synchronized long getNextTryTime() {
            return nextTryTime;
        }//end getNextTryTime

        /**
         * Computes the time (in milliseconds) used to determine when it is allowable -- after a
         * previous failure -- to again attempt unicast discovery of the lookup service referenced
         * in this class. <p/> Since this method is called multiple times for a particular lookup
         * service only when there is difficulty discovering that lookup service, the value computed
         * by this method increases in a graduated manner - increasing the amount of time to wait
         * before the next discovery attempt should be made - upon each invocation, eventually
         * reaching a maximum time interval over which discovery is re-tried. In this way, the
         * network is not flooded with unicast discovery requests referencing a lookup service that
         * may not be available for quite some time (if ever).
         */
        public void setNextTryTime() {
            nextTryTime = SystemTime.timeMillis() + sleepTime[tryIndx];
            if (tryIndx < sleepTime.length - 1) tryIndx++;
        }

        /**
         * This method gets called only from the public discard() method. The purpose of this method
         * is to prevent the discard method from being called too frequently. This method measures
         * the time from when the listener's discovered() method gets called to when its discard()
         * method gets called. If the time is less than MIN_RETRY, the next discovery attempt is
         * delayed.
         */
        public void fixupNextTryTime() {
            long curTime = SystemTime.timeMillis();
            if ((curTime - time) > MIN_RETRY) {
                tryIndx = 0;
                nextTryTime = curTime;
            } else {
                setNextTryTime();
            }//endif
        }//end fixupNextTryTime

        /**
         * Create the InvocationConstraints with connection absolute time based on an increasing
         * retry. When all retries are exhausted, return the InvocationConstraints.EMPTY.
         * InvocationConstraints.EMPTY is interpreted as an infinite timeout. The connection will
         * then block until established or an error occurs.
         *
         * @return InvocationConstraints with the right timeout.
         */
        private InvocationConstraints createInvocationConstraints() {
            if (MIN_RETRY >= sleepTime[tryIndx]) {
                return new InvocationConstraints(
                        new ConnectionAbsoluteTime(System.currentTimeMillis() + sleepTime[tryIndx]), null /*pref*/);
            } else {
                return InvocationConstraints.EMPTY;
            }
        }

        /**
         * Initiates unicast discovery of the lookup service referenced in this class.
         */
        public boolean tryGetProxy() {
            if (proxy != null) {
                throw new IllegalArgumentException
                        ("LookupLocator has been discovered already");
            }

            InvocationConstraints ic = createInvocationConstraints();

            if (l instanceof RemoteMethodControl) {
                MethodConstraints mc =
                        ((RemoteMethodControl) l).getConstraints();
                if (mc != null) {
                    ic = mc.getConstraints(getRegistrarMethod);
                }
            }
            try {
                long startTime = 0;
                if (loggerStats.isLoggable(Level.FINEST)) {
                    startTime = SystemTime.timeMillis();
                }
                doUnicastDiscovery(l, ic);
                if (loggerStats.isLoggable(Level.FINEST)) {
                    loggerStats.finest("Unicast Lookup took [" + (SystemTime.timeMillis() - startTime) + "ms]");
                }
                time = SystemTime.timeMillis();//mark the time of discovery
                logger.log(Level.INFO, "Connected to LUS using locator {0}:{1,number,#}", new Object[]{
                        l.getHost(),
                        l.getPort()});
                return true;
            } catch (Throwable throwable) {

                final int currTryIndx = tryIndx;
                setNextTryTime();//discovery failed; try again even later

                logger.log(Level.WARNING,
                        "{0} - using unicast locator {1}:{2,number,#} - delay next lookup by {3} ms",
                        new Object[]{
                                throwable,
                                l.getHost(),
                                l.getPort(),
                                sleepTime[currTryIndx]
                        });

                return false;
            }
        }//end tryGetProxy

        /**
         * This method employs the unicast discovery protocol to discover the registrar having
         * <code>LookupLocator</code> equal to the value contained in the <code>locator</code>
         * parameter of this class.
         */
        private void doUnicastDiscovery(LookupLocator locator,
                                        InvocationConstraints ic)
                throws IOException, ClassNotFoundException {
            UnicastResponse resp = new MultiIPDiscovery() {
                protected UnicastResponse performDiscovery(
                        Discovery disco,
                        DiscoveryConstraints dc,
                        Socket s)
                        throws IOException, ClassNotFoundException {
                    return disco.doUnicastDiscovery(
                            s,
                            dc.getUnfulfilledConstraints(),
                            null,
                            null,
                            null);

                }

                protected void socketCloseException(IOException e) {
                    logger.log(Level.FINEST,
                            "IOException on socket close upon "
                                    + "completion of unicast discovery",
                            e);
                }

                protected void singleResponseException(Exception e,
                                                       InetAddress addr,
                                                       int port) {
                    logger.log(
                            Levels.HANDLED,
                            "Exception occurred during unicast discovery " +
                                    addr + ":" + port, e);
                }

            }.getResponse(locator.getHost(), locator.getPort(), ic);

            /* Proxy preparation */
            proxy = (ServiceRegistrar) registrarPreparer.prepareProxy
                    (resp.getRegistrar());
            logger.log(Level.FINEST, "LookupLocatorDiscovery - prepared "
                    + "lookup service proxy: {0}", proxy);
            memberGroups = resp.getGroups();
        }//end doUnicastDiscovery

        /**
         * Returns true if the locators are equal.
         */
        public boolean equals(Object obj) {
            if (!(obj instanceof LocatorReg)) return false;
            return l.equals(((LocatorReg) obj).l);
        }//end equals

        /**
         * Returns the hash code of the locator referenced in this class.
         */
        public int hashCode() {
            return l.hashCode();
        }//end hashCode
    }//end class LocatorReg

    /**
     * Data structure containing task data processed by the Notifier Thread
     */
    private static class NotifyTask {
        /**
         * The listeners to notify
         */
        public final ArrayList listeners;
        /**
         * Map of discovered registrars to groups in which each is a member
         */
        public final Map groupsMap;
        /**
         * True if discarded, else discovered
         */
        public final boolean discard;

        public NotifyTask(ArrayList listeners,
                          Map groupsMap,
                          boolean discard) {
            this.listeners = listeners;
            this.groupsMap = groupsMap;
            this.discard = discard;
        }
    }//end class NotifyTask

    /**
     * Thread that retrieves data structures of type NotifyTask from a queue and, based on the
     * contents of the data structure, sends the appropriate event (discovered/discarded) to each
     * registered listener. <p/> Only 1 instance of this thread is run.
     */
    private class Notifier extends GSThread {
        /**
         * Construct a daemon thread
         */
        public Notifier() {
            super("event notifier");
            setDaemon(true);
        }//end constructor

        public void run() {
            logger.finest("LookupLocatorDiscovery - Notifier thread started");
            while (true) {
                NotifyTask task;
                synchronized (pendingNotifies) {
                    if (pendingNotifies.isEmpty()) {
                        notifierThread = null;
                        return;
                    }//endif
                    task = (NotifyTask) pendingNotifies.removeFirst();
                }//end sync(pendingNotifies)
                boolean firstListener = true;
                for (Iterator iter = task.listeners.iterator(); iter.hasNext(); ) {
                    DiscoveryListener l = (DiscoveryListener) iter.next();
                    DiscoveryEvent e =
                            new DiscoveryEvent(LookupLocatorDiscovery.this,
                                    deepCopy((HashMap) task.groupsMap));
                    /* Log the event info about the lookup(s) */
                    if (firstListener && (logger.isLoggable(Level.FINEST))) {
                        String eType = (task.discard ?
                                "discarded" : "discovered");
                        ServiceRegistrar[] regs = e.getRegistrars();
                        logger.finest(eType + " event  -- " + regs.length
                                + " lookup(s)");
                        Map groupsMap = e.getGroups();
                        for (int i = 0; i < regs.length; i++) {
                            LookupLocator loc = null;
                            try {
                                loc = regs[i].getLocator();
                            } catch (Throwable ex) { /* ignore */ }
                            String[] groups = (String[]) groupsMap.get(regs[i]);
                            logger.finest(
                                    "    " + eType + " locator  = " + loc);
                            if (groups.length == 0) {
                                logger.finest("    " + eType
                                        + " group    = NO_GROUPS");
                            } else {
                                for (int j = 0; j < groups.length; j++) {
                                    logger.finest(
                                            "    " + eType + " group[" + j + "] "
                                                    + "= " + groups[j]);
                                }//end loop
                            }//endif(groups.length)
                        }//end loop
                    }//endif(firstListener && isLoggable(Level.FINEST)
                    if (task.discard) {
                        l.discarded(e);
                    } else {
                        l.discovered(e);
                    }//endif
                }//end loop
            }//end loop
        }//end run
    }//end class Notifier

    /**
     * Task which retrieves elements from the set of undiscoveredLocators and attempts, through the
     * unicast discovery protocol, to discover the lookup service having the LookupLocator
     * referenced by the element. If a particular instance of this class fails to find the lookup
     * service that it references, this task will be rescheduled to be executed again at a later
     * time, using a "backoff strategy" as defined by the method <code>com.sun.jini.thread.RetryTask.retryTime</code>.
     *
     * @see com.sun.jini.thread.RetryTask
     * @see com.sun.jini.thread.WakeupManager
     */
    private class DiscoveryTask extends RetryTask {
        private final LocatorReg reg;

        public DiscoveryTask(LocatorReg reg,
                             TaskManager taskMgr,
                             WakeupManager wakeupMgr) {
            super(taskMgr, wakeupMgr);
            this.reg = reg;
        }//end constructor

        /**
         * Executes the current instance of this task once, attempting to discover - through unicast
         * discovery - the lookup service referenced in that instance. Upon successfully discovering
         * the indicated lookup service, this method returns <code>true</code> and the current
         * instance of this task is not executed again. For each unsuccessful discovery attempt made
         * by this method for the current instance of this task, this method returns
         * <code>false</code>, which causes the task to be scheduled by the
         * <code>WakeupManager</code> to be executed again at a later time as indicated by the value
         * returned by <code>retryTime</code>.
         */
        public boolean tryOnce() {
            logger.finest("LookupLocatorDiscovery - DiscoveryTask started");
            synchronized (LookupLocatorDiscovery.this) {
                if (terminated) {
                    return true;
                }
                /* Locators may have been removed (ex. removeLocators or
                 * setLocators) between the time they were added to the map,
                 * and the time this task is finally executed. Determine if
                 * this task should continue.
                 */
                if (undiscoveredLocators.isEmpty()) {
                    logger.finest("LookupLocatorDiscovery - DiscoveryTask "
                            + "completed");
                    return true;//true ==> done. Don't queue retry.
                }//endif
                if (!undiscoveredLocators.contains(reg)) {
                    logger.finest("LookupLocatorDiscovery - DiscoveryTask "
                            + "completed");
                    return true;//already removed, true ==> don't queue retry
                }
            }//end sync(LookupLocatorDiscovery.this)
            /* Use the unicast discovery protocol to perform the actual
             * discovery. Note that since this process involves remote,
             * interprocess (socket) communication, it is important that
             * this processing be performed outside of the sync block.
             */
            boolean noRetry = regTryGetProxy(reg);//t -> done, f -> queue retry
            synchronized (LookupLocatorDiscovery.this) {
                if (terminated) {
                    return true;
                }
                if (noRetry) {
                    logger.finest("LookupLocatorDiscovery - DiscoveryTask completed. locator: " + reg.l);
                } else {
                    logger.finest("LookupLocatorDiscovery - DiscoveryTask failed, will retry later. locator: " + reg.l);
                }//endif

                return noRetry;
            }

        }//end tryOnce

        /**
         * Returns the next absolute time (in milliseconds) at which another attempt to discover the
         * lookup service referenced in this class should be made. <p/> Overrides the version of
         * this method in the parent class.
         */
        public long retryTime() {
            return reg.getNextTryTime();
        }//end retryTime

        /**
         * Returns true if current instance must be run after task(s) in task manager queue.
         *
         * @param tasks the tasks to consider.
         * @param size  elements with index less than size are considered.
         */
        public boolean runAfter(java.util.List tasks, int size) {
            return false;
        }//end runAfter
    }//end class DiscoveryTask

    /**
     * Creates an instance of this class (<code>LookupLocatorDiscovery</code>), with an initial
     * array of <code>LookupLocator</code>s to be managed. For each managed
     * <code>LookupLocator</code>, unicast discovery is performed to obtain a
     * <code>ServiceRegistrar</code> proxy for that lookup service.
     *
     * @param locators the locators to discover
     * @throws java.lang.NullPointerException input array contains at least one <code>null</code>
     *                                        element
     */
    public LookupLocatorDiscovery(LookupLocator[] locators) {
        if (logger.isLoggable(Level.FINE))
            logger.log(Level.FINE, "Created tunable LookupLocatorDiscovery");
        try {
            beginDiscovery(locators, EmptyConfiguration.INSTANCE);
        } catch (ConfigurationException e) { /* swallow this exception */ }
    }//end constructor

    /**
     * Constructs a new lookup locator discovery object, set to discover the given set of locators,
     * and having the given <code>Configuration</code>. <p/> For each managed
     * <code>LookupLocator</code>, unicast discovery is performed to obtain a
     * <code>ServiceRegistrar</code> proxy for that lookup service.
     *
     * @param locators the locators to discover
     * @param config   an instance of <code>Configuration</code>, used to obtain the objects needed
     *                 to configure the current instance of this class
     * @throws net.jini.config.ConfigurationException indicates an exception occurred while
     *                                                retrieving an item from the given
     *                                                <code>Configuration</code>
     * @throws java.lang.NullPointerException         input array contains at least one
     *                                                <code>null</code> element or <code>null</code>
     *                                                is input for the configuration
     */
    public LookupLocatorDiscovery(LookupLocator[] locators,
                                  Configuration config)
            throws ConfigurationException {
        if (logger.isLoggable(Level.FINE))
            logger.log(Level.FINE, "Created tunable LookupLocatorDiscovery");
        beginDiscovery(locators, config);
    }//end constructor

    /**
     * Add a DiscoveryListener to the listener set. The listener's discovered method gets called
     * right way with an array of ServiceRegistrars that have already been discovered, and will be
     * called in the future whenever additional lookup services are discovered.
     *
     * @param l the new DiscoveryListener to add
     * @throws java.lang.NullPointerException  this exception occurs when <code>null</code> is input
     *                                         to the listener parameter <code>l</code>.
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see #removeDiscoveryListener
     */
    public void addDiscoveryListener(DiscoveryListener l) {
        if (l == null) {
            throw new NullPointerException("can't add null listener");
        }
        synchronized (this) {
            if (terminated) {
                throw new IllegalStateException("discovery terminated");
            }
            if (listeners.contains(l)) return; //already have this listener
            listeners.add(l);
            if (!discoveredLocators.isEmpty()) {
                HashMap groupsMap = new HashMap(discoveredLocators.size());
                Iterator iter = discoveredLocators.iterator();
                for (int i = 0; iter.hasNext(); i++) {
                    LocatorReg reg = (LocatorReg) iter.next();
                    groupsMap.put(reg.proxy, reg.memberGroups);
                }//end loop
                ArrayList list = new ArrayList(1);
                list.add(l);
                addNotify(list, groupsMap, false);
            }//endif
        }//end sync
    }//end addDiscoveryListener

    /**
     * Remove a DiscoveryListener from the listener set. It does nothing if the DiscoveryListener
     * does not exist in the the listener set.
     *
     * @param l the existing DiscoveryListener to remove
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see #addDiscoveryListener
     */
    public synchronized void removeDiscoveryListener(DiscoveryListener l) {
        if (terminated) {
            throw new IllegalStateException("discovery terminated");
        }
        int index = listeners.indexOf(l);
        if (index != -1) listeners.remove(index);
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
        synchronized (this) {
            if (terminated) {
                throw new IllegalStateException("discovery terminated");
            }
            if ((discoveredLocators == null) ||
                    (discoveredLocators.isEmpty())) {
                return new ServiceRegistrar[0];
            }
            return buildServiceRegistrar();
        }//end sync(this)
    }//end getRegistrars

    /**
     * Removes an instance of <code>ServiceRegistrar</code> from the managed set of lookup services,
     * making the corresponding lookup service eligible for re-discovery. This method takes no
     * action if the parameter input to this method is <code>null</code>, or if it does not match
     * (using <code>equals</code>) any of the elements in the managed set.
     *
     * @param proxy the instance of <code>ServiceRegistrar</code> to discard from the managed set of
     *              lookup services
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see net.jini.core.lookup.ServiceRegistrar
     * @see net.jini.discovery.DiscoveryManagement#discard
     */
    public void discard(ServiceRegistrar proxy) {
        synchronized (this) {
            if (terminated) {
                throw new IllegalStateException("discovery terminated");
            }
            if (proxy == null) return;
            LookupLocator lct = findRegFromProxy(proxy);
            if (lct == null) return;
            /* Remove locator from the set of already-discovered locators */
            LocatorReg reg = removeDiscoveredLocator(lct);
            /* Prepare the information for the discarded event */
            HashMap groupsMap = new HashMap(1);
            groupsMap.put(reg.proxy, reg.memberGroups);
            /* Prepare the discarded locatorReg for re-discovery */
            reg.proxy = null;
            reg.memberGroups = null;
            reg.fixupNextTryTime();
            addToMap(reg);//put discarded reg back in the not-discovered map
            /* Send a discarded event to all registered listeners */
            if (!listeners.isEmpty()) {
                addNotify((ArrayList) listeners.clone(), groupsMap, true);
            }//endif
        }//end sync(this)
    }//end discard

    /**
     * Terminates all threads, ending all locator discovery processing being performed by the
     * current instance of this class. <p/> After this method has been invoked, no new lookup
     * services will be discovered, and the effect of any new operations performed on the current
     * instance of this class are undefined.
     *
     * @see net.jini.discovery.DiscoveryManagement#terminate
     */
    public synchronized void terminate() {
        if (terminated)
            return;
        terminated = true;
        terminateTaskMgr();
        synchronized (pendingNotifies) {
            pendingNotifies.clear();
        }//end sync
        synchronized (lookupAliveTasks) {
            LookupAliveTask[] lats =
                    (LookupAliveTask[]) lookupAliveTasks.toArray(
                            new LookupAliveTask[lookupAliveTasks.size()]);
            for (int i = 0; i < lats.length; i++) {
                lats[i].interrupt();
            }
        }
    }//end terminate

    /**
     * Returns an array consisting of the elements of the managed set of locators; that is,
     * instances of <code>LookupLocator</code> in which each instance corresponds to a specific
     * lookup service to discover. The returned set will include both the set of
     * <code>LookupLocator</code>s corresponding to lookup services that have already been
     * discovered as well as the set of those that have not yet been discovered. If the managed set
     * of locators is empty, this method will return the empty array. This method returns a new
     * array upon each invocation.
     *
     * @return <code>LookupLocator</code> array consisting of the elements of the managed set of
     * locators
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see net.jini.discovery.DiscoveryLocatorManagement#getLocators
     * @see #setLocators
     */
    public synchronized LookupLocator[] getLocators() {
        if (terminated) {
            throw new IllegalStateException("discovery terminated");
        }
        /* Includes the set of already-discovered lookup services and
         * the set of not-yet-discovered lookup services.
         */
        int size = discoveredLocators.size() + undiscoveredLocators.size();
        LookupLocator[] ret = new LookupLocator[size];
        /* Retrieve the locators of the already-discovered lookup services */
        int k = 0;
        Iterator iter = discoveredLocators.iterator();
        while (iter.hasNext()) {
            ret[k++] = ((LocatorReg) iter.next()).l;
        }//end loop
        /* Append the locators of the not-yet-discovered lookup services */
        iter = undiscoveredLocators.iterator();
        while (iter.hasNext()) {
            ret[k++] = ((LocatorReg) iter.next()).l;
        }//end loop
        return ret;
    }//end getLocators

    /**
     * Adds a set of locators to the managed set of locators. Elements in the input set that
     * duplicate (using the <code>LookupLocator.equals</code> method) elements already in the
     * managed set will be ignored. If the empty array is input, the managed set of locators will
     * not change.
     *
     * @param locators <code>LookupLocator</code> array consisting of the locators to add to the
     *                 managed set.
     * @throws java.lang.NullPointerException  this exception occurs when either <code>null</code>
     *                                         is input to the <code>locators</code> parameter, or
     *                                         one or more of the elements of the <code>locators</code>
     *                                         parameter is <code>null</code>.
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see net.jini.discovery.DiscoveryLocatorManagement#addLocators
     * @see #removeLocators
     */
    public synchronized void addLocators(LookupLocator[] locators) {
        testSetForNull(locators);
        if (terminated) {
            throw new IllegalStateException("discovery terminated");
        }
        discoverLocators(locators);
    }//end addLocators

    /**
     * Replaces all of the locators in the managed set with locators from a new set, and discards
     * any already-discovered lookup service that corresponds to a locator that is removed from the
     * managed set as a result of an invocation of this method. For any such lookup service that is
     * discarded, a discard notification is sent; and that lookup service will not be eligible for
     * re-discovery (assuming it is not currently eligible for discovery through other means, such
     * as group discovery). <p/> If the empty array is input, locator discovery will cease until
     * this method is invoked with an input parameter that is non-<code>null</code> and non-empty.
     *
     * @param locators <code>LookupLocator</code> array consisting of the locators that will replace
     *                 the current locators in the managed set.
     * @throws java.lang.NullPointerException  this exception occurs when either <code>null</code>
     *                                         is input to the <code>locators</code> parameter, or
     *                                         one or more of the elements of the <code>locators</code>
     *                                         parameter is <code>null</code>.
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see net.jini.discovery.DiscoveryLocatorManagement#setLocators
     * @see #getLocators
     */
    public void setLocators(LookupLocator[] locators) {
        testSetForNull(locators);
        synchronized (this) {
            if (terminated) {
                throw new IllegalStateException("discovery terminated");
            }
            HashMap groupsMap = new HashMap(1);
            /* From the set of already-discovered locators, remove each 
             * element that is NOT in the input set of locators.
             */
            Iterator iter = discoveredLocators.iterator();
            while (iter.hasNext()) {
                LocatorReg reg = (LocatorReg) iter.next();
                if (!isArrayContains(locators, reg.l)) {
                    iter.remove();
                    groupsMap.put(reg.proxy, reg.memberGroups);
                }//endif
            }//end loop
            /* From the set of yet-to-be-discovered locators, remove each 
             * element that is NOT in the input set of replacement locators.
             * 
             * Note that if the discovery task is currently attempting to
             * discover a locator from this set, and if that locator is not
             * contained in the given input set of replacement locators (that
             * is, it is no longer desired that that locator be discovered),
             * then the discovery task, when it completes (either successfully
             * or un-successfully) the attempt to discover that locator, will
             * end all discovery processing with respect to the affected
             * locator.
             *
             * To inform the discovery task -- upon its return from the
             * unicast discovery process -- of the desire to terminate all
             * discovery processing for that particular locator, the element
             * in the set of undiscoveredLocators that corresponds to that
             * locator is removed. This means that if the discovery attempt
             * failed, the locator will no longer be considered one of the
             * yet-to-be-discovered locators; and if the attempt succeeded,
             * prevents the locator from being placed in the set of 
             * already-discovered locators. It also prevents any discarded
             * or discovered events from being sent.
             */
            iter = undiscoveredLocators.iterator();
            while (iter.hasNext()) {
                LocatorReg reg = (LocatorReg) iter.next();
                if (!isArrayContains(locators, reg.l)) {
                    iter.remove();
                }//endif
            }//end loop
            /* Initiate discovery process for any new, un-discovered locators*/
            discoverLocators(locators);
            /* Send a discarded event to all registered listeners for any
             * locators that were removed by this method.
             */
            if (!groupsMap.isEmpty() && !listeners.isEmpty()) {
                addNotify((ArrayList) listeners.clone(), groupsMap, true);
            }//endif
        }//end sync(this)
    }//end setLocators

    /**
     * Deletes a set of locators from the managed set of locators, and discards any
     * already-discovered lookup service that corresponds to a deleted locator. For any lookup
     * service that is discarded as a result of an invocation of this method, a discard notification
     * is sent; and that lookup service will not be eligible for re-discovery (assuming it is not
     * currently eligible for discovery through other means, such as group discovery). <p/> If the
     * empty array is input, this method takes no action.
     *
     * @param locators <code>LookupLocator</code> array consisting of the locators that will be
     *                 removed from the managed set.
     * @throws java.lang.NullPointerException  this exception occurs when either <code>null</code>
     *                                         is input to the <code>locators</code> parameter, or
     *                                         one or more of the elements of the <code>locators</code>
     *                                         parameter is <code>null</code>.
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     * @see net.jini.discovery.DiscoveryLocatorManagement#removeLocators
     * @see #addLocators
     */
    public void removeLocators(LookupLocator[] locators) {
        testSetForNull(locators);
        synchronized (this) {
            if (terminated) {
                throw new IllegalStateException("discovery terminated");
            }
            HashMap groupsMap = new HashMap(1);
            for (int i = 0; i < locators.length; i++) {
                LocatorReg reg = removeDiscoveredLocator(locators[i]);
                if (reg != null) {//removing an already-discovered reg
                    groupsMap.put(reg.proxy, reg.memberGroups);
                    continue;
                }//endif
                reg = findReg(locators[i]);
                if (reg != null) {//reg not yet discovered, stop discovery of it
                    undiscoveredLocators.remove(reg);
                }//endif
            }//end loop
            /* Send a discarded event to all registered listeners for any
             * locators that were removed by this method.
             */
            if (!groupsMap.isEmpty() && !listeners.isEmpty()) {
                addNotify((ArrayList) listeners.clone(), groupsMap, true);
            }//endif
        }//end sync
    }//end removeLocators

    /**
     * Returns the set of <code>LookupLocator</code> objects representing the desired lookup
     * services that are currently discovered. If no lookup services are currently discovered, this
     * method returns the empty array. This method returns a new array upon each invocation.
     *
     * @return <code>LookupLocator</code> array consisting of the elements from the managed set of
     * locators that correspond to lookup services that have already been discovered.
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     */
    public synchronized LookupLocator[] getDiscoveredLocators() {
        if (terminated) {
            throw new IllegalStateException("discovery terminated");
        }
        int size = discoveredLocators.size();
        LookupLocator[] ret = new LookupLocator[size];
        /* Retrieve the locators of the already-discovered lookup services */
        int k = 0;
        Iterator iter = discoveredLocators.iterator();
        while (iter.hasNext()) {
            ret[k++] = ((LocatorReg) iter.next()).l;
        }//end loop
        return ret;
    }//end getDiscoveredLocators

    /**
     * Returns the set of <code>LookupLocator</code> objects representing the desired lookup
     * services that have not yet been discovered. If all of the desired lookup services are
     * currently discovered, this method returns the empty array. This method returns a new array
     * upon each invocation.
     *
     * @return <code>LookupLocator</code> array consisting of the elements from the managed set of
     * locators that correspond to lookup services that have not yet been discovered.
     * @throws java.lang.IllegalStateException this exception occurs when this method is called
     *                                         after the <code>terminate</code> method has been
     *                                         called.
     */
    public synchronized LookupLocator[] getUndiscoveredLocators() {
        if (terminated) {
            throw new IllegalStateException("discovery terminated");
        }
        LookupLocator[] locs = new LookupLocator[undiscoveredLocators.size()];
        Iterator iter = undiscoveredLocators.iterator();
        for (int i = 0; iter.hasNext(); i++) {
            locs[i] = ((LocatorReg) iter.next()).l;
        }//end loop
        return locs;
    }//end getUndiscoveredLocators

    /**
     * Initiates the discovery process for the lookup services having the given locators.
     */
    private void discoverLocators(LookupLocator[] lcts) {
        assert Thread.holdsLock(this);
        discoverLocatorsCalled = true;
        if (lcts == null) return;
        for (int i = 0; i < lcts.length; i++) {
            boolean discovered = isDiscovered(lcts[i]);
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("discoverLocators " + lcts[i].getHost() + ":" + lcts[i].getPort() + " discovered=" + discovered);
            }
            if (discovered) continue;

            LocatorReg reg = findReg(lcts[i]);//in not-yet-discovered map?
            if (reg == null) {
                reg = new LocatorReg(lcts[i]);
                addToMap(reg);
            }//endif
        }//end loop
    }//end discoverLocators

    /**
     * From the sets of elements corresponding to yet-to-be-discovered locators, this methods finds
     * and returns the instance of LocatorReg corresponding to the given LookupLocator. This method
     * searches the set of undiscoveredLocators, and upon finding a matching LocatorReg object, that
     * object is returned; otherwise, null is returned.
     */
    private LocatorReg findReg(LookupLocator lct) {
        Iterator iter = undiscoveredLocators.iterator();
        while (iter.hasNext()) {
            LocatorReg reg = (LocatorReg) iter.next();
            if (reg.l.equals(lct)) return reg;
        }//end loop
        return null;
    }//end findReg

    /**
     * This method searches the set of discovered LocatorReg objects for the element that contains
     * the given ServiceRegistrar object. Upon finding such an element, the corresponding
     * LookupLocator is returned; otherwise, null is returned.
     */
    private LookupLocator findRegFromProxy(ServiceRegistrar proxy) {
        Iterator iter = discoveredLocators.iterator();
        while (iter.hasNext()) {
            LocatorReg reg = (LocatorReg) iter.next();
            if ((reg.proxy).equals(proxy)) return reg.l;
        }//end loop
        return null;
    }//end findRegFromProxy

    /**
     * Convenience method called from within the DiscoveryTask. Employing unicast discovery, this
     * method attempts to discover the lookup service associated with the given LocatorReg. After
     * successfully discovering the desired lookup service, this method queues the appropriate event
     * for dissemination to the registered listeners, and then returns <code>true</code>; otherwise
     * <code>false</code> is returned.
     */
    private boolean regTryGetProxy(LocatorReg reg) {
        /* The following call performs the actual unicast discovery attempt,
         * and should not be made within a synchronization block.
         */
        boolean b = reg.tryGetProxy();
        /* While the discovery attempt was being made above, the locator
         * corresponding to the given LocatorReg may have been removed from
         * the managed set of locators (by a call to set/removeLocators). If
         * it did happen to be removed, then there is no need to continue
         * with the discovery process of that locator, whether its proxy was
         * successfully retrieved or not. Thus, if it was removed  from the
         * set of undiscoveredLocators while unicast discovery was being
         * performed, then return true to stop any queuing of a retry of
         * the task used in the discovery attempt of the given locator.
         * 
         * If it wasn't removed, but its proxy could not be successfully
         * discovered (as indicated by a false return value), then leave
         * it in the set of undiscoveredLocators and schedule - at a later
         * time - a retry of the task which performs the discovery attempt.
         *
         * Finally, if the proxy of the locator was successfully discovered,
         * then remove the locator from the set of undiscoveredLocators, add
         * it to the set of discoveredLocators, notify all registered
         * listeners that the locator has been discovered, and return true
         * to prevent retries from being queued.
         */
        synchronized (this) {
            if (!undiscoveredLocators.contains(reg)) {
                return true;//already removed, true ==> don't queue retry
            }//endif
            /* Discovery un-successful, leave in set, try new wakeup task */
            if (!b) {
                return false;//this causes a retry to be queued
            }//endif
            /* Discovery was successful, move reg from undiscoveredLocators
             * to discoveredLocators, and notify listeners
             */
            undiscoveredLocators.remove(reg);
            discoveredLocators.add(reg);
            if (!listeners.isEmpty()) {
                addNotify((ArrayList) listeners.clone(),
                        mapRegToGroups(reg.proxy, reg.memberGroups),
                        false);
            }//endif
            return true;//done; don't queue any retries
        }//end sync(this)
    }//end regTryGetProxy

    /**
     * From each element of the set of LocatorReg objects that correspond to lookup services that
     * have been discovered, this method extracts the ServiceRegistrar reference and returns all of
     * the references in an array of ServiceRegistrar.
     */
    private ServiceRegistrar[] buildServiceRegistrar() {
        int k = 0;
        ServiceRegistrar[] proxys =
                new ServiceRegistrar[discoveredLocators.size()];
        Iterator iter = discoveredLocators.iterator();
        while (iter.hasNext()) {
            LocatorReg reg = (LocatorReg) iter.next();
            proxys[k++] = reg.proxy;
        }//end loop
        return proxys;
    }//end buildServiceRegistrar

    /**
     * Adds the given LocatorReg object to the set containing the objects corresponding to the
     * locators of desired lookup services that have not yet been discovered, and queues a
     * DiscoveryTask to attempt, through unicast discovery, to discover the associated lookup
     * service.
     */
    private void addToMap(LocatorReg reg) {
        undiscoveredLocators.add(reg);//add to set of not-yet-discovered locs
        queueDiscoveryTask(reg);
    }//end addToMap

    /**
     * Queues a DiscoveryTask to attempt, through unicast discovery, to discover the lookup service
     * associated with the given LocatorReg instance.
     */
    private void queueDiscoveryTask(LocatorReg reg) {
        discoveryTaskMgr.add
                (new DiscoveryTask(reg, discoveryTaskMgr, discoveryWakeupMgr));
    }//end queueDiscoveryTask

    /**
     * Determines whether or not the lookup service associated with the given LookupLocator has
     * already been discovered.
     */
    private boolean isDiscovered(LookupLocator lct) {
        Iterator iter = discoveredLocators.iterator();
        while (iter.hasNext()) {
            LocatorReg reg = (LocatorReg) iter.next();
            if (reg.l.equals(lct)) return true;
        }//end loop
        return false;
    }//end isDiscovered

    /**
     * Add a notification task to the pending queue, and start an instance of the Notifier thread if
     * one isn't already running.
     */
    private void addNotify(ArrayList notifies,
                           Map groupsMap,
                           boolean discard) {
        synchronized (pendingNotifies) {
            pendingNotifies.addLast(new NotifyTask(notifies,
                    groupsMap,
                    discard));
            if (notifierThread == null) {
                notifierThread = new Notifier();
                notifierThread.start();
            }//endif
        }//end sync
    }//end addNotify

    /**
     * Convenience method used to remove the LocatorReg - corresponding to the given LookupLocator -
     * from the set of LocatorReg objects that correspond to lookup services that have already been
     * discovered.
     */
    private LocatorReg removeDiscoveredLocator(LookupLocator lct) {
        Iterator iter = discoveredLocators.iterator();
        while (iter.hasNext()) {
            LocatorReg reg = (LocatorReg) iter.next();
            if (reg.l.equals(lct)) {
                iter.remove();
                return reg;
            }//endif
        }//end loop
        return null;
    }//end removeDiscoveredLocator

    /**
     * Convenience method that removes all pending and active tasks from the TaskManager, and
     * removes all pending tasks from the WakeupManager.
     */
    private void terminateTaskMgr() {
        /* Cancel all tasks scheduled for future retry by the wakeup manager */
        if (discoveryWakeupMgr != null) {
            discoveryWakeupMgr.stop();//stop execution of the wakeup manager
            discoveryWakeupMgr.cancelAll();//cancel all tickets
        }//endif
        /* Cancel/remove pending tasks from the task manager and terminate */
        if (discoveryTaskMgr != null) {
            List pendingTasks = discoveryTaskMgr.getPending();
            for (int i = 0; i < pendingTasks.size(); i++) {
                RetryTask pendingTask = (RetryTask) pendingTasks.get(i);
                pendingTask.cancel();//cancel wakeup ticket
                discoveryTaskMgr.remove(pendingTask);//remove from task mgr
            }//end loop
            discoveryTaskMgr.terminate();//interrupt all active tasks
            discoveryTaskMgr = null;
            discoveryWakeupMgr = null;
        }//endif
    }//end terminateTaskMgr

    /**
     * Determines if the given Object is an element of the given array.
     */
    private boolean isArrayContains(Object[] a, Object obj) {
        for (int i = 0; i < a.length; i++) {
            if (a[i].equals(obj)) return true;
        }//end loop
        return false;
    }//end isArrayContains

    /* Convenience method useful for debugging. */
    private void printMap() {
        Iterator iter = undiscoveredLocators.iterator();
        while (iter.hasNext()) {
            LocatorReg reg = (LocatorReg) iter.next();
            System.out.println("printMap reg:" + reg.id);
        }//end loop
    }//end printMap

    /**
     * This method is used by the public methods of this class that are specified to throw a
     * <code>NullPointerException</code> when the set of locators is either <code>null</code> or
     * contains one or more <code>null</code> elements; in either case, this method throws a
     * <code>NullPointerException</code> which should be allowed to propagate outward.
     *
     * @throws java.lang.NullPointerException this exception occurs when either <code>null</code> is
     *                                        input to the <code>locatorSet</code> parameter, or one
     *                                        or more of the elements of the <code>locatorSet</code>
     *                                        parameter is <code>null</code>.
     */
    private void testSetForNull(LookupLocator[] locatorSet) {
        if (locatorSet == null) {
            throw new NullPointerException("null locator array");
        }//endif
        for (int i = 0; i < locatorSet.length; i++) {
            if (locatorSet[i] == null) {
                throw new NullPointerException
                        ("null element in locator array");
            }//endif
        }//end loop
    }//end testSetForNull

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
        }//end loop
        return newMap;
    }//end deepCopy

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
     * Using the given <code>Configuration</code>, initializes the current instance of this utility,
     * and initiates the discovery process for the given set of locators.
     *
     * @param locators the set of locators to discover
     * @param config   an instance of <code>Configuration</code>, used to obtain the objects needed
     *                 to configure this utility
     * @throws net.jini.config.ConfigurationException indicates an exception occurred while
     *                                                retrieving an item from the given
     *                                                <code>Configuration</code>
     * @throws java.lang.NullPointerException         input array contains at least one
     *                                                <code>null</code> element or <code>null</code>
     *                                                is input for the configuration
     */
    private void beginDiscovery(final LookupLocator[] locators,
                                Configuration config)
            throws ConfigurationException {
        synchronized (this) {
            init(config);
            if (locators == null) {
                return;
            }
            addDiscoveryListener(new InternalDiscoverylListener());
            testSetForNull(locators);
            if (initialUnicastDelayRange > 0) {
                discoveryWakeupMgr.schedule(
                        SystemTime.timeMillis() +
                                (long) (Math.random() * initialUnicastDelayRange),
                        new Runnable() {
                            public void run() {
                                synchronized (LookupLocatorDiscovery.this) {
                                    if (terminated || discoverLocatorsCalled) {
                                        // discoverLocatorsCalled will be true
                                        // if there has been an intervening
                                        // addLocators or setLocators call.
                                        return;
                                    }
                                    discoverLocators(locators);
                                }
                            }
                        }
                );
            } else {
                discoverLocators(locators);
            }
        }
    }

    /* Convenience method that encapsulates the retrieval of the configurable
     * items from the given <code>Configuration</code> object.
     */
    private void init(Configuration config) throws ConfigurationException {
        if (config == null) throw new NullPointerException("config is null");
        /* Lookup service proxy preparer */
        registrarPreparer = (ProxyPreparer) config.getEntry
                (COMPONENT_NAME,
                        "registrarPreparer",
                        ProxyPreparer.class,
                        new BasicProxyPreparer());
        /* Task manager */
        try {
            discoveryTaskMgr = (TaskManager) config.getEntry(COMPONENT_NAME,
                    "taskManager",
                    TaskManager.class);
        } catch (NoSuchEntryException e) { /* use default */
            discoveryTaskMgr = new com.sun.jini.thread.TaskManager(15, 30000, 3.0F, "LookupLocatorDiscovery Task", 10);
        }
        /* Wakeup manager */
        try {
            discoveryWakeupMgr = (WakeupManager) config.getEntry
                    (COMPONENT_NAME,
                            "wakeupManager",
                            WakeupManager.class);
        } catch (NoSuchEntryException e) { /* use default */
            discoveryWakeupMgr = new WakeupManager
                    (new WakeupManager.ThreadDesc(null, true));
        }

        initialUnicastDelayRange = Config.getLongEntry(config,
                COMPONENT_NAME,
                "initialUnicastDelayRange",
                0,
                0,
                Long.MAX_VALUE);
    }//end init

    /**
     * Internal listener being notified of lookup service discovery. Creates LookupAliveTask
     * instances to monitor reachability of Lookup service
     */
    class InternalDiscoverylListener implements DiscoveryListener {
        public void discovered(DiscoveryEvent discoveryEvent) {
            ServiceRegistrar[] sr = discoveryEvent.getRegistrars();
            for (int i = 0; i < sr.length; i++) {
                LookupAliveTask lat = null;
                try {
                    lat = new LookupAliveTask(sr[i]);
                    synchronized (lookupAliveTasks) {
                        String key = lat.locatorString;
                        if (lookupAliveTasksMap.containsKey(key)) {
                            Set<LookupAliveTask> aliveTasks = lookupAliveTasksMap.get(key);
                            // As a quick fix we are limiting the number of keep alive tasks
                            // per locator to MAX_LOOKUP_ALIVE_TASKS_PER_LOCATOR
                            // This greately minimizes the change of a concurrency issue
                            // where a task is not added because there is already a running
                            // task for the locator, but just after that, the other task is removed
                            if (aliveTasks.size() == MAX_LOOKUP_ALIVE_TASKS_PER_LOCATOR) {
                                Iterator<LookupAliveTask> iterator = aliveTasks.iterator();
                                while (iterator.hasNext()) {
                                    LookupAliveTask task = iterator.next();
                                    if (!(task.isAlive() && task.keepAlive)) {
                                        iterator.remove();
                                    }
                                }
                                if (aliveTasks.size() == MAX_LOOKUP_ALIVE_TASKS_PER_LOCATOR) {
                                    continue;
                                }
                            }
                        } else {
                            lookupAliveTasksMap.put(key, new HashSet<LookupAliveTask>());
                        }

                        lookupAliveTasksMap.get(key).add(lat);
                        lookupAliveTasks.add(lat);

                        lat.start();
                    }

                } catch (RemoteException e) {
                    if (logger.isLoggable(Level.WARNING))
                        logger.log(Level.WARNING,
                                "Exception in InternalDiscoveryListener.discovered() going to discard the discoveryEvent: " + discoveryEvent.toString(), e);
                    discard(sr[i]);
                    if (e instanceof ProxyClosedException) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        public void discarded(DiscoveryEvent discoveryEvent) {

        }
    }

    /**
     * Periodically checks for Lookup Service reachability
     */
    class LookupAliveTask extends Thread {
        boolean keepAlive = true;
        final ServiceRegistrar proxy;
        static final int DEFAULT_INVOCATION_DELAY = 1000 * 60;
        private long invocationDelay = DEFAULT_INVOCATION_DELAY;
        final String locatorString;

        LookupAliveTask(ServiceRegistrar proxy) throws RemoteException {
            this.proxy = proxy;
            String s = System.getProperty("com.gigaspaces.unicast.ping");
            if (s != null) {
                try {
                    invocationDelay = Long.parseLong(s);
                } catch (NumberFormatException e) {
                    logger.log(Level.INFO, "Value [" + s + "] " +
                            "is illegal for property " +
                            "com.gigaspaces.unicast.ping, " +
                            "ignoring");
                }
                if (logger.isLoggable(Level.CONFIG))
                    logger.log(Level.CONFIG,
                            "Set unicast ping to [" + invocationDelay + "]");
            }
            LookupLocator locator = proxy.getLocator();
            locatorString =
                    "jini://" + locator.getHost() + ":" + locator.getPort();
        }

        public void interrupt() {
            if (logger.isLoggable(Level.FINEST))
                logger.finest("Terminating LookupAliveTask Thread for " +
                        locatorString);
            keepAlive = false;
            super.interrupt();
        }

        public void run() {

            if (logger.isLoggable(Level.FINE))
                logger.fine("Started LookupAliveTask for " + locatorString);
            while (!interrupted()) {
                if (!keepAlive) {
                    return;
                }
                try {
                    sleep(invocationDelay);
                } catch (InterruptedException ie) {
                } catch (IllegalArgumentException iae) {
                    logger.warning("LookupAliveTask: sleep time is off : "
                            + invocationDelay);
                }
                try {
                    if (logger.isLoggable(Level.FINE))
                        logger.fine(
                                "invoke getLocator() on : " + locatorString);
                    proxy.getLocator();
                } catch (ConnectException e) {
                    hardStop();
                } catch (Exception e) {
                    final int category = ThrowableConstants.retryable(e);
                    if (category == ThrowableConstants.BAD_INVOCATION ||
                            category == ThrowableConstants.BAD_OBJECT) {
                        if (logger.isLoggable(Level.FINE))
                            logger.log(Level.FINE,
                                    "Unrecoverable Exception invoking " +
                                            "getLocator()",
                                    e);
                        hardStop();
                    }
                }

            }
        }

        private void hardStop() {
            synchronized (LookupLocatorDiscovery.this) {
                if (terminated) return;
            }
            discard(proxy);
            synchronized (lookupAliveTasks) {
                lookupAliveTasks.remove(this);
                if (lookupAliveTasksMap.containsKey(this.locatorString)) {
                    lookupAliveTasksMap.get(this.locatorString).remove(this);
                }
            }
            interrupt();
        }
    }
}//end class LookupLocatorDiscovery
