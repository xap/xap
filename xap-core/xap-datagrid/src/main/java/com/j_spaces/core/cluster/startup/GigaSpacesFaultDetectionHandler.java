/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.j_spaces.core.cluster.startup;

import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.utils.concurrent.GSThread;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.core.IJSpace;
import com.sun.jini.config.Config;
import com.sun.jini.constants.ThrowableConstants;

import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.core.lookup.ServiceID;

import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides support to determine the reachability of the GigaSpace service. The
 * GigaSpacesFaultDetectionHandler can be used to monitor services which implement the
 * <code>com.j_spaces.core.IJSpace</code> interface (as well as for services that do not implement
 * this interface). <p> If the service does implement the <code>com.j_spaces.core.IJSpace</code>,
 * interface, the GigaSpacesFaultDetectionHandler will ensure that the proxy being used is a
 * non-clustered proxy, then invoke the {@link com.j_spaces.core.IJSpace#ping()} method
 * periodically. If this method invocation returns succesfully, the service is assumed to be
 * available. If this method invocation results in a failure, and all retry attempts have failed,
 * the GigaSpacesFaultDetectionHandler will notify FaultDetectionListener instances of the failure.
 * <p> Additionally, the GigaSpacesFaultDetectionHandler will register with Lookup Services for
 * ServiceRegistrar.TRANSITION_MATCH_NOMATCH transitions for the service being monitored. If the
 * service is adminstratively removed from the network, the transition will be noted and
 * FaultDetectionListener instances will be notified of the failure. <p> If the service does not
 * implement the <code>com.j_spaces.core.IJSpace</code> interface, the
 * GigaSpacesFaultDetectionHandler will periodically invoke the {@link
 * com.j_spaces.core.IJSpace#ping()} method, but will only create the event consumer for Lookup
 * Service TRANSITION_MATCH_NOMATCH transitions. <p> <b><font size="+1">Configuring
 * GigaSpacesFaultDetectionHandler</font> </b> <p> This implementation of
 * <code>GigaSpacesFaultDetectionHandler</code> supports the following configuration entries; where
 * each configuration entry name is associated with the component name
 * <code>com.gigaspaces.grid.handler.GigaSpacesFaultDetectionHandler</code>. <br> <br> <ul>
 * <li><span style="font-weight: bold; font-family: courier new,courier,monospace;">invocationDelay</span>
 * <table cellpadding="2" cellspacing="2" border="0" style="text-align: left; width: 100%;"> <tbody>
 * <tr><td style="vertical-align: top; text-align: right; font-weight: bold;">Type: <br> </td> <td
 * style="vertical-align: top; font-family: monospace;">long</td> </tr> <tr><td
 * style="vertical-align: top; text-align: right; font-weight: bold;">Default: <br> </td> <td
 * style="vertical-align: top;"><code>30*1000 (30 seconds)</code></td> </tr> <tr><td
 * style="vertical-align: top; text-align: right; font-weight: bold;">Description: <br> </td> <td
 * style="vertical-align: top;">The amount of time in milliseconds to wait between {@link
 * com.j_spaces.core.IJSpace#ping()} method invocations</td> </tr> </tbody> </table></li> </ul> <ul>
 * <li><span style="font-weight: bold; font-family: courier new,courier,monospace;">retryCount
 * </span> <table cellpadding="2" cellspacing="2" border="0" style="text-align: left; width: 100%;">
 * <tbody> <tr><td style="vertical-align: top; text-align: right; font-weight: bold;">Type: <br>
 * </td> <td style="vertical-align: top;"><code>int</code><br> </td> </tr> <tr><td
 * style="vertical-align: top; text-align: right; font-weight: bold;">Default: <br> </td> <td
 * style="vertical-align: top;"><code>3</code></td> </tr> <tr><td style="vertical-align: top;
 * text-align: right; font-weight: bold;">Description: <br> </td> <td style="vertical-align:
 * top;">The number of times to retry connecting to the service when invoking the {@link
 * com.j_spaces.core.IJSpace#ping()} method. If the service cannot be reached within the retry count
 * specified the service will be determined to be unreachable </td> </tr> </tbody> </table></li>
 * </ul> <ul> <li><span style="font-weight: bold; font-family: courier
 * new,courier,monospace;">retryTimeout </span> <table cellpadding="2" cellspacing="2" border="0"
 * style="text-align: left; width: 100%;"> <tbody> <tr><td style="vertical-align: top; text-align:
 * right; font-weight: bold;">Type: <br> </td> <td style="vertical-align:
 * top;"><code>long</code></td> </tr> <tr><td style="vertical-align: top; text-align: right;
 * font-weight: bold;">Default: <br> </td> <td style="vertical-align: top;"><code>1000 (1
 * second)</code></td> </tr> <tr><td style="vertical-align: top; text-align: right; font-weight:
 * bold;">Description: <br> </td> <td style="vertical-align: top;">How long to wait between retries
 * (in milliseconds). This value will be used between retry attempts, waiting the specified amount
 * of time to retry <br> </td> </tr> </tbody> </table></li> </ul> <br> The amount of time it takes
 * for the GigaSpacesFaultDetectionHandler to determine service failure for a service which
 * implements the {@link com.j_spaces.core.IJSpace} interface is calculated as follows :<br>
 *
 * <pre>
 * ((num_retries + 1) * (connectivity_timeout)) + (retry_delay * num_retries)
 * </pre>
 */
@com.gigaspaces.api.InternalApi
public class GigaSpacesFaultDetectionHandler extends AbstractFaultDetectionHandler {
    static final int DEFAULT_INVOCATION_DELAY = 1000 * 30;
    public static final String INVOCATION_DELAY_KEY = "invocationDelay";
    private long invocationDelay = DEFAULT_INVOCATION_DELAY;

    /**
     * Proxy to use when testing the space for reachability
     */
    private Object spaceProxy;

    /**
     * Component name, used for config and logger
     */
    private static final String COMPONENT = GigaSpacesFaultDetectionHandler.class.getName();

    /**
     * A Logger
     */
    static Logger logger = LoggerFactory.getLogger(COMPONENT);

    /**
     * @see org.jini.rio.core.FaultDetectionHandler#setConfiguration
     */
    @Override
    public void setConfiguration(String[] configArgs) {
        if (configArgs == null)
            throw new NullPointerException("configArgs is null");
        try {
            this.configArgs = new String[configArgs.length];
            System.arraycopy(configArgs, 0, this.configArgs, 0, configArgs.length);

            this.config = ConfigurationProvider.getInstance(configArgs);

            invocationDelay = Config.getLongEntry(config,
                    COMPONENT,
                    INVOCATION_DELAY_KEY,
                    DEFAULT_INVOCATION_DELAY,
                    0,
                    Long.MAX_VALUE);
            retryCount = Config.getIntEntry(config,
                    COMPONENT,
                    RETRY_COUNT_KEY,
                    DEFAULT_RETRY_COUNT,
                    0,
                    Integer.MAX_VALUE);
            retryTimeout = Config.getLongEntry(config,
                    COMPONENT,
                    RETRY_TIMEOUT_KEY,
                    DEFAULT_RETRY_TIMEOUT,
                    0,
                    Long.MAX_VALUE);

            if (logger.isTraceEnabled()) {
                StringBuilder buffer = new StringBuilder("GigaSpacesFaultDetectionHandler Properties : ");
                buffer.append("retry count=" + retryCount + ", ");
                buffer.append("retry timeout=" + retryTimeout + ", ");
                buffer.append("invocationDelay=" + invocationDelay);
                logger.trace(buffer.toString());
            }
        } catch (ConfigurationException e) {
            logger.error("Setting Configuration", e);
        }
    }

    /**
     * Override the monitor method to ensure a non-clustered proxy is used
     */
    @Override
    public void monitor(Object proxy, ServiceID serviceID)
            throws Exception {
        spaceProxy = proxy;
        super.monitor(proxy, serviceID);
    }

    /**
     * Get the class which implements the ServiceMonitor
     */
    @Override
    protected void monitor() throws Exception {
        checkAvailability();

        if (serviceMonitor == null)
            serviceMonitor = new GSPingManager();

    }

    private void checkAvailability() throws RemoteException {
        //We want the next ping operation to receive system priority in order to avoid false failure detection due to high
        //applicative load
        LRMIInvocationContext.enableLivenessPriorityForNextInvocation();
        if (spaceProxy instanceof IJSpace)
            ((IJSpace) spaceProxy).ping();
        else
            ((IRemoteSpace) spaceProxy).ping();
    }

    /**
     * Invoke the service's {@link com.j_spaces.core.IJSpace#ping()} method periodically
     */
    class GSPingManager extends GSThread implements ServiceMonitor {
        boolean keepAlive = true;

        /**
         * Create a ServiceLeaseManager
         */
        GSPingManager() {
            super("GSPingManager:"
                    + spaceProxy.getClass().getName()
                    + ":"
                    + System.currentTimeMillis());
            setDaemon(true);
            start();
        }

        /**
         * Its all over
         */
        public void drop() {
            if (logger.isTraceEnabled())
                logger.trace("Terminating GigaSpacesFaultDetectionHandler " +
                        "Thread");
            keepAlive = false;
            interrupt();
        }

        /**
         * Verify service can be reached. If the service cannot be reached return false
         */
        public boolean verify() {
            if (!keepAlive)
                return (false);
            boolean verified = false;
            try {
                /* Get the service's admin object. If the service isnt active
                 * we'll get a RemoteException */
                checkAvailability();
                verified = true;
            } catch (RemoteException e) {
                if (logger.isTraceEnabled())
                    logger.trace("RemoteException reaching space, " +
                            "space cannot be reached");
                keepAlive = false;
            } catch (Throwable t) {
                final int category = ThrowableConstants.retryable(t);
                if (category == ThrowableConstants.BAD_INVOCATION ||
                        category == ThrowableConstants.BAD_OBJECT) {
                    keepAlive = false;
                    if (logger.isDebugEnabled())
                        logger.debug(
                                "Unrecoverable Exception invoking " +
                                        "IJSpace.ping()",
                                t);
                }
            }
            return (verified);
        }

        @Override
        public void run() {
            while (!isInterrupted()) {
                if (!keepAlive) {
                    return;
                }
                if (logger.isTraceEnabled())
                    logger.trace("GSPingManager: Wait for " +
                            "[" + invocationDelay + "] millis " +
                            "to invoke ping() on " +
                            spaceProxy.getClass().getName());
                try {
                    sleep(invocationDelay);
                } catch (InterruptedException ie) {

                    if (logger.isTraceEnabled()) {
                        logger.trace(Thread.currentThread().getName() + " interrupted.", ie);
                    }
                    //Restore the interrupted status
                    interrupt();

                    //fall through
                } catch (IllegalArgumentException iae) {
                    logger.warn("GSPingManager: sleep time is off : "
                            + invocationDelay);
                }
                try {
                    if (logger.isTraceEnabled())
                        logger.trace("invoke ping() on : "
                                + spaceProxy.getClass().getName());
                    checkAvailability();
                } catch (Exception e) {
                    final int category = ThrowableConstants.retryable(e);
                    if (category == ThrowableConstants.BAD_INVOCATION ||
                            category == ThrowableConstants.BAD_OBJECT) {
                        keepAlive = false;
                        if (logger.isDebugEnabled())
                            logger.debug(
                                    "Unrecoverable Exception invoking " +
                                            "ping()",
                                    e);
                    }
                    /*
                     * If we failed to renew the Lease we should try and
                     * re-establish comms to the Service and get another
                     * Lease
                     */
                    if (keepAlive) {
                        if (logger.isTraceEnabled())
                            logger.trace("Failed to invoke ping() on : "
                                    + spaceProxy.getClass().getName()
                                    + ", retry [" + retryCount + "] " +
                                    "times, waiting [" + retryTimeout + "] " +
                                    "millis between attempts");
                        boolean connected = false;
                        for (int i = 0; i < retryCount; i++) {
                            long t0 = 0;
                            long t1 = 0;
                            try {
                                if (logger.isTraceEnabled())
                                    logger.trace("Attempt to invoke ping() " +
                                            "on : " +
                                            spaceProxy.getClass().getName() +
                                            ", attempt [" + i + "]");
                                t0 = System.currentTimeMillis();
                                checkAvailability();
                                if (logger.isTraceEnabled())
                                    logger.trace("Re-established connection to : " +
                                            spaceProxy.getClass().getName());
                                connected = true;
                                break;
                            } catch (Exception e1) {
                                t1 = System.currentTimeMillis();
                                if (logger.isTraceEnabled())
                                    logger.trace("Invocation attempt [" + i + "] " +
                                            "took [" + (t1 - t0) + "] " +
                                            "millis to fail for : " +
                                            spaceProxy.getClass().getName());
                                if (retryTimeout > 0) {
                                    try {
                                        sleep(retryTimeout);
                                    } catch (InterruptedException ie) {
                                        if (logger.isTraceEnabled()) {
                                            logger.trace(Thread.currentThread().getName() + " interrupted.", ie);
                                        }
                                        //Restore the interrupted status
                                        interrupt();

                                        //fall through
                                    }
                                }
                            }
                        }
                        if (!connected) {
                            if (logger.isTraceEnabled()) {
                                if (spaceProxy != null)
                                    logger.trace("Unable to invoke ping() on " +
                                            "[" + spaceProxy.getClass().getName() +
                                            "], notify listeners and exit");
                                else
                                    logger.trace("Unable to invoke ping() " +
                                            "[null spaceProxy], notify listeners " +
                                            "and exit");
                            }
                            notifyListeners();
                            break;
                        }
                    } else {
                        notifyListeners();
                        break;
                    }
                }
            }
            terminate();
            return;
        }
    }

    public Object getSpaceProxy() {
        return spaceProxy;
    }
}
