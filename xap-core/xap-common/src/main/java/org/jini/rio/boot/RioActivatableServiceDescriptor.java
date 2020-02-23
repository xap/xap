/*
 * Copyright 2005 Sun Microsystems, Inc.
 * Copyright 2006 GigaSpaces, Inc.
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

import com.gigaspaces.logger.LogUtils;
import com.sun.jini.action.GetIntegerAction;
import com.sun.jini.config.Config;
import com.sun.jini.start.ClassLoaderUtil;
import com.sun.jini.start.HTTPDStatus;
import com.sun.jini.start.ServiceProxyAccessor;

import net.jini.config.Configuration;
import net.jini.security.BasicProxyPreparer;
import net.jini.security.ProxyPreparer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.rmi.MarshalledObject;
import java.rmi.Naming;
import java.rmi.activation.ActivationException;
import java.rmi.activation.ActivationGroupID;
import java.rmi.activation.ActivationID;
import java.rmi.activation.ActivationSystem;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class used to launch shared, activatable services. Clients construct this object with the details
 * of the service to be launched, then call <code>create</code> to launch the service in the
 * activation system group identified by the <code>sharedGroupLog</code> constructor parameter. <P>
 * This class depends on {@link ActivateWrapper} to provide separation of the import codebase (where
 * the server classes are loaded from) from the export codebase (where clients should load classes
 * from for stubs,etc.) as well as providing an independent security policy file for each service
 * object. This functionality allows multiple service objects to be placed in the same activation
 * system group, with each object maintaining a distinct codebase and policy. <P> Services need to
 * implement the constructor required by {@link ActivateWrapper}. The following items are discussed
 * below: <ul> <li><a href="#configEntries">Configuring RioActivatableServiceDescriptor </a> <li><a
 * href="#logging">Logging </a> </ul> <a name="configEntries"> <h3>Configuring
 * RioActivatableServiceDescriptor</h3> </a> This implementation of <code>RioActivatableServiceDescriptor</code>
 * supports the following configuration entries, with component <code>org.jini.rio.boot</code>:
 *
 * <table summary="Describes the activationIdPreparer configuration entry" border="0"
 * cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"><font size="+1">&#X2022;
 * </font> <th scope="col" align="left" colspan="2"><font size="+1"> <code>
 * activationIdPreparer</code> </font> <tr valign="top"> <td>&nbsp <th scope="row"
 * align="right">Type: <td>{@link net.jini.security.ProxyPreparer} <tr valign="top"> <td>&nbsp <th
 * scope="row" align="right">Default: <td><code> new {@link net.jini.security.BasicProxyPreparer}()
 * </code> <tr valign="top"> <td>&nbsp <th scope="row" align="right">Description: <td>The proxy
 * preparer for the service's activation ID. The value should not be <code>null</code>. This class
 * calls the {@link java.rmi.activation.ActivationID#activate} method on instances of {@link
 * java.rmi.activation.ActivationID} when they need to re/activate the service. </table>
 *
 * <table summary="Describes the activationSystemPreparer configuration entry" border="0"
 * cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"><font size="+1">&#X2022;
 * </font> <th scope="col" align="left" colspan="2"><font size="+1"> <code>
 * activationSystemPreparer</code> </font> <tr valign="top"> <td>&nbsp <th scope="row"
 * align="right">Type: <td>{@link net.jini.security.ProxyPreparer} <tr valign="top"> <td>&nbsp <th
 * scope="row" align="right">Default: <td><code> new {@link net.jini.security.BasicProxyPreparer}()</code>
 * <tr valign="top"> <td>&nbsp <th scope="row" align="right">Description: <td>The proxy preparer for
 * the proxy for the activation system. The value should not be <code>null</code>. The service
 * starter calls the {@link java.rmi.activation.ActivationSystem#unregisterObject unregisterObject}
 * method on the java.rmi.activation.ActivationSystem when there is a problem creating a service.
 * </table>
 *
 * <a name="logging"> <h3>Loggers and Logging Levels</h3> </a> The implementation uses the
 * java.util.logging.Logger, named <code>org.jini.rio.boot</code>. The following table describes the
 * type of information logged as well as the levels of information logged. <p> <table border="1"
 * cellpadding="5" summary="Describes logging performed by service.starter at different logging
 * levels"> <caption halign="center" valign="top"> <b><code> org.jini.rio.boot</code> </b>
 * </caption> <tr> <th scope="col">Level <th scope="col">Description <tr> <td>{@link
 * java.util.logging.Level#FINER FINER} <td>for high level service operation tracing <tr> <td>{@link
 * java.util.logging.Level#FINEST FINEST} <td>for low level service operation tracing </table>
 */
@com.gigaspaces.api.InternalApi
public class RioActivatableServiceDescriptor extends RioServiceDescriptor {
    private final String sharedGroupLog;
    private final boolean restart;
    private final String host;
    private final int port;
    private static final String GROUP_COOKIE_FILE = "cookie";
    private static final Logger logger = RioServiceDescriptor.logger;

    /**
     * Object returned by {@link RioActivatableServiceDescriptor#create(net.jini.config.Configuration)
     * RioActivatableServiceDescriptor.create()} method that returns the proxy, group identifier,
     * and activation identifier for the created service.
     */
    public static class Created {
        /**
         * The activation group id of the service
         */
        public final ActivationGroupID gid;
        /**
         * The activation id of the service
         */
        public final ActivationID aid;
        /**
         * The reference to the proxy of the created service
         */
        public final Object proxy;

        /**
         * Constructs an instance of this class.
         *
         * @param gid   activation group id of the created service
         * @param aid   activation id of the created service
         * @param proxy reference to the proxy of the created service
         */
        public Created(ActivationGroupID gid, ActivationID aid, Object proxy) {
            this.gid = gid;
            this.aid = aid;
            this.proxy = proxy;
        }//end constructor
    }//end class Created

    /**
     * Create a RioActivatableServiceDescriptor, calling the other overloaded constructor with the
     * <code>host</code> and <code>port</code> parameters set to <code>null</code> and 0,
     * respectively.
     */
    public RioActivatableServiceDescriptor(String name, String codebase,
                                           String policy,
                                           String classpath,
                                           String implClassName,
                                           String sharedGroupLog,
                                           // Optional Args,
                                           String[] serverConfigArgs,
                                           boolean restart) {
        this(name, codebase,
                policy,
                classpath,
                implClassName,
                sharedGroupLog,
                serverConfigArgs,
                restart,
                null,
                getDefaultActivationSystemPort());
    }

    /**
     * Create a RioActivatableServiceDescriptor, assigning given parameters to their associated,
     * internal fields.
     *
     * @param codebase         location where clients can download required service-related classes
     *                         (for example, stubs, proxies, etc.). Codebase components must be
     *                         separated by spaces in which each component is in <code>URL</code>
     *                         format.
     * @param policy           server policy filename or URL
     * @param classpath        location where server implementation classes can be found. Classpath
     *                         components must be separated by path separators.
     * @param implClassName    name of server implementation class
     * @param serverConfigArgs service configuration arguments
     * @param restart          boolean flag passed through as the <code>restart</code> parameter to
     *                         the {@linkplain java.rmi.activation.ActivationDesc#ActivationDesc(java.rmi.activation.ActivationGroupID,
     *                         java.lang.String, java.lang.String, java.rmi.MarshalledObject,
     *                         boolean) ActivationDesc constructor} used to register the service
     *                         with the activation system.
     * @param host             hostname of desired activation system. If <code>null</code>, defaults
     *                         to the localhost.
     * @param port             port of desired activation system. If value is <= 0, then defaults to
     *                         {@link java.rmi.activation.ActivationSystem#SYSTEM_PORT
     *                         ActivationSystem.SYSTEM_PORT}.
     */
    public RioActivatableServiceDescriptor(String name,
                                           String codebase,
                                           String policy,
                                           String classpath,
                                           String implClassName,
                                           String sharedGroupLog,
                                           // Optional Args,
                                           String[] serverConfigArgs,
                                           boolean restart,
                                           String host,
                                           int port) {
        super(name,
                codebase,
                policy,
                classpath,
                implClassName,
                serverConfigArgs,
                null);
        if (sharedGroupLog == null)
            throw new NullPointerException("Shared VM log cannot be null");
        this.sharedGroupLog = sharedGroupLog;
        this.restart = restart;
        this.host = (host == null) ? "" : host;
        this.port = (port <= 0) ? getDefaultActivationSystemPort() : port;
    }

    /**
     * Shared group log accessor method.
     *
     * @return The Shared group log associated with this service descriptor.
     */
    final public String getSharedGroupLog() {
        return sharedGroupLog;
    }

    /**
     * Restart accessor method.
     *
     * @return The restart mode associated with this service descriptor.
     */
    final public boolean getRestart() {
        return restart;
    }

    /**
     * Activation system host accessor method.
     *
     * @return The activation system host associated with this service descriptor.
     */
    final public String getActivationSystemHost() {
        return host;
    }

    /**
     * Activation system port accessor method.
     *
     * @return The activation system port associated with this service descriptor.
     */
    final public int getActivationSystemPort() {
        return port;
    }

    /**
     * Method that attempts to create a service based on the service description information
     * provided via constructor parameters. <P> This method: <UL> <LI>creates an
     * <code>ActivateWrapper.ActivateDesc</code> with the provided constructor parameter information
     * <LI>retrieves the java.rmi.activation.ActivationGroupID group identifie} associated with the
     * provided shared group log. <LI>invokes ActivateWrapper.register()with the provided
     * information. <LI>obtains an "inner" proxy by calling {@link java.rmi.activation.ActivationID#activate(boolean)
     * ActivationID#activate(true)} on the object returned from <code>ActivateWrapper.register()</code>,
     * which also activates the service instance. <LI>The service proxy is obtained by calling
     * {@link ServiceProxyAccessor#getServiceProxy() ServiceProxyAccessor.getServiceProxy()} on the
     * "inner" proxy instance. Otherwise, the "inner" proxy reference is used in the return value.
     * </UL>
     *
     * @return A <code>org.jini.rio.boot.RioActivatableServiceDescriptor.Created </code> object that
     * contains the group identifier, activation ID, and proxy associated with the newly created
     * service instance.
     */
    public Object create(Configuration config) throws Exception {
        LogUtils.entering(logger, RioActivatableServiceDescriptor.class, "create", config);
        if (config == null) {
            throw new NullPointerException("Configuration argument cannot be null");
        }        
        
        /* Warn user of inaccessible codebase(s) */
        HTTPDStatus.httpdWarning(getCodebase());

        URL[] defaultCommonJARs = null;
        String rioHome = System.getProperty("RIO_HOME");
        if (rioHome == null) {
            defaultCommonJARs = new URL[0];
            if (logger.isLoggable(Level.FINEST))
                logger.finest("RIO_HOME not defined, defaultCommonJARs " +
                        "set to zero-length array");
        } else {
            File rio = new File(rioHome + File.separator + "lib" +
                    File.separator + "rio.jar");
            defaultCommonJARs = new URL[]{rio.toURL()};
        }
        URL[] commonJARs = (URL[]) config.getEntry(COMPONENT,
                "commonJARs",
                URL[].class,
                defaultCommonJARs);

        if (commonJARs.length == 0)
            throw new RuntimeException("No commonJARs have been defined");

        // Get prepared activation system reference
        Created created = null;
        ActivationSystem sys = getActivationSystem(getActivationSystemHost(),
                getActivationSystemPort(),
                config);
        ProxyPreparer activationIDPreparer =
                (ProxyPreparer) Config.getNonNullEntry(config,
                        COMPONENT,
                        "activationIdPreparer",
                        ProxyPreparer.class,
                        new BasicProxyPreparer());
        ProxyPreparer servicePreparer =
                (ProxyPreparer) Config.getNonNullEntry(config,
                        COMPONENT,
                        "servicePreparer",
                        ProxyPreparer.class,
                        new BasicProxyPreparer());
        /* Warn user of inaccessible codebase(s) */
        HTTPDStatus.httpdWarning(getCodebase());
        ActivationGroupID gid = null;
        ActivationID aid = null;
        Object proxy = null;
        try {
            /* Create the ActivateWrapper descriptor for the desired service */
            MarshalledObject params = new MarshalledObject(getServerConfigArgs());
            ActivateWrapper.ActivateDesc adesc =
                    new ActivateWrapper.ActivateDesc(
                            getImplClassName(),
                            ClassLoaderUtil.getClasspathURLs(getClasspath()),
                            ClassLoaderUtil.getCodebaseURLs(getCodebase()),
                            commonJARs,
                            getPolicy(),
                            params);
            logger.finest("ActivateDesc: " + adesc);
            // Get hosting activation group
            gid = restoreGroupID(getSharedGroupLog());
            /* Register the desired service with the activation system */
            aid = ActivateWrapper.register(gid, adesc, getRestart(), sys);
            aid = (ActivationID) activationIDPreparer.prepareProxy(aid);
            proxy = aid.activate(true);
            if (proxy != null) {
                proxy = servicePreparer.prepareProxy(proxy);
                if (proxy instanceof ServiceProxyAccessor) {
                    proxy = ((ServiceProxyAccessor) proxy).getServiceProxy();
                    proxy = servicePreparer.prepareProxy(proxy);
                }
            }//endif
        } catch (Exception e) {
            try {
                if (aid != null)
                    sys.unregisterObject(aid);
            } catch (Exception ee) {
                // ignore -- did the best we could.
                logger.log(Level.FINEST,
                        "Unable to unregister with activation system",
                        ee);
            }
            if (e instanceof IOException)
                throw (IOException) e;
            else if (e instanceof ActivationException)
                throw (ActivationException) e;
            else if (e instanceof ClassNotFoundException)
                throw (ClassNotFoundException) e;
            else
                throw new RuntimeException("Unexpected Exception", e);
        }
        created = new Created(gid, aid, proxy);
        LogUtils.exiting(logger, RioActivatableServiceDescriptor.class, "create", created);
        return created;
    }

    /**
     * Utility routine that returns a "prepared" activation system proxy for a system at the given
     * <code>host</code> and <code>port</code>.
     *
     * @param host   The host of the desired activation system
     * @param port   The port of the desired activation system
     * @param config The <code>Configuration</code> used to prepare the system proxy.
     * @return A prepared activation system proxy
     * @throws ActivationException If there was a problem communicating with the activation system.
     * @see net.jini.config.Configuration
     */
    static ActivationSystem getActivationSystem(String host, int port,
                                                Configuration config)
            throws ActivationException {
        if (config == null) {
            throw new NullPointerException("Configuration argument cannot be null");
        }
        ActivationSystem sys = null;
        final String h = (host == null) ? "" : host;
        final int p = (port <= 0) ? getDefaultActivationSystemPort() : port;
        try {
            sys = (ActivationSystem) Naming.lookup(
                    "//" + h + ":" + p +
                            "/java.rmi.activation.ActivationSystem");
            ProxyPreparer activationSystemPreparer =
                    (ProxyPreparer) Config.getNonNullEntry(config,
                            COMPONENT,
                            "activationSystemPreparer",
                            ProxyPreparer.class,
                            new BasicProxyPreparer());
            sys = (ActivationSystem) activationSystemPreparer.prepareProxy(sys);
        } catch (Exception e) {
            throw new ActivationException("ActivationSystem @ "
                    + host
                    + ":"
                    + port
                    + " could not be obtained", e);
        }
        return sys;
    }

    /**
     * Utility routine that returns a "default" activation system port. The default port is
     * determined by: <UL> <LI> the value of the <code>java.rmi.activation.port</code> system
     * property, if set <LI> the value of <code>ActivationSystem.SYSTEM_PORT</code> </UL>
     *
     * @return The activation system port
     * @see java.rmi.activation.ActivationSystem
     */
    static int getDefaultActivationSystemPort() {
        return ((Integer) java.security.AccessController.doPrivileged(
                new GetIntegerAction("java.rmi.activation.port",
                        ActivationSystem.SYSTEM_PORT))).intValue();
    }

    /**
     * Utility method that restores the object stored in a well known file under the provided
     * <code>dir</code> path.
     */
    static ActivationGroupID restoreGroupID(final String dir)
            throws IOException, ClassNotFoundException {
        File log = new File(dir);
        String absDir = log.getAbsolutePath();
        if (!log.exists() || !log.isDirectory()) {
            throw new IOException("Log directory ["
                    + absDir
                    + "] does not exist.");
        }
        File cookieFile = new File(log, GROUP_COOKIE_FILE);
        ObjectInputStream ois = null;
        ActivationGroupID obj = null;
        try {
            //TODO - lock out strategy for concurrent r/w file access
            ois = new ObjectInputStream(
                    new BufferedInputStream(
                            new FileInputStream(cookieFile)));
            MarshalledObject mo = (MarshalledObject) ois.readObject();
            obj = (ActivationGroupID) mo.get();
        } finally {
            if (ois != null)
                ois.close();
        }
        return obj;
    }

    public String toString() {
        // Would like to call super(), but need different formatting
        ArrayList fields = new ArrayList(10);
        fields.add(getCodebase());
        fields.add(getPolicy());
        fields.add(getClasspath());
        fields.add(getImplClassName());
        fields.add(Arrays.asList(getServerConfigArgs()));
        fields.add(getLifeCycle());
        fields.add(sharedGroupLog);
        fields.add(Boolean.valueOf(restart));
        fields.add(host);
        fields.add(new Integer(port));
        return fields.toString();
    }
}