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
package com.sun.jini.start;

import com.sun.jini.config.Config;

import net.jini.config.Configuration;
import net.jini.security.BasicProxyPreparer;
import net.jini.security.ProxyPreparer;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.rmi.MarshalledObject;
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
 * of the service to be launched, then call {@link #create(net.jini.config.Configuration)
 * create(Configuration config) } to launch the service in an existing {@linkplain
 * SharedActivationGroupDescriptor shared activation group} identified by the
 * <code>sharedGroupLog</code> constructor parameter. <P> This class depends on {@link
 * ActivateWrapper} to provide separation of the import codebase (where the service implementation
 * classes are loaded from) from the export codebase (where service clients should load classes
 * from, for example stubs) as well as providing an independent security policy file for each
 * service object. This functionality allows multiple service objects to be placed in the same
 * activation system group, with each object maintaining distinct export codebase and security
 * policy settings. <P> Services need to implement the "activatable" <A
 * HREF="ActivateWrapper.html#serviceConstructor">constructor</A> required by {@link
 * ActivateWrapper}. <P> <A NAME="serviceProxy"></A> A service implementation can return its service
 * proxy (via the <code>proxy</code> field of the {@link Created Created} object returned by {@link
 * #create(net.jini.config.Configuration) create}) in the following ways: <DL> <DT><A
 * NAME="innerProxy">Return Inner Proxy</A></DT> <DD><P> The service's inner proxy is the {@link
 * java.rmi.Remote} object returned from {@link java.rmi.activation.ActivationID#activate(boolean)}
 * using {@link ActivateWrapper} to "wrap" and register the desired service with the activation
 * system. A "wrapped" service's inner proxy is returned as follows:
 *
 * <UL> <LI>If the newly created service instance implements {@link net.jini.export.ProxyAccessor},
 * a proxy is obtained by invoking the {@link net.jini.export.ProxyAccessor#getProxy getProxy}
 * method on that instance. If the obtained proxy is not <code>null</code>, that proxy is returned
 * in a {@link java.rmi.MarshalledObject}; otherwise, an {@link java.io.InvalidObjectException} is
 * thrown.
 *
 * <LI>If the newly created instance does not implement {@link net.jini.export.ProxyAccessor}, the
 * instance is returned in a {@link java.rmi.MarshalledObject}.  In this case, the instance must be
 * serializable, and marshalling the instance must produce a suitable proxy for the remote object
 * (for example, the object implements {@link java.io.Serializable} and defines a
 * <code>writeReplace</code> method that returns the object's proxy). </UL>
 *
 * <P></DD> <DT><A NAME="outerProxy">Return Outer Proxy</A></DT> <DD><P> The service's outer proxy
 * is the object returned from invoking {@link ServiceProxyAccessor#getServiceProxy()} on the
 * service's <A HREF="#innerProxy">inner proxy</A>. <P></DD> </DL>
 *
 * The following items are discussed below: <ul> <li><a href="#configEntries">Configuring
 * SharedActivatableServiceDescriptor</a> <li><a href="#logging">Logging</a> </ul>
 *
 * <a name="configEntries"> <h3>Configuring SharedActivatableServiceDescriptor</h3> </a>
 *
 * <code>SharedActivatableServiceDescriptor</code> depends on {@link ActivateWrapper}, which can
 * itself be configured. See {@linkplain ActivateWrapper}'s <A HREF="ActivateWrapper.html#configEntries">configuration</A>
 * information for details. <p> This implementation obtains its configuration entries from the
 * {@link net.jini.config.Configuration Configuration} object passed into the {@link
 * #create(net.jini.config.Configuration) create} method. The following configuration entries use
 * the component prefix "<code>com.sun.jini.start</code>": <p>
 *
 * <table summary="Describes the activationIdPreparer configuration entry" border="0"
 * cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1"><code>
 * activationIdPreparer</code></font> <tr valign="top"> <td> &nbsp <th scope="row" align="right">
 * Type: <td> {@link net.jini.security.ProxyPreparer} <tr valign="top"> <td> &nbsp <th scope="row"
 * align="right"> Default: <td> <code> new {@link net.jini.security.BasicProxyPreparer}() </code>
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> The proxy preparer
 * for the service's activation ID. The value should not be <code>null</code>.
 *
 * This class calls the {@link java.rmi.activation.ActivationID#activate activate} method on
 * instances of {@link java.rmi.activation.ActivationID} when they need to re/activate the service.
 * </table>
 *
 * <table summary="Describes the activationSystemPreparer configuration entry" border="0"
 * cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1"><code>
 * activationSystemPreparer</code></font> <tr valign="top"> <td> &nbsp <th scope="row"
 * align="right"> Type: <td> {@link net.jini.security.ProxyPreparer} <tr valign="top"> <td> &nbsp
 * <th scope="row" align="right"> Default: <td> <code> new {@link net.jini.security.BasicProxyPreparer}()</code>
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> The proxy preparer
 * for the proxy for the activation system. The value should not be <code>null</code>.
 *
 * The service starter calls the {@link java.rmi.activation.ActivationSystem#unregisterObject
 * unregisterObject} method on the {@link java.rmi.activation.ActivationSystem} when there is a
 * problem creating a service. </table>
 *
 * <a name="servicePreparer"> <table summary="Describes the default service preparer configuration
 * entry" border="0" cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1"><code>
 * servicePreparer</code></font> <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type:
 * <td> {@link net.jini.security.ProxyPreparer} <tr valign="top"> <td> &nbsp <th scope="row"
 * align="right"> Default: <td> <code> new {@link net.jini.security.BasicProxyPreparer}() </code>
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> The default proxy
 * preparer used to prepare service proxies. This value should not be <code>null</code>. This entry
 * is obtained during the invocation of {@link #create(net.jini.config.Configuration) create} and is
 * used, to prepare the <A HREF="#innerProxy">inner</A> and <A HREF="#outerProxy">outer</A> service
 * proxies returned by the service implementation (see <a href="#serviceProxy">service proxy</a>
 * section for details). This entry is superseded by explicitly passing a {@link ProxyPreparer
 * ProxyPreparer} to one of the constructors that accept a {@linkplain ProxyPreparer proxy preparer}
 * argument. </table>
 *
 * <a name="logging"> <h3>Loggers and Logging Levels</h3> </a>
 *
 * The implementation uses the {@link java.util.logging.Logger}, named
 * <code>com.sun.jini.start.service.starter</code>. The following table describes the type of
 * information logged as well as the levels of information logged. <p>
 *
 * <table border="1" cellpadding="5" summary="Describes logging performed by the shared, activatable
 * service descriptor at different logging levels">
 *
 * <caption halign="center" valign="top"><b><code> com.sun.jini.start.service.starter</code></b></caption>
 *
 * <tr> <th scope="col"> Level <th scope="col"> Description
 *
 * <tr> <td> {@link java.util.logging.Level#FINER FINER} <td> for high level service operation
 * tracing <tr> <td> {@link java.util.logging.Level#FINEST FINEST} <td> for low level service
 * operation tracing
 *
 * </table> <p>
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */

@com.gigaspaces.api.InternalApi
public class SharedActivatableServiceDescriptor
        extends NonActivatableServiceDescriptor {
    private static final long serialVersionUID = 1L;

    /**
     * @serial <code>String</code> representing the directory where the associated shared group
     * identifier information is persisted
     */
    private final String sharedGroupLog;

    /**
     * @serial <code>boolean</code> flag passed through as the <code>restart</code> parameter to the
     * {@linkplain java.rmi.activation.ActivationDesc#ActivationDesc(java.rmi.activation.ActivationGroupID,
     * java.lang.String, java.lang.String, java.rmi.MarshalledObject, boolean) ActivationDesc
     * constructor} used to register the service with the activation system.
     */
    private final boolean restart;

    /**
     * @serial <code>String</code> containing hostname of desired activation system.
     */
    private final String host;

    /**
     * @serial <code>int</code> representing port of desired activation system.
     */
    private final int port;

    private static final Logger logger = ServiceStarter.logger;

    /*
     * <code>ProxyPreparer</code> reference for preparing the service's proxy
     * returned from the call to 
     * {@linkplain java.rmi.activation.ActivationID#activate(boolean) activate}.
     * If this "inner" proxy implements
     * {@linkplain ServiceProxyAccessor ServiceProxyAccessor} it will then be used to invoke
     * {@linkplain ServiceProxyAccessor#getServiceProxy() getServiceProxy}.
     */
    private /*final*/ transient ProxyPreparer innerProxyPreparer;

    /**
     * Object returned by {@link SharedActivatableServiceDescriptor#create(net.jini.config.Configuration)
     * SharedActivatableServiceDescriptor.create()} method that returns the associated proxy,
     * activation group identifier, and activation identifier for the created service.
     */
    public static class Created {
        /**
         * The activation group id of the group hosting the service
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
     * Convenience constructor. Equivalent to calling this {@link #SharedActivatableServiceDescriptor(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String[],
     * net.jini.security.ProxyPreparer, net.jini.security.ProxyPreparer, boolean, java.lang.String,
     * int) constructor} with the <code>innerProxyPreparer</code>, <code>outerProxyPreparer</code>
     * and <code>host</code> arguments set to <code>null</code> and the <code>port</code> argument
     * set to the currently configured activation system port. The activation system port defaults
     * to {@link ActivationSystem#SYSTEM_PORT} unless it is overridden by the
     * <code>java.rmi.activation.port</code> system property.
     */
    public SharedActivatableServiceDescriptor(
            // Required Args
            String exportCodebase,
            String policy,
            String importCodebase,
            String implClassName,
            String sharedGroupLog,
            // Optional Args,
            String[] serverConfigArgs,
            boolean restart) {
        this(exportCodebase, policy, importCodebase, implClassName,
                sharedGroupLog, serverConfigArgs, null,
                null, restart, null, ServiceStarter.getActivationSystemPort());
    }

    /**
     * Convenience constructor. Equivalent to calling this {@link #SharedActivatableServiceDescriptor(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String[],
     * net.jini.security.ProxyPreparer, net.jini.security.ProxyPreparer, boolean, java.lang.String,
     * int) constructor} with the <code>host</code> argument set to <code>null</code> and the
     * <code>port</code> argument set to the currently configured activation system port. The
     * activation system port defaults to {@link ActivationSystem#SYSTEM_PORT} unless it is
     * overridden by the <code>java.rmi.activation.port</code> system property.
     */
    public SharedActivatableServiceDescriptor(
            // Required Args
            String exportCodebase,
            String policy,
            String importCodebase,
            String implClassName,
            String sharedGroupLog,
            // Optional Args,
            String[] serverConfigArgs,
            ProxyPreparer innerProxyPreparer,
            ProxyPreparer outerProxyPreparer,
            boolean restart) {
        this(exportCodebase, policy, importCodebase, implClassName,
                sharedGroupLog, serverConfigArgs, innerProxyPreparer,
                outerProxyPreparer, restart, null, ServiceStarter.getActivationSystemPort());
    }

    /**
     * Convenience constructor. Equivalent to calling this {@link #SharedActivatableServiceDescriptor(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String[],
     * net.jini.security.ProxyPreparer, net.jini.security.ProxyPreparer, boolean, java.lang.String,
     * int) constructor} with the <code>innerProxyPreparer</code> and <code>outerProxyPreparer</code>
     * arguments set to <code>null</code>.
     */
    public SharedActivatableServiceDescriptor(
            // Required Args
            String exportCodebase,
            String policy,
            String importCodebase,
            String implClassName,
            String sharedGroupLog,
            // Optional Args,
            String[] serverConfigArgs,
            boolean restart,
            String host,
            int port) {
        this(exportCodebase, policy, importCodebase, implClassName,
                sharedGroupLog, serverConfigArgs, null, null, restart,
                host, port);
    }

    /**
     * Main constructor. Simply assigns given parameters to their associated, internal fields unless
     * otherwise noted.
     *
     * @param exportCodebase     location where clients can download required service-related
     *                           classes (for example, stubs, proxies, etc.). Codebase components
     *                           must be separated by spaces in which each component is in
     *                           <code>URL</code> format.
     * @param policy             server policy filename or URL
     * @param importCodebase     location where server implementation classes can be found. This
     *                           <code>String</code> assumed (in order) to be either 1) a space
     *                           delimited set of <code>URL</code>(s) representing a codebase or 2)
     *                           a <code>File.pathSeparator</code> delimited set of class paths.
     * @param implClassName      name of server implementation class
     * @param sharedGroupLog     location where the associated shared group identifier information
     *                           is persisted
     * @param serverConfigArgs   service configuration arguments
     * @param innerProxyPreparer <code>ProxyPreparer</code> reference. This object will be used to
     *                           prepare the service's <A HREF="#innerProxy">inner proxy</A>. If the
     *                           inner proxy implements {@link ServiceProxyAccessor} it will then be
     *                           used to invoke {@link ServiceProxyAccessor#getServiceProxy()} in
     *                           order to get the service's <A HREF="#outerProxy">outer proxy</A>.
     * @param outerProxyPreparer <code>ProxyPreparer</code> reference. This object will be used to
     *                           prepare the service's <A HREF="#outerProxy">outer proxy</A> before
     *                           it is returned to the caller of {@link SharedActivatableServiceDescriptor#create(net.jini.config.Configuration)}.
     * @param restart            boolean flag passed through as the <code>restart</code> parameter
     *                           to the {@linkplain java.rmi.activation.ActivationDesc#ActivationDesc(java.rmi.activation.ActivationGroupID,
     *                           java.lang.String, java.lang.String, java.rmi.MarshalledObject,
     *                           boolean) ActivationDesc constructor} used to register the service
     *                           with the activation system.
     * @param host               hostname of desired activation system. If <code>null</code>,
     *                           defaults to the localhost.
     * @param port               port of desired activation system. If value is <= 0, then defaults
     *                           to {@link java.rmi.activation.ActivationSystem#SYSTEM_PORT
     *                           ActivationSystem.SYSTEM_PORT}.
     */
    public SharedActivatableServiceDescriptor(
            // Required Args
            String exportCodebase,
            String policy,
            String importCodebase,
            String implClassName,
            String sharedGroupLog,
            // Optional Args,
            String[] serverConfigArgs,
            ProxyPreparer innerProxyPreparer,
            ProxyPreparer outerProxyPreparer,
            boolean restart,
            String host,
            int port) {
        super(exportCodebase, policy, importCodebase, implClassName,
                serverConfigArgs, outerProxyPreparer);
        if (sharedGroupLog == null)
            throw new NullPointerException(
                    "Shared VM log cannot be null");
        this.sharedGroupLog = sharedGroupLog;
        this.restart = restart;
        this.host = (host == null) ? "" : host;
        this.port =
                (port <= 0) ? ServiceStarter.getActivationSystemPort() : port;
        this.innerProxyPreparer = innerProxyPreparer;
    }

    /**
     * Shared group log accessor method.
     *
     * @return the Shared group log associated with this service descriptor.
     */
    final public String getSharedGroupLog() {
        return sharedGroupLog;
    }

    /**
     * Restart accessor method.
     *
     * @return the restart mode associated with this service descriptor.
     */
    final public boolean getRestart() {
        return restart;
    }

    /**
     * Activation system host accessor method.
     *
     * @return the activation system host associated with this service descriptor.
     */
    final public String getActivationSystemHost() {
        return host;
    }

    /**
     * Activation system port accessor method.
     *
     * @return the activation system port associated with this service descriptor.
     */
    final public int getActivationSystemPort() {
        return port;
    }

    /**
     * <code>ProxyPreparer</code> accessor method.
     *
     * @return the <A HREF="#innerProxy">inner proxy</A> preparer associated with this service
     * descriptor.
     */
    final public ProxyPreparer getInnerProxyPreparer() {
        return innerProxyPreparer;
    }

    /**
     * Sets the <A HREF="#innerProxy">inner</A> <code>ProxyPreparer</code> for this descriptor. This
     * needs to be called on the service descriptor prior to calling <code>create()</code>. Useful
     * for (re-)setting the the associated inner <code>ProxyPreparer</code> upon deserialization of
     * the descriptor.
     *
     * @param pp The inner <code>ProxyPreparer</code> object to be associated with this service
     *           descriptor.
     * @throws IllegalStateException if called after <code>create()</code> is invoked
     */
    final public void setInnerProxyPreparer(ProxyPreparer pp) {
        synchronized (descCreatedLock) {
            if (descCreated) {
                throw new IllegalStateException("Can't set ProxyPreparer after descriptor creation");
            } else {
                innerProxyPreparer = pp;
            }
        }
    }

    /**
     * Method that attempts to create a service based on the service description information
     * provided via constructor parameters. <P> This method: <UL> <LI> creates an
     * <code>ActivateWrapper.ActivateDesc</code> with the provided constructor parameter information
     * for the desired service <LI> retrieves the {@linkplain java.rmi.activation.ActivationGroupID
     * group identifier} associated with the provided shared group log. <LI> invokes {@link
     * ActivateWrapper#register(java.rmi.activation.ActivationGroupID,
     * com.sun.jini.start.ActivateWrapper.ActivateDesc, boolean, java.rmi.activation.ActivationSystem)
     * ActivateWrapper.register()} with the provided information. <LI> obtains an <A
     * HREF="#innerProxy">inner proxy</A> by calling {@link java.rmi.activation.ActivationID#activate(boolean)
     * activate(true)} on the object returned from <code>ActivateWrapper.register()</code>, which
     * also activates the service instance. <LI> obtains the service proxy in the following order of
     * precedence: <UL> <LI>if the <A HREF="#innerProxy">inner proxy</A> implements {@link
     * ServiceProxyAccessor} then the return value of {@link ServiceProxyAccessor#getServiceProxy()
     * getServiceProxy} is used <LI>if the <A HREF="#innerProxy">inner proxy</A> is not
     * <code>null</code> then it is used <LI>Otherwise, <code>null</code> is used </UL>
     *
     * @return a {@link com.sun.jini.start.SharedActivatableServiceDescriptor.Created Created}
     * object that contains the group identifier, activation ID, and proxy associated with the newly
     * created service instance.
     * @throws java.lang.Exception Thrown if there was any problem creating the object.
     */
    public Object create(Configuration config) throws Exception {
        logger.entering(SharedActivatableServiceDescriptor.class.getName(),
                "create", new Object[]{config});

        if (config == null) {
            throw new NullPointerException(
                    "Configuration argument cannot be null");
        }

        ProxyPreparer globalServicePreparer =
                (ProxyPreparer) Config.getNonNullEntry(config,
                        ServiceStarter.START_PACKAGE,
                        "servicePreparer", ProxyPreparer.class,
                        new BasicProxyPreparer());

        // Needs to be called prior to setting the "descCreated" flag
        if (getServicePreparer() == null) {
            setServicePreparer(globalServicePreparer);
        }

        synchronized (descCreatedLock) {
            descCreated = true;
        }

        // Get prepared activation system reference
        Created created = null;
        ActivationSystem sys =
                ServiceStarter.getActivationSystem(
                        getActivationSystemHost(),
                        getActivationSystemPort(),
                        config);
        ProxyPreparer activationIDPreparer =
                (ProxyPreparer) Config.getNonNullEntry(config,
                        ServiceStarter.START_PACKAGE, "activationIdPreparer",
                        ProxyPreparer.class, new BasicProxyPreparer());

        if (innerProxyPreparer == null) {
            innerProxyPreparer = globalServicePreparer;
        }
        
	/* Warn user of inaccessible codebase(s) */
        HTTPDStatus.httpdWarning(getExportCodebase());

        ActivationGroupID gid = null;
        ActivationID aid = null;
        Object proxy = null;
        try {
            /* Create the ActivateWrapper descriptor for the desired service */
            MarshalledObject params =
                    new MarshalledObject(getServerConfigArgs());
            ActivateWrapper.ActivateDesc adesc =
                    new ActivateWrapper.ActivateDesc(
                            getImplClassName(),
                            ClassLoaderUtil.getImportCodebaseURLs(getImportCodebase()),
                            ClassLoaderUtil.getCodebaseURLs(getExportCodebase()),
                            getPolicy(),
                            params);
            logger.finest("ActivateDesc: " + adesc);
            // Get hosting activation group
            gid = SharedActivationGroupDescriptor.restoreGroupID(
                    getSharedGroupLog());

            /* Register the desired service with the activation system */
            aid = ActivateWrapper.register(
                    gid, adesc, getRestart(), sys);
            aid = (ActivationID) activationIDPreparer.prepareProxy(aid);

            proxy = aid.activate(true);

            if (proxy != null) {
                proxy = innerProxyPreparer.prepareProxy(proxy);
                if (proxy instanceof ServiceProxyAccessor) {
                    proxy = ((ServiceProxyAccessor) proxy).getServiceProxy();
                    if (proxy != null) {
                        proxy = getServicePreparer().prepareProxy(proxy);
                    } else {
                        logger.log(Level.FINE,
                                "Service's getServiceProxy() returned null");
                    }
                }
            }//endif
        } catch (Exception e) {
            try {
                if (aid != null) sys.unregisterObject(aid);
            } catch (Exception ee) {
                // ignore -- did the best we could.
                logger.log(Level.FINEST,
                        "Unable to unregister with activation system", ee);
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
        logger.exiting(SharedActivatableServiceDescriptor.class.getName(),
                "create", created);
        return created;
    }

    @Override
    public String toString() {
        // Would like to call super(), but need different formatting
        ArrayList fields = new ArrayList(12);
        fields.add(getExportCodebase());
        fields.add(getPolicy());
        fields.add(getImportCodebase());
        fields.add(getImplClassName());
        fields.add(
                ((getServerConfigArgs() == null)
                        ? null : Arrays.asList(getServerConfigArgs())));
        fields.add(getLifeCycle());
        fields.add(getServicePreparer());
        fields.add(getInnerProxyPreparer());
        fields.add(sharedGroupLog);
        fields.add(Boolean.valueOf(restart));
        fields.add(host);
        fields.add(new Integer(port));
        return fields.toString();
    }

    /**
     * Reads the default serializable field values for this object. Also, verifies that the
     * deserialized values are legal.
     */
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // Verify serialized fields
        if (sharedGroupLog == null) {
            throw new InvalidObjectException("null shared group log");
        }
        if (host == null) {
            throw new InvalidObjectException("null activation host name");
        }
        if (port <= 0) {
            throw new InvalidObjectException("invalid activation port value: " + port);
        }

        //Reinitialize transient fields upon de-serialization.
        innerProxyPreparer = null;
    }

    /**
     * Throws InvalidObjectException, since data for this class is required.
     */
    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException("no data");
    }
}


