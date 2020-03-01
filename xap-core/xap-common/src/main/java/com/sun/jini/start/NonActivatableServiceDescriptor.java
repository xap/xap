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

import com.gigaspaces.logger.LogUtils;
import com.sun.jini.config.Config;

import net.jini.config.Configuration;
import net.jini.export.ProxyAccessor;
import net.jini.security.BasicProxyPreparer;
import net.jini.security.ProxyPreparer;
import net.jini.security.policy.DynamicPolicy;
import net.jini.security.policy.DynamicPolicyProvider;
import net.jini.security.policy.PolicyFileProvider;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.rmi.MarshalledObject;
import java.security.AllPermission;
import java.security.Permission;
import java.security.Policy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class used to launch shared, non-activatable, in-process services. Clients construct this object
 * with the details of the service to be launched, then call {@link #create(net.jini.config.Configuration)
 * create(Configuration config) } to launch the service in the invoking object's VM. <P> This class
 * provides separation of the import codebase (where the service implementation classes are loaded
 * from) from the export codebase (where service clients should load classes from, for example
 * stubs). as well as providing an independent security policy file for each service object. This
 * functionality allows multiple service objects to be placed in the same VM, with each object
 * maintaining distinct export codebase and security policy settings. <P> <a
 * name="serviceConstructor"> Services need to implement the following "non-activatable
 * constructor":
 * <blockquote><pre>&lt;impl&gt;(String[] args, {@link LifeCycle LifeCycle} lc)</blockquote></pre>
 * where, <UL> <LI>args - are the service configuration arguments <LI>lc   - is the hosting
 * environment's {@link LifeCycle} reference. </UL>
 *
 * <a name="serviceProxy"> A service implementation can return its service proxy (via the
 * <code>proxy</code> field of the {@link Created Created} object returned by {@link
 * #create(net.jini.config.Configuration) create}) in the following order of precedence: <OL> <LI>
 * if the service class implements {@link ServiceProxyAccessor ServiceProxyAccessor} then return
 * value of {@link ServiceProxyAccessor#getServiceProxy() getServiceProxy} will be used as the
 * service proxy. <LI> if the service class implements {@link net.jini.export.ProxyAccessor
 * ProxyAccessor} then the return value of {@link net.jini.export.ProxyAccessor#getProxy() getProxy}
 * will be used as the service proxy. <LI> Otherwise <code>null</code> will be returned as the
 * service proxy. </OL>
 *
 * The following items are discussed below: <ul> <li><a href="#configEntries">Configuring
 * NonActivatableServiceDescriptor</a> <li><a href="#logging">Logging</a> </ul>
 *
 * <a name="configEntries"> <h3>Configuring NonActivatableServiceDescriptor</h3> </a>
 *
 * <code>NonActivatableServiceDescriptor</code> depends on {@link ActivateWrapper}, which can itself
 * be configured. See {@linkplain ActivateWrapper}'s <A HREF="ActivateWrapper.html#configEntries">configuration</A>
 * information for details. <p> This implementation obtains its configuration entries from the
 * {@link net.jini.config.Configuration Configuration} object passed into the {@link
 * #create(net.jini.config.Configuration) create} method. The following configuration entries use
 * the component prefix "<code>com.sun.jini.start</code>": <p>
 *
 * <a name="servicePreparer"> <table summary="Describes the default service preparer configuration
 * entry" border="0" cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1"><code>
 * servicePreparer</code></font> <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Type:
 * <td> {@link net.jini.security.ProxyPreparer} <tr valign="top"> <td> &nbsp <th scope="row"
 * align="right"> Default: <td> <code> new {@link net.jini.security.BasicProxyPreparer}() </code>
 * <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> The default proxy
 * preparer used to prepare the <a href="#serviceProxy">service proxy</a>. This value should not be
 * <code>null</code>. This entry is obtained during the invocation of {@link
 * #create(net.jini.config.Configuration) create} and is used to prepare the service proxy returned
 * by the service implementation (see <a href="#serviceProxy">service proxy</a> section for
 * details). This entry is superseded by explicitly passing a {@link ProxyPreparer ProxyPreparer} to
 * one of the constructors that accept a {@linkplain ProxyPreparer proxy preparer} argument.
 * </table>
 *
 * <a name="logging"> <h3>Loggers and Logging Levels</h3> </a>
 *
 * This implementation uses the {@link java.util.logging.Logger}, named
 * <code>com.sun.jini.start.service.starter</code>. The following table describes the type of
 * information logged as well as the levels of information logged. <p>
 *
 * <table border="1" cellpadding="5" summary="Describes logging performed by the non-activatable,
 * service descriptor at different logging levels">
 *
 * <caption halign="center" valign="top"><b><code> com.sun.jini.start.service.starter</code></b></caption>
 *
 * <tr> <th scope="col"> Level <th scope="col"> Description
 *
 * <tr> <td> {@link java.util.logging.Level#SEVERE SEVERE} <td> for significant service creation
 * problems <tr> <td> {@link java.util.logging.Level#FINER FINER} <td> for high level service
 * operation tracing <tr> <td> {@link java.util.logging.Level#FINEST FINEST} <td> for low level
 * service operation tracing
 *
 * </table> <p>
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 */
// TODO - add discussion about how to provide a service's proxy

@com.gigaspaces.api.InternalApi
public class NonActivatableServiceDescriptor
        implements ServiceDescriptor, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The parameter types for the "activation constructor".
     */
    private static final Class[] actTypes = {
            String[].class, LifeCycle.class
    };

    /**
     * @serial <code>String</code> containing export codebase. That is, location where clients can
     * download required service-related classes (for example, stubs, proxies, etc.).
     */
    private final String codebase;

    /**
     * @serial <code>String</code> containing server policy filename or URL
     */
    private final String policy;

    /**
     * @serial <code>String</code> containing import codebase location. That is, location where
     * server implementation classes can be found.
     */
    private final String classpath;

    /**
     * @serial <code>String</code> containing the name of server implementation class
     */
    private final String implClassName;

    /**
     * @serial <code>String[]</code> containing server configuration arguments
     */
    private final String[] serverConfigArgs;

    /*
     * <code>LifeCycle</code> reference for hosting environment
     */
    private /*final*/ transient LifeCycle lifeCycle;

    /*
     * <code>ProxyPreparer</code> reference for preparing the service's proxy
     */
    private /*final*/ transient ProxyPreparer servicePreparer;

    /**
     * Flag indicating when create() has been called
     */
    protected transient boolean descCreated = false;

    /**
     * Lock object for <code>descCreated</code> flag
     */
    protected transient Object descCreatedLock = new Object();

    private static LifeCycle NoOpLifeCycle =
            new LifeCycle() { // default, no-op object
                public boolean unregister(Object impl) {
                    return false;
                }
            };

    private static AggregatePolicyProvider globalPolicy = null;
    private static Policy initialGlobalPolicy = null;
    private static final Logger logger = ServiceStarter.logger;

    public static AggregatePolicyProvider getGlobalPolicy() {
        return globalPolicy;
    }

    /**
     * Object returned by {@link NonActivatableServiceDescriptor#create(net.jini.config.Configuration)
     * NonActivatableServiceDescriptor.create()} method that returns the proxy and implementation
     * references for the created service.
     */
    public static class Created {
        /**
         * The reference to the proxy of the created service
         */
        public final Object proxy;
        /**
         * The reference to the implementation of the created service
         */
        public final Object impl;

        /**
         * Constructs an instance of this class.
         *
         * @param impl  reference to the implementation of the created service
         * @param proxy reference to the proxy of the created service
         */
        public Created(Object impl, Object proxy) {
            this.proxy = proxy;
            this.impl = impl;
        }//end constructor
    }//end class Created

    /**
     * Main constructor. Simply assigns given parameters to their associated, internal fields unless
     * otherwise noted.
     *
     * @param exportCodebase   location where clients can download required service-related classes
     *                         (for example, stubs, proxies, etc.). Codebase components must be
     *                         separated by spaces in which each component is in <code>URL</code>
     *                         format.
     * @param policy           server policy filename or URL
     * @param importCodebase   location where server implementation classes can be found. This
     *                         <code>String</code> assumed (in order) to be either 1) a space
     *                         delimited set of <code>URL</code>(s) representing a codebase or 2) a
     *                         <code>File.pathSeparator</code> delimited set of class paths.
     * @param implClassName    name of server implementation class
     * @param serverConfigArgs service configuration arguments. This parameter is passed as the
     *                         <code>String[]</code> argument to the implementation's <a
     *                         href="#serviceConstructor">constructor</a>.
     * @param lifeCycle        <code>LifeCycle</code> reference for hosting environment. This
     *                         parameter is passed as the <code>LifeCycle</code> argument to the
     *                         implementation's <a href="#serviceConstructor">constructor</a>. If
     *                         this argument is null, then a default, no-op <code>LifeCycle</code>
     *                         object will be assigned.
     * @param preparer         <code>ProxyPreparer</code> reference. This object will be used to
     *                         prepare the service's proxy object, if any (see <a
     *                         href="#serviceProxy">service proxy</a> section for details). If this
     *                         argument is null, then the default <a href="#servicePreparer">service
     *                         preparer</a> will be used.
     */
    public NonActivatableServiceDescriptor(
            // Required Args
            String exportCodebase,
            String policy,
            String importCodebase,
            String implClassName,
            // Optional Args
            String[] serverConfigArgs,
            LifeCycle lifeCycle,
            ProxyPreparer preparer) {
        if (exportCodebase == null || policy == null ||
                importCodebase == null || implClassName == null)
            throw new NullPointerException(
                    "export codebase, policy, import codebase, and"
                            + " implementation cannot be null");
        this.codebase = exportCodebase;
        this.policy = policy;
        this.classpath = importCodebase;
        this.implClassName = implClassName;
        this.serverConfigArgs = serverConfigArgs;
        this.lifeCycle =
                (lifeCycle == null) ? NoOpLifeCycle : lifeCycle;
        this.servicePreparer = preparer;
    }

    /**
     * Convenience constructor. Equivalent to calling this {@link #NonActivatableServiceDescriptor(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.String[],
     * com.sun.jini.start.LifeCycle, net.jini.security.ProxyPreparer) contructor} with
     * <code>null</code> for the <code>preparer</code> reference.
     */
    public NonActivatableServiceDescriptor(
            // Required Args
            String exportCodebase,
            String policy,
            String importCodebase,
            String implClassName,
            // Optional Args
            String[] serverConfigArgs,
            LifeCycle lifeCycle) {
        this(exportCodebase, policy, importCodebase, implClassName,
                serverConfigArgs, lifeCycle, null);
    }

    /**
     * Convenience constructor. Equivalent to calling this {@link #NonActivatableServiceDescriptor(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.String[],
     * com.sun.jini.start.LifeCycle, net.jini.security.ProxyPreparer) contructor} with
     * <code>null</code> for the <code>lifeCycle</code> and <code>preparer</code> references.
     */
    public NonActivatableServiceDescriptor(
            // Required Args
            String exportCodebase,
            String policy,
            String importCodebase,
            String implClassName,
            // Optional Args
            String[] serverConfigArgs) {
        this(exportCodebase, policy, importCodebase, implClassName,
                serverConfigArgs, null, null);
    }

    /**
     * Convenience constructor. Equivalent to calling this {@link #NonActivatableServiceDescriptor(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.String[],
     * com.sun.jini.start.LifeCycle, net.jini.security.ProxyPreparer) contructor} with
     * <code>null</code> for the <code>lifeCycle</code> reference.
     */
    public NonActivatableServiceDescriptor(
            // Required Args
            String exportCodebase,
            String policy,
            String importCodebase,
            String implClassName,
            // Optional Args
            String[] serverConfigArgs,
            ProxyPreparer preparer) {
        this(exportCodebase, policy, importCodebase, implClassName,
                serverConfigArgs, null, preparer);
    }

    /**
     * Codebase accessor method.
     *
     * @return the export codebase string associated with this service descriptor.
     */
    final public String getExportCodebase() {
        return codebase;
    }

    /**
     * Policy accessor method.
     *
     * @return the policy string associated with this service descriptor.
     */
    final public String getPolicy() {
        return policy;
    }

    /**
     * Classpath accessor method.
     *
     * @return the import codebase string associated with this service descriptor.
     */
    final public String getImportCodebase() {
        return classpath;
    }

    /**
     * Implementation class accessor method.
     *
     * @return the implementation class string associated with this service descriptor.
     */
    final public String getImplClassName() {
        return implClassName;
    }

    /**
     * Service configuration arguments accessor method.
     *
     * @return the service configuration arguments associated with this service descriptor.
     */
    final public String[] getServerConfigArgs() {
        return (serverConfigArgs != null)
                ? (String[]) serverConfigArgs.clone()
                : null;
    }

    /**
     * <code>LifeCycle</code> accessor method.
     *
     * @return the <code>LifeCycle</code> object associated with this service descriptor.
     */
    final public LifeCycle getLifeCycle() {
        return lifeCycle;
    }

    /**
     * Sets the <code>LifeCycle</code> object for this descriptor. This needs to be called on the
     * service descriptor prior to calling <code>create()</code>. Useful for (re-)setting the the
     * associated <code>LifeCycle</code> upon deserialization of this descriptor.
     *
     * @param lc The <code>LifeCycle</code> object to be associated with this service descriptor.
     * @throws IllegalStateException if called after <code>create()</code> is invoked
     */
    final public void setLifeCycle(LifeCycle lc) {
        synchronized (descCreatedLock) {
            if (descCreated) {
                throw new IllegalStateException("Can't set LifeCycle after descriptor creation");
            } else {
                lifeCycle = lc;
            }
        }
    }

    /**
     * <code>ProxyPreparer</code> accessor method.
     *
     * @return the <code>ProxyPreparer</code> object associated with this service descriptor.
     */
    final public ProxyPreparer getServicePreparer() {
        return servicePreparer;
    }

    /**
     * Sets the <code>ProxyPreparer</code> for this descriptor. This needs to be called on the
     * service descriptor prior to calling <code>create()</code>. Useful for (re-)setting the the
     * associated <code>ProxyPreparer</code> upon deserialization of this descriptor.
     *
     * @param serviceProxyPreparer The <code>ProxyPreparer</code> object to be associated with this
     *                             service descriptor.
     * @throws IllegalStateException if called after <code>create()</code> is invoked
     */
    final public void setServicePreparer(ProxyPreparer serviceProxyPreparer) {
        synchronized (descCreatedLock) {
            if (descCreated) {
                throw new IllegalStateException("Can't set ProxyPreparer after descriptor creation");
            } else {
                servicePreparer = serviceProxyPreparer;
            }
        }
    }

    /**
     * Attempts to create a service instance based on the service description information provided
     * via constructor parameters. <P> This method: <UL> <LI> installs an {@link
     * java.rmi.RMISecurityManager RMISecurityManager} if no security manager is already in place
     * <LI> installs an {@link AggregatePolicyProvider AggregatePolicyProvider} as the VM-global
     * policy object (upon the first invocation of this method) <LI> creates an
     * <code>ActivateWrapper.ExportClassLoader</code> with the associated service's import codebase,
     * export codebase and the current thread's context class loader as its arguments <LI>
     * associates the newly created <code>ExportClassLoader</code> and the associated service's
     * policy file with the {@link AggregatePolicyProvider AggregatePolicyProvider} <LI> sets the
     * newly created <code>ExportClassLoader</code> as the current thread's context class loader
     * <LI> loads the service object's class and calls a constructor with the following signature:
     * <blockquote><pre>&lt;impl&gt;(String[], LifeCycle)</blockquote></pre>
     * <LI> obtains the service proxy by calling {@link ServiceProxyAccessor#getServiceProxy()
     * ServiceProxyAccessor.getServiceProxy()} or {@link net.jini.export.ProxyAccessor#getProxy()
     * ProxyAccessor.getProxy()}, respectively, on the implementation instance. If neither interface
     * is supported, the proxy reference is set to <code>null</code> <LI> resets the context class
     * loader to the context classloader in effect when the method was called </UL>
     *
     * @return a {@link com.sun.jini.start.NonActivatableServiceDescriptor.Created Created} instance
     * with the service's proxy and implementation references.
     * @throws java.lang.Exception Thrown if there was any problem creating the object.
     */
    public Object create(Configuration config) throws Exception {
        LogUtils.entering(logger, NonActivatableServiceDescriptor.class, "create", config);
        if (config == null) {
            throw new NullPointerException(
                    "Configuration argument cannot be null");
        }
        synchronized (descCreatedLock) {
            descCreated = true;
        }

        if (servicePreparer == null) {
            servicePreparer =
                    (ProxyPreparer) Config.getNonNullEntry(config,
                            ServiceStarter.START_PACKAGE,
                            "servicePreparer", ProxyPreparer.class,
                            new BasicProxyPreparer());
        }
            
        /* Warn user of inaccessible codebase(s) */
        HTTPDStatus.httpdWarning(getExportCodebase());

        Object proxy = null;
        Object impl = null;

        // Create custom class loader that preserves codebase annotations
        Thread curThread = Thread.currentThread();
        ClassLoader oldClassLoader = curThread.getContextClassLoader();
        logger.log(Level.FINEST, "Saved current context class loader: {0}",
                oldClassLoader);
        
        /* hack to disable ActivateWrapper.ExportClassLoader - see comment below */
        ClassLoader newClassLoader = oldClassLoader;
        
/*        
 * GS-2321 ActivateWrapper.ExportClassLoader in NonActivatableServiceDescriptor of jini causes 
 * collision with ClassLoaders of DynamicSmartStub.
 * Temporary this ActivateWrapper.ExportClassLoader disabled.
  
        URLClassLoader newClassLoader = null;
        try {
            newClassLoader = 
	        new ActivateWrapper.ExportClassLoader(
		    ClassLoaderUtil.getImportCodebaseURLs(getImportCodebase()),
		    ClassLoaderUtil.getCodebaseURLs(getExportCodebase()),
		    oldClassLoader);
	    logger.log(Level.FINEST, "Created ExportClassLoader: {0}", 
		newClassLoader);
        } catch (IOException ioe) {
            logger.log(Level.SEVERE, "Could not create class loader with classpath={0} and codebase={1}",
	        new Object[] {getImportCodebase(), getExportCodebase()});
            throw ioe;
        } 
*/

        synchronized (NonActivatableServiceDescriptor.class) {
            // supplant global policy 1st time through
            if (globalPolicy == null) {
                initialGlobalPolicy = Policy.getPolicy();
                if (!(initialGlobalPolicy instanceof DynamicPolicy)) {
                    initialGlobalPolicy =
                            new DynamicPolicyProvider(initialGlobalPolicy);
                }
                globalPolicy =
                        new AggregatePolicyProvider(initialGlobalPolicy);
                Policy.setPolicy(globalPolicy);
                logger.log(Level.FINEST,
                        "Global policy set: {0}", globalPolicy);
            }

            Policy service_policy =
                    ActivateWrapper.getServicePolicyProvider(
                            new PolicyFileProvider(getPolicy()));
            Policy backstop_policy =
                    ActivateWrapper.getServicePolicyProvider(initialGlobalPolicy);
            LoaderSplitPolicyProvider split_service_policy =
                    new LoaderSplitPolicyProvider(
                            newClassLoader, service_policy, backstop_policy);
        /* Grant "this" code enough permission to do its work
         * under the service policy, which takes effect (below)
	     * after the context loader is (re)set.
	     * Note: Throws UnsupportedOperationException if dynamic grants
	     * aren't supported (because underlying policies don't support it).
	     */
            split_service_policy.grant(
                    this.getClass(),
                    null, /* Principal[] */
                    new Permission[]{new AllPermission()});
            globalPolicy.setPolicy(newClassLoader, split_service_policy);
        }

        logger.finest("Attempting to get implementation class");
        Class implClass = null;
        implClass =
                Class.forName(getImplClassName(), false, newClassLoader);
        logger.finest("Setting context class loader");
        curThread.setContextClassLoader(newClassLoader);

        try {
            logger.finest("Attempting to get implementation constructor");
            Constructor constructor =
                    implClass.getDeclaredConstructor(actTypes);
            logger.log(Level.FINEST,
                    "Obtained implementation constructor: {0}",
                    constructor);
            constructor.setAccessible(true);
            impl =
                    constructor.newInstance(
                            new Object[]{getServerConfigArgs(), lifeCycle});
            logger.log(Level.FINEST,
                    "Obtained implementation instance: {0}", impl);
            if (impl instanceof ServiceProxyAccessor) {
                proxy = ((ServiceProxyAccessor) impl).getServiceProxy();
            } else if (impl instanceof ProxyAccessor) {
                proxy = ((ProxyAccessor) impl).getProxy();
            } else {
                proxy = null; // just for insurance
            }

            logger.log(Level.FINEST, "Proxy =  {0}", proxy);
            curThread.setContextClassLoader(oldClassLoader);
//TODO - factor in code integrity for MO
            proxy = (new MarshalledObject(proxy)).get();
        } finally {
            curThread.setContextClassLoader(oldClassLoader);
        }

        if (proxy != null) {
            proxy = servicePreparer.prepareProxy(proxy);
        }
        Created created = new Created(impl, proxy);
        LogUtils.exiting(logger, NonActivatableServiceDescriptor.class,"create", created);
        return created;
    }

    /**
     * Prints out a field summary
     */
    public String toString() {
        ArrayList fields = new ArrayList(7);
        fields.add(codebase);
        fields.add(policy);
        fields.add(classpath);
        fields.add(implClassName);
        fields.add(
                ((serverConfigArgs == null)
                        ? null : Arrays.asList(serverConfigArgs)));
        fields.add(lifeCycle);
        fields.add(servicePreparer);
        return fields.toString();
    }

    /**
     * Reads the default serializable field values for this object and resets the tranisient fields
     * to legal values. Also, verifies that the deserialized values are legal.
     */
    private void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // Verify that serialized fields aren't null
        if (codebase == null) {
            throw new InvalidObjectException("null export codebase");
        }
        if (policy == null) {
            throw new InvalidObjectException("null policy");
        }
        if (classpath == null) {
            throw new InvalidObjectException("null import codebase");
        }
        if (implClassName == null) {
            throw new InvalidObjectException("null implementation class name");
        }

        //Reinitialize transient fields upon de-serialization.
        lifeCycle = NoOpLifeCycle;
        servicePreparer = null;
        descCreated = false;
        descCreatedLock = new Object();
    }

    /**
     * Throws InvalidObjectException, since data for this class is required.
     */
    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException("no data");
    }
} 

