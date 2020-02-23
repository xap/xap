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
import net.jini.export.ProxyAccessor;
import net.jini.id.Uuid;
import net.jini.id.UuidFactory;
import net.jini.loader.pref.PreferredClassLoader;
import net.jini.security.policy.DynamicPolicy;
import net.jini.security.policy.DynamicPolicyProvider;
import net.jini.security.policy.PolicyFileProvider;

import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.rmi.MarshalException;
import java.rmi.MarshalledObject;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.activation.ActivationDesc;
import java.rmi.activation.ActivationException;
import java.rmi.activation.ActivationGroupID;
import java.rmi.activation.ActivationID;
import java.rmi.activation.ActivationSystem;
import java.security.AccessController;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.Policy;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.security.Security;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A wrapper for activatable objects, providing separation of the import codebase (where the server
 * classes are loaded from by the activation group) from the export codebase (where clients should
 * load classes from for stubs, etc.) as well as providing an independent security policy file for
 * each activatable object. This functionality allows multiple activatable objects to be placed in
 * the same activation group, with each object maintaining a distinct codebase and policy. <p> This
 * wrapper class is assumed to be available directly in the activation group VM; that is, it is
 * assumed to be in the application classloader, the extension classloader, or the boot classloader,
 * rather than being downloaded. Since this class also needs considerable permissions, the easiest
 * thing to do is to make it an installed extension. <p> This wrapper class performs a security
 * check to control what policy files can be used with a given codebase. It does this by querying
 * the VM's (global) policy for {@link com.sun.jini.start.SharedActivationPolicyPermission} grants.
 * The service's associated {@link com.sun.jini.start.ActivateWrapper.ActivateDesc#importLocation
 * ActivateDesc.importLocation} is used as the {@link java.security.CodeSource} for selecting the
 * appropriate permission set to check against. If multiple codebases are used, then all the
 * codebases must have the necessary <code>SharedActivationPolicyPermission</code> grants. <p> An
 * example of how to use this wrapper:
 * <pre>
 * URL[] importURLs = new URL[] {new URL("http://myhost:8080/service.jar")};
 * URL[] exportURLs = new URL[] {new URL("http://myhost:8080/service-dl.jar")};
 * ActivationID aid
 *     = ActivateWrapper.register(
 * 		gid,
 * 		new ActivateWrapper.ActivateDesc(
 * 			"foo.bar.ServiceImpl",
 * 			importURLs,
 * 			exportURLs,
 * 			"http://myhost:8080/service.policy",
 * 			new MarshalledObject(
 *                          new String[] { "/tmp/service.config" })
 *              ),
 * 		true,
 *              activationSystem);
 * </pre>
 * <A NAME="serviceConstructor"> Clients of this wrapper service need to implement the following
 * "activation constructor":
 * <blockquote><pre>
 * &lt;impl&gt;(ActivationID activationID, MarshalledObject data)
 * </blockquote></pre>
 * where, <UL> <LI>activationID - is the object's activation identifier <LI>data         - is the
 * object's activation data </UL>
 *
 * Clients of this wrapper service can also implement {@link net.jini.export.ProxyAccessor}, which
 * allows the service implementation to provide a remote reference of its choosing. <P> <A
 * NAME="configEntries"> This implementation of <code>ActivateWrapper</code> supports the following
 * {@link java.security.Security} property:
 *
 * <table summary="Describes the com.sun.jini.start.servicePolicyProvider security property"
 * border="0" cellpadding="2"> <tr valign="top"> <th scope="col" summary="layout"> <font
 * size="+1">&#X2022;</font> <th scope="col" align="left" colspan="2"> <font size="+1"><code>
 * com.sun.jini.start.servicePolicyProvider</code></font> <tr valign="top"> <td> &nbsp <th
 * scope="row" align="right"> Default: <td> <code> "net.jini.security.policy.DynamicPolicyProvider"
 * </code> <tr valign="top"> <td> &nbsp <th scope="row" align="right"> Description: <td> The fully
 * qualified class name of a dynamic policy provider (see {@link net.jini.security.policy.DynamicPolicy})
 * which will be used to "wrap" all service policy files. The implementation class needs to: <UL>
 * <LI> implement the following constructor:
 * <blockquote><pre>
 *   public &lt;impl&gt;(Policy servicePolicy)
 *           </blockquote></pre>
 * where, <UL> <LI>servicePolicy - is the service policy object to be wrapped </UL> <LI> implement
 * {@link net.jini.security.policy.DynamicPolicy} <LI> be a public, non-interface, non-abstract
 * class </UL>
 *
 * <P> A custom service policy provider can be very useful when trying to debug security related
 * issues. <code>com.sun.jini.tool.DebugDynamicPolicyProvider</code> is an example policy provider
 * that provides this functionality and can be located via the following URL: <A
 * HREF="http://starterkit-examples.jini.org/"> http://starterkit-examples.jini.org/ </A><BR>
 * <I>Note:</I>The custom policy implementation is assumed to be available from the system
 * classloader of the virtual machine hosting the service. Its codebase should also be granted
 * {@link java.security.AllPermission}. </table>
 *
 * @author Sun Microsystems, Inc.
 * @see com.sun.jini.start.SharedActivationPolicyPermission
 * @see java.rmi.activation.ActivationID
 * @see java.rmi.MarshalledObject
 * @see java.rmi.Remote
 * @see java.security.CodeSource
 * @see net.jini.export.ProxyAccessor
 */

@com.gigaspaces.api.InternalApi
public class ActivateWrapper implements Remote, Serializable {
    private static final long serialVersionUID = 9142405005980676707L;

    static final Logger logger = Logger.getLogger("com.sun.jini.start.wrapper");
    /**
     * The <code>Policy</code> object that aggregates the individual service policy objects.
     */
    private static AggregatePolicyProvider globalPolicy;

    /**
     * The <code>Policy</code> object in effect at startup.
     */
    private static Policy initialGlobalPolicy;

    /**
     * The "wrapped" activatable object.
     *
     * @serial
     */
    private /*final*/ Object impl;

    /**
     * The parameter types for the "activation constructor".
     */
    private static final Class[] actTypes = {
            ActivationID.class, MarshalledObject.class
    };

    /**
     * Fully qualified name of custom, service policy provider
     */
    private static String servicePolicyProvider =
            ((String) AccessController.doPrivileged(
                    new PrivilegedAction() {
                        public Object run() {
                            return Security.getProperty(
                                    "com.sun.jini.start." +
                                            "servicePolicyProvider");
                        }
                    }));

    /**
     * The parameter types for the "custom, service policy constructor".
     */
    private static final Class[] policyTypes = {
            Policy.class
    };

    /**
     * Descriptor for registering a "wrapped" activatable object. This descriptor gets stored as the
     * <code>MarshalledObject</code> initialization data in the <code>ActivationDesc</code>.
     */
    public static class ActivateDesc implements Serializable {

        private static final long serialVersionUID = 2L;

        /**
         * The activatable object's class name.
         *
         * @serial
         */
        public final String className;
        /**
         * The codebase where the server classes are loaded from by the activation group.
         *
         * @serial
         */
        public final URL[] importLocation;
        /**
         * The codebase where clients should load classes from for stubs, etc.
         *
         * @serial
         */
        public final URL[] exportLocation;
        /**
         * The security policy filename or URL.
         *
         * @serial
         */
        public final String policy;
        /**
         * The activatable object's initialization data.
         *
         * @serial
         */
        public final MarshalledObject data;

        /**
         * Trivial constructor.
         */
        public ActivateDesc(String className,
                            URL[] importLocation,
                            URL[] exportLocation,
                            String policy,
                            MarshalledObject data) {
//TODO - clone non-String objects?
            this.className = className;
            this.importLocation = importLocation;
            this.exportLocation = exportLocation;
            this.policy = policy;
            this.data = data;
        }

        // Javadoc inherited from supertype
        public String toString() {
            return "[className=" + className + ","
                    + "importLocation="
                    + ((importLocation == null)
                    ? null : Arrays.asList(importLocation))
                    + ","
                    + "exportLocation="
                    + ((exportLocation == null)
                    ? null : Arrays.asList(exportLocation))
                    + ","
                    + "policy=" + policy + ","
                    + "data=" + data + "]";
        }
    }

    /**
     * A simple subclass of <code>PreferredClassLoader</code> that overrides <code>getURLs</code> to
     * return the <code>URL</code>s of the provided export codebase. <code>getURLs</code> is called
     * by the RMI subsystem in order to annotate objects leaving the virtual machine.
     */
    /* 
     * Implementation note. Subclasses of this class that override 
     * getClassAnnotation might need to override getURLs because getURLs
     * uses a "cached" version of the export annotation.
     */
    static class ExportClassLoader extends PreferredClassLoader {
        /**
         * Cached value of the provided export codebase <code>URL</code>s
         */
        private final URL[] exportURLs;

        /**
         * Id field used to make toString() unique
         */
        private final Uuid id = UuidFactory.generate();

        /**
         * Trivial constructor that calls
         * <pre>
         * super(importURLs, parent, urlsToPath(exportURLs), false);
         * </pre>
         * and assigns <code>exportURLs</code> to an internal field.
         */
        public ExportClassLoader(URL[] importURLs, URL[] exportURLs,
                                 ClassLoader parent) {
            super(importURLs, parent, urlsToPath(exportURLs), false);
            // Not safe to call getClassAnnotation() w/i cons if subclassed,
            // so need to redo "super" logic here.
            if (exportURLs == null) {
                this.exportURLs = importURLs;
            } else {
                this.exportURLs = exportURLs;
            }
        }

        //Javadoc inherited from super type
        public URL[] getURLs() {
            return exportURLs.clone();
        }

        // Javadoc inherited from supertype
        public String toString() {
            URL[] urls = super.getURLs();
            return this.getClass().getName()
                    + "[importURLs="
                    + (urls == null ? null : Arrays.asList(urls))
                    + ","
                    + "exportURLs="
                    + (exportURLs == null ? null : Arrays.asList(exportURLs))
                    + ","
                    + "parent=" + getParent()
                    + ","
                    + "id=" + id
                    + "]";
        }
    }

    /**
     * Activatable constructor. This constructor: <UL> <LI>Retrieves an <code>ActivateDesc</code>
     * from the provided <code>data</code> parameter. <LI>creates an <code>ExportClassLoader</code>
     * using the import and export codebases obtained from the provided <code>ActivateDesc</code>,
     * <LI>checks the import codebase(s) for the required <code>SharedActivationPolicyPermission</code>
     * <LI>associates the newly created <code>ExportClassLoader</code> and the corresponding policy
     * file obtained from the <code>ActivateDesc</code> with the <code>AggregatePolicyProvider</code>
     * <LI>loads the "wrapped" activatable object's class and calls its activation constructor with
     * the context classloader set to the newly created <code>ExportClassLoader</code>. <LI> resets
     * the context class loader to the original context classloader </UL> The first instance of this
     * class will also replace the VM's existing <code>Policy</code> object, if any, with a
     * <code>AggregatePolicyProvider</code>.
     *
     * @param id   The <code>ActivationID</code> of this object
     * @param data The activation data for this object
     * @see com.sun.jini.start.ActivateWrapper.ExportClassLoader
     * @see com.sun.jini.start.ActivateWrapper.ActivateDesc
     * @see com.sun.jini.start.AggregatePolicyProvider
     * @see com.sun.jini.start.SharedActivationPolicyPermission
     * @see java.security.Policy
     */
    public ActivateWrapper(ActivationID id, MarshalledObject data)
            throws Exception {
        try {
            LogUtils.entering(logger, ActivateWrapper.class, "ActivateWrapper",
                    new Object[]{id, data});

            ActivateDesc desc = (ActivateDesc) data.get();
            logger.log(Level.FINEST, "ActivateDesc: {0}", desc);

            Thread t = Thread.currentThread();
            ClassLoader ccl = t.getContextClassLoader();
            logger.log(Level.FINEST, "Saved current context class loader: {0}",
                    ccl);

            ExportClassLoader cl = null;
            try {
                cl = new ExportClassLoader(desc.importLocation,
                        desc.exportLocation,
                        ccl);
                logger.log(Level.FINEST, "Created ExportClassLoader: {0}", cl);
            } catch (Exception e) {
                LogUtils.throwing(logger, ActivateWrapper.class, "ActivateWrapper", e);
                throw e;
            }

            checkPolicyPermission(desc.policy, desc.importLocation);

            synchronized (ActivateWrapper.class) {
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
                        getServicePolicyProvider(
                                new PolicyFileProvider(desc.policy));
                Policy backstop_policy =
                        getServicePolicyProvider(initialGlobalPolicy);
                LoaderSplitPolicyProvider split_service_policy =
                        new LoaderSplitPolicyProvider(
                                cl, service_policy, backstop_policy);
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
                globalPolicy.setPolicy(cl, split_service_policy);
                logger.log(Level.FINEST,
                        "Added policy to set: {0}", desc.policy);
            }

            boolean initialize = false;
            Class ac = Class.forName(desc.className, initialize, cl);
            logger.log(Level.FINEST, "Obtained implementation class: {0}", ac);

            t.setContextClassLoader(cl);

            try {
                logger.log(Level.FINEST,
                        "Set new context class loader: {0}", cl);
                Constructor constructor =
                        ac.getDeclaredConstructor(actTypes);
                logger.log(Level.FINEST,
                        "Obtained implementation constructor: {0}",
                        constructor);
                constructor.setAccessible(true);
                impl =
                        constructor.newInstance(new Object[]{id, desc.data});
                logger.log(Level.FINEST,
                        "Obtained implementation instance: {0}", impl);
            } finally {
                t.setContextClassLoader(ccl);
                logger.log(Level.FINEST, "Context class loader reset to: {0}",
                        ccl);
            }
        } catch (Exception e) {
            LogUtils.throwing(logger, ActivateWrapper.class, "ActivateWrapper", e);
            throw e;
        }
        LogUtils.exiting(logger, ActivateWrapper.class,"ActivateWrapper");
    }


    /**
     * Return a reference to service being wrapped in place of this object.
     */
    private Object writeReplace() throws ObjectStreamException {
        Object impl_proxy = impl;
        if (impl instanceof ProxyAccessor) {
            impl_proxy = ((ProxyAccessor) impl).getProxy();
            logger.log(Level.FINEST,
                    "Obtained implementation proxy: {0}", impl_proxy);
            if (impl_proxy == null) {
                throw new InvalidObjectException(
                        "Implementation's getProxy() returned null");
            }
        }
        return impl_proxy;
    }

    /**
     * Analog to {@link java.rmi.activation.Activatable#register(java.rmi.activation.ActivationDesc)
     * Activatable.register()} for activatable objects that want to use this wrapper mechanism.
     *
     * @return activation ID of the registered service
     * @throws ActivationException if there was a problem registering the activatable class with the
     *                             activation system
     * @throws RemoteException     if there was a problem communicating with the activation system
     */
    public static ActivationID register(ActivationGroupID gid,
                                        ActivateDesc desc,
                                        boolean restart,
                                        ActivationSystem sys)
            throws ActivationException, RemoteException {
        LogUtils.entering(logger, ActivateWrapper.class, "register",
                new Object[]{gid, desc, Boolean.valueOf(restart), sys});

        MarshalledObject data;
        try {
            data = new MarshalledObject(desc);
        } catch (Exception e) {
            MarshalException me =
                    new MarshalException("marshalling ActivateDesc", e);
            LogUtils.throwing(logger, ActivateWrapper.class, "register", me);
            throw me;
        }

        ActivationDesc adesc =
                new ActivationDesc(gid,
                        ActivateWrapper.class.getName(),
                        null,
                        data,
                        restart
                );
        logger.log(Level.FINEST,
                "Registering descriptor with activation: {0}", adesc);

        ActivationID aid = sys.registerObject(adesc);

        LogUtils.exiting(logger, ActivateWrapper.class, "register", aid);
        return aid;
    }

    /**
     * Checks that all the provided <code>URL</code>s have permission to use the given policy.
     */
    private static void checkPolicyPermission(String policy, URL[] urls) {
        LogUtils.entering(logger, ActivateWrapper.class, "checkPolicyPermission", new Object[]{policy, urlsToPath(urls)});
        // Create desired permission object
        Permission perm = new SharedActivationPolicyPermission(policy);
        Certificate[] certs = null;
        CodeSource cs = null;
        ProtectionDomain pd = null;
        // Loop over all codebases
        for (int i = 0; i < urls.length; i++) {
            // Create ProtectionDomain for given codesource
            cs = new CodeSource(urls[i], certs);
            pd = new ProtectionDomain(cs, null, null, null);
            logger.log(Level.FINEST,
                    "Checking protection domain: {0}", pd);

            // Check if current domain allows desired permission
            if (!pd.implies(perm)) {
                SecurityException se = new SecurityException(
                        "ProtectionDomain " + pd
                                + " does not have required permission: " + perm);
                LogUtils.throwing(logger, ActivateWrapper.class, "checkPolicyPermission", se);
                throw se;
            }
        }
        LogUtils.exiting(logger, ActivateWrapper.class,"checkPolicyPermission");
    }

    /**
     * Utility method that converts a <code>URL[]</code> into a corresponding, space-separated
     * string with the same array elements.
     *
     * Note that if the array has zero elements, the return value is null, not the empty string.
     */
    private static String urlsToPath(URL[] urls) {
//TODO - check if spaces in file paths are properly escaped (i.e.% chars)	
        if (urls == null) {
            return null;
        } else if (urls.length == 0) {
            return "";
        } else if (urls.length == 1) {
            return urls[0].toExternalForm();
        } else {
            StringBuffer path = new StringBuffer(urls[0].toExternalForm());
            for (int i = 1; i < urls.length; i++) {
                path.append(' ');
                path.append(urls[i].toExternalForm());
            }
            return path.toString();
        }
    }

    static Policy getServicePolicyProvider(Policy service_policy) throws Exception {
        Policy servicePolicyWrapper = null;
        if (servicePolicyProvider != null) {
            Class sp = Class.forName(servicePolicyProvider);
            logger.log(Level.FINEST,
                    "Obtained custom service policy implementation class: {0}", sp);
            Constructor constructor =
                    sp.getConstructor(policyTypes);
            logger.log(Level.FINEST,
                    "Obtained custom service policy implementation constructor: {0}",
                    constructor);
            servicePolicyWrapper = (Policy)
                    constructor.newInstance(new Object[]{service_policy});
            logger.log(Level.FINEST,
                    "Obtained custom service policy implementation instance: {0}",
                    servicePolicyWrapper);
        } else {
            servicePolicyWrapper = new DynamicPolicyProvider(service_policy);
            logger.log(Level.FINEST,
                    "Using default service policy implementation instance: {0}",
                    servicePolicyWrapper);
        }
        return servicePolicyWrapper;
    }


}
