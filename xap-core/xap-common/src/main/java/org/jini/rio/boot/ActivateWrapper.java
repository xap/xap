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
import com.sun.jini.start.AggregatePolicyProvider;
import com.sun.jini.start.LoaderSplitPolicyProvider;
import com.sun.jini.start.SharedActivationPolicyPermission;

import net.jini.export.ProxyAccessor;
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
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * codebases must have the necessary <code>SharedActivationPolicyPermission</code> grants.
 */
@com.gigaspaces.api.InternalApi
public class ActivateWrapper implements Remote, Serializable {
    private static final long serialVersionUID = 1L;
    static final String CONFIG_COMPONENT = "org.jini.rio.boot";
    /**
     * Configure logger
     */
    static final Logger logger = LoggerFactory.getLogger("org.jini.rio.boot.wrapper");
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
    private/* final */ Object impl;
    /**
     * The parameter types for the "activation constructor".
     */
    private static final Class[] actTypes = {ActivationID.class,
            MarshalledObject.class};

    /**
     * Descriptor for registering a "wrapped" activatable object. This descriptor gets stored as the
     * {@link java.rmi.MarshalledObject} initialization data in the <code>ActivationDesc</code>.
     */
    public static class ActivateDesc implements Serializable {
        private static final long serialVersionUID = 1L;
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
         * JARs to be added into the CommonClassLoader
         *
         * @serial
         */
        public final URL[] commonJARs;
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
                            URL[] commonJARs,
                            String policy,
                            MarshalledObject data) {
            //TODO - clone non-String objects?
            this.className = className;
            this.importLocation = importLocation;
            this.exportLocation = exportLocation;
            this.commonJARs = commonJARs;
            this.policy = policy;
            this.data = data;
        }

        // Javadoc inherited from supertype
        public String toString() {
            return "[className="
                    + className
                    + ","
                    + "importLocation="
                    + Arrays.asList(importLocation)
                    + ","
                    + "exportLocation="
                    + Arrays.asList(exportLocation)
                    + ","
                    + "policy="
                    + policy
                    + ","
                    + "data="
                    + data
                    + "]";
        }
    }

    /**
     * Activatable constructor. This constructor: <UL> <LI>Retrieves an <code>ActivateDesc</code>
     * from the provided <code>data</code> parameter. <LI>creates a {@link ServiceClassLoader} using
     * the import and export codebases obtained from the provided <code>ActivateDesc</code>,
     * <LI>checks the import codebase(s) for the required <code>SharedActivationPolicyPermission</code>
     * <LI>associates the newly created {@link ServiceClassLoader} and the corresponding policy file
     * obtained from the <code>ActivateDesc</code> with the <code>AggregatePolicyProvider</code>
     * <LI>loads the "wrapped" activatable object's class and calls its activation constructor with
     * the context classloader set to the newly created {@link ServiceClassLoader}. <LI>resets the
     * context class loader to the original context classloader </UL> The first instance of this
     * class will also replace the VM's existing <code>Policy</code> object, if any, with a
     * <code>AggregatePolicyProvider</code>.
     *
     * @param id   The <code>ActivationID</code> of this object
     * @param data The activation data for this object
     * @see ServiceClassLoader
     * @see com.sun.jini.start.AggregatePolicyProvider
     * @see com.sun.jini.start.SharedActivationPolicyPermission
     * @see java.security.Policy
     */
    public ActivateWrapper(ActivationID id, MarshalledObject data)
            throws Exception {
        LogUtils.entering(logger, ActivateWrapper.class, "ActivateWrapper", new Object[]{id, data});
        ActivateDesc desc = (ActivateDesc) data.get();
        logger.trace("ActivateDesc: {0}", desc);

        CommonClassLoader commonCL = CommonClassLoader.getInstance();
        commonCL.addCommonJARs(desc.commonJARs);
        if (logger.isTraceEnabled())
            logger.trace(
                    "Created CommonClassLoader: {0}",
                    commonCL);

        ServiceClassLoader cl = null;
        try {
            cl = new ServiceClassLoader("active-wrapper", desc.importLocation,
                    new ClassAnnotator(desc.exportLocation,
                            new Properties()),
                    commonCL);
            if (logger.isTraceEnabled())
                logger.trace(
                        "Created ServiceClassLoader: {0}",
                        cl);
        } catch (Exception e) {
            LogUtils.throwing(logger, ActivateWrapper.class, "ActivateWrapper",
                    e);
            throw e;
        }
        checkPolicyPermission(desc.policy, desc.importLocation);
        synchronized (ActivateWrapper.class) {
            // supplant global policy 1st time through
            if (globalPolicy == null) {
                initialGlobalPolicy = Policy.getPolicy();
                globalPolicy = new AggregatePolicyProvider(initialGlobalPolicy);
                Policy.setPolicy(globalPolicy);
                logger.trace(
                        "Global policy set: {0}",
                        globalPolicy);
            }
            DynamicPolicyProvider service_policy =
                    new DynamicPolicyProvider(new PolicyFileProvider(desc.policy));
            LoaderSplitPolicyProvider split_service_policy =
                    new LoaderSplitPolicyProvider(
                            cl,
                            service_policy,
                            new DynamicPolicyProvider(initialGlobalPolicy));
            split_service_policy.grant(this.getClass(),
                    null, /* Principal[] */
                    new Permission[]{new AllPermission()});
            globalPolicy.setPolicy(cl, split_service_policy);
            logger.trace(
                    "Added policy to set: {0}",
                    desc.policy);
        }
        Thread t = Thread.currentThread();
        ClassLoader ccl = t.getContextClassLoader();
        logger.trace(
                "Saved current context class loader: {0}",
                ccl);
        t.setContextClassLoader(cl);
        logger.trace("Set new context class loader: {0}", cl);
        try {
            boolean initialize = false;
            Class ac = Class.forName(desc.className, initialize, cl);
            logger.trace(
                    "Obtained implementation class: {0}",
                    ac);
            Constructor constructor = ac.getDeclaredConstructor(actTypes);
            logger.trace(
                    "Obtained implementation constructor: {0}",
                    constructor);
            constructor.setAccessible(true);
            impl = constructor.newInstance(new Object[]{id, desc.data});
            logger.trace(
                    "Obtained implementation instance: {0}",
                    impl);
        } finally {
            t.setContextClassLoader(ccl);
            logger.trace(
                    "Context class loader reset to: {0}",
                    ccl);

        }
        LogUtils.exiting(logger, ActivateWrapper.class, "ActivateWrapper");
    }

    /**
     * Return a reference to service being wrapped in place of this object.
     */
    private Object writeReplace() throws ObjectStreamException {
        Object impl_proxy = impl;
        if (impl instanceof ProxyAccessor) {
            impl_proxy = ((ProxyAccessor) impl).getProxy();
            logger.trace(
                    "Obtained implementation proxy: {0}",
                    impl_proxy);
            if (impl_proxy == null) {
                throw new InvalidObjectException("Implementation's getProxy() returned null");
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
                                        ActivateDesc desc, boolean restart,
                                        ActivationSystem sys)
            throws ActivationException, RemoteException {
        LogUtils.entering(logger, ActivateWrapper.class, "register", new Object[]{gid, desc, Boolean.valueOf(restart), sys});
        MarshalledObject data;
        try {
            data = new MarshalledObject(desc);
        } catch (Exception e) {
            MarshalException me = new MarshalException("marshalling ActivateDesc",
                    e);
            LogUtils.throwing(logger, ActivateWrapper.class, "register", me);
            throw me;
        }
        ActivationDesc adesc = new ActivationDesc(gid,
                ActivateWrapper.class.getName(),
                null,
                data,
                restart);
        logger.trace(
                "Registering descriptor with activation: {0}",
                adesc);
        ActivationID aid = sys.registerObject(adesc);
        LogUtils.exiting(logger, ActivateWrapper.class, "register", aid);
        return aid;
    }

    /**
     * Checks that all the provided <code>URL</code> s have permission to use the given policy.
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
            logger.trace("Checking protection domain: {0}", pd);
            // Check if current domain allows desired permission
            if (!pd.implies(perm)) {
                SecurityException se = new SecurityException("ProtectionDomain "
                        + pd
                        + " does not have required permission: "
                        + perm);
                LogUtils.throwing(logger, ActivateWrapper.class, "checkPolicyPermission",
                        se);
                throw se;
            }
        }
        LogUtils.exiting(logger, ActivateWrapper.class, "checkPolicyPermission");
    }

    /**
     * Utility method that converts a <code>URL[]</code> into a corresponding, space-separated
     * string with the same array elements. Note that if the array has zero elements, the return
     * value is null, not the empty string.
     */
    private static String urlsToPath(URL[] urls) {
        //TODO - check if spaces in file paths are properly escaped (i.e.%
        // chars)
        if (urls.length == 0) {
            return null;
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
}
