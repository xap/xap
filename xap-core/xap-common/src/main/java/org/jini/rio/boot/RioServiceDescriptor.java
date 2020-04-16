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

import com.sun.jini.config.Config;
import com.sun.jini.start.AggregatePolicyProvider;
import com.sun.jini.start.ClassLoaderUtil;
import com.sun.jini.start.HTTPDStatus;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.LoaderSplitPolicyProvider;
import com.sun.jini.start.ServiceDescriptor;
import com.sun.jini.start.ServiceProxyAccessor;

import net.jini.config.Configuration;
import net.jini.export.ProxyAccessor;
import net.jini.security.BasicProxyPreparer;
import net.jini.security.ProxyPreparer;
import net.jini.security.policy.DynamicPolicyProvider;
import net.jini.security.policy.PolicyFileProvider;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.security.AllPermission;
import java.security.Permission;
import java.security.Policy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RioServiceDescriptor class is a utility that conforms to the Jini technology ServiceStarter
 * framework, and will start a service using the {@link org.jini.rio.boot.CommonClassLoader} as a
 * shared, non-activatable, in-process service. Clients construct this object with the details of
 * the service to be launched, then call <code>create</code> to launch the service in invoking
 * object's VM. <P> This class provides separation of the import codebase (where the server classes
 * are loaded from) from the export codebase (where clients should load classes from for stubs,etc.)
 * as well as providing an independent security policy file for each service object. This
 * functionality allows multiple service objects to be placed in the same VM, with each object
 * maintaining a distinct codebase and policy. <P> Services need to implement the following
 * "non-activatable constructor": <blockquote>
 *
 * <pre>
 * &lt;impl&gt;(String[] args, LifeCycle lc)
 * </pre>
 *
 * where, <UL> <LI>args - are the service configuration arguments <LI>lc - is the hosting
 * environment's {@link LifeCycle} reference. </UL>
 */
@com.gigaspaces.api.InternalApi
public class RioServiceDescriptor implements ServiceDescriptor {
    static String COMPONENT = "org.jini.rio.boot";
    static Logger logger = LoggerFactory.getLogger(COMPONENT);
    /**
     * The parameter types for the "activation constructor".
     */
    private static final Class[] actTypes = {String[].class, LifeCycle.class};
    private String name;
    private String codebase;
    private String policy;
    private String classpath;
    private String implClassName;
    private String[] serverConfigArgs;
    private LifeCycle lifeCycle;
    private static LifeCycle NoOpLifeCycle = new LifeCycle() { // default, no-op
        // object
        public boolean unregister(Object impl) {
            return false;
        }
    };
    private static AggregatePolicyProvider globalPolicy = null;
    private static Policy initialGlobalPolicy = null;

    public static AggregatePolicyProvider getGlobalPolicy() {
        return globalPolicy;
    }

    /**
     * Create a RioServiceDescriptor, assigning given parameters to their associated, internal
     * fields.
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
     * @param lifeCycle        <code>LifeCycle</code> reference for hosting environment
     */
    public RioServiceDescriptor(String name,
                                String codebase,
                                String policy,
                                String classpath,
                                String implClassName,
                                // Optional Args
                                String[] serverConfigArgs,
                                LifeCycle lifeCycle) {
        if (codebase == null
                || classpath == null
                || implClassName == null)
            throw new NullPointerException("Codebase, classpath, and "
                    + "implementation cannot be null");
        this.name = name;
        this.codebase = codebase;
        this.policy = policy;
        this.classpath = classpath;
        this.implClassName = implClassName;
        this.serverConfigArgs = serverConfigArgs;
        this.lifeCycle = (lifeCycle == null) ? NoOpLifeCycle : lifeCycle;
    }

    /**
     * Create a RioServiceDescriptor. Equivalent to calling the other overloaded constructor with
     * <code>null</code> for the <code>LifeCycle</code> reference.
     */
    public RioServiceDescriptor(String name, String codebase,
                                String classpath,
                                String implClassName,
                                // Optional Args
                                String[] serverConfigArgs) {
        this(name, codebase,
                System.getProperty("java.security.policy"),
                classpath,
                implClassName,
                serverConfigArgs,
                null);
    }

    /**
     * Codebase accessor method.
     *
     * @return The codebase string associated with this service descriptor.
     */
    public String getCodebase() {
        return codebase;
    }

    /**
     * Policy accessor method.
     *
     * @return The policy string associated with this service descriptor.
     */
    public String getPolicy() {
        return policy;
    }

    /**
     * <code>LifeCycle</code> accessor method.
     *
     * @return The <code>LifeCycle</code> object associated with this service descriptor.
     */
    public LifeCycle getLifeCycle() {
        return lifeCycle;
    }

    /**
     * LifCycle accessor method.
     *
     * @return The classpath string associated with this service descriptor.
     */
    public String getClasspath() {
        return classpath;
    }

    /**
     * Implementation class accessor method.
     *
     * @return The implementation class string associated with this service descriptor.
     */
    public String getImplClassName() {
        return implClassName;
    }

    /**
     * Service configuration arguments accessor method.
     *
     * @return The service configuration arguments associated with this service descriptor.
     */
    public String[] getServerConfigArgs() {
        return (serverConfigArgs != null)
                ? (String[]) serverConfigArgs.clone()
                : null;
    }

    /**
     * @see com.sun.jini.start.ServiceDescriptor#create
     */
    public Object create(Configuration config) throws Exception {
        Object proxy = null;
                
        /* Warn user of inaccessible codebase(s) */
        // Commented since it is downloading jars for nothing!
        if (logger.isTraceEnabled()) {
            HTTPDStatus.httpdWarning(getCodebase());
        }
                                            
        /* Set common JARs to the CommonClassLoader */
        URL[] defaultCommonJARs = null;
        String rioHome = System.getProperty("RIO_HOME");
        if (rioHome == null) {
            defaultCommonJARs = new URL[0];
            if (logger.isTraceEnabled())
                logger.trace("RIO_HOME not defined, defaultCommonJARs " +
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
        /*
        if(commonJARs.length==0)
            throw new RuntimeException("No commonJARs have been defined");
        */
        if (logger.isTraceEnabled()) {
            StringBuffer buffer = new StringBuffer();
            for (int i = 0; i < commonJARs.length; i++) {
                if (i > 0)
                    buffer.append("\n");
                buffer.append(commonJARs[i].toExternalForm());
            }
            logger.trace("commonJARs=\n" + buffer.toString());
        }

        CommonClassLoader commonCL = CommonClassLoader.getInstance();
        commonCL.addCommonJARs(commonJARs);
        final Thread currentThread = Thread.currentThread();
        ClassLoader currentClassLoader = currentThread.getContextClassLoader();

        ClassAnnotator annotator = new ClassAnnotator(
                ClassLoaderUtil.getCodebaseURLs(getCodebase()), new Properties());
        ServiceClassLoader jsbCL =
                new ServiceClassLoader(name, ClassLoaderUtil.getClasspathURLs(getClasspath()),
                        annotator,
                        commonCL);
        currentThread.setContextClassLoader(jsbCL);
        /* Get the ProxyPreparer */
        ProxyPreparer servicePreparer =
                (ProxyPreparer) Config.getNonNullEntry(config,
                        COMPONENT,
                        "servicePreparer",
                        ProxyPreparer.class,
                        new BasicProxyPreparer());
        synchronized (RioServiceDescriptor.class) {
            /* supplant global policy 1st time through */
            if (globalPolicy == null) {
                initialGlobalPolicy = Policy.getPolicy();
                globalPolicy = new AggregatePolicyProvider(initialGlobalPolicy);
                Policy.setPolicy(globalPolicy);
                if (logger.isTraceEnabled())
                    logger.trace(
                            "Global policy set: {0}",
                            globalPolicy);
            }
            final String policy = getPolicy();
            if (policy != null) {
                DynamicPolicyProvider service_policy = new DynamicPolicyProvider(new PolicyFileProvider(policy));
                LoaderSplitPolicyProvider splitServicePolicy = new LoaderSplitPolicyProvider(jsbCL, service_policy,
                                new DynamicPolicyProvider(initialGlobalPolicy));

                /*
                 * Grant "this" code enough permission to do its work under the
                 * service policy, which takes effect (below) after the context
                 * loader is (re)set.
                 */
                splitServicePolicy.grant(RioServiceDescriptor.class,
                        null, /* Principal[] */
                        new Permission[]{new AllPermission()});
                globalPolicy.setPolicy(jsbCL, splitServicePolicy);
            }
        }
        try {
            Object impl = null;
            Class implClass = null;
            String implClassName = getImplClassName();
            if (logger.isTraceEnabled()) {
                logger.trace("Attempting to get implementation class name: " + implClassName);
                logger.trace("jsbCL searchPath: " + Arrays.toString(jsbCL.getURLs()) + "  jsbCL: " + jsbCL);
            }

            implClass = Class.forName(implClassName, false, jsbCL);
            if (logger.isTraceEnabled())
                logger.trace("Attempting to get implementation constructor");
            Constructor constructor = implClass.getDeclaredConstructor(actTypes);
            if (logger.isTraceEnabled())
                logger.trace(
                        "Obtained implementation constructor: {0}",
                        constructor);
            constructor.setAccessible(true);
            impl = constructor.newInstance(new Object[]{getServerConfigArgs(),
                    lifeCycle});
            if (logger.isTraceEnabled())
                logger.trace(
                        "Obtained implementation instance: {0}",
                        impl);
            if (impl instanceof ServiceProxyAccessor) {
                proxy = ((ServiceProxyAccessor) impl).getServiceProxy();
            } else if (impl instanceof ProxyAccessor) {
                proxy = ((ProxyAccessor) impl).getProxy();
            } else {
                proxy = null; // just for insurance
            }
            if (proxy != null) {
                proxy = servicePreparer.prepareProxy(proxy);
            }
            if (logger.isTraceEnabled())
                logger.trace("Proxy =  {0}", proxy);
            currentThread.setContextClassLoader(currentClassLoader);
            //TODO - factor in code integrity for MO
            // Don't do that, we end up with different class loaders because
//            proxy = (new MarshalledObject(proxy)).get();
        } finally {
            currentThread.setContextClassLoader(currentClassLoader);
        }
        return (proxy);
    }

    public String toString() {
        ArrayList fields = new ArrayList(6);
        fields.add(codebase);
        fields.add(policy);
        fields.add(classpath);
        fields.add(implClassName);
        fields.add(Arrays.asList(serverConfigArgs));
        fields.add(lifeCycle);
        return fields.toString();
    }
}
