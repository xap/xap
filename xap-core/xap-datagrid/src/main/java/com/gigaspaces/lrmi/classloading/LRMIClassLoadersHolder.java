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

package com.gigaspaces.lrmi.classloading;

import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.gigaspaces.internal.classloader.IClassLoaderCacheStateListener;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.logger.TraceableLogger;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.classloading.protocol.lrmi.LRMIConnection;
import com.gigaspaces.lrmi.nio.filters.IOFilterException;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.SystemProperties;

import org.jini.rio.boot.AdditionalClassProviderFactory;
import org.jini.rio.boot.IAdditionalClassProvider;

import java.io.IOException;
import java.util.logging.Level;

/**
 * acts as a classloader delegator to an IClassProvider based classloader that loads class
 * definitions from a specific source.
 *
 * every class loader is assign to a specific target using the class provider. every is class will
 * be loaded by one and only one classloader.
 *
 * the devision into source related classloaders allow late classloading to be performed
 * transparently as the caller classloader of the requested class will use the class provider to
 * retreive additional class definitions from the same source.
 *
 * @author asy ronen
 * @since 6.5
 */
@com.gigaspaces.api.InternalApi
public class LRMIClassLoadersHolder {
    final private static CopyOnUpdateMap<Long, ServiceClassLoaderContext> serviceClassLoaderContextMap;

    final private static TraceableLogger _logger = TraceableLogger.getLogger(Constants.LOGGER_LRMI_CLASSLOADING);

    final private static boolean enabled;

    final private static Object loadClassLock = new Object();

    //Keep a strong reference to the listener to avoid the ClassLoaderCache from removing this listener
    //since it keeps it as a weak reference
    final private static IClassLoaderCacheStateListener listener = new IClassLoaderCacheStateListener() {
        public void onClassLoaderRemoved(Long classLoaderKey,
                                         boolean explicit) {
            synchronized (LRMIClassLoadersHolder.class) {
                serviceClassLoaderContextMap.remove(classLoaderKey);
            }
        }
    };

    static {
        enabled = Boolean.parseBoolean(System.getProperty(SystemProperties.LRMI_CLASSLOADING, "true")) && Boolean.parseBoolean(System.getProperty(SystemProperties.LRMI_CLASSLOADING_IMPORT, "true"));

        if (enabled) {
            serviceClassLoaderContextMap = new CopyOnUpdateMap<Long, ServiceClassLoaderContext>();
            ClassLoaderCache.getCache().registerCacheStateListener(listener);

            AdditionalClassProviderFactory.setClassProvider(new IAdditionalClassProvider() {

                public Class<?> loadClass(String className, boolean fastPathOnly) throws ClassNotFoundException {
                    if (fastPathOnly)
                        return LRMIClassLoadersHolder.loadClassFromExistingOnly(className);
                    return LRMIClassLoadersHolder.loadClass(className);
                }
            });

            if (_logger.isLoggable(Level.FINE))
                logFine("LRMI class loading enabled");
        } else {
            serviceClassLoaderContextMap = null;
            if (_logger.isLoggable(Level.FINE))
                logFine("LRMI class loading disabled");
        }
    }

    //GS-7354: Protect from stack overflow which can be caused by a recursive call to remote loading of the same class
    //Will happen if this mechanism is trying to load gigaspaces classses from the AppClassLoader inside service grid
    private static final ThreadLocal<String> _currentLoadingClass = new ThreadLocal<String>();

    public static Class<?> loadClass(String className) throws ClassNotFoundException {

        try {
            if (className.equals(_currentLoadingClass.get()))
                throw new ClassNotFoundException("Could not load " + className + " using LRMI remote class loading");
            _currentLoadingClass.set(className);

            //Synchronized under different lock to avoid possible deadlock with getServiceClassLoaderContext dynamic loading inside
            //the same jvm between two different class loaders, because this method can call execute a remote invocation that will
            //return to this class by another thread
            synchronized (loadClassLock) {
                if (!enabled) {
                    throw new ClassNotFoundException("LRMI class loading is disabled. Class [" + className + "] not loaded");
                }

                if (_logger.isLoggable(Level.FINE))
                    logFine("Trying to get class loader for class: " + className + ", context class loader is " + ClassLoaderHelper.getClassLoaderLogName(ClassLoaderHelper.getContextClassLoader()));

                LRMIClassLoader cl = getClassLoader(className);

                if (cl == null) {
                    try {
                        if (_logger.isLoggable(Level.FINEST))
                            logFinest("no previously created LRMIClassLoader found for class: " + className + " creating new one");
                        cl = createClassLoader(className);

                        if (_logger.isLoggable(Level.FINE))
                            logFine("trying to load " + className + " using classloader: " + cl);
                        return Class.forName(className, true, cl);
                    } catch (LRMIClassLoaderCreationException e) {
                        throw new ClassNotFoundException("Exception caught while trying to create LRMIClassLoader for class [" + className + "]", e);
                    }
                }

                if (_logger.isLoggable(Level.FINEST))
                    logFinest("Found previously created LRMIClassLoader for class: " + className + " using classloader: " + cl);
                if (_logger.isLoggable(Level.FINE))
                    logFine("trying to load " + className + " using classloader: " + cl);
                return Class.forName(className, true, cl);
            }
        } finally {
            _currentLoadingClass.remove();
        }
    }

    /**
     * Attempts to load the specified class name from the current service class loader context lrmi
     * class loader childs This is done in a non blocking fashion to reduce contention, it will not
     * create any new lrmi class loader instances like {@link #loadClass(String)} method, it will
     * fail if there isn't an already existing lrmi class loader which has loaded this class
     *
     * @return class instance
     */
    public static Class<?> loadClassFromExistingOnly(String className) throws ClassNotFoundException {
        if (!enabled)
            throw new ClassNotFoundException("LRMI class loading is disabled. Class [" + className + "] not loaded");

        ServiceClassLoaderContext classLoaderContext = getServiceClassLoaderContextExistingOnly();
        if (classLoaderContext == null)
            throw new ClassNotFoundException("No service class loader context associated with the current " +
                    "class loader " + ClassLoaderHelper.getClassLoaderLogName(Thread.currentThread().getContextClassLoader()) +
                    ". Class [" + className + "] not loaded");

        LRMIClassLoader lrmiClassLoader = classLoaderContext.getClassLoaderByClassNameNonBlocking(className);
        if (lrmiClassLoader == null)
            throw new ClassNotFoundException("No LRMI class loader associated with the specified class [" + className + "]");

        return lrmiClassLoader.getLoadedClass(className);
    }

    /**
     * Create a LRMIClassLoader according to the current thread context
     */
    private synchronized static LRMIClassLoader createClassLoader(String className) throws LRMIClassLoaderCreationException {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader == null)
            throw new LRMIClassLoaderCreationException("attempt to create a LRMIClassLoader when there is no context class loader");
        ServiceClassLoaderContext serviceClassLoaderContext = getServiceClassLoaderContext();
        if (serviceClassLoaderContext == null) {
            if (_logger.isLoggable(Level.FINEST))
                logFinest("creating service class loader context [" + ClassLoaderHelper.getClassLoaderLogName(contextClassLoader) + "]");

            serviceClassLoaderContext = new ServiceClassLoaderContext(ClassLoaderHelper.getClassLoaderLogName(contextClassLoader));
            long classLoaderKey = ClassLoaderCache.getCache().putClassLoader(contextClassLoader);
            serviceClassLoaderContextMap.put(classLoaderKey, serviceClassLoaderContext);
        }

        LRMIRemoteClassLoaderIdentifier identifier = null;
        try {
            identifier = LRMIConnection.getRemoteClassLoaderIdentifier();
            if (identifier == null)
                throw new LRMIClassLoaderCreationException("attempt to create a LRMIClassLoader when there is no remote class loader context");

            //Get the LRMIClassLoader associated to this remote class loader context
            LRMIClassLoader cl = serviceClassLoaderContext.getClassLoaderByRemoteId(identifier);

            if (cl != null) {
                if (_logger.isLoggable(Level.FINEST))
                    logFinest("current target has already associated LRMIClassLoader, using it");
                return cl;
            }

            //Need to create new LRMIClassLoader
            if (_logger.isLoggable(Level.FINEST))
                logFinest("retrieving remote class provider [" + identifier.getRemoteLrmiRuntimeId() + "]");
            IClassProvider classProvider = LRMIConnection.getClassProvider();
            if (classProvider == null) {
                if (_logger.isLoggable(Level.FINE))
                    logFine("could not get LRMIClassLoader for remote class loader [" + identifier.toString() + "]");
                throw new LRMIClassLoaderCreationException("could not get LRMIClassLoader for remote class loader [" + identifier.toString() + "]");
            }
            if (_logger.isLoggable(Level.FINE))
                logFine("creating new LRMIClassLoader connected to remote class loader [" + identifier.toString() + "]");

            cl = new LRMIClassLoader(classProvider, contextClassLoader, serviceClassLoaderContext, identifier.getRemoteLrmiRuntimeId(), identifier.getRemoteClassLoaderId());
            LRMIClassLoader previousClassLoader = serviceClassLoaderContext.putClassLoaderByRemoteId(identifier, cl);
            if (previousClassLoader != null)
                //If a lrmi class loader was created concurrently to this one to the same target, return it instead
                cl = previousClassLoader;
            return cl;
        } catch (IOException e) {
            logWarningOnExceptionCreatingLRMIClassLoader(className,
                    identifier,
                    e);
            throw new LRMIClassLoaderCreationException("exception caught while creating LRMIClassLoader", e);
        } catch (IOFilterException e) {
            logWarningOnExceptionCreatingLRMIClassLoader(className,
                    identifier,
                    e);
            throw new LRMIClassLoaderCreationException("exception caught while creating LRMIClassLoader", e);
        }
    }

    protected static void logWarningOnExceptionCreatingLRMIClassLoader(
            String className, LRMIRemoteClassLoaderIdentifier identifier,
            Exception e) {
        if (_logger.isLoggable(Level.WARNING))
            _logger.log(Level.WARNING,
                    "Failed creating an LRMIClassLoader ["
                            + identifier
                            + "] to client ["
                            + LRMIInvocationContext.getEndpointAddress()
                            + "] when trying to load class ["
                            + className
                            + "].\n" +
                            "Make sure the server is able to create a connection to the client (firewall, ip address mapping and binding).\nReason - " + e.getMessage(),
                    e);
    }

    private synchronized static ServiceClassLoaderContext getServiceClassLoaderContext() {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader == null)
            throw new IllegalStateException("cannot get service class loader context without context class loader");
        Long classLoaderKey = ClassLoaderCache.getCache().getClassLoaderKey(contextClassLoader);
        if (classLoaderKey == null)
            return null;
        return serviceClassLoaderContextMap.get(classLoaderKey);
    }

    /**
     * Attempts to locate the associated service class loader context of this class, assumes that if
     * there is one it must have already been fully created prior to this call
     */
    private static ServiceClassLoaderContext getServiceClassLoaderContextExistingOnly() {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if (contextClassLoader == null)
            throw new IllegalStateException("cannot get service class loader context without context class loader");
        Long classLoaderKey = ClassLoaderCache.getCache().getClassLoaderKey(contextClassLoader);
        if (classLoaderKey == null)
            return null;
        return serviceClassLoaderContextMap.get(classLoaderKey);
    }

    public synchronized static void dropAllClasses() {
        if (!enabled)
            return;

        ServiceClassLoaderContext serviceClassLoaderContext = getServiceClassLoaderContext();

        if (serviceClassLoaderContext != null) {
            if (_logger.isLoggable(Level.FINE))
                logFine("dropping all classes from service class loader context [" + serviceClassLoaderContext + "]");
            serviceClassLoaderContext.clear();
        } else {
            if (_logger.isLoggable(Level.FINE))
                logFine("drop all classes: context class loader [" + ClassLoaderHelper.getClassLoaderLogName(Thread.currentThread().getContextClassLoader()) + "] has no service context, skipping dropping all classes");
        }
    }

    public synchronized static void dropClass(String className) {
        if (!enabled)
            return;
        if (_logger.isLoggable(Level.FINE))
            logFine("dropping class " + className);

        ServiceClassLoaderContext serviceClassLoaderContext = getServiceClassLoaderContext();

        if (serviceClassLoaderContext != null) {
            if (_logger.isLoggable(Level.FINE))
                logFine("dropping class [ " + className + "] from service class loader context [" + serviceClassLoaderContext + "]");
            serviceClassLoaderContext.removeClass(className);
        } else {
            if (_logger.isLoggable(Level.FINE))
                logFine("drop class [ " + className + "]: context class loader [" + ClassLoaderHelper.getClassLoaderLogName(Thread.currentThread().getContextClassLoader()) + "] has no service context, skipping dropping class");
        }
    }

    /**
     * Finds a classloader for the specific class name if one exists.
     */
    private synchronized static LRMIClassLoader getClassLoader(String className) {
        ServiceClassLoaderContext serviceClassLoaderContext = getServiceClassLoaderContext();

        if (serviceClassLoaderContext == null)
            return null;

        return serviceClassLoaderContext.getClassLoaderByClassName(className);
    }

    private static void logFinest(String message) {
        _logger.finest("LRMIClassLoadersHolder [" + LRMIRuntime.getRuntime().getID() + "]: " + message);
    }

    private static void logFine(String message) {
        _logger.fine("LRMIClassLoadersHolder [" + LRMIRuntime.getRuntime().getID() + "]: " + message);
    }

    public synchronized static ServiceClassLoaderContext getServiceClassLoaderContext(ClassLoader classLoader) {
        if (_logger.isLoggable(Level.FINEST))
            logFinest("getting service class loader context of class loader " + ClassLoaderHelper.getClassLoaderLogName(classLoader));

        Long classLoaderKey = ClassLoaderCache.getCache().getClassLoaderKey(classLoader);
        if (classLoaderKey != null)
            return serviceClassLoaderContextMap.get(classLoaderKey);
        return null;
    }
}
