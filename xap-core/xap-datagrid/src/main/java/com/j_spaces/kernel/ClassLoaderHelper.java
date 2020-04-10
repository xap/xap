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

package com.j_spaces.kernel;

import com.gigaspaces.lrmi.classloading.LRMIClassLoadersHolder;
import com.j_spaces.kernel.threadpool.FastContextClassLoaderThread;

import org.jini.rio.boot.LoggableClassLoader;

import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.rmi.server.RMIClassLoader;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This utility class which helps to load the desired class by the context ClassLoader for current
 * Thread. The context ClassLoader is provided by the creator of the current thread for use by code
 * running in this thread when loading classes and resources. <p/> Performs the specified
 * <code>PrivilegedAction</code> with privileges enabled. The action is performed with <i>all</i> of
 * the permissions possessed by the caller's protection domain.
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @since 5.1
 */
@com.gigaspaces.api.InternalApi
public class ClassLoaderHelper {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_KERNEL);
    private static final Map<String, Class<?>> _primitiveTypes = loadPrimitiveTypes();

    /**
     * direct Thread.class {@link Field} of context ClassLoader
     */
    final static private Field _directContextClassLoaderThreadField;

    static {
        /** initialize direct contextClassLoader thread field */
        _directContextClassLoaderThreadField = getContextThreadCLField();
    }

    /**
     * @return direct private contextClassLoader field of Thread class or <code>null</code> if
     * failed to aquire field.
     */
    final static private Field getContextThreadCLField() {
        Field threadCLField = null;
        try {
            Thread currThread = Thread.currentThread();
            threadCLField = Thread.class.getDeclaredField("contextClassLoader");
            threadCLField.setAccessible(true);
            ClassLoader cl = (ClassLoader) threadCLField.get(currThread);
            threadCLField.set(currThread, cl);
        } catch (Throwable th) {
            // if we got here, we don't have permissions or the contextClassLoader field doesn't exists in Thread.class
            if (_logger.isTraceEnabled()) {
                _logger.trace("Failed to get access to 'contextClassLoader' field.", th);
            }
        }

        return threadCLField;
    }

    /**
     * Load a map of primitive type names mapped to their java class.
     */
    private static Map<String, Class<?>> loadPrimitiveTypes() {
        Map<String, Class<?>> types = new HashMap<String, Class<?>>();
        types.put(byte.class.getName(), byte.class);
        types.put(short.class.getName(), short.class);
        types.put(int.class.getName(), int.class);
        types.put(long.class.getName(), long.class);
        types.put(float.class.getName(), float.class);
        types.put(double.class.getName(), double.class);
        types.put(boolean.class.getName(), boolean.class);
        types.put(char.class.getName(), char.class);
        return types;
    }

    public static <T> T newInstance(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class cls = loadClass(className);
        Object instance = cls.newInstance();
        return (T) instance;
    }

    public static Class loadClass(String className) throws ClassNotFoundException {
        return loadClass(className, false);
    }

    public static Class loadLocalClass(String className) throws ClassNotFoundException {
        return loadClass(className, true);
    }

    /**
     * Load the desired class by the context ClassLoader for current Thread.
     *
     * @param className the desired class to load.
     * @param localOnly whether or not to use the remote LRMI class loader.
     * @return the loaded class.
     * @throws ClassNotFoundException Failed to load the desired class.
     */
    public static Class loadClass(String className, boolean localOnly) throws ClassNotFoundException {
        Class<?> primitiveType = _primitiveTypes.get(className);
        if (primitiveType != null)
            return primitiveType;

        ClassLoader loader = getContextClassLoader();
        if (loader == null)
            loader = ClassLoaderHelper.class.getClassLoader();

        try {
            Class loadClass = Class.forName(className, true, loader);

            if (_logger.isTraceEnabled()) {
                _logger.trace(
                        "Load class: [" + className + "] Thread: [" + Thread.currentThread().getName()
                                + "] using ClassLoader: [" + loader + "]\n"
                                + JSpaceUtilities.getStackTrace(new Exception("Debugging stack trace: ")));
            } else if (_logger.isDebugEnabled()) {
                StringBuilder classLoaderHierarchy = new StringBuilder("ClassLoader Hierarchy: ");
                ClassLoader classLoaders = loader;
                while (classLoaders != null) {
                    classLoaderHierarchy.append(classLoaders.getClass().toString()).append(" <-- ");
                    classLoaders = classLoaders.getParent();
                }
                _logger.debug("Load class: [" + className + "] Thread: [" + Thread.currentThread().getName()
                        + "] using ClassLoader: [" + loader + "] \n"
                        + " [ " + classLoaderHierarchy.toString() + " ] \n");
            }
            return loadClass;
        } catch (ClassNotFoundException ex) {
            if (localOnly)
                throw ex;

            if (_logger.isTraceEnabled())
                _logger.trace("Thread: [" + Thread.currentThread().getName()
                        + "] failed to load class [" + className
                        + "] by Thread ContextClassLoader: [" + loader
                        + "]. Attempting to load by Class.forName()", ex);


            return LRMIClassLoadersHolder.loadClass(className);
        }
    }

    public static Class loadClass(String className, boolean localOnly, Class defaultClass) {
        if (className == null)
            return defaultClass;

        try {
            return loadClass(className, localOnly);
        } catch (ClassNotFoundException e) {
            return defaultClass;
        }
    }

    public static Class loadClass(String codebase, String className, ClassLoader classLoader)
            throws ClassNotFoundException, MalformedURLException {
        return loadClass(codebase, className, classLoader, false);
    }

    /**
     * Load the desired class by the context ClassLoader for current Thread, or by use of
     * RMIClassLoader which loads a class from a codebase URL path, optionally using the supplied
     * loader. <p/> This method should be used when the caller would like to make available to the
     * provider implementation an additional contextual class loader to consider, such as the loader
     * of a caller on the stack. Typically, a provider implementation will attempt to resolve the
     * named class using the given defaultLoader, if specified, before attempting to resolve the
     * class from the codebase URL path. <p/> This method first delegates to the {@link
     * #loadClass(String)} methods with the <em>className</em> as argument, and if fails, delegates
     * to the RMIClassLoaderSpi.loadClass(String,String,ClassLoader) method of the provider
     * instance, passing <em>codebase</em> as the first argument, <em>className</em> as the second
     * argument, and <em>defaultLoader</em> as the third argument.
     *
     * @param codebase    the list of URLs (separated by spaces) to load the class from, or null
     *                    name the name of the class to load
     * @param className   the name of the class to load
     * @param classLoader additional contextual class loader to use, or null
     * @param localOnly   whether or not to use the remote class loader.
     * @return the Class object representing the loaded class
     * @throws ClassNotFoundException if a definition for the class could not be found at the
     *                                specified location
     * @throws MalformedURLException  if codebase is non-null and contains an invalid URL, or if
     *                                codebase is null and a provider-specific URL used to load
     *                                classes is invalid
     */
    public static Class loadClass(String codebase, String className, ClassLoader classLoader, boolean localOnly)
            throws ClassNotFoundException, MalformedURLException {
        try {
            return loadClass(className, localOnly);
        } catch (ClassNotFoundException cnfe) {
            if (localOnly)
                throw cnfe;
            // fallback;
        }

        try {
            Class loaded = RMIClassLoader.loadClass(codebase, className, classLoader);

            if (_logger.isTraceEnabled()) {
                _logger.trace("Load class: [" + className
                        + "] Thread: [" + Thread.currentThread().getName()
                        + "] using RMIClassLoader passing codebase: [" + codebase
                        + "] and additional ClassLoader: [" + classLoader + "] \n");
            }

            return loaded;
        } catch (ClassNotFoundException cnfe2) {
            // fallback
        }

        try {
            return LRMIClassLoadersHolder.loadClass(className);
        } catch (ClassNotFoundException e) {
            if (_logger.isTraceEnabled()) {
                _logger.trace("Thread: ["
                        + Thread.currentThread().getName()
                        + "] failed to load class [" + className
                        + "] using LRMIClassLoader \n", e);
            }

            throw e; //throw this CNFE, original doesn't interest us

        }
    }

    /**
     * @return The context ClassLoader of the current Thread.
     */
    public static ClassLoader getContextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    /**
     * Set current thread context ClassLoader.
     *
     * @param cl             the classLoader to set.
     * @param ignoreSecurity if <code>true</code> the java security will be ignored(to gain
     *                       performance), the direct reflection-field call will attempt to set
     *                       contextClassLoader without calling thread method {@link
     *                       Thread#setContextClassLoader(ClassLoader)}. If non secured call was
     *                       failed, the context will be set by regular way via {@link
     *                       Thread#setContextClassLoader(ClassLoader)}.
     */
    public static void setContextClassLoader(ClassLoader cl, boolean ignoreSecurity) {
        if (ignoreSecurity)
            setDirectContextClassLoader(cl);
        else
            Thread.currentThread().setContextClassLoader(cl);
    }

    /**
     * Set contextClassLoader to the current thread. <p/> NOTE: This method provides direct
     * reflection-field call to the Thread.class without calling {@link
     * Thread#setContextClassLoader(ClassLoader)}, and without java security verification. If direct
     * call was failed, the context will be set by regular way via {@link
     * Thread#setContextClassLoader(ClassLoader)}.
     *
     * @param cl the ClassLoader to set.
     */
    private static void setDirectContextClassLoader(ClassLoader cl) {
        Thread t = Thread.currentThread();
        if (t instanceof FastContextClassLoaderThread) {
            t.setContextClassLoader(cl);
            return;
        }
        if (_directContextClassLoaderThreadField != null) {
            if (_logger.isDebugEnabled())
                _logger.debug("Thread: " + Thread.currentThread() + " set direct contextClassLoader: " + cl);

            try {
                _directContextClassLoaderThreadField.set(Thread.currentThread(), cl);
            } catch (Throwable e) {
                throw new IllegalArgumentException("Unexpected behavior of Thread.class. Failed to setContextClassLoader.", e);
            }
        } else {
            Thread.currentThread().setContextClassLoader(cl);
        }
    }

    final private static String NULL_CL_LOGNAME = "null";

    public static String getClassLoaderLogName(ClassLoader cl) {
        if (cl == null)
            return NULL_CL_LOGNAME;
        return (cl instanceof LoggableClassLoader) ? ((LoggableClassLoader) cl).getLogName() : cl.toString();
    }
}
