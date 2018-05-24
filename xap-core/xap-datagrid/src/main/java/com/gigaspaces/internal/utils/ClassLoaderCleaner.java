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

package com.gigaspaces.internal.utils;

import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.sun.jini.start.NonActivatableServiceDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author kimchy (shay.banon)
 */
@com.gigaspaces.api.InternalApi
public class ClassLoaderCleaner {

    private static final List<String> JVM_THREAD_GROUP_NAMES =
            new ArrayList<String>();

    static {
        JVM_THREAD_GROUP_NAMES.add("system");
        JVM_THREAD_GROUP_NAMES.add("RMI Runtime");
    }

    final private static Logger logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLASSLOADERS_CLEANER);

    /**
     * Should Tomcat attempt to terminate threads that have been started by the web application?
     * Stopping threads is performed via the deprecated (for good reason) <code>Thread.stop()</code>
     * method and is likely to result in instability. As such, enabling this should be viewed as an
     * option of last resort in a development environment and is not recommended in a production
     * environment. If not specified, the default value of <code>false</code> will be used. Note
     * that instances of java.util.TimerThread will always be terminate since a safe method exists
     * to do so.
     */
    private final static boolean clearReferencesStopThreads = false;

    /**
     * Clear references.
     */
    public static void clearReferences(ClassLoader classLoader) {
        ClassLoaderCache.getCache().removeClassLoader(classLoader);

        if (NonActivatableServiceDescriptor.getGlobalPolicy() != null) {
            NonActivatableServiceDescriptor.getGlobalPolicy().setPolicy(classLoader, null);
        }

        // De-register any remaining JDBC drivers
        clearReferencesJdbc(classLoader);

        // Stop any threads the web application started
        clearReferencesThreads(classLoader);

        // Clear any ThreadLocals loaded by this class loader
        clearReferencesThreadLocals(classLoader);

        // Clear RMI Targets loaded by this class loader
        clearReferencesRmiTargets(classLoader);

        clearRmiLoaderHandler(classLoader);

        // Clear the classloader reference in common-logging
        try {
            Class clazz = classLoader.loadClass("org.apache.commons.logging.LogFactory");
            clazz.getMethod("release", ClassLoader.class).invoke(null, classLoader);
        } catch (Throwable t) {
            // ignore
        }
        try {
            Class clazz = classLoader.loadClass("org.apache.juli.logging.LogFactory");
            clazz.getMethod("release", ClassLoader.class).invoke(null, classLoader);
        } catch (Throwable t) {
            // ignore
        }

        // Clear the resource bundle cache
        // This shouldn't be necessary, the cache uses weak references but
        // it has caused leaks. Oddly, using the leak detection code in
        // standard host allows the class loader to be GC'd. This has been seen
        // on Sun but not IBM JREs. Maybe a bug in Sun's GC impl?
        clearReferencesResourceBundles(classLoader);

        // Clear the classloader reference in the VM's bean introspector
        java.beans.Introspector.flushCaches();

    }


    private static void clearRmiLoaderHandler(ClassLoader classLoader) {
        // HACK: Clear it from the RMI class loader as well
        // We can safely remove this since we no longer have this class loader around
        // and the table key is based on the class loader
        try {
            final Class<?> loaderHandlerClass = classLoader.loadClass("sun.rmi.server.LoaderHandler");
            synchronized (loaderHandlerClass) {
                Field loadTableField = loaderHandlerClass.getDeclaredField("loaderTable");
                loadTableField.setAccessible(true);
                Map<?, ?> loaderTable = (Map<?, ?>) loadTableField.get(null);
                for (Iterator it = loaderTable.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry<?, ?> entry = (Map.Entry<?, ?>) it.next();
                    Object loaderKey = entry.getKey();
                    Field parentField = loaderKey.getClass().getDeclaredField("parent");
                    parentField.setAccessible(true);
                    if (parentField.get(loaderKey) == classLoader) {
                        it.remove();
                    }
                }
            }
        } catch (Throwable t) {
            // ignore
        }
    }

    /**
     * Deregister any JDBC drivers registered by the webapp that the webapp forgot. This is made
     * unnecessary complex because a) DriverManager checks the class loader of the calling class (it
     * would be much easier if it checked the context class loader) b) using reflection would create
     * a dependency on the DriverManager implementation which can, and has, changed.
     *
     * We can't just create an instance of JdbcLeakPrevention as it will be loaded by the common
     * class loader (since it's .class file is in the $CATALINA_HOME/lib directory). This would fail
     * DriverManager's check on the class loader of the calling class. So, we load the bytes via our
     * parent class loader but define the class with this class loader so the JdbcLeakPrevention
     * looks like a webapp class to the DriverManager.
     *
     * If only apps cleaned up after themselves...
     */
    private final static void clearReferencesJdbc(ClassLoader classLoader) {
        InputStream is = classLoader.getResourceAsStream(
                "com/gigaspaces/internal/utils/JdbcLeakPrevention.class");
        // We know roughly how big the class will be (~ 1K) so allow 2k as a
        // starting point
        byte[] classBytes = new byte[2048];
        int offset = 0;
        try {
            int read = is.read(classBytes, offset, classBytes.length - offset);
            while (read > -1) {
                offset += read;
                if (offset == classBytes.length) {
                    // Buffer full - double size
                    byte[] tmp = new byte[classBytes.length * 2];
                    System.arraycopy(classBytes, 0, tmp, 0, classBytes.length);
                    classBytes = tmp;
                }
                read = is.read(classBytes, offset, classBytes.length - offset);
            }
            Method defineClassMethod = ClassLoader.class.getDeclaredMethod("defineClass", String.class, byte[].class, int.class, int.class);
            defineClassMethod.setAccessible(true);
            Class<?> lpClass =
                    (Class<?>) defineClassMethod.invoke(classLoader, "com.gigaspaces.internal.utils.JdbcLeakPrevention", classBytes, 0, offset);
            Object obj = lpClass.newInstance();
            @SuppressWarnings("unchecked")
            List<String> driverNames = (List<String>) obj.getClass().getMethod("clearJdbcDriverRegistrations").invoke(obj);
            for (String name : driverNames) {
                if (logger.isLoggable(Level.FINE))
                    logger.fine("A class loader registered the JDBC driver [" + name + "] but failed to unregister it when the web application was stopped. To prevent a memory leak, the JDBC Driver has been forcibly unregistered.");
            }
        } catch (Exception e) {
            //So many things to go wrong above...
            if (logger.isLoggable(Level.FINE))
                logger.log(Level.FINE, "JDBC driver de-registration failed", e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ioe) {
                    // ignore
                }
            }
        }
    }

    @SuppressWarnings("deprecation")
    private static void clearReferencesThreads(ClassLoader classLoader) {
        Thread[] threads = getThreads();

        // Iterate over the set of threads
        for (Thread thread : threads) {
            if (thread != null) {
                ClassLoader ccl = thread.getContextClassLoader();
                if (ccl != null && ccl == classLoader) {
                    // Don't warn about this thread
                    if (thread == Thread.currentThread()) {
                        continue;
                    }

                    // Skip threads that have already died
                    if (!thread.isAlive()) {
                        continue;
                    }

                    // Don't warn about JVM controlled threads
                    ThreadGroup tg = thread.getThreadGroup();
                    if (tg != null &&
                            JVM_THREAD_GROUP_NAMES.contains(tg.getName())) {
                        continue;
                    }

                    // TimerThread is not normally visible
                    if (thread.getClass().getName().equals("java.util.TimerThread")) {
                        clearReferencesStopTimerThread(thread);
                        continue;
                    }

                    if (logger.isLoggable(Level.FINE))
                        logger.fine("A thread named [" + thread.getName() + "] started but has failed to stop it. This is very likely to create a memory leak.");

                    // Don't try an stop the threads unless explicitly
                    // configured to do so
                    if (!clearReferencesStopThreads) {
                        continue;
                    }

                    // If the thread has been started via an executor, try
                    // shutting down the executor
                    try {
                        Field targetField =
                                thread.getClass().getDeclaredField("target");
                        targetField.setAccessible(true);
                        Object target = targetField.get(thread);

                        if (target != null &&
                                target.getClass().getCanonicalName().equals(
                                        "java.util.concurrent.ThreadPoolExecutor.Worker")) {
                            Field executorField =
                                    target.getClass().getDeclaredField("this$0");
                            executorField.setAccessible(true);
                            Object executor = executorField.get(target);
                            if (executor instanceof ThreadPoolExecutor) {
                                ((ThreadPoolExecutor) executor).shutdownNow();
                            }
                        }
                    } catch (Exception e) {
                        logger.log(Level.WARNING, "Failed to terminate thread named [" + thread.getName() + "]", e);
                    }

                    // This method is deprecated and for good reason. This is
                    // very risky code but is the only option at this point.
                    // A *very* good reason for apps to do this clean-up
                    // themselves.
                    thread.stop();
                }
            }
        }
    }


    private static void clearReferencesStopTimerThread(Thread thread) {

        // Need to get references to:
        // - newTasksMayBeScheduled field
        // - queue field
        // - queue.clear()

        try {
            Field newTasksMayBeScheduledField =
                    thread.getClass().getDeclaredField("newTasksMayBeScheduled");
            newTasksMayBeScheduledField.setAccessible(true);
            Field queueField = thread.getClass().getDeclaredField("queue");
            queueField.setAccessible(true);

            Object queue = queueField.get(thread);

            Method clearMethod = queue.getClass().getDeclaredMethod("clear");
            clearMethod.setAccessible(true);

            synchronized (queue) {
                newTasksMayBeScheduledField.setBoolean(thread, false);
                clearMethod.invoke(queue);
                queue.notify();  // In case queue was already empty.
            }

            if (logger.isLoggable(Level.FINE))
                logger.fine("A web application appears to have started a TimerThread named [" + thread.getName() + "] via the java.util.Timer API but has failed to stop it. To prevent a memory leak, the timer (and hence the associated thread) has been forcibly cancelled.");

        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to terminate TimerThread named [" + thread.getName() + "]", e);
        }
    }

    private static void clearReferencesThreadLocals(ClassLoader classLoader) {
        Thread[] threads = getThreads();

        try {
            // Make the fields in the Thread class that store ThreadLocals
            // accessible
            Field threadLocalsField =
                    Thread.class.getDeclaredField("threadLocals");
            threadLocalsField.setAccessible(true);
            Field inheritableThreadLocalsField =
                    Thread.class.getDeclaredField("inheritableThreadLocals");
            inheritableThreadLocalsField.setAccessible(true);
            // Make the underlying array of ThreadLoad.ThreadLocalMap.Entry objects
            // accessible
            Class<?> tlmClass =
                    Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
            Field tableField = tlmClass.getDeclaredField("table");
            tableField.setAccessible(true);

            for (int i = 0; i < threads.length; i++) {
                Object threadLocalMap;
                if (threads[i] != null) {
                    // Clear the first map
                    threadLocalMap = threadLocalsField.get(threads[i]);
                    clearThreadLocalMap(threadLocalMap, tableField, classLoader);
                    // Clear the second map
                    threadLocalMap =
                            inheritableThreadLocalsField.get(threads[i]);
                    clearThreadLocalMap(threadLocalMap, tableField, classLoader);
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to clear ThreadLocal references", e);
        }
    }


    /*
     * Clears the given thread local map object. Also pass in the field that
     * points to the internal table to save re-calculating it on every
     * call to this method.
     */

    private static void clearThreadLocalMap(Object map, Field internalTableField, ClassLoader classLoader)
            throws NoSuchMethodException, IllegalAccessException,
            NoSuchFieldException, InvocationTargetException {
        if (map != null) {
            Method mapRemove =
                    map.getClass().getDeclaredMethod("remove",
                            ThreadLocal.class);
            mapRemove.setAccessible(true);
            final Object[] table = (Object[]) internalTableField.get(map);
            int staleEntriesCount = 0;
            if (table != null) {
                for (int j = 0; j < table.length; j++) {
                    final Object tableEntry = table[j];
                    if (tableEntry != null) {
                        boolean remove = false;
                        // Check the key
                        Object key = ((Reference<?>) tableEntry).get();
                        if (classLoader.equals(key) || (key != null &&
                                classLoader == key.getClass().getClassLoader())) {
                            remove = true;
                        }
                        // Check the value
                        Field valueField =
                                tableEntry.getClass().getDeclaredField("value");
                        valueField.setAccessible(true);
                        Object value = valueField.get(tableEntry);
                        if (classLoader.equals(value) || (value != null &&
                                classLoader == value.getClass().getClassLoader())) {
                            remove = true;
                        }
                        if (remove) {
                            Object[] args = new Object[4];
                            if (key != null) {
                                args[0] = key.getClass().getCanonicalName();
                                args[1] = key.toString();
                            }
                            if (value != null) {
                                args[2] = value.getClass().getCanonicalName();
                                args[3] = value.toString();
                            }
                            if (value == null) {
                                if (logger.isLoggable(Level.FINE)) {
                                    logger.fine("A created a ThreadLocal with key of type [" + args[0] + "] (value [" + args[1] + "]). The ThreadLocal has been correctly set to null and the key will be removed by GC. However, to simplify the process of tracing memory leaks, the key has been forcibly removed.");
                                }
                            } else {
                                if (logger.isLoggable(Level.FINE))
                                    logger.fine("A created ThreadLocal with key of type [" + args[0] + "] (value [" + args[1] + "]) and a value of type [" + args[2] + "] (value [" + args[3] + "]) but failed to remove it when class loader is removed. To prevent a memory leak, the ThreadLocal has been forcibly removed.");
                            }
                            if (key == null) {
                                staleEntriesCount++;
                            } else {
                                mapRemove.invoke(map, key);
                            }
                        }
                    }
                }
            }
            if (staleEntriesCount > 0) {
                Method mapRemoveStale =
                        map.getClass().getDeclaredMethod("expungeStaleEntries");
                mapRemoveStale.setAccessible(true);
                mapRemoveStale.invoke(map);
            }
        }
    }

    /*
     * Get the set of current threads as an array.
     */

    private static Thread[] getThreads() {
        // Get the current thread group
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        // Find the root thread group
        while (tg.getParent() != null) {
            tg = tg.getParent();
        }

        int threadCountGuess = tg.activeCount() + 50;
        Thread[] threads = new Thread[threadCountGuess];
        int threadCountActual = tg.enumerate(threads);
        // Make sure we don't miss any threads
        while (threadCountActual == threadCountGuess) {
            threadCountGuess *= 2;
            threads = new Thread[threadCountGuess];
            // Note tg.enumerate(Thread[]) silently ignores any threads that
            // can't fit into the array
            threadCountActual = tg.enumerate(threads);
        }

        return threads;
    }


    /**
     * This depends on the internals of the Sun JVM so it does everything by reflection.
     */
    private static void clearReferencesRmiTargets(ClassLoader classLoader) {
        try {
            // Need access to the ccl field of sun.rmi.transport.Target
            Class<?> objectTargetClass =
                    Class.forName("sun.rmi.transport.Target");
            Field cclField = objectTargetClass.getDeclaredField("ccl");
            cclField.setAccessible(true);

            // Clear the objTable map
            Class<?> objectTableClass =
                    Class.forName("sun.rmi.transport.ObjectTable");
            Field objTableField = objectTableClass.getDeclaredField("objTable");
            objTableField.setAccessible(true);
            Object objTable = objTableField.get(null);
            if (objTable == null) {
                return;
            }

            // Iterate over the values in the table
            if (objTable instanceof Map<?, ?>) {
                Iterator<?> iter = ((Map<?, ?>) objTable).values().iterator();
                while (iter.hasNext()) {
                    Object obj = iter.next();
                    Object cclObject = cclField.get(obj);
                    if (classLoader == cclObject) {
                        iter.remove();
                    }
                }
            }

            // Clear the implTable map
            Field implTableField = objectTableClass.getDeclaredField("implTable");
            implTableField.setAccessible(true);
            Object implTable = implTableField.get(null);
            if (implTable == null) {
                return;
            }

            // Iterate over the values in the table
            if (implTable instanceof Map<?, ?>) {
                Iterator<?> iter = ((Map<?, ?>) implTable).values().iterator();
                while (iter.hasNext()) {
                    Object obj = iter.next();
                    Object cclObject = cclField.get(obj);
                    if (classLoader == cclObject) {
                        iter.remove();
                    }
                }
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to clear context class loader referenced from sun.rmi.transport.Target ", e);
        }
    }


    /**
     * Clear the {@link ResourceBundle} cache of any bundles loaded by this class loader or any
     * class loader where this loader is a parent class loader.
     *
     * The ResourceBundle is using WeakReferences so it shouldn't be pinning the class loader in
     * memory. However, it is. Therefore clear ou the references.
     */
    private static void clearReferencesResourceBundles(ClassLoader classLoader) {
        // Get a reference to the cache
        try {
            Field cacheListField =
                    ResourceBundle.class.getDeclaredField("cacheList");
            cacheListField.setAccessible(true);

            // Java 6 uses ConcurrentMap
            // Java 5 uses SoftCache extends Abstract Map
            // So use Map and it *should* work with both
            Map<?, ?> cacheList = (Map<?, ?>) cacheListField.get(null);

            // Get the keys (loader references are in the key)
            Set<?> keys = cacheList.keySet();

            Set<Field> fieldsRef = new HashSet<Field>();

            // Iterate over the keys looking at the loader instances
            Iterator<?> keysIter = keys.iterator();

            int countRemoved = 0;

            while (keysIter.hasNext()) {
                Object key = keysIter.next();

                if (fieldsRef.isEmpty()) {
                    for (Field f : key.getClass().getDeclaredFields()) {
                        if (f.getName().equals("loaderRef")) {
                            f.setAccessible(true);
                            fieldsRef.add(f);
                            break; // Prior to Java 9, there should be only one relevant field in the CacheKey
                        } else if (f.getName().equals("callerRef")
                                || f.getName().equals("moduleRef")) { // Relevant for Java 9+
                            f.setAccessible(true);
                            fieldsRef.add(f);
                        }
                    }
                    if (fieldsRef.isEmpty()) {
                        logger.log(Level.WARNING, "Failed to clear ResourceBundle references");
                        return;
                    }
                }


                for (Field fieldRef : fieldsRef) {
                    ClassLoader loader = null;

                    WeakReference<?> weakRef =
                            (WeakReference<?>) fieldRef.get(key);

                    Object refVal = weakRef.get();
                    if (refVal != null) {
                        if (refVal instanceof ClassLoader) {
                            loader = (ClassLoader) refVal;
                        } else if (refVal.getClass().getName().equals("java.lang.Module")) { // Java 9
                            Method method = refVal.getClass().getMethod("getClassLoader");
                            loader = (ClassLoader) method.invoke(refVal);
                        }

                        while (loader != null && loader != classLoader) {
                            loader = loader.getParent();
                        }

                        if (loader != null) {
                            keysIter.remove();
                            countRemoved++;
                            break;
                        }
                    }
                }
            }

            if (countRemoved > 0 && logger.isLoggable(Level.FINE)) {
                logger.fine("Removed [" + countRemoved + "] ResourceBundle references from the cache");
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Failed to clear ResourceBundle references", e);
        }
    }
}
