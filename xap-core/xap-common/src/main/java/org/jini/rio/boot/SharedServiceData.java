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
package org.jini.rio.boot;

import com.gigaspaces.classloader.CustomURLClassLoader;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Statically hold different class loaders per jee container
 *
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SharedServiceData {

    private static final Map<String, ClassLoader> jeeClassLoaders = new HashMap<String, ClassLoader>();

    private static final Map<String, ClassLoader> webAppClassLoaders = new ConcurrentHashMap<String, ClassLoader>();

    private static final Map<String, List<Callable>> serviceDetails = new ConcurrentHashMap<String, List<Callable>>();

    private static final Map<String, List<Callable>> serviceMonitors = new ConcurrentHashMap<String, List<Callable>>();

    private static final Map<String, List<Callable<Boolean>>> memberAliveIndicators = new ConcurrentHashMap<String, List<Callable<Boolean>>>();

    private static final Map<String, List<Callable>> undeployingEventListeners = new ConcurrentHashMap<String, List<Callable>>();

    private static final Map<String, List<Object>> dumpProcessors = new ConcurrentHashMap<String, List<Object>>();

    private SharedServiceData() {

    }

    public static ClassLoader getJeeClassLoader(String jeeContainer, String... classesToLoad) throws Exception {
        synchronized (jeeClassLoaders) {
            ClassLoader classLoader = jeeClassLoaders.get(jeeContainer);
            if (classLoader == null) {
                List<URL> urls = BootUtil.toURLs(SystemLocations.singleton().libOptional(jeeContainer).toString());
                String name = "Gs" + capitalizeFirst(jeeContainer) + "Server";
                classLoader = new CustomURLClassLoader(name, urls.toArray(new URL[urls.size()]), CommonClassLoader.getInstance());
                if (classesToLoad != null) {
                    ClassLoader prevClassLoader = Thread.currentThread().getContextClassLoader();
                    Thread.currentThread().setContextClassLoader(classLoader);
                    try {
                        for (String classToLoad : classesToLoad) {
                            classLoader.loadClass(classToLoad);
                        }
                    } finally {
                        Thread.currentThread().setContextClassLoader(prevClassLoader);
                    }
                }

                jeeClassLoaders.put(jeeContainer, classLoader);
            }
            return classLoader;
        }
    }

    private static String capitalizeFirst(String s) {
        if (s == null || s.isEmpty())
            return s;
        char first = s.charAt(0);
        char firstUpper = Character.toUpperCase(first);
        return firstUpper == first ? s : firstUpper + s.substring(1);
    }

    public static void putWebAppClassLoader(String key, ClassLoader webAppClassLoader) {
        webAppClassLoaders.put(key, webAppClassLoader);
    }

    public static ClassLoader removeWebAppClassLoader(String key) {
        return webAppClassLoaders.remove(key);
    }

    public static void addServiceDetails(String key, Callable provider) {
        List<Callable> list = serviceDetails.get(key);
        if (list == null) {
            list = new ArrayList<Callable>();
            serviceDetails.put(key, list);
        }
        list.add(provider);
    }

    public static List<Callable> removeServiceDetails(String key) {
        return serviceDetails.remove(key);
    }

    public static void addServiceMonitors(String key, Callable provider) {
        List<Callable> list = serviceMonitors.get(key);
        if (list == null) {
            list = new ArrayList<Callable>();
            serviceMonitors.put(key, list);
        }
        list.add(provider);
    }

    public static List<Callable> removeServiceMonitors(String key) {
        return serviceMonitors.remove(key);
    }

    public static void addMemberAliveIndicator(String key, Callable<Boolean> provider) {
        List<Callable<Boolean>> list = memberAliveIndicators.get(key);
        if (list == null) {
            list = new ArrayList<Callable<Boolean>>();
            memberAliveIndicators.put(key, list);
        }
        list.add(provider);
    }

    public static List<Callable<Boolean>> removeMemberAliveIndicator(String key) {
        return memberAliveIndicators.remove(key);
    }

    public static void addUndeployingEventListener(String key, Callable provider) {
        List<Callable> list = undeployingEventListeners.get(key);
        if (list == null) {
            list = new ArrayList<Callable>();
            undeployingEventListeners.put(key, list);
        }
        list.add(provider);
    }

    public static List<Callable> removeUndeployingEventListeners(String key) {
        return undeployingEventListeners.remove(key);
    }

    public static void addDumpProcessors(String key, Object provider) {
        List<Object> list = dumpProcessors.get(key);
        if (list == null) {
            list = new ArrayList<Object>();
            dumpProcessors.put(key, list);
        }
        list.add(provider);
    }

    public static List<Object> removeDumpProcessors(String key) {
        return dumpProcessors.remove(key);
    }
}
