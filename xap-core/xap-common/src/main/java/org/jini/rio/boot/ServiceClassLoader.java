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

import net.jini.loader.ClassAnnotation;

import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The ServiceClassLoader overrides getURLs(), ensuring all classes that need to be annotated with
 * specific location(s) are returned appropriately
 */
@com.gigaspaces.api.InternalApi
public class ServiceClassLoader extends CustomURLClassLoader implements ClassAnnotation {
    final private static Logger logger = Logger.getLogger("com.gigaspaces.lrmi.classloading.level");
    /**
     * URLs that this class loader will to search for and load classes
     */
    private List<URL> searchPath;
    private List<URL> libPath;
    private URL slashPath;
    /**
     * The ClassAnnotator to use
     */
    private final ClassAnnotator annotator;

    private boolean parentFirst = Boolean.parseBoolean(System.getProperty("com.gs.pu.classloader.parentFirst", "false"));

    private ManagerTaskClassLoader managerTaskClassLoader;

    /**
     * Constructs a new ServiceClassLoader for the specified URLs having the given parent. The
     * constructor takes two sets of URLs. The first set is where the class loader loads classes
     * from, the second set is what it returns when getURLs() is called.
     *
     * @param searchPath Array of URLs to search for classes
     * @param annotator  Array of URLs to use for the codebase
     * @param parent     Parent ClassLoader to delegate to
     */
    public ServiceClassLoader(String name, URL[] searchPath, ClassAnnotator annotator, ClassLoader parent) {
        super(name, searchPath, parent);
        if (annotator == null)
            throw new NullPointerException("annotator is null");
        this.annotator = annotator;
        this.searchPath = Collections.unmodifiableList(
                searchPath != null ? Arrays.asList(searchPath) : new ArrayList<URL>());
        managerTaskClassLoader = new ManagerTaskClassLoader(this);
    }

    public ManagerTaskClassLoader getManagerTaskClassLoader() {
        return managerTaskClassLoader;
    }

    /**
     * Get the {@link org.jini.rio.boot.ClassAnnotator} created at construction time
     */
    public ClassAnnotator getClassAnnotator() {
        return (annotator);
    }

    /**
     * Get the URLs to be used for class annotations as determined by the {@link
     * org.jini.rio.boot.ClassAnnotator}
     */
    public URL[] getURLs() {
        return (annotator.getURLs());
    }

    public void setParentClassLoader(ClassLoader classLoader) throws Exception {
        Field field = ClassLoader.class.getDeclaredField("parent");
        field.setAccessible(true);
        field.set(this, classLoader);
    }

    /**
     * Get the search path of URLs for loading classes and resources
     *
     * @return The array of <code>URL[]</code> which corresponds to the search path for the class
     * loader; that is, the array elements are the locations from which the class loader will load
     * requested classes.
     */
    public synchronized List<URL> getSearchPath() {
        return searchPath;
    }

    /**
     * Appends the specified URLs to the list of URLs to search for classes and resources.
     */
    public synchronized void addURLs(List<URL> urls) {
        for (URL url : urls)
            super.addURL(url);

        ArrayList searchList = new ArrayList();
        searchList.addAll(searchPath);
        searchList.addAll(urls);
        searchPath = Collections.unmodifiableList(searchPath);
    }

    public synchronized void setLibPath(List<URL> urls) {
        addURLs(urls);
        libPath = urls;
    }

    public synchronized void setSlashPath(URL url) {
        addURLs(Collections.singletonList(url));
        slashPath = url;
    }

    public synchronized List<URL> getLibPath() {
        return libPath;
    }

    public synchronized URL getSlashPath() {
        return slashPath;
    }

    /**
     * Get the class annotations as determined by the {@link org.jini.rio.boot.ClassAnnotator}
     *
     * @see net.jini.loader.ClassAnnotation#getClassAnnotation
     */
    public String getClassAnnotation() {
        return (annotator.getClassAnnotation());
    }

    @Override
    public synchronized URL getResource(String name) {
        // 1. if parent first, try it
        URL url = null;
        if (parentFirst && getParent() != null) {
            url = getParent().getResource(name);
        }
        if (url != null) {
            return url;
        }
        // 2. try locally
        url = findResource(name);
        if (url != null) {
            return url;
        }
        // 3. if parent last, try it
        if (!parentFirst && getParent() != null) {
            url = getParent().getResource(name);
        }

        return url;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        return loadClass(name, false);
    }

    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class clazz = null;
        // 1. check if we already loaded the class
        clazz = findLoadedClass(name);
        if (clazz != null) {
            if (resolve) {
                resolveClass(clazz);
            }
            return clazz;
        }

        // 2. check in the system class loader, so processing units won't be able to override it
        try {
            clazz = getSystemClassLoader().loadClass(name);
            if (clazz != null) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        } catch (ClassNotFoundException e) {
            // ignore
        }

        // 3. if parent first, try it first
        if (parentFirst && getParent() != null) {
            try {
                clazz = getParent().loadClass(name);
                if (clazz != null) {
                    if (resolve) {
                        resolveClass(clazz);
                    }
                    return clazz;
                }
            } catch (ClassNotFoundException e) {
                // ignore
            }
        }
        // 4. search locally
        try {
            clazz = findClass(name);
            if (clazz != null) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        } catch (ClassNotFoundException e) {
            // ignore
        }
        // 5. attempt at child lrmi class loaders
        try {
            clazz = AdditionalClassProviderFactory.loadClass(name, true);
            if (clazz != null) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }
        } catch (ClassNotFoundException e) {
            // ignore
        }

        // 6. if parent last, try it
        if (!parentFirst && getParent() != null) {
            try {
                clazz = getParent().loadClass(name);
                if (clazz != null) {
                    if (resolve) {
                        resolveClass(clazz);
                    }
                    return clazz;
                }
            } catch (ClassNotFoundException e) {
                // ignore
            }
        }
        throw new ClassNotFoundException(name);
    }

    public ClassLoader getTaskClassLoader(SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer) {
        ClassLoader classLoader = managerTaskClassLoader.getTaskClassLoader(supportCodeChangeAnnotationContainer);
        if(logger.isLoggable(Level.FINEST)){
            logger.finest("In ServiceClassLoader["+this+"], asked for class-loader with version ["+ supportCodeChangeAnnotationContainer.getVersion() + "] " +
                    " from ManagerTaskClassLoader ["+managerTaskClassLoader+"]. " +
                    "Got class-loader ["+classLoader+" ]");
        }
        return classLoader;
    }
}