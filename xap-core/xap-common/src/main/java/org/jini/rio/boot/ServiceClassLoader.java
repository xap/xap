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

import com.gigaspaces.classloader.CustomURLClassLoader;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.start.ClasspathBuilder;
import net.jini.loader.ClassAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.function.Supplier;

/**
 * The ServiceClassLoader overrides getURLs(), ensuring all classes that need to be annotated with
 * specific location(s) are returned appropriately
 */
@com.gigaspaces.api.InternalApi
public class ServiceClassLoader extends CustomURLClassLoader implements ClassAnnotation {
    private static final Logger logger = LoggerFactory.getLogger("com.gigaspaces.service.classloading");
    /**
     * URLs that this class loader will to search for and load classes
     */
    private URL[] searchPath;
    private List<URL> libPath;
    private URL slashPath;
    /**
     * The ClassAnnotator to use
     */
    private final ClassAnnotator annotator;

    private boolean parentFirst = Boolean.parseBoolean(System.getProperty("com.gs.pu.classloader.parentFirst", "false"));
    private final boolean autoDetectSlf4JBinding = Boolean.parseBoolean(System.getProperty("com.gs.pu.classloader.auto-detect-slf4j", "true"));
    private boolean slf4jBindingCheckRequired = autoDetectSlf4JBinding;

    private CodeChangeClassLoadersManager codeChangeClassLoadersManager;
    private final Set<String> systemClassExcludes = initSystemClassExcludes();
    private final static String SLF4J_STATIC_LOGGER_BINDER_PATH = "org/slf4j/impl/StaticLoggerBinder.class";

    private static Set<String> initSystemClassExcludes() {
        Set<String> result = new LinkedHashSet<>();
        String s = GsEnv.property("com.gs.pu.classloader.system-classes.exclude").get();
        if (s != null) {
            Collections.addAll(result, s.split(","));
        }
        return result;
    }

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
        this.searchPath = searchPath != null ? searchPath : new URL[0];
    }

    public CodeChangeClassLoadersManager getCodeChangeClassLoadersManager() {
        return codeChangeClassLoadersManager;
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
    @Override
    public synchronized URL[] getSearchPath() {
        return searchPath;
    }

    /**
     * Appends the specified URLs to the list of URLs to search for classes and resources.
     */
    public synchronized void addURLs(List<URL> urls) {
        ArrayList<URL> searchList = new ArrayList<URL>();
        for (URL url : searchPath)
            searchList.add(url);

        for (URL url : urls) {
            super.addURL(url);
            searchList.add(url);
        }

        searchPath = searchList.toArray(new URL[searchList.size()]);
    }

    public void addURLs(ClasspathBuilder classpathBuilder) {
        try {
            addURLs(classpathBuilder.toURLs());
        } catch (MalformedURLException e) {
            throw new RuntimeException("Failed to add jars: " + classpathBuilder.toFilesNames(), e);
        }
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

        // 2. If class is a system class, check in the system class loader, so processing units won't be able to override it
        if (isSystemClass(name)) {
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


    /**
     * NOTE: this method is not thread-safe, should only be called from a synchronized context
     */
    private boolean isSystemClass(String name) {
        // Special case to ensure GigaSpaces-custom-jul-slf4j bridge is preferred.
        if (name.equals("org.slf4j.bridge.SLF4JBridgeHandler")) {
            return true;
        }
        if (slf4jBindingCheckRequired && name.startsWith("org.slf4j.")) {
            // This approach is derived from SLF4J's LoggerFactory.bind().
            // Note: this is valid for slf4j 1.7 and lower, but not for later versions: http://www.slf4j.org/faq.html#changesInVersion18
            boolean hasSlf4jStaticLoggerBinder = this.findResource(SLF4J_STATIC_LOGGER_BINDER_PATH) != null;
            if (logger.isDebugEnabled())
                logger.debug("autoDetectSlf4JBinding enabled: hasSlf4jStaticLoggerBinder = " + hasSlf4jStaticLoggerBinder + " (while loading class " + name +")");
            if (hasSlf4jStaticLoggerBinder) {
                systemClassExcludes.addAll(Arrays.asList(
                        "org.slf4j.",                   // SLF4J
                        "org.apache.commons.logging.",  // JCL
                        "ch.qos.logback.",              // Logback
                        "org.apache.log4j.",            // Log4j
                        "org.apache.logging.log4j."     // Log4j2
                ));
            }
            slf4jBindingCheckRequired = false;
        }
        return !matches(name, systemClassExcludes);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        if (autoDetectSlf4JBinding && name.equals(SLF4J_STATIC_LOGGER_BINDER_PATH)) {
            final URL resource = this.findResource(SLF4J_STATIC_LOGGER_BINDER_PATH);
            if (logger.isTraceEnabled()) {
                logger.debug("autoDetectSlf4JBinding enabled: resource loaded from service class loader");
            }
            if (resource != null)
                return Collections.enumeration(Collections.singleton(resource));
        }

        return super.getResources(name);
    }

    protected static boolean matches(String className, Collection<String> patterns) {
        for (String pattern : patterns) {
            if (pattern.endsWith(".")) {
                if (className.startsWith(pattern))
                    return true;
            } else {
                if (className.equals(pattern))
                    return true;
            }
        }
        return false;
    }

    public ClassLoader getCodeChangeClassLoader(SupportCodeChangeAnnotationContainer supportCodeChangeAnnotationContainer) {
        ClassLoader classLoader = codeChangeClassLoadersManager.getCodeChangeClassLoader(supportCodeChangeAnnotationContainer);
        if(logger.isTraceEnabled()){
            logger.trace("In ServiceClassLoader["+this+"], asked for class-loader with version ["+ supportCodeChangeAnnotationContainer.getVersion() + "] " +
                    " from CodeChangeClassLoadersManager ["+ codeChangeClassLoadersManager +"]. " +
                    "Got class-loader ["+classLoader+" ]");
        }
        return classLoader;
    }

    public void initCodeChangeClassLoadersManager(boolean supportCodeChange, int maxClassLoaders) {
        codeChangeClassLoadersManager = new CodeChangeClassLoadersManager(this, supportCodeChange, maxClassLoaders);
    }

    public static boolean appendIfContext(Supplier<ClasspathBuilder> classpathBuilderSupplier) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader instanceof ServiceClassLoader) {
            ((ServiceClassLoader) classLoader).addURLs(classpathBuilderSupplier.get());
            return true;
        } else {
            return false;
        }
    }
}
