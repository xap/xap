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

import com.gigaspaces.classloader.CustomURLClassLoader;
import org.jini.rio.core.jsb.ComponentLoader;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The CommonClassLoader implements {@link org.jini.rio.core.jsb.ComponentLoader} interface and is
 * created by the {@link org.jini.rio.boot.RioServiceDescriptor} or {@link
 * org.jini.rio.boot.RioActivatableServiceDescriptor} when starting a Rio service and contains
 * common declared platform JARs to be made available to its children.
 *
 * <p>The CommonClassLoader enables a platform oriented framework conducive towards creating a
 * layered product. The resulting capabilities allow the declaration of JARs that are added to the
 * CommonClassLoader, making the classes accessible by all ClassLoader instances which delegate to
 * the CommonClassLoader. In this fashion a platform can be declared, initialized and made
 * available.
 *
 * <p>The ClassLoader hierarchy when starting a Rio service is as follows : <br> <br> <table
 * cellpadding="2" cellspacing="2" border="0" style="text-align: left; width: 50%;"> <tbody> <tr>
 * <td style="vertical-align: top;"><span style="font-family: monospace;">&nbsp; &nbsp;
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp; AppCL</span><br style="font-family: monospace;"> <span
 * style="font-family: monospace;">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * |</span><br style="font-family: monospace;"> <span style="font-family:
 * monospace;">&nbsp;&nbsp;&nbsp; CommonClassLoader </span><br style="font-family: monospace;">
 * <span style="font-family: monospace;">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * +</span><br style="font-family: monospace;"> <span style="font-family:
 * monospace;">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; |</span><br
 * style="font-family: monospace;"> <span style="font-family: monospace;">&nbsp;&nbsp;&nbsp;
 * +-------+-------+----...---+</span><br style="font-family: monospace;"> <span style="font-family:
 * monospace;">&nbsp;&nbsp;&nbsp; |&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
 * |&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; |</span><br style="font-family:
 * monospace;"> <span style="font-family: monospace;">&nbsp; Service-1CL&nbsp;&nbsp;
 * Service-2CL&nbsp; Service-nCL</span><br> </td> </tr> </tbody> </table>
 */
@com.gigaspaces.api.InternalApi
public class CommonClassLoader extends CustomURLClassLoader implements ComponentLoader {
    private static final String COMPONENT = "org.jini.rio.boot";
    private static Logger logger = LoggerFactory.getLogger(COMPONENT);
    private static Map components = new HashMap();
    private static ArrayList codebaseComponents = new ArrayList();
    private static CommonClassLoader instance;

    private volatile boolean disableSmartGetUrl = false;

    public void setDisableSmartGetUrl(boolean disableSmartGetUrl) {
        this.disableSmartGetUrl = disableSmartGetUrl;
    }

    /**
     * Create a CommonClassLoader
     */
    private CommonClassLoader(ClassLoader parent) {
        super("CommonClassLoader", new URL[0], parent);
    }

    /**
     * Get an instance of the CommonCLassLoader
     *
     * @return The CommonClassLoader
     */
    public static synchronized CommonClassLoader getInstance() {
        if (instance == null) {
            instance = new CommonClassLoader(ClassLoader.getSystemClassLoader());
        }
        return (instance);
    }

    /**
     * Override getURLs to ensure when an Object is marshalled its annotation is correct
     *
     * @return Array of URLs
     */
    public URL[] getURLs() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = this;
        }
        URL[] urls = doGetURLs(cl);
        if (logger.isTraceEnabled()) {
            StringBuffer buffer = new StringBuffer();
            for (int i = 0; i < urls.length; i++) {
                if (i > 0)
                    buffer.append(", ");
                buffer.append(urls[i].toExternalForm());
            }
            logger.trace("Context ClassLoader={} URLs={}", cl.toString(), buffer.toString());
        }
        return (urls);
    }

    @Override
    public URL[] getSearchPath() {
        return super.getURLs();
    }

    /**
     * Get the URLs, ensuring when an Object is marshalled the annotation is correct
     *
     * @param cl The current context ClassLoader
     * @return Array of URLs
     */
    private URL[] doGetURLs(ClassLoader cl) {
        if (disableSmartGetUrl) {
            return super.getURLs();
        }
        URL[] urls = null;
        if (cl.equals(this)) {
            urls = super.getURLs();
        } else {
            if (cl instanceof ServiceClassLoader) {
                ServiceClassLoader scl = (ServiceClassLoader) cl;
                urls = scl.getURLs();
            } else {
                urls = super.getURLs();
            }
        }
        return (urls);
    }

    /**
     * @see org.jini.rio.core.jsb.ComponentLoader#testComponentExistence
     */
    public boolean testComponentExistence(String name) {
        boolean exists = false;
        /* First check if the class is registered in the component Map */
        synchronized (components) {
            if (components.containsKey(name))
                exists = true;
        }
        if (!exists) {
            /* Although not registered, it may be in our search path,
			 * so try and load the class */
            try {
                loadClass(name);
                exists = true;
            } catch (Throwable t) {
                if (logger.isTraceEnabled())
                    logger.trace("Failed to find class " + name);
            }
        }
        return (exists);
    }

    /**
     * Add common JARs
     *
     * @param jars Array of URLs
     */
    public void addCommonJARs(URL[] jars) {
        if (jars == null)
            return;
        for (int i = 0; i < jars.length; i++) {
            if (!hasURL(jars[i]))
                addURL(jars[i]);
        }
    }

    /**
     * Add common codebase JARs
     *
     * @param jars Array of URLs
     */
    void addCodebaseComponents(URL[] jars) {
        if (jars == null)
            return;
        for (int i = 0; i < jars.length; i++) {
            if (!codebaseComponents.contains(jars[i]))
                codebaseComponents.add(jars[i]);
        }
        addCommonJARs(jars);
    }

    /**
     * @see org.jini.rio.core.jsb.ComponentLoader#testResourceExistence
     */
    public boolean testResourceExistence(String name) {
        return ((getResource(name) == null ? false : true));
    }

    /**
     * @see org.jini.rio.core.jsb.ComponentLoader#addComponent
     */
    public void addComponent(String name, URL[] urls) {
        boolean added = false;
        boolean toAdd = false;
        boolean toReplace = false;
        synchronized (components) {
            if (components.containsKey(name)) {
			    /* Check if codebase matches */
                URL[] fetched = (URL[]) components.get(name);
                if (fetched.length == urls.length) {
                    for (int i = 0; i < fetched.length; i++) {
                        /* There is a difference, replace */
                        if (!fetched[i].equals(urls[i])) {
                            toReplace = true;
                            break;
                        }
                    }
                } else {
                    /* Since the codebase is different, replace the entry */
                    toReplace = true;
                }

            } else {
                /* Not found, add the entry */
                toAdd = true;
            }

            if (toAdd || toReplace) {
                added = true;
                if (logger.isTraceEnabled()) {
                    String action = (toAdd ? "Adding" : "Replacing");
                    logger.trace(action + " Component " + name);
                }
                components.put(name, urls);
            } else {
                if (logger.isTraceEnabled()) {
                    StringBuffer buffer = new StringBuffer();
                    URL[] codebase = (URL[]) components.get(name);
                    for (int i = 0; i < codebase.length; i++) {
                        if (i > 0)
                            buffer.append(":");
                        buffer.append(codebase[i].toExternalForm());
                    }
                    logger.trace(
                            "Component " + name + " has " +
                                    "already been registered with a " +
                                    "codebase of " + buffer.toString());
                }
            }
        }
        if (added) {
            for (int i = 0; i < urls.length; i++) {
                if (!hasURL(urls[i]))
                    addURL(urls[i]);
            }
        }
    }

    /**
     * @see org.jini.rio.core.jsb.ComponentLoader#load
     */
    public Object load(String name) throws
            ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (name == null)
            throw new NullPointerException("name is null");
        boolean registered = false;
        synchronized (components) {
            registered = components.containsKey(name);
        }
        if (!registered) {
            if (testComponentExistence(name)) {
                if (logger.isTraceEnabled())
                    logger.trace("Loading unregistered component " + name);
            } else {
                throw new ClassNotFoundException("Unregistered component " + name);
            }
        }
        Class component = loadClass(name);
        return (component.newInstance());
    }

    /**
     * Check if the URL already is registered
     */
    private boolean hasURL(URL url) {
        URL[] urls = getURLs();
        for (int i = 0; i < urls.length; i++) {
            if (urls[i].equals(url))
                return (true);
        }
        return (false);
    }
}
