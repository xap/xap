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

package net.jini.config;

import net.jini.security.Security;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides a standard means for obtaining {@link Configuration} instances, using a configurable
 * provider. This class cannot be instantiated. The configuration provider can be specified by
 * providing a resource named "META-INF/services/net.jini.config.Configuration" containing the name
 * of the provider class. If multiple resources with that name are available, then the one used will
 * be the last one returned by {@link ClassLoader#getResources ClassLoader.getResources}. If the
 * resource is not found, the {@link ConfigurationFile} class is used. <p>
 *
 * Downloaded code can specify its own class loader in a call to {@link #getInstance(String[],
 * ClassLoader) getInstance(String[], ClassLoader)} in order to use the configuration provider
 * specified in the JAR file from which it was downloaded. <p>
 *
 * The provider class must be a public, non-abstract class that implements
 * <code>Configuration</code>, and has a public constructor that has <code>String[]</code> and
 * <code>ClassLoader</code> parameters. The <code>String[]</code> parameter supplies
 * provider-specific options, for example specifying a source location or values for particular
 * entries. The constructor should throw {@link ConfigurationNotFoundException} if the configuration
 * specified by the <code>String[]</code> options argument cannot be found, or if <code>null</code>
 * was specified for the options argument and the class does not provide defaults. The constructor
 * should use the class loader argument when loading resources and classes, and should interpret a
 * <code>null</code> class loader argument as signifying the context class loader. <p>
 *
 * The resource file should contain the fully qualified name of the provider class. Space and tab
 * characters surrounding each name, as well as blank lines, are ignored. The comment character is
 * <tt>'#'</tt> (<tt>0x23</tt>); on each line, all characters following the first comment character
 * are ignored. The resource file must be encoded in UTF-8.
 *
 * @author Sun Microsystems, Inc.
 * @since 2.0
 *
 *
 * This implementation uses the {@link Logger} named <code>net.jini.config</code> to log information
 * at the following logging levels: <p>
 *
 * <table border="1" cellpadding="5" summary="Describes logging performed by the
 * ConfigurationProvider class at different logging levels">
 *
 * <caption halign="center" valign="top"><b><code> net.jini.config</code></b></caption>
 *
 * <tr> <th scope="col"> Level <th scope="col"> Description
 *
 * <tr> <td> {@link Level#FINE FINE} <td> problems getting a configuration
 *
 * </table>
 */
@com.gigaspaces.api.InternalApi
public class ConfigurationProvider {

    // hack for now just to disable from cyclic loading
    public static volatile boolean disableServicesConfig = false;

    private static final String resourceName =
            "META-INF/services/" + Configuration.class.getName();

    /**
     * Config logger.
     */
    private static final Logger logger = Logger.getLogger("net.jini.config");

    /**
     * This class cannot be instantiated.
     */
    private ConfigurationProvider() {
        throw new AssertionError();
    }

    /**
     * Creates and returns an instance of the configuration provider, using the specified options.
     * Specifying <code>null</code> for <code>options</code> uses provider-specific default options,
     * if available. Uses the current thread's context class loader to load resources and classes,
     * and supplies <code>null</code> as the class loader to the provider constructor.
     *
     * @param options values to use when constructing the configuration, or <code>null</code> if
     *                default options should be used
     * @return an instance of the configuration provider constructed with the specified options and
     * <code>null</code> for the class loader
     * @throws ConfigurationNotFoundException if <code>options</code> specifies a source location
     *                                        that cannot be found, or if <code>options</code> is
     *                                        <code>null</code> and the provider does not supply
     *                                        default options
     * @throws ConfigurationException         if an I/O exception occurs; if there are problems with
     *                                        the <code>options</code> argument or the format of the
     *                                        contents of any source location it specifies; if there
     *                                        is a problem with the contents of the resource file
     *                                        that names the configuration provider; if the
     *                                        configured provider class does not exist, is not
     *                                        public, is abstract, does not implement
     *                                        <code>Configuration</code>, or does not have a public
     *                                        constructor that has <code>String[]</code> and
     *                                        <code>ClassLoader</code> parameters; if the calling
     *                                        thread does not have permission to obtain information
     *                                        from the specified source; or if the provider does not
     *                                        have permission to access the context class loader.
     *                                        Any <code>Error</code> thrown while creating the
     *                                        provider instance is propagated to the caller; it is
     *                                        not wrapped in a <code>ConfigurationException</code>.
     */
    public static Configuration getInstance(String[] options)
            throws ConfigurationException {
        return getInstance(options, null);
    }

    /**
     * Creates and returns an instance of the configuration provider, using the specified options
     * and class loader. Specifying <code>null</code> for <code>options</code> uses
     * provider-specific default options, if available. Uses the specified class loader to load
     * resources and classes, or the current thread's context class loader if <code>cl</code> is
     * <code>null</code>. Supplies <code>cl</code> as the class loader to the provider constructor.
     * <p>
     *
     * Downloaded code can specify its own class loader in a call to this method in order to use the
     * configuration provider specified in the JAR file from which it was downloaded.
     *
     * @param options values to use when constructing the configuration, or <code>null</code> if
     *                default options should be used
     * @param cl      the class loader to load resources and classes, and to pass when constructing
     *                the provider. If <code>null</code>, uses the context class loader.
     * @return an instance of the configuration provider constructed with the specified options and
     * class loader
     * @throws ConfigurationNotFoundException if <code>options</code> specifies a source location
     *                                        that cannot be found, or if <code>options</code> is
     *                                        <code>null</code> and the provider does not supply
     *                                        default options
     * @throws ConfigurationException         if an I/O exception occurs; if there are problems with
     *                                        the <code>options</code> argument or the format of the
     *                                        contents of any source location it specifies; if there
     *                                        is a problem with the contents of the resource file
     *                                        that names the configuration provider; if the
     *                                        configured provider class does not exist, is not
     *                                        public, is abstract, does not implement
     *                                        <code>Configuration</code>, or does not have a public
     *                                        constructor that has <code>String[]</code> and
     *                                        <code>ClassLoader</code> parameters; if the calling
     *                                        thread does not have permission to obtain information
     *                                        from the specified source; or if <code>cl</code> is
     *                                        <code>null</code> and the provider does not have
     *                                        permission to access the context class loader. Any
     *                                        <code>Error</code> thrown while creating the provider
     *                                        instance is propagated to the caller; it is not
     *                                        wrapped in a <code>ConfigurationException</code>.
     */
    public static Configuration getInstance(String[] options, ClassLoader cl)
            throws ConfigurationException {
        ClassLoader resourceLoader = (cl != null) ? cl :
                (ClassLoader) Security.doPrivileged(
                        new PrivilegedAction() {
                            public Object run() {
                                return Thread.currentThread().getContextClassLoader();
                            }
                        });
        final ClassLoader finalResourceLoader = (resourceLoader == null)
                ? Utilities.bootstrapResourceLoader : resourceLoader;
        String cname = null;
        ConfigurationException configEx = null;
        try {
            cname = (String) Security.doPrivileged(
                    new PrivilegedExceptionAction() {
                        public Object run()
                                throws ConfigurationException, IOException {
                            URL resource = null;
                            Enumeration providers =
                                    finalResourceLoader.getResources(resourceName);
                            while (providers.hasMoreElements()) {
                                resource = (URL) providers.nextElement();
                            }
                            return (resource == null)
                                    ? null : getProviderName(resource);
                        }
                    });
        } catch (PrivilegedActionException e) {
            Exception e2 = e.getException();
            if (e2 instanceof ConfigurationException) {
                configEx = (ConfigurationException) e2;
            } else {
                configEx = new ConfigurationException(
                        "problem accessing provider resources", e2);
            }
        } catch (RuntimeException e) {
            configEx = new ConfigurationException(
                    "problem accessing provider resources", e);
        }
        if (configEx != null) {
            logger.log(Level.FINE, "getting configuration provider throws",
                    configEx);
            throw configEx;
        }
        if (!disableServicesConfig && options.length > 0 && options[0].endsWith("services.config")) {
            Class serviceConfigLoaderClass = null;
            try {
                serviceConfigLoaderClass = resourceLoader.loadClass("com.j_spaces.core.service.ServiceConfigLoader");
            } catch (ClassNotFoundException e) {
                // not running wiht JSpaces, disabled
            }
            if (serviceConfigLoaderClass != null) {
                Configuration configuration = null;
                try {
                    Method method = serviceConfigLoaderClass.getMethod("getConfiguration", new Class[0]);
                    configuration = (Configuration) method.invoke(null, null);
                } catch (InvocationTargetException e) {
                    throw new ConfigurationException("Failed to invoke getConfiguration", e.getTargetException());
                } catch (Exception e) {
                    throw new ConfigurationException("Failed to invoke getConfiguration", e);
                }
                if (options.length > 1) {
                    String[] newOptions = new String[options.length];
                    newOptions[0] = "-";
                    System.arraycopy(options, 1, newOptions, 1, newOptions.length - 1);
                    configuration = new AggregateConfig(configuration, newOptions, cl);
                }
                return configuration;
            }
        }
        if (cname == null) {
            return new ConfigurationFile(options, cl);
        }
        try {
            Class cls = Class.forName(cname, true, resourceLoader);
            if (!Configuration.class.isAssignableFrom(cls)) {
                configEx = new ConfigurationException(
                        "provider class " + cname +
                                " does not implement Configuration");
            } else {
                Constructor cons = cls.getConstructor(
                        new Class[]{String[].class, ClassLoader.class});
                return (Configuration) cons.newInstance(
                        new Object[]{options, cl});
            }
        } catch (ClassNotFoundException e) {
            configEx = new ConfigurationException(
                    "provider class " + cname + " not found");
        } catch (NoSuchMethodException e) {
            configEx = new ConfigurationException(
                    "provider class " + cname +
                            " does not have the right constructor");
        } catch (IllegalAccessException e) {
            configEx = new ConfigurationException(
                    "provider class " + cname + " constructor is not public");
        } catch (InstantiationException e) {
            configEx = new ConfigurationException(
                    "provider class " + cname + " is abstract");
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof ConfigurationException) {
                configEx = (ConfigurationException) t;
            } else {
                configEx = new ConfigurationException(
                        "problem with provider class", t);
            }
        } catch (RuntimeException e) {
            configEx = new ConfigurationException(
                    "problem with provider class", e);
        }
        logger.log(Level.FINE, "getting configuration throws", configEx);
        throw configEx;
    }

    /**
     * Returns the configuration provider class name specified in the contents of the URL.
     */
    private static String getProviderName(URL url)
            throws ConfigurationException, IOException {
        try (InputStream in = url.openStream()) {
            String result;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, "utf-8"))) {
                result = null;
                while (true) {
                    String line = reader.readLine();
                    if (line == null) {
                        break;
                    }
                    int commentPos = line.indexOf('#');
                    if (commentPos >= 0) {
                        line = line.substring(0, commentPos);
                    }
                    line = line.trim();
                    int len = line.length();
                    if (len != 0) {
                        if (result != null) {
                            throw new ConfigurationException(
                                    "resource specifies multiple providers");
                        }
                        result = line;
                    }
                }
            }
            if (result == null) {
                throw new ConfigurationException(
                        "resource specifies no providers");
            }
            return result;
        }
    }
}
