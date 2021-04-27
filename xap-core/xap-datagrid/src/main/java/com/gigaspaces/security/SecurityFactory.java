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


package com.gigaspaces.security;

import com.j_spaces.kernel.SystemProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.Properties;

/**
 * A factory for creating an {@link SecurityManager} and locating of security properties file by
 * name.
 *
 * @author Moran Avigdor
 * @since 7.0.1
 */

public class SecurityFactory {

    public final static String DEFAULT_SECURITY_RESOURCE = "security.properties";
    public final static String DEFAULT_SECURITY_DIRECTORY = "config/security/";
    private static final Logger logger = LoggerFactory.getLogger("com.gigaspaces.security");
    /**
     * Reference-implementation for backwards compatibility
     */
    private final static String DEFAULT_SECURITY_MANAGER_CLASS_REFERENCE_IMPL = "com.gigaspaces.security.fs.FileSecurityManager";


    /**
     * Creates a {@link SecurityManager} instance using the provided security properties. The
     * property key {@link SecurityManager#SECURITY_MANAGER_CLASS_PROPERTY_KEY} identifies the class
     * name to use to load the security manager implementation.
     *
     * @param securityProperties The security properties to use to create the security manager and
     *                           underlying components.
     * @return The security manager instance.
     * @throws SecurityException if failed to create the security manager.
     */
    public static SecurityManager createSecurityManager(Properties securityProperties) {
        try {
            if (securityProperties == null) {
                securityProperties = new Properties();
            }
            if (logger.isDebugEnabled()) {
                String property = securityProperties.getProperty(SecurityManager.SECURITY_MANAGER_CLASS_PROPERTY_KEY);
                if (property != null) {
                    logger.debug("Security security-manager class: " + property);
                }
            }
            String classname = securityProperties.getProperty(SecurityManager.SECURITY_MANAGER_CLASS_PROPERTY_KEY,
                    DEFAULT_SECURITY_MANAGER_CLASS_REFERENCE_IMPL).trim();
            Class<? extends SecurityManager> securityManagerClass = Thread.currentThread().getContextClassLoader().loadClass(classname).asSubclass(
                    SecurityManager.class);
            SecurityManager securityManager = securityManagerClass.newInstance();
            securityManager.init(securityProperties);
            return securityManager;
        } catch (Exception e) {
            throw new SecurityException("Failed to create Security Manager", e);
        }
    }

    /**
     * Loads the specified properties file from the resource. see {@link
     * #findSecurityProperties(String)}. If no resource is found, a {@link SecurityException} is
     * thrown. <p> The properties file is used to configure the different security components. </p>
     *
     * @param resourceName The full resource name. If <code>null</code> loads the default.
     * @return a properties file holding security related key-value pairs.
     * @throws SecurityException if any exception was thrown while trying to load the resource, or
     *                           if the resource could not be located.
     */
    public static Properties loadSecurityProperties(String resourceName) {
        InputStream resourceAsStream = findSecurityProperties(resourceName);
        try {
            if (resourceAsStream == null) {
                throw new FileNotFoundException("Could not locate resource [" + resourceName + "] or the default ["
                        + DEFAULT_SECURITY_RESOURCE + "] in the classpath or under [" + DEFAULT_SECURITY_DIRECTORY
                        + "] directory.");
            }

            Properties props = new Properties();
            props.load(resourceAsStream);
            return props;
        } catch (IOException e) {
            throw new SecurityException("Failed to load security properties file", e);
        } finally {
            try {
                if (resourceAsStream != null) resourceAsStream.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Load a component's security properties file.
     *
     * @param component     The component name, e.g. "grid"
     * @param useMinusDLast if -Dcom.gs.security.properties-file is set, use it after looking up default component file
     * @return The loaded properties
     * @throws SecurityException if couldn't locate/load security file
     * @since 12.2
     */
    public static Properties loadComponentSecurityProperties(String component, boolean useMinusDLast) {
        Properties securityProperties = new Properties();
        String fullName = component + "-" + SecurityFactory.DEFAULT_SECURITY_RESOURCE;
        InputStream resourceStream;
        String securityPropertyFile = System.getProperty(SystemProperties.SECURITY_PROPERTIES_FILE, fullName);
        if (useMinusDLast) {
            resourceStream = SecurityFactory.findSecurityProperties(fullName);
            if (resourceStream == null) {
                resourceStream = SecurityFactory.findSecurityProperties(securityPropertyFile);
                if (logger.isDebugEnabled())
                    logger.debug("found security properties file by matching the component name [ " + fullName + " ]");
            }
        } else {
            resourceStream = SecurityFactory.findSecurityProperties(securityPropertyFile);
            if (logger.isDebugEnabled())
                logger.debug("found security property by matching the sys-prop provided name[ " + securityPropertyFile + " ]");
        }
        if (resourceStream != null) {
            try {
                securityProperties.load(resourceStream);
            } catch (IOException e) {
                throw new SecurityException("Failed to load security properties file", e);
            } finally {
                try {
                    resourceStream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        } else {
            //GS-8065: user has supplied a path to a properties file which could not be located
            String property = System.getProperty(SystemProperties.SECURITY_PROPERTIES_FILE);
            if (property != null) {
                throw new SecurityException(
                        "Failed to locate security properties file specified via system property: -D"
                                + SystemProperties.SECURITY_PROPERTIES_FILE
                                + "=" + property);
            }
        }
        return securityProperties;
    }

    /**
     * Finds the security properties file based on the resource name. The <tt>resourceName</tt> can
     * be a direct path to the file, or a reference to a resource located in the classpath. If the
     * resource is not found, it attempts to load the default <tt>security.properties</tt>. If no
     * resource is found, will return <code>null</code>.
     *
     * @param resourceName The full resource name. If <code>null</code> loads the default.
     * @return an input stream to the properties file. <code>null</code> if no resource was found.
     */
    public static InputStream findSecurityProperties(String resourceName) {
        InputStream resourceAsStream = null;
        if (resourceName != null) {
            //try loading it using url
            String resourceNameTrimmed = resourceName.trim().toLowerCase();
            if (resourceNameTrimmed.startsWith("http://") || resourceNameTrimmed.startsWith("https://")) {
                try {
                    resourceAsStream = new URL(resourceName).openStream();
                } catch (IOException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Security properties file url is malformed");
                    }
                    e.printStackTrace();
                }
            } else {
                //try loading it using direct path, otherwise look it up in the classpath
                File file = new File(resourceName);
                if (file.exists()) {
                    try {
                        resourceAsStream = new FileInputStream(file);
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException("Should not happen", e);
                    }
                } else {
                    //look for <resourceName>
                    resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
                    if (resourceAsStream == null) {
                        //look for config/security/<resourceName>
                        resourceName = DEFAULT_SECURITY_DIRECTORY + resourceName;
                        resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
                    }
                }
            }

        }

        //could not locate, try to locate default file
        //only if security properties file is not configured
        final boolean propertyNotDef = (null == System.getProperty(SystemProperties.SECURITY_PROPERTIES_FILE));
        if (resourceAsStream == null && propertyNotDef) {
            //look for security.properties
            resourceName = DEFAULT_SECURITY_RESOURCE;
            resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
            if (resourceAsStream == null) {
                //look for config/security/security.properties
                resourceName = DEFAULT_SECURITY_DIRECTORY + DEFAULT_SECURITY_RESOURCE;
                resourceAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName);
            }
        }

        if (resourceAsStream != null && logger.isDebugEnabled()) {
            logger.debug("Security properties file: " + resourceName);
        }

        return resourceAsStream;
    }
}
