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

import com.gigaspaces.config.ConfigurationException;
import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.internal.io.XmlUtils;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.Constants;
import com.j_spaces.core.Constants.DCache;
import com.j_spaces.core.cluster.ClusterXML;
import com.j_spaces.core.exception.ClusterConfigurationException;

import org.jini.rio.boot.BootUtil;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.ParserConfigurationException;

/**
 * The class is responsible on loading the product resources such as space/container/cluster schema
 * xsl files, static cluster xml files, custom properties files, DCache and JMS config files etc.
 *
 * It also provides the common methods to get the resource InputStream or URL using the current
 * thread context class loader. The useful methods for such purpose will be: {@link
 * com.j_spaces.kernel.ResourceLoader#getResourceStream(String)} {@link
 * com.j_spaces.kernel.ResourceLoader#getResourceURL(String)}
 *
 * TODO currently uses classpath for loading resources. We should provide later capability to load
 * resources from httpxx and other methods and protocols.
 *
 * @author Gershon Diner
 * @version 1.0
 * @since 5.2
 */
@com.gigaspaces.api.InternalApi
public class ResourceLoader {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_RESOURCE_LOADER);

    public static URL getServicesConfigUrl() {
        URL servicesConfig = ResourceLoader.getResourceURL("config/services/services.config");
        if (servicesConfig == null) {
            servicesConfig = ResourceLoader.getResourceURL("config/services/services.config.template");
            if (servicesConfig == null) {
                try {
                    //we don't use services.config anymore and the services.config.template was removed in version 12.0.0
                    //TODO remove getResourceURL calls and simplify Configuration loaded by com.j_spaces.core.service.ServiceConfigLoader
                    return new URL("file://dummy--services.config");
                } catch (MalformedURLException e) {
                    return null;
                }
            }
            // now remove the template (we take care to load the correct one)
            try {
                servicesConfig = new URL(servicesConfig.toExternalForm().substring(0, servicesConfig.toExternalForm().lastIndexOf(".template")));
            } catch (MalformedURLException e) {
                return null;
            }
        }
        return servicesConfig;
    }


    static public InputStream getResourceStream(String name, String locatorBaseDir) {
        return getResourceStream(name, locatorBaseDir, true);
    }

    /**
     * Several attempts to search for the requested resource as an input stream:
     *
     * 1. It calls the (current thread) contextClassLoader.getResource(config/schemas/yyy.xml). if
     * not found --> 1.1 It calls the (current thread) contextClassLoader.getResource(yyy.xml). if
     * not found --> 2. It calls this classLoader.getResource(config/schemas/yyy.xml) (e.g. if
     * running in AppServer) if not found --> 2.1 It calls this classLoader.getResource(yyy.xml)
     * (e.g. if running in AppServer) if not found AND useOnlyClasspath == false (expensive
     * operation!) --> 2.2 if the resource was not found, instead of returning null, we attempt to
     * load it as a File IF it was passed as file format e.g. D:/gigaspaces/GenericJDBCProperties/HSQLProperties/jdbc.properties
     * 3. If Uses the com.gigaspaces.start.Locator.locate(yyy.xml) that searches recursively
     * under the GS, using root file system. Not the default.
     *
     * Some notes: In some scenarios, the ResourceLoader does not use the right class loader and
     * returns no resource. E.g. when running within application server the config and jar files
     * loaded using the Thread.currentThread().getContextClassLoader().getResource() but since the
     * lower level CL is used (of the APPServer) we should try also and use the
     * getClass().getResource() in case the first attempt failed to find the resource. A third
     * attempt, if the resource still was not found will be using the Locator facility that searches
     * for the resource under a base directory in the file system.
     *
     * @param name           name of resource to load
     * @param locatorBaseDir if not null we will try to search for the resource in the file system
     *                       under the locatorBaseDir as the base search path
     * @return InputStream containing the resource
     */
    static public InputStream getResourceStream(String name, String locatorBaseDir, boolean createIfNotExists) {
        InputStream result = null;
        try {
            //do not allow search using / prefix which does not work with classLoader.getResource()
            if (name.startsWith("/")) {
                name = name.substring(1);
            }
            Thread currentThread = Thread.currentThread();
            ClassLoader classLoader = currentThread.getContextClassLoader();
            int lastChar = name.lastIndexOf('/');
            String onlyResourceName = null;
            boolean searchedOnlyResourceName = false;
            if (lastChar > -1) {
                onlyResourceName = name.substring(lastChar + 1);
            }
            if (classLoader != null) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Going to load the resource <" + name + "> from the ContextClassLoader using CL: " + classLoader.getClass().getName());
                }
                result = classLoader.getResourceAsStream(name);
                if (result == null && onlyResourceName != null) {
                    //try search only with resource name
                    searchedOnlyResourceName = true;
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Going to load the resource <" + onlyResourceName + "> from the ContextClassLoader using CL: " + classLoader.getClass().getName());
                    }
                    result = classLoader.getResourceAsStream(onlyResourceName);
                }
                if (_logger.isDebugEnabled()) {
                    StringBuilder classLoaderHierarchy = new StringBuilder("ClassLoader Hierarchy: ");
                    ClassLoader tmpCL = classLoader;
                    while (tmpCL != null) {
                        classLoaderHierarchy.append(tmpCL.getClass().toString()).append(" <-- ");
                        tmpCL = tmpCL.getParent();
                    }
                    _logger.debug("Load resource: [" + (searchedOnlyResourceName ? onlyResourceName : name) + "] Thread: [" + currentThread.getName()
                            + "] using ClassLoader: [" + classLoader + "] \n"
                            + " [ " + classLoaderHierarchy.toString() + " ] \n"
                            + " [ Returning result: " + result + " ] \n"
                            + (_logger.isTraceEnabled() ? JSpaceUtilities.getStackTrace(new Exception("Debugging stack trace only (can be ignored): ")) : ""));
                }
            }
            if (result == null) {
                ClassLoader cl = ResourceLoader.class.getClassLoader();
                if (cl != null && !cl.equals(classLoader)) {
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("2nd try, going to load the resource <" + name + "> NOT from the ContextClassLoader but using CL: " + cl.getClass().getName());
                    }
                    result = cl.getResourceAsStream(name);
                    if (result == null && onlyResourceName != null) {
                        //try search only with resource name
                        searchedOnlyResourceName = true;
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("2nd try, going to load the resource <" + onlyResourceName + "> NOT from the ContextClassLoader but using CL: " + cl.getClass().getName());
                        }
                        result = cl.getResourceAsStream(onlyResourceName);
                    }
                    if (_logger.isDebugEnabled()) {
                        StringBuilder classLoaderHierarchy = new StringBuilder("ClassLoader Hierarchy: ");
                        ClassLoader tmpCL = cl;
                        while (tmpCL != null) {
                            classLoaderHierarchy.append(tmpCL.getClass().toString()).append(" <-- ");
                            tmpCL = tmpCL.getParent();
                        }
                        _logger.debug("Load resource: [" + (searchedOnlyResourceName ? onlyResourceName : name) + "] Thread: [" + currentThread.getName()
                                + "] using ClassLoader: [" + cl + "] \n"
                                + " [ " + classLoaderHierarchy.toString() + " ] \n"
                                + " [ Returning result: " + result + " ] \n"
                                + (_logger.isTraceEnabled() ? JSpaceUtilities.getStackTrace(new Exception("Debugging stack trace only (can be ignored): ")) : ""));
                    }
                }
            }

            if (result == null) {
                File file = new File(name);
                if (file.isFile() && file.exists()) {
                    URL fileURL = file.toURI().toURL();
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Going to load the resource <" + name + "> from the path: " + fileURL);
                    }
                    result = fileURL.openStream();
                    if (result != null) {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug("Load resource: [" + fileURL + "] \n"
                                    + " [ Returning result: " + result + " ] \n"
                                    + (_logger.isTraceEnabled() ? JSpaceUtilities.getStackTrace(new Exception("Debugging stack trace only (can be ignored): ")) : ""));
                        }
                        return result;
                    }
                }
            }


            if (result == null && locatorBaseDir != null && createIfNotExists) {
                File resourcePath = BootIOUtils.locate(locatorBaseDir, (onlyResourceName != null ? onlyResourceName : name));
                if (resourcePath != null) {
                    result = resourcePath.toURI().toURL().openStream();
                }
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Load resource using Locator utility searching from < " + locatorBaseDir + " > base directory: \n [" + (onlyResourceName != null ? onlyResourceName : name) + "] Thread: [" + currentThread.getName()
                            + "] using resource path: [" + resourcePath + "] \n"
                            + " [ Returning result: " + result + " ] \n"
                            + (_logger.isTraceEnabled() ? JSpaceUtilities.getStackTrace(new Exception("Debugging stack trace only (can be ignored): ")) : ""));
                }
            }
        } catch (Exception e) {
            //wrap ConfigurationException and throw relevant exception
            if (_logger.isDebugEnabled()) {
                _logger.debug("Failed to load resource: [" + name + "] ", e);
            }
        }
        return result;
    }

    /**
     * Loads the requested resource and returns its URL. Several attempts to search for the
     * requested resource:
     *
     * 1. It calls the (current thread) contextClassLoader.getResource(config/schemas/yyy.xml). if
     * not found --> 1.1 It calls the (current thread) contextClassLoader.getResource(yyy.xml). if
     * not found --> 2. It calls this classLoader.getResource(config/schemas/yyy.xml) (e.g. if
     * running in AppServer) if not found --> 2.1 It calls this classLoader.getResource(yyy.xml)
     * (e.g. if running in AppServer) if not found AND useOnlyClasspath == false (expensive
     * operation!) --> 2.2 if the resource was not found, instead of returning null, we attempt to
     * load it as a File IF it was passed as file format e.g. D:/gigaspaces/GenericJDBCProperties/HSQLProperties/jdbc.properties
     * 3. Uses the com.gigaspaces.start.Locator.locate(yyy.xml) that searches recursively under
     * the GS, using root file system. Not the default behavior
     *
     * Notes: In some scenarios, the ResourceLoader does not use the right class loader and returns
     * no resource. E.g. when running within application server the config and jar files loaded
     * using the Thread.currentThread().getContextClassLoader().getResource() but since the lower
     * level CL is used (of the APPServer) we should try also and use the getClass().getResource()
     * in case the first attempt failed to find the resource. A third attempt, if the resource still
     * was not found, it will be using the Locator facility that searches for the resource (using
     * only the resource name) under a base directory in the file system.
     *
     * @param name           name of resource to load
     * @param locatorBaseDir if not null we will try to search for the resource in the file system
     * @return URL containing the resource
     */
    static public URL getResourceURL(String name, String locatorBaseDir) {
        return BootUtil.getResourceURL(name, locatorBaseDir);
    }

    /**
     * Several attempts to search for the requested resource as an input stream:
     *
     * 1. It calls the (current thread) contextClassLoader.getResource(config/schemas/yyy.xml). if
     * not found --> 1.1 It calls the (current thread) contextClassLoader.getResource(yyy.xml). if
     * not found --> 2. It calls this classLoader.getResource(config/schemas/yyy.xml) (e.g. if
     * running in AppServer) if not found --> 2.1 It calls this classLoader.getResource(yyy.xml)
     * (e.g. if running in AppServer)
     *
     * Some notes: In some scenarios, the ResourceLoader does not use the right class loader and
     * returns no resource. E.g. when running within application server the config and jar files
     * loaded using the Thread.currentThread().getContextClassLoader().getResource() but since the
     * lower level CL is used (of the APPServer) we should try also and use the
     * getClass().getResource() in case the first attempt failed to find the resource.
     *
     * @param name name of resource to get
     * @return InputStream containing the resource
     */
    static public InputStream getResourceStream(String name) {
        return getResourceStream(name, null);
    }

    /**
     * Loads the requested resource and returns its URL. Several attempts to search for the
     * requested resource:
     *
     * 1. It calls the (current thread) contextClassLoader.getResource(config/schemas/yyy.xml). if
     * not found --> 1.1 It calls the (current thread) contextClassLoader.getResource(yyy.xml). if
     * not found --> 2. It calls this classLoader.getResource(config/schemas/yyy.xml) (e.g. if
     * running in AppServer) if not found --> 2.1 It calls this classLoader.getResource(yyy.xml)
     * (e.g. if running in AppServer) Notes: In some scenarios, the ResourceLoader does not use the
     * right class loader and returns no resource. E.g. when running within application server the
     * config and jar files loaded using the Thread.currentThread().getContextClassLoader().getResource()
     * but since the lower level CL is used (of the APPServer) we should try also and use the
     * getClass().getResource() in case the first attempt failed to find the resource.
     *
     * @param name name of resource to get
     * @return URL containing the resource
     */
    static public URL getResourceURL(String name) {
        return getResourceURL(name, null);
    }

    /**
     * Used to load xxx.properties file, parse it and create a Properties object out of it. The
     * system looks for the file using the Resource Bundle in the /config directory.
     *
     * @return Properties object contains the xxx.properties values.
     * @throws {@link ConfigurationException} in case the resource could not be loaded
     */
    static public Properties findCustomPropertiesObj(
            String schemaPropertiesFileName) throws IOException, ConfigurationException {
        InputStream schemaInputStream = null;
        String schemaPropertiesPath = null;
        boolean downloadingPropsFromHTTP = schemaPropertiesFileName.startsWith("http:");
        if (downloadingPropsFromHTTP) {
            try {
                URL theUrl = new URL(schemaPropertiesFileName);
                //go get the file from the http url (it is obviously somewhere under an HTTPD/web server
                URLConnection con = theUrl.openConnection();
                schemaInputStream = con.getInputStream();
                schemaPropertiesPath = schemaPropertiesFileName;
            } catch (Exception e) {
                throw new ConfigurationException("Failed to download properties file from '"
                        + schemaPropertiesFileName
                        + "' cause: "
                        + e.getClass().getName() + " message: " + e.getMessage(), e);
            }
        } else {
            schemaPropertiesPath = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                    + schemaPropertiesFileName + ".properties";

            schemaInputStream = getResourceStream(schemaPropertiesPath);
        }
        Properties schemaProperties = null;
        if (schemaInputStream != null) {
            if (_logger.isInfoEnabled()) {
                _logger.info("Loading properties file from: "
                        + (downloadingPropsFromHTTP ? schemaPropertiesFileName
                        : getResourceURL(schemaPropertiesPath).toString()));
            }
            //System.getProperties().load( new FileInputStream ( schemaPropertiesFile ) );
            schemaProperties = new Properties();
            schemaProperties.load(schemaInputStream);
            schemaInputStream.close();

            if (_logger.isDebugEnabled()) {
                _logger.debug("custom properties file values: ");
                schemaProperties.list(System.out);
            }
        } else {
            throw new ConfigurationException("Failed to load properties file: " + schemaPropertiesPath);
        }
        return schemaProperties;
    }

    /**
     * Using the _clusterMembersStream InputStream, we parse the DOM and fetch the value of the
     * <cluster-schema-name> tag. This name is the prefix of the requested cluster XSL schema file.
     * Using this, we now go and find the Cluster XSL schema. Look for the requested cluster XSL
     * schema file in the ResourceBundle (classpath) under <EAG home dir>/config/schemas/<reqested_schema_name>-cluster-schema.xsl.
     * If it does no find it in the disk (the default) it looks for it in the JSpaces.jar, in same
     * path. if the requested schema file does not exist in the disk config/schemas dir.. In this
     * case we load the default schema file which always exist in the resource under the path
     * config/schemas/default-cluster-schema.xml. NOTE that the <com.gs.home> must be part of the
     * classpath, in this case.
     *
     * @return InputStream to the found schema file. Note that this is a read case, so we do not
     * need to write the file to disk, just load it to the memory and return the InputStream.
     * @throws ClusterConfigurationException if the requested cluster schema was not found
     */
    static public InputStream findClusterXSLSchema(
            InputStream _clusterXMLInputStream)
            throws ClusterConfigurationException {
        Document clusterXMLDocumet;
        String schemaNameToBeUsed = null;
        try {
            // Obtaining a org.w3c.dom.Document from XML
            // TODO don't create factory every time
            /*
             * DocumentBuilderFactory factory =
             * DocumentBuilderFactory.newInstance(); DocumentBuilder _docBuilder =
             * factory.newDocumentBuilder();
             */
            clusterXMLDocumet = XmlUtils.getDocumentBuilder().parse(_clusterXMLInputStream);
            schemaNameToBeUsed = ClusterXML.getNodeValueIfExists(clusterXMLDocumet.getDocumentElement(),
                    ClusterXML.CLUSTER_SCHEMA_NAME_TAG);
            if (JSpaceUtilities.isEmpty(schemaNameToBeUsed)) {
                String missingSchemaMsg = "Could not find the <cluster-schema-name> tag in the cluster members xml file.";
                // if the _clusterXSLSchema is missing, it means no schema were ever
                // passed by the client
                // then we throw exception back to user. (we do NOT
                // load any default cluster XSL Schema files)
                throw new ClusterConfigurationException(missingSchemaMsg);

            }
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }
        return ResourceLoader.findClusterXSLSchema(schemaNameToBeUsed);
    }

    /**
     * Resolving cluster schema XSL from cluster xml. Using the _clusterMembersStream InputStream,
     * we parse the DOM and fetch the value of the <cluster-schema-name> tag. This name is the
     * prefix of the requested cluster XSL schema file. Using this, we now go and find the Cluster
     * XSL schema. Look for the requested cluster XSL schema file in the ResourceBundle (classpath)
     * under <EAG home dir>/config/schemas/<reqested_schema_name>-cluster-schema.xsl. If it does no
     * find it in the disk (the default) it looks for it in the JSpaces.jar, in same path. if the
     * requested schema file does not exist in the disk config/schemas dir.. In this case we throw
     * exception (which does not interrupt the system load but acts as a warning) NOTE that the
     * <com.gs.home> must be part of the classpath, in this case.
     *
     * @return InputStream to the found schema file. Note that this is a read case, so we do not
     * need to write the file to disk, just load it to the memory and return the InputStream.
     * @throws ClusterConfigurationException if the requested cluster schema was not found
     */
    static public InputStream resolveClusterXSLSchema(String _clusterMembers)
            throws ClusterConfigurationException {
        Document clusterXMLDocumet;
        String schemaNameToBeUsed = null;
        InputStream membersStream = ResourceLoader.findClusterXML(_clusterMembers);
        try {
            // Obtaining a org.w3c.dom.Document from XML
            /*
             * DocumentBuilderFactory factory =
             * DocumentBuilderFactory.newInstance(); DocumentBuilder _docBuilder =
             * factory.newDocumentBuilder();
             */
            clusterXMLDocumet = XmlUtils.getDocumentBuilder().parse(membersStream);
            schemaNameToBeUsed = ClusterXML.getNodeValueIfExists(clusterXMLDocumet.getDocumentElement(),
                    ClusterXML.CLUSTER_SCHEMA_NAME_TAG);
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        }

        InputStream schemaInputStream;
        if (JSpaceUtilities.isEmpty(schemaNameToBeUsed)) {
            String missingSchemaMsg = "Could not find the <cluster-schema-name> tag for cluster members xml file: "
                    + _clusterMembers;
            // if the _clusterXSLSchema is missing, it means no schema were ever
            // passed by the client
            // then we throw exception back to user. (we do NOT
            // load any default cluster XSL Schema files)
            throw new ClusterConfigurationException(missingSchemaMsg);

        }
        /*************************************************************************
         * look for the requested cluster XSL schema file in the ResourceBundle
         * (classpath) under <EAG home dir>/config/schemas/<reqested_schema_name>-cluster-schema.xsl.
         * If it does no find it in the disk (the default) it looks for it in the
         * JSpaces.jar, in same path. NOTE that the <com.gs.home> must be part of
         * the classpath, in this case.
         ************************************************************************/
        String schemaFilePath = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                + Constants.Schemas.SCHEMAS_FOLDER + "/" + schemaNameToBeUsed
                + ClusterXML.CLUSTER_SCHEMA_XSL_FILE_SUFFIX;

        schemaInputStream = getResourceStream(schemaFilePath);
        if (schemaInputStream != null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Loaded the cluster xsl schema < "
                        + getResourceURL(schemaFilePath)
                        + " > for the cluster config setup.");
            }
            return schemaInputStream;
        } else {
            /**
             * if the requested schema file does not exist in the disk
             * config/schemas dir.. In this case we load the default schema file
             * which always exist in the resource under the path
             * config/schemas/default-cluster-schema.xml
             */
            /*
             * String defaultSchemaFileName = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/" +
             * Constants.Schemas.SCHEMAS_FOLDER + "/" +
             * Constants.Schemas.DEFUALT_SCHEMA +
             * ClusterXML.CLUSTER_SCHEMA_XSL_FILE_SUFFIX; schemaInputStream =
             * getResourceStream(defaultSchemaFileName);
             * if(schemaInputStream != null) { System.out.println("Could not find
             * the cluster xsl schema file: " + schemaFilePath + ".\n Loaded the
             * default cluster xsl schema < " + defaultSchemaFileName + " > for the
             * cluster config setup.\n"); return schemaInputStream; } else {
             * //nothing to do in this case System.out.println("\nThe requested
             * cluster xsl schema file does not exist in the following path: " +
             * defaultSchemaFileName); }
             */
            String missingSchemaMsg = "Could not find the cluster xsl schema file: "
                    + schemaFilePath;
            // if the _clusterXSLSchema is missing, it means no schema were ever
            // passed by the client
            // then we throw exception back to user. (we do NOT
            // load any default cluster XSL Schema files)
            throw new ClusterConfigurationException(missingSchemaMsg);
        }
    }

    /**
     * Look for the requested cluster XML file in the ResourceBundle (classpath) under <EE home
     * dir>/config/<reqested_schema_name>-cluster.xml. If it does no find it in the disk (the
     * default) it looks for it in the JSpaces.jar, in same path. if the requested schema file does
     * not exist in the disk config/schemas dir.. In this case, DO we load the default schema file
     * which always exist in the resource under the path config/default-cluster.xml. NOTE that the
     * <com.gs.home> must be part of the classpath, in this case.
     *
     * @return InputStream to the found schema file. Note that this is a read case, so we do not
     * need to write the file to disk, just load it to the memory and return the InputStream.
     */
    static public InputStream findClusterXML(String _clusterXML) {
        String schemaNameToBeUsed = _clusterXML;
        InputStream schemaInputStream;
        /** Look for the requested cluster XML file in the ResourceBundle (classpath)
         * under <EE home dir>/config/<reqested_schema_name>-cluster.xml.
         * If it does no find it in the disk (the default) it looks for it in the JSpaces.jar, in same path.
         * if the requested schema file does not exist in the disk config/schemas dir..
         */
        String schemaFilePath = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                + schemaNameToBeUsed + ClusterXML.CLUSTER_XML_FILE_SUFFIX;

        schemaInputStream = getResourceStream(schemaFilePath);
        if (schemaInputStream != null) {
            if (_logger.isInfoEnabled()) {
                _logger.info("Loaded the cluster XML < "
                        + getResourceURL(schemaNameToBeUsed)
                        + " > for the cluster config setup.");
            }
            return schemaInputStream;
        } else {
            /**
             * if the requested schema file does not exist in the disk config/schemas dir..
             * In this case we load the default schema file which always exist in the resource
             * under the path /config/default-cluster.xml
             */
            String defaultSchemaFileName = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                    + Constants.Schemas.DEFAULT_SCHEMA
                    + ClusterXML.CLUSTER_XML_FILE_SUFFIX;

            schemaInputStream = getResourceStream(defaultSchemaFileName);
            if (schemaInputStream != null) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn("Could not find the cluster xml file: "
                            + schemaFilePath + ".\n Loaded the default cluster xml < "
                            + getResourceURL(defaultSchemaFileName)
                            + " > for the cluster config setup.");
                }
                return schemaInputStream;
            } else {
                //nothing to do in this case
                if (_logger.isWarnEnabled()) {
                    _logger.warn("The requested cluster xml file does not exist in the following path: "
                            + defaultSchemaFileName);
                }
            }
        }
        return schemaInputStream;
    }

    /**
     * Look for the requested cluster XSL schema file in the ResourceBundle (classpath) under <EE
     * home dir>/config/schemas/<reqested_schema_name>-cluster-schema.xsl. If it does no find it in
     * the disk (the default) it looks for it in the JSpaces.jar, in same path. if the requested
     * schema file does not exist in the disk config/schemas dir.. In this case we throw exception
     * (which does not interrupt the system load but acts as a warning) NOTE that the <com.gs.home>
     * must be part of the classpath, in this case.
     *
     * @return InputStream to the found schema file. Note that this is a read case, so we do not
     * need to write the file to disk, just load it to the memory and return the InputStream.
     * @throws ClusterConfigurationException if the requested cluster schema was not found
     */
    static public InputStream findClusterXSLSchema(String _clusterXSLSchema)
            throws ClusterConfigurationException {
        String schemaNameToBeUsed = _clusterXSLSchema;
        if (schemaNameToBeUsed.equalsIgnoreCase(ClusterXML.CLUSTER_SCHEMA_NAME_PARTITIONED_SYNC2BACKUP))
            schemaNameToBeUsed = ClusterXML.CLUSTER_SCHEMA_NAME_PARTITIONED;

        InputStream schemaInputStream;
        /** look for the requested cluster XSL schema file in the ResourceBundle (classpath)
         * under <EE home dir>/config/schemas/<reqested_schema_name>-cluster-schema.xsl.
         * If it does no find it in the disk (the default) it looks for it in the JSpaces.jar, in same path.
         * NOTE that the <com.gs.home> must be part of the classpath, in this case. **/
        String schemaFilePath = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                + Constants.Schemas.SCHEMAS_FOLDER + "/" + schemaNameToBeUsed
                + ClusterXML.CLUSTER_SCHEMA_XSL_FILE_SUFFIX;

        schemaInputStream = getResourceStream(schemaFilePath);
        if (schemaInputStream != null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Loaded the cluster xsl schema < "
                        + getResourceURL(schemaFilePath)
                        + " > for the cluster config setup.");
            }
            return schemaInputStream;
        } else {
            /**
             * if the requested schema file does not exist in the disk config/schemas dir..
             * In this case we load the default schema file which always exist in the resource
             * under the path config/schemas/default-cluster-schema.xml
             */
            /*String defaultSchemaFileName = Constants.Container.CONTAINER_CONFIG_DIRECTORY +  "/"
             + Constants.Schemas.SCHEMAS_FOLDER + "/"
			 + Constants.Schemas.DEFUALT_SCHEMA
			 + ClusterXML.CLUSTER_SCHEMA_XSL_FILE_SUFFIX;

			 schemaInputStream = getResourceStream(defaultSchemaFileName);
			 if(schemaInputStream != null)
			 {
			 System.out.println("Could not find the cluster xsl schema file: "
			 + schemaFilePath +  ".\n Instead, loaded the default cluster xsl schema < " + defaultSchemaFileName
			 + " > for the cluster config setup.\n");
			 return schemaInputStream;
			 }
			 else
			 {
			 //nothing to do in this case
			 System.out.println("\nThe requested cluster xsl schema file does not exist in the following path: "
			 + defaultSchemaFileName);
			 }*/
            String missingSchemaMsg = "Could not find the cluster xsl schema file: "
                    + _clusterXSLSchema;
            //if the _clusterXSLSchema is missing, it means no schema were ever passed by the client
            //then we throw exception back to user. (we do NOT
            //load any default cluster XSL Schema files)
            throw new ClusterConfigurationException(missingSchemaMsg);
        }
    }

    static public InputStream findContainerSchema(String schemaName) {
        return findContainerSchema(schemaName, true);
    }

    /**
     * Look for the requested container schema file in the ResourceBundle (classpath) under <EE home
     * dir>/config/schemas/<reqested_schema_name>-container-schema.xml. If it does no find it in the
     * disk (the default) it looks for it in the JSpaces.jar, in same path. if the requested schema
     * file does not exist in the disk config/schemas dir.. In this case we load the default schema
     * file which always exist in the resource under the path config/schemas/default-container-schema.xml.
     * NOTE that the <com.gs.home> must be part of the classpath, in this case.
     *
     * @return InputStream to the found schema file. Note that this is a read case, so we do not
     * need to write the file to disk, just parse it in memory.
     */
    static public InputStream findContainerSchema(String schemaName, boolean createIfNotExists) {
        String schemaNameToBeUsed = schemaName;
        InputStream schemaInputStream;
        if (JSpaceUtilities.isEmpty(schemaName)) {
            //if the schema name is missing, it means no schema were ever passed by the client
            //we use the default schema name only in case the old regular <container name>-config.xml
            //is missing, then we load the default container schema file. (same case is with the space config)
            schemaNameToBeUsed = Constants.Schemas.DEFAULT_SCHEMA;

        }
        /** Look for the requested container schema XML file in the ResourceBundle (classpath)
         * under <EE home dir>/config/schemas/<reqested_schema_name>-container-schema.xml.
         * If it does no find it in the disk (the default) it looks for it in the JSpaces.jar, in same path.
         * if the requested schema file does not exist in the disk config/schemas dir..
         */
        String schemaFilePath = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                + Constants.Schemas.SCHEMAS_FOLDER + "/" + schemaNameToBeUsed
                + Constants.Schemas.CONTAINER_SCHEMA_FILE_SUFFIX;

        schemaInputStream = getResourceStream(schemaFilePath, null, createIfNotExists);

        if (schemaInputStream != null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Loaded the container schema < "
                        + getResourceURL(schemaFilePath)
                        + " > for the container configuration.");
            }
            return schemaInputStream;
        } else if (createIfNotExists || !createIfNotExists &&
                JSpaceUtilities.getSpaceSchemas().contains(schemaName)) {
            /**
             * if the requested schema file does not exist in the disk config/schemas dir..
             * In this case we load the default schema file which always exist in the resource
             * under the path config/schemas/default-container-schema.xml
             */
            String defaultSchemaFileName = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                    + Constants.Schemas.SCHEMAS_FOLDER + "/"
                    + Constants.Schemas.DEFAULT_SCHEMA
                    + Constants.Schemas.CONTAINER_SCHEMA_FILE_SUFFIX;

            schemaInputStream = getResourceStream(defaultSchemaFileName);
            if (schemaInputStream != null) {
                if (_logger.isDebugEnabled()) {
                    URL urlToLoad = getResourceURL(schemaFilePath);
                    if (urlToLoad != null) {
                        _logger.debug("Could not find the container schema file at: "
                                + urlToLoad
                                + ".\nInstead, loaded the default container schema < "
                                + defaultSchemaFileName
                                + " > for the container configuration.");
                    } else {
                        _logger.debug("Could not find the container schema: "
                                + schemaFilePath
                                + ".\nInstead, loaded the default container schema < "
                                + defaultSchemaFileName
                                + " > for the container configuration.");
                    }
                }
                return schemaInputStream;
            } else {
                //nothing to do in this case
                if (_logger.isWarnEnabled()) {
                    _logger.warn("The requested container schema file does not exist in the following path: "
                            + defaultSchemaFileName);
                }
            }
        }
        return schemaInputStream;
    }

    public static InputStream findConfigDCache(String fileName) {
        if (!Constants.DCache.DCACHE_CONFIG_NAME_DEFAULT.equals(fileName) && _logger.isWarnEnabled())
            _logger.warn("Using a custom dcache configuration file is deprecated.");

        final String filePath = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/" + fileName + DCache.FILE_SUFFIX_EXTENTION;
        final InputStream inputStream = getResourceStream(filePath);
        if (inputStream != null && _logger.isDebugEnabled())
            _logger.debug("Loaded DCache configuration from < " + getResourceURL(filePath) + " > ");
        return inputStream;
    }


    /**
     * Find in classpath jms-config file.
     *
     * @param customConfigFileName e.g. myFile-jms-config.xml which is been looked under
     *                             config/jms/
     * @return JMS config xml input stream
     */
    static public InputStream findJMSConfig(String customConfigFileName)
            throws Exception {
        URL configURL = null;
        String filePath = Constants.Jms.JMS_CONFIG_DIRECTORY;
        boolean isFullURLPath = false;
        try {
            configURL = new URL(customConfigFileName);
        } catch (MalformedURLException e) {
            // e.printStackTrace();
        }
        if (configURL != null) {
            if (!JSpaceUtilities.isEmpty(configURL.getProtocol())) {
                filePath = configURL.getFile();
                isFullURLPath = true;
            }
        }

        if (!isFullURLPath) {
            if (customConfigFileName != null) {
                filePath = Constants.Jms.JMS_CONFIG_DIRECTORY
                        + customConfigFileName;
            } else {
                filePath = Constants.Jms.JMS_CONFIG_DIRECTORY
                        + Constants.Jms.JMS_CONFIG_FILE_NAME;
            }
        }
        InputStream inputStream = getResourceStream(filePath);

        if (inputStream == null) {
            String missingJMSConfigFileMsg = "Could not find the JMS configuration file '"
                    + filePath + "'. It does not exist or not readable.";
            throw new Exception(missingJMSConfigFileMsg);
        } else {
            if (_logger.isInfoEnabled()) {
                _logger.info("Loaded the JMS configuration file from < "
                        + getResourceURL(filePath) + " > ");
            }
        }
        return inputStream;
    }

    /**
     * Look for the requested space schema file in the ResourceBundle (classpath) under <EE home
     * dir>/config/schemas/<reqested_schema_name>-space-schema.xml. If it does no find it in the
     * disk (the default) it looks for it in the JSpaces.jar, in same path. if the requested schema
     * file does not exist in the disk config/schemas dir.. In this we throw a {@link
     * ConfigurationException} NOTE that the <com.gs.home> must be part of the classpath, in this
     * case.
     *
     * @return SchemaProperties objects that encapsulates found schema properties file: InputStream
     * and file path. Note that this is a read case, so we do not need to write the file to disk,
     * just parse it in memory.
     * @throws ConfigurationException in case the requested resource could not be found
     */
    static public SchemaProperties findSpaceSchema(String _schemaName) throws ConfigurationException {
        String schemaNameToBeUsed = _schemaName;
        InputStream schemaInputStream;
        if (JSpaceUtilities.isEmpty(_schemaName)) {
            //if the schema name is missing, it means no schema were ever passed by the client
            //we use the default schema name only in case the old regular <space name>.xml
            //is missing, then we load the default space schema file.
            schemaNameToBeUsed = Constants.Schemas.DEFAULT_SCHEMA;
        }
        /** Look for the requested space schema XML file in the ResourceBundle (classpath)
         * under <EE home dir>/config/schemas/<requested_schema_name>-space-schema.xml.
         * If it does no find it in the disk (the default) it looks for it in the JSpaces.jar, in same path.
         * if the requested schema file does not exist in the disk config/schemas dir..
         */
        String schemaFilePath = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                + Constants.Schemas.SCHEMAS_FOLDER + "/" + schemaNameToBeUsed
                + Constants.Schemas.SPACE_SCHEMA_FILE_SUFFIX;

        schemaInputStream = getResourceStream(schemaFilePath);

        if (schemaInputStream != null) {
            URL schemaURL = getResourceURL(schemaFilePath);
            if (_logger.isDebugEnabled()) {
                _logger.debug("Loaded the space schema < "
                        + schemaURL
                        + " > for the space configuration.");
            }
            SchemaProperties schemaProperties = new SchemaProperties(schemaInputStream, schemaURL);
            return schemaProperties;
        } else {
            URL urlToLoad = getResourceURL(schemaFilePath);
            String msg;
            if (urlToLoad != null) {
                msg = "Could not find the space schema file at: < " + urlToLoad + " >.";
            } else {
                msg = "Could not find the space schema file at: < " + schemaFilePath + " >.";
            }
            throw new ConfigurationException(msg);
        }
    }

    /**
     * Look for the requested container xml file in the classpath (resource) under <some root
     * dir>/config/<reqested_container_name>.xml. If the classpath contains <some root dir> first in
     * classpath, it will attempt to look for the container xml file first in the file system under
     * <some root dir>/config/<requested_container_name>.xml. Container xml file contains the list
     * of the spaces which are part of this container.
     *
     * Fixed bug 26/06/06 Gershon - http://62.90.11.164:8080/browse/APP-90 Container.XML location
     * mechanism must use getResource() and load the container xml from resource according to the
     * classpath
     *
     * @return URL to the found container xml file. Note that this is a read case, so we do not need
     * to write the file to disk, just parse it in memory.
     */
    static public URL findContainerXML(String _containerName) {
        URL _containerNameURL = null;
        /** Look for the requested space schema XML file in the ResourceBundle (classpath)
         * under <GS home dir>/config/schemas/<requested_schema_name>-space-schema.xml.
         * If it does no find it in the disk (the default) it looks for it in the JSpaces.jar, in same path.
         * if the requested schema file does not exist in the disk config/schemas dir..
         */
        String _containerNameFilePath = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                + _containerName + ".xml";
        _containerNameURL = getResourceURL(_containerNameFilePath);

        if (_containerNameURL != null) {
            if (_logger.isInfoEnabled()) {
                _logger.info("Loaded the container xml file < "
                        + _containerNameURL + " >.");
            }
        }
        return _containerNameURL;
    }

    /**
     * Returns an array of strings naming the schemas in the default ${com.gs.home}/config/schemas
     * directory.
     *
     * @return result
     */
    public static String[] getClusterSchemas() {
        File directory = SystemLocations.singleton().config(Constants.Schemas.SCHEMAS_FOLDER).toFile();
        return getClusterSchemas(directory);
    }

    /**
     * Returns an array of strings naming the schemas in the specified directory.
     *
     * @param schemasDir - directory that contains a schemas
     * @return result
     */
    public static String[] getClusterSchemas(File schemasDir) {
        if (schemasDir == null || !schemasDir.exists()
                || !schemasDir.isDirectory())
            return new String[]{};

        String[] result = schemasDir.list(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name != null
                        && name.endsWith(ClusterXML.CLUSTER_SCHEMA_XSL_FILE_SUFFIX)) {
                    return true;
                }
                return false;
            }
        });

        for (int i = 0; i < result.length; i++) {
            int endIndex = result[i].indexOf(ClusterXML.CLUSTER_SCHEMA_XSL_FILE_SUFFIX);
            result[i] = result[i].substring(0, endIndex);
        }
        Arrays.sort(result);

        for (int i = 1; i < result.length; i++) {
            //set async replicated schema to first place
            if (result[i].equals(ClusterXML.CLUSTER_SCHEMA_NAME_ASYNC_REPLICATED)) {
                String firstElement = result[0];
                result[0] = ClusterXML.CLUSTER_SCHEMA_NAME_ASYNC_REPLICATED;
                result[i] = firstElement;
                break;
            }
        }
        return result;
    }

    /**
     * This class encapsulates schema file properties: InputStream, and full path ( including host
     * IP or name )
     *
     * @author evgenyf
     * @version 1.0
     * @since 5.0
     */
    static public class SchemaProperties {
        private final InputStream _inputStream;
        private final URL _url;
        private static final String _hostName = SystemInfo.singleton().network().getHostId();

        SchemaProperties(InputStream inputStream, URL url) {
            _inputStream = inputStream;
            _url = url;

        }

        /**
         * @return schema InpuStream instance
         */
        public InputStream getInputStream() {
            return _inputStream;
        }

        /**
         * @return host IP
         */
        public static String getHostName() {
            return _hostName;
        }

        /**
         * @return host IP address and file path
         */
        public String getFullPath() {
            return "[" + _hostName + "] " + _url.getPath();
        }
    }
}
