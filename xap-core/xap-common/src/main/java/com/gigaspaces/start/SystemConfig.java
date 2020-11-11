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

package com.gigaspaces.start;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.internal.io.FileUtils;
import com.gigaspaces.internal.jmx.JMXUtilities;
import com.gigaspaces.internal.jvm.JavaUtils;
import com.gigaspaces.internal.services.RestServiceFactory;
import com.gigaspaces.internal.services.ServiceFactory;
import com.gigaspaces.internal.services.WebuiServiceFactory;
import com.gigaspaces.internal.services.ZooKeeperServiceFactory;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.start.manager.XapManagerConfig;
import com.sun.jini.start.ServiceDescriptor;
import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import org.jini.rio.boot.BootUtil;
import org.jini.rio.boot.CommonClassLoader;
import org.jini.rio.boot.RioServiceDescriptor;
import org.jini.rio.jmx.MBeanServerFactory;
import org.jini.rio.tools.webster.Webster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import java.io.Closeable;
import java.io.File;
import java.net.BindException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

/**
 * Provides system configuration support.
 */
@com.gigaspaces.api.InternalApi
public class SystemConfig {
    /**
     * System property set indicating the address and port of the Webster instance created by this
     * utility, in the form of :
     * <pre>http://address:port</pre>
     */
    public static final String CODESERVER = "com.gigaspaces.system.codeserver";
    static final private String BASE_COMPONENT = "com.gigaspaces";
    static final private String COMPONENT = BASE_COMPONENT + ".start";
    private static SystemConfig instance;
    final private Configuration config;
    final private String[] configArgs;
    final private String[] configParms;
    private static String[] overrideArgs;
    private Webster webster;
    private final SystemLocations locations;

    private static final Logger logger = LoggerFactory.getLogger(COMPONENT);

    private List<URL> addedPlatformJars;

    private static final int DEFAULT_PORT_RETRIES = 20;

    private final Map<String, ServiceFactory> serviceFactoryMap = initServiceFactories();

    private static Map<String, ServiceFactory> initServiceFactories() {
        ServiceFactory[] serviceFactories = new ServiceFactory[] {
                new RestServiceFactory(),
                new ZooKeeperServiceFactory(),
                new WebuiServiceFactory()
        };
        Map<String, ServiceFactory> result = new HashMap<>();
        for (ServiceFactory serviceFactory : serviceFactories)
            result.put(serviceFactory.getServiceName(), serviceFactory);
        return result;
    }

    /**
     * Create an instance of the SystemConfig
     *
     * @param confArgs An array whose first element is the location of the configuration source and
     *                 remaining elements specify override values for entries that will be used to
     *                 create the SystemConfig singleton for the system.
     * @throws net.jini.config.ConfigurationException If errors occur accessing the config
     */
    private SystemConfig(String[] confArgs) throws ConfigurationException {
        locations = SystemLocations.singleton();

        if (confArgs == null || confArgs.length == 0) {
            this.configArgs = new String[]{"-"};
            overrideArgs = new String[0];
            configParms = new String[0];
        } else {
            /* GS-9495: Trim extra whitespace at the end of the command line. In Win-2008, shows up as a comma. */
            if (",".equals(confArgs[confArgs.length - 1])) {
                List<String> trimConfArgs = new ArrayList<>(confArgs.length - 1);
                for (int i = 0; i < confArgs.length - 1; ++i) {
                    trimConfArgs.add(confArgs[i]);
                }
                confArgs = trimConfArgs.toArray(new String[trimConfArgs.size()]);
            }

            configParms = new String[confArgs.length];
            System.arraycopy(confArgs, 0, configParms, 0, configParms.length);
            ConfigurationParser configParser = null;
            try {
                configParser =
                        new ConfigurationParser(
                                SystemConfig.class.getClassLoader());
            } catch (Exception e) {
                logger.warn("Creating ConfigurationParser", e);
            }
            String configFile = null;
            ArrayList<String> configList = new ArrayList<>();
            if (confArgs[0].endsWith(".config"))
                configFile = confArgs[0];
            for (int i = 0; i < confArgs.length; i++) {
                if (confArgs[i].endsWith(".xml")) {
                    if (configParser != null) {
                        try {
                            String[] args =
                                    configParser.parseConfiguration(confArgs[i]);
                            for (int j = 0; j < args.length; j++)
                                configList.add(args[j]);
                        } catch (Exception e) {
                            logger.warn("Parsing override config file [" + confArgs[i] + "]", e);
                        }
                    }
                } else {
                    /* make sure we dont add the config file if passed */
                    if (!confArgs[i].endsWith(".config")) {
                        if (!confArgs[i].contains("=") && !confArgs[i].contains("-") && i + 1 < confArgs.length) {
                            if (!confArgs[i + 1].contains("=")) {
                                configList.add(confArgs[i] + "=\"" + confArgs[++i] + "\"");
                            }
                        } else {
                            configList.add(confArgs[i]);
                        }
                    }
                }
            }

            //process the addPlatformJARs now since JINI configuration does not allow dups
            addedPlatformJars = new ArrayList<>(configList.size());
            for (Iterator<String> iterator = configList.iterator(); iterator.hasNext(); ) {
                String configArg = iterator.next();
                if (configArg.indexOf("addPlatformJARs") != -1) {
                    iterator.remove();
                    Configuration config = ConfigurationProvider.getInstance(new String[]{"-", configArg});
                    URL[] urls = (URL[]) config.getEntry(COMPONENT, "addPlatformJARs", URL[].class, new URL[0]);
                    addedPlatformJars.addAll(Arrays.asList(urls));
                }
            }

            overrideArgs = configList.toArray(new String[configList.size()]);
            if (configFile != null) {
                configArgs = new String[configList.size() + 1];
                //loading the config files using resource loader
                //meaning file name such as /config/tools/adminui.config can be loaded from the jar file.
                URL configFileURL = BootUtil.getResourceURL(configFile, null);
                if (configFileURL != null) {
                    try {
                        configArgs[0] = configFileURL.toExternalForm();
                    } catch (Exception e) {
                        logger.warn("Failed to parse Jini Configuration file [" + configFileURL + "].", e);
                    }
                } else {
                    configArgs[0] = configFile;
                }

                for (int i = 1; i < configArgs.length; i++)
                    configArgs[i] = configList.get(i - 1);
            } else {
                configArgs = new String[configList.size() + 1];
                configArgs[0] = "-";
                for (int i = 1; i < configArgs.length; i++)
                    configArgs[i] = configList.get(i - 1);
            }
        }

        config = ConfigurationProvider.getInstance(configArgs);
    }

    /**
     * Get an instance of the SystemConfig object
     *
     * @param configArgs An array whose first element is the location of the configuration source
     *                   and remaining elements specify override values for entries that will be
     *                   used to create the SystemConfig singleton for the system.
     *
     *                   If the SystemConfig instance has already been created this parameter is
     *                   optional
     * @return The SystemConfig instance
     */
    public static synchronized SystemConfig getInstance(String[] configArgs)
            throws ConfigurationException {
        if (instance == null) {
            instance = new SystemConfig(configArgs);
        }
        return (instance);
    }

    /**
     * Get an instance of the SystemConfig object that has been previously created with
     * configuration arguments. This is a utilty method for clients to obtain the previously created
     * SystemConfig instance
     *
     * @return The SystemConfig instance. If the SystemConfig instance has not been previously
     * created usng a configurationwhen this method is called, a RuntimeException is thrown
     */
    public static synchronized SystemConfig getInstance() {
        if (instance == null) {
            throw new RuntimeException("SystemConfig instance must be " +
                    "created with " +
                    "a configuration");
        }
        return (instance);
    }

    /**
     * Get the configuration parameters used to create this utility. If this utility was created
     * with no (or <code>null</code>) parameters, a zero-length String array will be returned
     */
    public String[] getConfigurationParms() {
        return (configParms);
    }

    /**
     * Get the {@link net.jini.config.Configuration} created by this utility
     */
    public Configuration getConfiguration() {
        return (config);
    }

    /**
     * If this utility has been started with override arguments, append the override arguments to
     * the configuration file
     *
     * @param configFile The file to append to, this parameter must not be <code>null</code>
     * @return A String array, with the first element being the <code>configFile</code> parameter,
     * each subsequent element a corresponding override argument this utility was started with. If
     * there are no override arguments, the array will contain only the <code>configFile</code>
     * parameter.
     */
    public static String[] appendOverrides(String configFile) {
        if (configFile == null)
            throw new NullPointerException("configFie is null");
        String[] confArgs;
        if (overrideArgs != null)
            confArgs = new String[overrideArgs.length + 1];
        else
            confArgs = new String[]{""};
        confArgs[0] = configFile;
        if (overrideArgs != null && overrideArgs.length > 0) {
            for (int i = 1; i < confArgs.length; i++)
                confArgs[i] = overrideArgs[i - 1];
        }
        return (confArgs);
    }

    /**
     * Get the configuration overrides this utility was created with
     *
     * @return The configuration overrides this utility was created with. If no configuration
     * overrides were provided, this method returns <code>null</code>
     */
    public String[] getOverrides() {
        return (overrideArgs);
    }

    private List<URL> getDefaultCommonClassLoaderClasspath() throws MalformedURLException {
        ClasspathBuilder classpathBuilder = new ClasspathBuilder();
        /* "spring-jcl-" / "commons-logging" moved to system class loader.
        try {
            String commonsLoggingJarFilename = findFilenameByPrefix(locations.libRequired(), "spring-jcl-");
            classpathBuilder.append(locations.libRequired().resolve(commonsLoggingJarFilename));
        } catch (FileNotFoundException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Missing JAR file", e);
            }
        }
         */

        for (XapModules module : XapModules.getByClassLoaderType(ClassLoaderType.COMMON)) {
            classpathBuilder.appendJar(module);
        }

        classpathBuilder.appendOptionalJars("jee");// Different J2EE jars support
        classpathBuilder.appendJars(locations.libPlatformExt()); // ext support
        classpathBuilder.appendOptionalJars("jms");
        classpathBuilder.appendOptionalJars("metrics");
        classpathBuilder.appendOptionalJars("spatial");
        classpathBuilder.appendOptionalJars("full-text-search");
        classpathBuilder.appendOptionalJars("jpa", FileUtils.Filters.nameStartsWith(XapModules.JPA_SPRING.getArtifactName()).negate());
        //classpathBuilder.appendPlatformJars("commons"); // Apache Commons libraries !!!
        classpathBuilder.appendOptionalJars("groovy");
        classpathBuilder.appendOptionalJars("jruby");
        classpathBuilder.appendJars(Paths.get(System.getProperty("com.gs.pu.classloader.scala-lib-path", locations.libOptional("scala").resolve("lib").toString())));// Scala support
        classpathBuilder.appendPlatformJars("zookeeper");
        if(JavaUtils.greaterOrEquals(11)){
            classpathBuilder.appendPlatformJars("javax");
        }
        //GS-13825 added hsql jar
        classpathBuilder.appendOptionalJars("jdbc", FileUtils.Filters.nameStartsWith( "hsqldb" ) );

        // I don't expect anybody to use this feature, but its here just to be on the safe side
        boolean osInCommonClassLoader = Boolean.parseBoolean(System.getProperty("com.gs.pu.classloader.os-in-common-classloader", "false"));
        // if we are use parent first, then we are working in shared lib mode, so we need to add openspaces and spring
        // otherwise, don't add openspaces and spring
        if (osInCommonClassLoader) {
            classpathBuilder.appendLibRequiredJars(ClassLoaderType.SERVICE);
            classpathBuilder.appendOptionalJars("spring");
        }

        return classpathBuilder.toURLs();
    }

    /**
     * Get the platformJars
     *
     * @return URL[] An array of URL resources indicating the jars the common ClassLoader must add.
     * @throws MalformedURLException  If the URL resources result in an improperly formatted URL
     * @throws ConfigurationException If there are errors accessing the configuration
     */
    public URL[] getPlatformJars()
            throws MalformedURLException, ConfigurationException {

        /* Get the platformJARs to load. The default is the list above */
        List<URL> defaultPlatformJARsList = getDefaultCommonClassLoaderClasspath();
        URL[] defaultPlatformJARs = defaultPlatformJARsList.toArray(new URL[defaultPlatformJARsList.size()]);
        URL[] platformJARs = (URL[]) config.getEntry(COMPONENT, "platformJARs", URL[].class, defaultPlatformJARs);

        URL[] addPlatformJARs = addedPlatformJars.toArray(new URL[addedPlatformJars.size()]);
        if (addPlatformJARs.length > 0) {
            if (logger.isDebugEnabled()) {
                StringBuilder buffer = new StringBuilder();
                for (int i = 0; i < addPlatformJARs.length; i++) {
                    if (i > 0)
                        buffer.append("\n");
                    buffer.append("    " + addPlatformJARs[i].toExternalForm());
                }
                logger.debug("addPlatformJARs\n" + buffer.toString());
            }
            ArrayList<URL> list = new ArrayList<>();
            for (int i = 0; i < platformJARs.length; i++)
                list.add(platformJARs[i]);
            for (int i = 0; i < addPlatformJARs.length; i++)
                list.add(addPlatformJARs[i]);
            platformJARs = list.toArray(new URL[list.size()]);
        }
        if (logger.isDebugEnabled()) {
            StringBuilder buffer = new StringBuilder();
            for (int i = 0; i < platformJARs.length; i++) {
                if (i > 0)
                    buffer.append("\n");
                buffer.append("    " + platformJARs[i].toExternalForm());
            }
            logger.debug("platform JARs\n" + buffer.toString());
        }
        return (platformJARs);
    }

    /**
     * Get configured system properties
     */
    public Properties getSystemProperties()
            throws ConfigurationException, UnknownHostException {
        String[] gridGroups = (String[]) config.getEntry(BASE_COMPONENT + ".grid", "groups", String[].class, new String[]{"gs-grid"});

        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < gridGroups.length; i++) {
            if (i > 0)
                buff.append(",");
            buff.append(gridGroups[i]);
        }
        String groupNames = buff.toString();

        Properties sysProps = new Properties();
        /* Set default system properties */
        sysProps.setProperty("java.protocol.handler.pkgs", "net.jini.url");
        sysProps.setProperty("com.gs.localhost.name", getDefaultHostAddress());
        sysProps.setProperty(BASE_COMPONENT + ".grid.groups", groupNames);

        /* Get additional system properties */
        String[] systemProperties =
                (String[]) config.getEntry(COMPONENT, "systemProperties", String[].class, new String[0]);
        if (systemProperties.length > 0) {
            if (systemProperties.length % 2 != 0) {
                logger.warn("systemProperties elements has odd length : " + systemProperties.length);
            } else {
                for (int i = 0; i < systemProperties.length; i += 2) {
                    String name = systemProperties[i];
                    String value = systemProperties[i + 1];
                    sysProps.setProperty(name, value);
                }
            }
        }

        return (sysProps);
    }

    String fix(String s) {
        if (s.endsWith(File.separator))
            s = s.substring(0, s.length() - 1);
        return (s);
    }

    public String getWebsterProtocol() {
        if (webster != null) {
            return webster.getProtocol();
        }
        return "http";
    }

    public int getWebsterPort() {
        if (webster != null) {
            return webster.getPort();
        }
        // just return a port. Does not really matter since
        return -1;
    }

    /**
     * Create a Webster instance
     *
     * @return A Webster instance
     * @throws BindException          If the Webster instance could not be created
     * @throws ConfigurationException If errors occur reading the configuration
     */
    public Webster getWebster() throws BindException, ConfigurationException,
            UnknownHostException {
        if (webster == null) {
            File deployRootFile = locations.deploy().toFile();
            if (!deployRootFile.exists()) {
                deployRootFile.mkdirs();
            }
            String defaultRoots = locations.lib().toString() + ";" + locations.libRequired().toString() + ";" + deployRootFile.toString();
            String httpRoots = (String) config.getEntry(COMPONENT,
                    "httpRoots",
                    String.class,
                    defaultRoots);

            //override with sys. property -Dcom.gigaspaces.start.httpRoots
            httpRoots = System.getProperty(COMPONENT + ".httpRoots", httpRoots);

            String addHttpRoots = (String) config.getEntry(COMPONENT,
                    "addHttpRoots",
                    String.class,
                    null);

            //override with sys. property -Dcom.gigaspaces.start.addHttpRoots
            addHttpRoots = System.getProperty(COMPONENT + ".addHttpRoots", addHttpRoots);

            if (addHttpRoots != null)
                httpRoots = httpRoots + ";" + addHttpRoots;

            //using for Webster
            int httpPort = (Integer) config.getEntry(COMPONENT, "httpPort", int.class, 0);

            //override with sys. property -Dcom.gigaspaces.start.httpPort
            httpPort = GsEnv.propertyInt(COMPONENT + ".httpPort", "WEBSTER_HTTP_PORT").get(httpPort);

            int httpServerRetries = (Integer) config.getEntry(COMPONENT, "httpServerRetries",
                    Integer.class, DEFAULT_PORT_RETRIES);

            //override with sys. property -Dcom.gigaspaces.start.httpServerRetries
            httpServerRetries = Integer.getInteger(COMPONENT + ".httpServerRetries", httpServerRetries);

            //override with sys. property inside
            final XapManagerConfig currServer = SystemInfo.singleton().getManagerClusterInfo().getCurrServer();
            String hostAddress = currServer != null ? SystemInfo.singleton().network().getHost().getHostAddress() : getDefaultHostAddress();

            for (int i = 0; i < httpServerRetries; i++) {
                try {
                    webster = new Webster(httpPort, httpRoots, hostAddress);
                    break;
                } catch (BindException e) {
                    if (httpPort == 0)
                        throw e;
                    if (logger.isTraceEnabled())
                        logger.trace("Failed to create HTTP server using " +
                                "port [" + httpPort + "], increment port " +
                                "and try again");
                    httpPort++;
                }
            }
            if (webster == null)
                throw new RuntimeException("Failed to create HTTP server");
            /* Set system property */
            System.setProperty(CODESERVER, webster.getURL());

            if (logger.isInfoEnabled())
                logger.info("Created Webster on " + webster.getURL() + " [roots=" + httpRoots + "]");
        }
        return (webster);
    }

    private ServiceDescriptor getLookupHandlerServiceDescriptor()
            throws BindException, UnknownHostException, ConfigurationException {
        String handlerCodebase = getDefaultCodebase();
        String handlerClasspath = "";

        String[] confArgs = new String[overrideArgs.length + 1];
        //confArgs[0] = configDir+"services.config";
        confArgs[0] = getServiceConfig("reggieConfig");
        if (overrideArgs.length > 0) {
            for (int i = 1; i < confArgs.length; i++)
                confArgs[i] = overrideArgs[i - 1];
        }

        return (new RioServiceDescriptor("LUS", handlerCodebase,
                handlerClasspath,
                "com.gigaspaces.grid.lookup.LookupHandler",
                confArgs));
    }

    private String getServiceConfig(String name) throws ConfigurationException {
        return (String) config.getEntry(COMPONENT, name, String.class,
                locations.config("services").resolve("services.config").toString());
    }

    private String getDefaultCodebase() throws UnknownHostException {
        return BootUtil.getCodebase(new String[]{XapModules.DATA_GRID.getJarFileName()}, getWebsterProtocol(), Integer.toString(getWebsterPort()));
    }

    private ServiceDescriptor getMahaloServiceDescriptor()
            throws BindException, UnknownHostException, ConfigurationException {
        String handlerCodebase = getDefaultCodebase();
        String handlerClasspath = "";

        String[] confArgs = new String[overrideArgs.length + 1];
        //confArgs[0] = configDir+"services.config";
        confArgs[0] = getServiceConfig("mahaloConfig");
        if (overrideArgs.length > 0) {
            for (int i = 1; i < confArgs.length; i++)
                confArgs[i] = overrideArgs[i - 1];
        }

        return (new RioServiceDescriptor("TM", handlerCodebase,
                handlerClasspath,
                "com.sun.jini.mahalo.TransientMahaloImpl",
                confArgs));
    }

    public ServiceDescriptor getServiceDescriptor(String key)
            throws BindException, ConfigurationException, UnknownHostException {
        if (key.equals(SystemBoot.GSC))
            return getGSCServiceDescriptor();
        if (key.equals(SystemBoot.GSA))
            return getGSAServiceDescriptor();
        if (key.equals(SystemBoot.GSM))
            return getGSMServiceDescriptor();
        if (key.equals(SystemBoot.SPACE))
            return getGSServiceDescriptor();
        if (key.equals(SystemBoot.LH))
            return getLookupHandlerServiceDescriptor();
        if (key.equals(SystemBoot.TM))
            return getMahaloServiceDescriptor();
        if (key.equals(SystemBoot.ESM))
            return getESMServiceDescriptor();
        return null;
    }

    public Closeable getCustomService(String key) {
        ServiceFactory serviceFactory = serviceFactoryMap.get(key);
        if (serviceFactory != null)
            return serviceFactory.createService();
        logger.warn("Service " + key + " was not created - service factory was not found");
        return null;
    }

    private ServiceDescriptor getGSAServiceDescriptor()
            throws UnknownHostException, ConfigurationException {

        ServiceDescriptor svcDesc =
                (ServiceDescriptor) config.getEntry(COMPONENT + ".gsa",
                        "svcDesc",
                        ServiceDescriptor.class,
                        null);
        if (svcDesc == null) {
            ClasspathBuilder classpath = new ClasspathBuilder();
            classpath.appendLibRequiredJars(ClassLoaderType.SERVICE);
            classpath.appendOptionalJars("spring");
            classpath.appendJars(locations.libOptionalSecurity());

            String gsaClasspath =
                    (String) config.getEntry(COMPONENT + ".gsa",
                            "classpath",
                            String.class,
                            classpath.toString());
            String gsaCodebase = getDefaultCodebase();

            String[] confArgs = new String[overrideArgs.length + 1];
            confArgs[0] = getServiceConfig("gsaConfig");
            if (overrideArgs.length > 0) {
                for (int i = 1; i < confArgs.length; i++)
                    confArgs[i] = overrideArgs[i - 1];
            }
            svcDesc =
                    new RioServiceDescriptor("GSA", gsaCodebase,
                            gsaClasspath,
                            "com.gigaspaces.grid.gsa.GSAImpl",
                            confArgs);
        }
        return (svcDesc);
    }

    private ServiceDescriptor getGSCServiceDescriptor()
            throws UnknownHostException, BindException, ConfigurationException {

        ServiceDescriptor svcDesc =
                (ServiceDescriptor) config.getEntry(COMPONENT + ".gsc",
                        "svcDesc",
                        ServiceDescriptor.class,
                        null);
        if (svcDesc == null) {
            ClasspathBuilder classpath = new ClasspathBuilder();
            classpath.appendLibRequiredJars(ClassLoaderType.SERVICE);
            classpath.appendOptionalJars("spring");
            classpath.appendJars(locations.libOptionalSecurity());

            String gscClasspath =
                    (String) config.getEntry(COMPONENT + ".gsc",
                            "classpath",
                            String.class,
                            classpath.toString());

            logger.debug("GSC configuration Classpath is: " + gscClasspath);
            String gscCodebase = getDefaultCodebase();

            String[] confArgs = new String[overrideArgs.length + 1];
            confArgs[0] = getServiceConfig("gscConfig");
            if (overrideArgs.length > 0) {
                for (int i = 1; i < confArgs.length; i++)
                    confArgs[i] = overrideArgs[i - 1];
            }
            svcDesc =
                    new RioServiceDescriptor("GSC", gscCodebase,
                            gscClasspath,
                            "com.gigaspaces.grid.gsc.GSCImpl",
                            confArgs);
        }
        return (svcDesc);
    }

    private ServiceDescriptor getGSMServiceDescriptor()
            throws UnknownHostException, BindException, ConfigurationException {

        ServiceDescriptor svcDesc =
                (ServiceDescriptor) config.getEntry(COMPONENT + ".gsm",
                        "svcDesc",
                        ServiceDescriptor.class,
                        null);
        if (svcDesc == null) {
            // adding openspaces to GSM since it needs to know the PUServiceBean (for FDH for example)
            ClasspathBuilder classpath = new ClasspathBuilder();
            classpath.appendLibRequiredJars(ClassLoaderType.SERVICE);
            classpath.appendOptionalJars("spring");
            classpath.appendJars(locations.libOptionalSecurity());
            classpath.appendJar(XapModules.ADMIN);

            String gsmClasspath =
                    (String) config.getEntry(COMPONENT + ".gsm",
                            "classpath",
                            String.class,
                            classpath.toString());
            String gsmCodebase = getDefaultCodebase();

            String[] confArgs = new String[overrideArgs.length + 1];
            confArgs[0] = getServiceConfig("gsmConfig");
            if (overrideArgs.length > 0) {
                for (int i = 1; i < confArgs.length; i++)
                    confArgs[i] = overrideArgs[i - 1];
            }
            svcDesc =
                    new RioServiceDescriptor("GSM", gsmCodebase,
                            gsmClasspath,
                            "com.gigaspaces.grid.gsm.GSMImpl",
                            confArgs);
        }
        return (svcDesc);
    }

    private ServiceDescriptor getESMServiceDescriptor()
            throws UnknownHostException, BindException, ConfigurationException {

        ServiceDescriptor svcDesc =
                (ServiceDescriptor) config.getEntry(COMPONENT + ".esm",
                        "svcDesc",
                        ServiceDescriptor.class,
                        null);
        if (svcDesc == null) {
            ClasspathBuilder classpath = new ClasspathBuilder();
            classpath.appendLibRequiredJars(ClassLoaderType.SERVICE);
            classpath.appendPlatformJars("esm");
            classpath.appendOptionalJars("spring");
            classpath.appendJars(locations.libOptionalSecurity());
            classpath.appendJar(XapModules.ADMIN);

            String esmClasspath =
                    (String) config.getEntry(COMPONENT + ".esm",
                            "classpath",
                            String.class,
                            classpath.toString());
            String esmCodebase = getDefaultCodebase();

            String[] confArgs = new String[overrideArgs.length + 1];
            confArgs[0] = getServiceConfig("esmConfig");
            if (overrideArgs.length > 0) {
                for (int i = 1; i < confArgs.length; i++)
                    confArgs[i] = overrideArgs[i - 1];
            }
            svcDesc =
                    new RioServiceDescriptor("ESM", esmCodebase,
                            esmClasspath,
                            "org.openspaces.grid.esm.ESMImpl",
                            confArgs);
        }
        return (svcDesc);
    }

    /**
     * Get the ServiceDescriptor to start JMX
     *
     * @throws ConfigurationException If errors occur reading the configuration or the default
     *                                configuration file cannot be located
     */
    public ServiceDescriptor getJMXServiceDescriptor()
            throws ConfigurationException {

        ServiceDescriptor svcDesc =
                (ServiceDescriptor) config.getEntry(COMPONENT + ".jmx",
                        "svcDesc",
                        ServiceDescriptor.class,
                        null);
        if (svcDesc == null) {
            svcDesc = new JMXServiceDescriptor();
        }
        return (svcDesc);
    }

    private ServiceDescriptor getGSServiceDescriptor()
            throws UnknownHostException, BindException, ConfigurationException {

        String gsClasspath = "";
        String gsCodebase = getDefaultCodebase();
        String[] confArgs = new String[overrideArgs.length + 1];
        confArgs[0] = getServiceConfig("gsConfig");

        if (overrideArgs.length > 0) {
            for (int i = 1; i < confArgs.length; i++)
                confArgs[i] = overrideArgs[i - 1];
        }
        ServiceDescriptor svcDesc =
                new RioServiceDescriptor("GS", gsCodebase,
                        gsClasspath,
                        "com.j_spaces.start.JSpaceServiceImpl",
                        confArgs);
        return (svcDesc);
    }

    /**
     * Get the default host address
     */
    String getDefaultHostAddress() throws UnknownHostException,
            ConfigurationException {
        String defaultAddress = SystemInfo.singleton().network().getHostId();
        String hostAddress = (String) config.getEntry(COMPONENT,
                "hostAddress",
                String.class,
                defaultAddress);

        //override with system property -Dcom.gigaspaces.start.hostAddress
        hostAddress = System.getProperty(COMPONENT + ".hostAddress", hostAddress);
        return (hostAddress);
    }

    /**
     * Initialize RMI Registry and JMX Platform MBeanServer
     */
    public static class JMXServiceDescriptor implements ServiceDescriptor {
        /**
         * @see com.sun.jini.start.ServiceDescriptor#create
         */
        public Object create(Configuration config) throws Exception {
            MBeanServer mbs = null;
            if(  GsEnv.propertyBoolean( CommonSystemProperties.JMX_ENABLED_PROP ).get( CommonSystemProperties.JMX_ENABLED_DEFAULT_BOOLEAN_VALUE ) ) {
                int registryPort = (Integer) getConfigEntry(config, COMPONENT, "registryPort", int.class, CommonSystemProperties.REGISTRY_PORT, 10098);
                int registryRetries = (Integer) getConfigEntry(config, COMPONENT, "registryRetries", Integer.class, CommonSystemProperties.REGISTRY_RETRIES, 20);

                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(CommonClassLoader.getInstance());

                int jmxRemotePort = -1;
                String jmxRemotePortStr = System.getProperty(CommonSystemProperties.JMX_REMOTE_PORT);
                if (jmxRemotePortStr != null) {
                    try {
                        jmxRemotePort = Integer.parseInt(jmxRemotePortStr);
                    } catch (Exception e) {
                        logger.warn(e.toString(), e);
                    }
                }
                if (jmxRemotePort > 0) {
                    logger.info("System property [" + CommonSystemProperties.JMX_REMOTE_PORT + "] has value:" + jmxRemotePort);
                }
                Registry registry = null;
                if (jmxRemotePort < 0) {

                    if (logger.isDebugEnabled())
                        logger.debug("Starting RMI Registry initialization: initial port=" + registryPort + ", retries=" + registryRetries);
                    RemoteException registryCreationException = null;
                    for (int i = 0; i < registryRetries; i++) {
                        try {
                            registry = LocateRegistry.createRegistry(registryPort);
                            break;
                        } catch (RemoteException e) {
                            registryCreationException = e;
                            if (logger.isTraceEnabled())
                                logger.trace("Failed to create RMI Registry using port [" + registryPort +
                                        "], increment port and try again");
                        }
                        registryPort++;
                    }

                    Thread.currentThread().setContextClassLoader(cl);

                    if (registry == null) {
                        logger.error("Unable to create RMI Registry, tried port range ["
                                        + (registryPort - (registryRetries - 1))
                                        + "-"
                                        + (registryPort - 1)
                                        + "], you can change the port and range using '"
                                        + CommonSystemProperties.REGISTRY_PORT + ", " + CommonSystemProperties.REGISTRY_RETRIES
                                        + "'",
                                registryCreationException);
                    } else {
                        System.setProperty(CommonSystemProperties.REGISTRY_PORT, Integer.toString(registryPort));
                        if (logger.isDebugEnabled())
                            logger.debug("Created RMI Registry: " + registry.toString() + " using port " + registryPort);
                        String defaultAddress = SystemInfo.singleton().network().getHostId();
                        String hostAddress =
                                (String) config.getEntry(COMPONENT,
                                        "hostAddress",
                                        String.class,
                                        defaultAddress);
                        String publicHostAddress = SystemInfo.singleton().network().getPublicHostId();
                        //mbs = ManagementFactory.getPlatformMBeanServer();
                        mbs = MBeanServerFactory.getMBeanServer();
                        if (mbs != null) {
                            final String jmxServiceURL = JMXUtilities.createJMXUrl(hostAddress, registryPort);
                            final String jmxServicePublicURL = JMXUtilities.createJMXUrl(publicHostAddress, registryPort);
                            /* Set the JMX property to true */
                            System.setProperty(CommonSystemProperties.JMX_ENABLED_PROP, Boolean.TRUE.toString());
                            System.setProperty(CommonSystemProperties.CREATE_JMX_CONNECTOR_PROP, Boolean.FALSE.toString());

                            final long start = System.currentTimeMillis();
                            JMXConnectorServer jmxConn = JMXConnectorServerFactory.newJMXConnectorServer(
                                    new JMXServiceURL(jmxServiceURL), (Map) System.getProperties(), mbs);
                            jmxConn.start();
                            System.setProperty(CommonSystemProperties.JMX_SERVICE_URL, jmxServiceURL);
                            System.setProperty(CommonSystemProperties.JMX_PUBLIC_SERVICE_URL, jmxServicePublicURL);
                            final long duration = System.currentTimeMillis() - start;
                            if (logger.isInfoEnabled())
                                logger.info("Exported JMX Platform MBeanServer with RMI Connector " +
                                        "[duration=" + BootUtil.formatDuration(duration) +
                                        ", url=" + jmxServiceURL + "]");
                        } else {
                            logger.info("Unable to acquire JMX Platform MBeanServer, running with Java version " + JavaUtils.getVersion());
                        }
                    }
                }
                //if com.sun.management.jmxremote.port system property was passed
                //added by Evgeny , Fix for GS-12109
                else {
                    String defaultAddress = SystemInfo.singleton().network().getHostId();
                    String hostAddress =
                            (String) config.getEntry(COMPONENT,
                                    "hostAddress",
                                    String.class,
                                    defaultAddress);
                    mbs = MBeanServerFactory.getMBeanServer();
                    if (mbs != null) {
                        String publicHostAddress = SystemInfo.singleton().network().getPublicHostId();
                        final String jmxServiceURL = JMXUtilities.createJMXUrl(hostAddress, jmxRemotePort);
                        final String jmxServicePublicURL = JMXUtilities.createJMXUrl(publicHostAddress, jmxRemotePort);
                        /* Set the JMX property to true */
                        System.setProperty(CommonSystemProperties.JMX_ENABLED_PROP, Boolean.TRUE.toString());
                        System.setProperty(CommonSystemProperties.CREATE_JMX_CONNECTOR_PROP, Boolean.FALSE.toString());
                        System.setProperty(CommonSystemProperties.JMX_SERVICE_URL, jmxServiceURL);
                        System.setProperty(CommonSystemProperties.JMX_PUBLIC_SERVICE_URL, jmxServicePublicURL);
                    } else {
                        logger.info("Unable to acquire JMX Platform MBeanServer, running with Java version " + JavaUtils.getVersion());
                    }
                }
            }
            return (mbs);
        }

        private static Object getConfigEntry(Configuration config, String component, String name, Class<?> type, String systemProperty, int defaultValue)
                throws ConfigurationException {
            String propertyValue = System.getProperty(systemProperty);
            if (propertyValue != null)
                defaultValue = Integer.parseInt(propertyValue);
            return config.getEntry(component, name, type, defaultValue);
        }

    }
}