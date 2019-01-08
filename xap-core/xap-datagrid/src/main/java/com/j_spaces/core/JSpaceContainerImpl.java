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

/*
 * @(#)JSpaceContainerImpl.java 1.0 27/09/2000 11:54AM
 */

package com.j_spaces.core;

import com.gigaspaces.CommonSystemProperties;
import com.gigaspaces.admin.cli.RuntimeInfo;
import com.gigaspaces.client.transaction.MahaloFactory;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.jmx.JMXUtilities;
import com.gigaspaces.internal.lookup.RegistrarFactory;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.utils.AbstractShutdownHook;
import com.gigaspaces.internal.utils.GsEnv;
import com.gigaspaces.internal.utils.ValidationUtils;
import com.gigaspaces.logger.GSLogConfigLoader;
import com.gigaspaces.security.service.SecurityResolver;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.Constants.QueryProcessorInfo;
import com.j_spaces.core.admin.ContainerConfig;
import com.j_spaces.core.admin.IJSpaceContainerAdmin;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.admin.SpaceConfig;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.cluster.ClusterXML;
import com.j_spaces.core.exception.InvalidServiceNameException;
import com.j_spaces.core.exception.SpaceConfigurationException;
import com.j_spaces.core.service.ServiceConfigLoader;
import com.j_spaces.kernel.*;
import com.j_spaces.kernel.ResourceLoader.SchemaProperties;
import com.j_spaces.kernel.log.JProperties;
import com.j_spaces.sadapter.datasource.DataAdaptorIterator;
import com.sun.jini.admin.DestroyAdmin;
import com.sun.jini.mahalo.TxnManager;
import com.sun.jini.start.LifeCycle;
import com.sun.jini.start.NonActivatableServiceDescriptor;
import com.sun.jini.start.ServiceProxyAccessor;
import net.jini.admin.Administrable;
import net.jini.config.ConfigurationException;
import net.jini.export.Exporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.URI;
import java.net.URL;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.j_spaces.core.Constants.Cluster.*;
import static com.j_spaces.core.Constants.Container.*;
import static com.j_spaces.core.Constants.DCache.*;
import static com.j_spaces.core.Constants.DataAdapter.*;
import static com.j_spaces.core.Constants.LeaderSelector.*;
import static com.j_spaces.core.Constants.LeaseManager.*;
import static com.j_spaces.core.Constants.LookupManager.*;
import static com.j_spaces.core.Constants.Mirror.MIRROR_SERVICE_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.Mirror.MIRROR_SERVICE_ENABLED_PROP;
import static com.j_spaces.core.Constants.RemoteCode.*;
import static com.j_spaces.core.Constants.Schemas.*;
import static com.j_spaces.core.Constants.StorageAdapter.PERSISTENT_ENABLED_DEFAULT;
import static com.j_spaces.core.Constants.StorageAdapter.PERSISTENT_ENABLED_PROP;

/**
 * A J-Space Server is a physical process (JVM) that has a J-Space Container instance. A J-Space
 * container manages the life cycle of J-Space instances. Life cycle operations include creation,
 * destruction and even moving a J-Space from one container to another. The J-Space container is a
 * remote object that implements the <code>IJSpaceContainer</code> interface. A J-Space instance is
 * an instance of <code>SpaceInternal</code>, which implements the remote interface
 * <code>ISpaceInternal</code>. Each J-Space in a container has a unique name. This name can be used
 * after creation to receive a reference to the specific J-Space. The container is also responsible
 * for registering its J-Spaces in the lookup services in the environment and maintaining the leases
 * it receives. It is listening to new lookup services entering the environment and makes sure its
 * J-Spaces are registered to these lookup managers too. The container keeps a map between spaces
 * names and spaces attributes. All of the container information is kept in a persistent storage,
 * which is an XML file, under the [J-Spaces-Home]/config directory. The name of the container is
 * determined by a property in the JSpaces.properties file. The default is the name of the machine
 * J-Spaces is installed on. Using <code>shutdown</code> function container terminate LookupManager
 * and close space connection versus Storage adapters. A J-Space container may be launched as a
 * Unicast remote object or as an Activatable remote object.
 *
 * @author Igor Goldenberg
 * @version 1.0
 */
@com.gigaspaces.api.InternalApi
public class JSpaceContainerImpl implements IJSpaceContainer, IJSpaceContainerAdmin {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONTAINER);
    private static final Logger licenseLogger = LoggerFactory.getLogger("com.gigaspaces.license");
    /**
     * Maximum delay for unexport attempts
     */
    private static final long MAX_UNEXPORT_DELAY = 1000 * 60; // 1 minute
    private static final String COLON = ":";

    private final static String SPACE_TAG = "JSpaces";
    private final static String SPACE_CONFIG = "space-config";

    /* Container status */
    public static final int STARTING_STATUS = 0;
    public static final int RUNNING_STATUS = 1;
    public static final int SHUTDOWN_STATUS = 2;

    private final LifeCycle _lifeCycle;
    private final String _schemaName;
    private final SpaceURL _url;
    private final Properties _customProperties;
    private final String _containerName;
    private final String _spaceName;
    private final String _clusterSchema;
    private final Object _lock;
    private final String _hostname;
    // Exporter for the space container. Uses the GenericExporter by default
    private final Exporter _containerExporter;
    private final long _startTime;
    private IJSpaceContainer _containerStub;
    // ShutdownHook (if enabled) responsible for container abrupt shutdown
    private final ShutdownHook _shutdownHook;
    private ContainerEntry _containerEntry;

    /**
     * The cluster Config Debug Output dump of the Cluster Configuration structure. if debug mode is
     * turned on.
     */
    private StringBuilder clusterConfigOutput;

    // All the container <code>Element</code> instances in the container XML file
    private Element m_rootContainerElement;
    private Element m_rootSpaceElement;

    // Document of the container-config XML file.
    private Document m_containerConfigFileDocument;
    // Document of the container XML file.
    private Document m_containerFileDocument;
    // reference to [space-config] and [dist-cache] tag in configuration XML file of container
    private Node m_spaceConfigNode;

    private LookupManager m_LookupMng;

    // XML Container file
    private File m_ContainerFile;
    private String m_FileNameWithoutExtention;


    // default container status is Running
    private int m_containerStatus = STARTING_STATUS;

    /**
     * Using this config-directory container will be load and create all xml files in this
     * directory. Where located [container-name]-config.xml file, will be config directory.
     */
    private String m_configDirectory;
    private volatile ContainerConfig m_containerConfig;

    private String _rmiHostAndPort;

    // this flag indicates if at least one space was already created and loaded,
    // relevant for semi-dynamic mode when spaceURL passed and
    // _clusterSchema might be passed also.
    // So after first space created value of this flag set as true.
    private boolean isFirstSpaceCreated;

    private Map<String, SpaceConfig> _allSpaceSchemasMap;

    public ContainerEntry getContainerEntry() {
        return _containerEntry;
    }

    private ContainerEntry getContainerEntry(String spaceName) throws NoSuchNameException {
        if (spaceName == null)
            throw new IllegalArgumentException("The spaceName parameter can not be null.");

        synchronized (_lock) {
            if (_containerEntry == null || !_spaceName.equals(spaceName))
                throw new NoSuchNameException("Space <" + spaceName + "> does not exist in <" + _containerName + "> container.");

            return _containerEntry;
        }
    }

    public static class ContainerEntry {
        private final SpaceImpl _spaceImpl;
        private final JSpaceAttributes _spaceAttr;
        private final IJSpace _nonClusteredSpaceProxy;
        private final IJSpace _clusteredSpaceProxy;

        private ContainerEntry(SpaceImpl spaceImpl, JSpaceAttributes spaceAttr)
                throws RemoteException {
            this._spaceImpl = spaceImpl;
            this._spaceAttr = spaceAttr;
            this._nonClusteredSpaceProxy = spaceImpl.getSingleProxy();
            this._clusteredSpaceProxy = spaceImpl.getSpaceProxy();
        }

        public SpaceImpl getSpaceImpl() {
            return _spaceImpl;
        }

        public JSpaceAttributes getSpaceAttributes() {
            return _spaceAttr;
        }

        public IJSpace getNonClusteredSpaceProxy() {
            return _nonClusteredSpaceProxy;
        }

        public IJSpace getClusteredSpaceProxy() {
            return _clusteredSpaceProxy;
        }

        public String getName() {
            return _nonClusteredSpaceProxy.getName();
        }
    }

    /**
     * The container shutdown in response to a user interrupt, such as typing <tt>^C</tt>, or a
     * system-wide event, such as user logoff or system shutdown.
     */
    private class ShutdownHook extends AbstractShutdownHook {
        ShutdownHook() {
            super("ShutdownHook-JSpaceContainerImpl");
        }

        @Override
        protected void onShutdown() {
            shutdownInternal();
        }
    }

    static {
        initSecurity();
    }

    public JSpaceContainerImpl(LifeCycle lifeCycle, String schemaName, SpaceURL spaceURL, Properties schemaProperties)
            throws Exception {
        this._startTime = System.currentTimeMillis();
        this._lifeCycle = lifeCycle;
        this._schemaName = schemaName;
        this._url = spaceURL;
        this._customProperties = schemaProperties;
        this._containerName = spaceURL.getContainerName();
        this._spaceName = spaceURL.getSpaceName();
        this._clusterSchema = spaceURL.getClusterSchema();
        this._lock = new Object();
        this._hostname = SystemInfo.singleton().network().getHost().getHostName();


        // print system/GS info
        RuntimeInfo.logRuntimeInfo(_logger, "Starting space...");

        _containerExporter = ServiceConfigLoader.getExporter();

        // Exports the remote object (container) to make it available to bind in JNDI (rmiregistry) and create container proxy
        try {
            _containerStub = new LRMIJSpaceContainer(this, (IJSpaceContainer) _containerExporter.export(this), _containerName);
        } catch (Exception ex) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to exports the remote object for <" + getName() + "> container: ", ex);
        }

        try {
            initContainer();
        } catch (Exception e) {
            try {
                shutdownInternal();
            } catch (Exception e2) {
                _logger.debug("Could not shutdown cleanly after failed initialization.", e);
            }
            throw e;
        }

        _shutdownHook = initShutdownHook();
    }

    public long getStartTime() {
        return _startTime;
    }

    private static void initSecurity() {
        // This will eliminate the need to modify the IBM JDK java.security when using IBM JDK.
        try {
            java.security.Security.addProvider(new sun.security.provider.Sun());
        } catch (SecurityException e) {
            /**
             * SecurityException might be caught, if a security manager exists and
             * its <code>{@link
             * java.lang.SecurityManager#checkSecurityAccess}</code>
             * method denies access to add a new provider. The security exception
             * might happen while running in the context of other container e.g.
             * embedded space inside Application server without granting implicit
             * policy permissions.
             */
            if (_logger.isErrorEnabled())
                _logger.error("Failed to add the sun.security.provider.Sun SecurityManager: " + e.toString(), e);
        }
    }

    private ShutdownHook initShutdownHook() {
        ShutdownHook shutdownHook = null;
        try {
            // Initialize shutdown hooker thread
            String addShutdownHook = JProperties.getContainerProperty(
                    _containerName, CONTAINER_SHUTDOWN_HOOK_PROP,
                    CONTAINER_SHUTDOWN_HOOK_PROP_DEFAULT, true);

            if (addShutdownHook.equalsIgnoreCase("true")) {
                shutdownHook = new ShutdownHook(); //construct hook
                // Register a new virtual-machine shutdown hook. This hook will be removed upon a proper call to #shutdown();
                Runtime.getRuntime().addShutdownHook(shutdownHook);
            }
        } catch (Throwable t) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to add ShutdownHook for <" + getName() + "> container: ", t);
        }

        return shutdownHook;
    }

    private void initContainer() throws Exception {
        m_containerStatus = STARTING_STATUS;

        GSLogConfigLoader.getLoader();
        /** starting RMIRegistry if required * */
        // _url might be null when using old JSpaceServerImpl
        if (_url != null &&
                GsEnv.propertyBoolean( CommonSystemProperties.JNDI_ENABLED_PROP ).get( CommonSystemProperties.JNDI_ENABLED_DEFAULT_BOOLEAN_VALUE ) ) {
            String registeredRmiHostAndPort = loadRMIRegistry(_url.getHost());

            //update all desired objects with registered rmi host and port
            //since that value might be changed within method loadRMIRegistry()
            //set jndi url to SpaceURL object
            _url.setProperty(SpaceURL.HOST_NAME, registeredRmiHostAndPort);
            JProperties.getContainerProperties(_containerName)
                    .setProperty(LOOKUP_JNDI_URL_PROP, registeredRmiHostAndPort);
        }

        String prevGroups = setLookupgroups(_url.getProperty(SpaceURL.GROUPS));
        String prevLocators = setLookupLocators(_customProperties.getProperty(XPathProperties.CONTAINER_JINI_LUS_UNICAST_HOSTS));
        _reggieCreatedRef = createLookupServiceIfNeeded(_containerName);
        _mahaloCreatedRef = createMahaloIfNeeded(_containerName);
        SystemInfo.singleton().lookup().setGroups(prevGroups);
        SystemInfo.singleton().lookup().setLocators(prevLocators);

        // Create LookupManager for this JSpace container.
        m_LookupMng = new LookupManager(this.getName());

        /**
         * Initialize XML property file and load the container.xml with all its
         * defined spaces if the spaceNameToLoad is set (from SpaceFinder) it
         * means we need to bypass the container.xml and load only the
         * spaceNameToLoad. Even If the load-on-startup flag set to false, and the
         * spaceNameToLoad != null we still load this space.
         */
        xmlInit();

        // Create and register container MBean if JMX enabled.
        if (isJMXEnabled()) {
            try {
                com.j_spaces.jmx.JMXProvider.registerContainerMBean(_containerName, this);
            } catch (Throwable ex) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn("Failed registering ContainerMBean: "
                            + ex.toString(), ex);
                }
            }

            com.j_spaces.jmx.JMXProvider.registerTransportMBean(_containerName);
            com.j_spaces.jmx.JMXProvider.registerLocalTimeMBean(_containerName);
        } else {
            if (_logger.isInfoEnabled()) {
                _logger.info("MBean Server did not start since the JMX support is disabled.");
            }
        }

        m_containerStatus = RUNNING_STATUS;

        if (Boolean.getBoolean(SystemProperties.ENV_REPORT)) {
            StringBuilder envDumpInfo = getSystemDump();

            if (_logger.isInfoEnabled()) {
                _logger.info("\n " + envDumpInfo.toString());
            }
        }
    }

    public String getRuntimeConfigReport() {
        return getSystemDump().toString();
    }

    private StringBuilder getSystemDump() {
        /*************************************************************************
         * dump of the overall system configurations (spaces, container, cluster)
         * ,system env, sys properties etc.
         ************************************************************************/
        StringBuilder envDumpInfo = new StringBuilder("\n\n==============================================================================================\n");
        envDumpInfo.append(JProperties.getContainerPropertiesDump(_containerName));
        envDumpInfo.append(JProperties.getAllSpacesPropertiesDump());
        if (clusterConfigOutput != null) {
            envDumpInfo.append(clusterConfigOutput.toString());
        }
        envDumpInfo.append("Space initialized using SpaceURL:" + _url);
        envDumpInfo.append(RuntimeInfo.getEnvironmentInfo(true));


        return envDumpInfo;
    }

    public IJSpaceContainer getContainerProxy() {
        return _containerStub;
    }

    /** Implements abstract methods. */

    /**
     * Release any remote objects associated with this <code>IJSpaceContainer</code>.
     */
    protected void unregister() {
        try {
            long endTime = SystemTime.timeMillis() + MAX_UNEXPORT_DELAY;
            boolean unexported = false;

            /** first try unexporting politely */
            while (!unexported && (SystemTime.timeMillis() < endTime)) {
                unexported = _containerExporter.unexport(true);
                if (!unexported)
                    Thread.yield();
            }

            /** if still not unexported, forcibly unexport */
            if (!unexported)
                _containerExporter.unexport(true);
        } catch (Exception ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error(ex.toString(), ex);
            }
        }
    }

    /**
     * Initialize XML Property file of JSpace container. Default location of this file is
     * <%JSPACE_HOME_DIR%>/config, if no schemas are used (backwards compatability). If schemas are
     * used, then the default configuration folder is /config/schemas (its a ResourceBundle
     * location, so it doesn't have to be on disk, it can be in the JSpaces.jar as well). If name of
     * container XML file not defined in space config xml file, the default file name will be name
     * of local host.
     *
     * @throws Exception Standard exception.
     */
    private void xmlInit() throws Exception {
        // check if config-directory valid where located
        // [container-name]-config.xml file
        // this is for backwards compatability, if schemas are used, we use the
        // /config/schemas path.
        String containerConfigURL = JProperties.getURL();
        // if containerURL == null, means we use schemas.
        File confDir = null;
        // support case when the JProperties.getURL() will be null (e.g. when
        // InputStream is used instead)
        if (!JSpaceUtilities.isEmpty(containerConfigURL))
            confDir = new File(new File(containerConfigURL).getParent());

        if (confDir != null && confDir.canRead())
            m_configDirectory = confDir.getPath();
        else
            m_configDirectory = SystemLocations.singleton().config().toString();

        /**
         * Fixed bug 26/06/06 Gershon - http://62.90.11.164:8080/browse/APP-90
         * Container.XML location mechanism must use getResource() and load the
         * container xml from resource according to the classpath
         */
        // did not find it using a file system path, go and look for container xml
        // using resource bundle
        // and fetch it from classpath
        /**
         * Looking for container.xml in classpath and if exists, we load/parse it
         * and load all the list of space it contains.
         */
        URL containerXMLURL = ResourceLoader.findContainerXML(_containerName);
        if (containerXMLURL != null) {
            m_ContainerFile = new File(new URI(containerXMLURL.toString()));
            String _containerFileName = m_ContainerFile.getPath();
            m_FileNameWithoutExtention = _containerFileName.substring(0,
                    _containerFileName.length()
                            - ".xml".length());
        }
        //if such container configuration file still
        //does not exist then create it under [GS_HOME]/config folder
        else {
            m_FileNameWithoutExtention = SystemLocations.singleton().config(_containerName).toString();
        }

        synchronized (_lock) {
            // if the container.xml does NOT exist and a specific space name to be
            // load, was passed
            // (meaning we coming from SpaceFinder() then we do not load container xml
            // nor we do not create a new file either. We just go and load the space.
            if (_containerEntry == null && m_ContainerFile == null && !JSpaceUtilities.isEmpty(_spaceName)) {
                boolean isLoadOnStartup = false;
                // check for semi-dynamic mode, load spaces with loadOnStartup value
                // true
                if (!isFirstSpaceCreated
                        && !JSpaceUtilities.isEmpty(_spaceName))
                    isLoadOnStartup = true;

                loadSpace(_spaceName, new JSpaceAttributes(_schemaName,
                        _customProperties,
                        isLoadOnStartup));
                return;
            }
        }

        // Obtaining a org.w3c.dom.Document from XML
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        if (!JSpaceUtilities.isEmpty(containerConfigURL)) {
            // get reference to Document object of container
            m_containerConfigFileDocument = builder.parse(containerConfigURL/* fileName */);
            JSpaceUtilities.normalize(m_containerConfigFileDocument);

            // find SPACE_CONFIG tag in the container configuration XML file
            NodeList spConfNL = m_containerConfigFileDocument.getElementsByTagName(SPACE_CONFIG);
            // NodeList dCacheNL =
            // m_containerConfigFileDocument.getElementsByTagName(ClusterXML.DCACHE_TAG);

            // throw exception if [space-config] tag not found
            if (spConfNL.getLength() == 0) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("<"
                            + SPACE_CONFIG
                            + "> tag does not exist in "
                            + m_configDirectory
                            + File.separator
                            + _containerName
                            + ".xml file. Space config XML file of space will not be created.");
                }
            } else
                m_spaceConfigNode = spConfNL.item(0);
        }

        // Load all the spaces from the container XML file (if exists)
        if (m_ContainerFile != null && m_ContainerFile.exists()) {
            loadSpacesFromXMLFile(builder);

            if (_logger.isDebugEnabled()) {
                _logger.debug("This container " + _containerName
                        + " loaded successful from " + m_ContainerFile + " XML file.");
            }
        }
    }

    /**
     * Creates JSpace with unique space-name in this container and write the created space to
     * appropriate Storage Adapter. If space already exists in this container
     * <code>CreateException</code> will be throws. The created space will be registered with all
     * discovered Lookup Services in the network. Created space will added to XML file.
     *
     * @param spaceName Space name for creating.
     * @param spaceAttr If spaceAttr.isPrivate == true this space will not register with
     *                  LookupService and Rmi registry.
     * @return IJSpace Return collocated space if spaceAttr.isPrivate == true. If spaceAttr == null
     * or spaceAttr.isPrivate == false return standard proxy.
     * @throws CreateException If this <code>spaceName</code> already exists in JSpaceContainer,
     *                         during creating space in Storage Adapter or if <code>spaceName</code>
     *                         has same name as container Name.
     * @throws RemoteException Rmi remote exception.
     */
    public IJSpace createSpace(String spaceName, final JSpaceAttributes spaceAttr)
            throws CreateException, RemoteException {
        if (spaceName == null)
            throw new IllegalArgumentException("The spaceName parameter can not be null.");

        spaceName = spaceName.trim();

        try {
            ValidationUtils.checkServiceNameForValidation(spaceName, "Space name");
        } catch (InvalidServiceNameException exc) {
            throw new CreateException("Invalid space name.", new InvalidServiceNameException(exc.getMessage()));

        }

        final String fullSpaceName = JSpaceUtilities.createFullSpaceName(_containerName, spaceName);

        // Evgeny 3.11.2005
        // set properties that will be used for db name evaluation from space
        // schema
        System.setProperty(SystemProperties.DB_CONTAINER_NAME, _containerName);
        System.setProperty(SystemProperties.DB_SPACE_NAME, spaceName);

        SpaceImpl spaceImpl;

        synchronized (_lock) {
            if (!spaceName.equalsIgnoreCase(_containerName) && _containerEntry == null && _spaceName.equals(spaceName)) {
                /**
                 * check if the space.xml file already was created, using this boolean
                 * we can know, delete the xml file if exception occurred or not.
                 */
                boolean isSpaceFileAreadyWasExists = new File(m_configDirectory
                        + File.separator + spaceName + ".xml").exists();

                try {

                    String schemaName = spaceAttr.getSchemaName();
                    // check if this is existing schema,
                    // if not - create schema file and put it under
                    // [GIGASPACES_HOME]\config\schemas
                    if (schemaName != null) {
                        String schemaFilePath = Constants.Container.CONTAINER_CONFIG_DIRECTORY + "/"
                                + Constants.Schemas.SCHEMAS_FOLDER + "/" + schemaName
                                + Constants.Schemas.SPACE_SCHEMA_FILE_SUFFIX;

                        InputStream schemaInputStream = ResourceLoader.getResourceStream(schemaFilePath);
                        if (schemaInputStream == null) {
                            String schemasFolderPath = Environment.createSchemasFolderIfNotExists();
                            String schemaFileFullPath = schemasFolderPath
                                    + File.separator + schemaName
                                    + Constants.Schemas.SPACE_SCHEMA_FILE_SUFFIX;
                            // set right schema name in properties
                            spaceAttr.setProperty(Constants.SPACE_CONFIG_PREFIX
                                    + SCHEMA_ELEMENT, schemaName);

                            File schemaFile = new File(schemaFileFullPath);
                            // write new schema file to disk, create it from
                            // SpaceAttributes
                            SpaceConfigFactory.performSaveAs(schemaFile, spaceName,
                                    _containerName, spaceAttr);

                        }
                    }
                    /*
                     * Create XML configuration file and build JProperties. We need to
                     * create XML config file before creation space, because
                     * SpaceEngine, need already JProperties. If exception occurs,
                     * remove space-property from JProperties and delete spaceName.xml
                     * file.
                     */
                    createSpaceXML(spaceName, spaceAttr);

                    // set resolved db name
                    Properties spaceProperties = JProperties.getSpaceProperties(fullSpaceName);

                    // get cluster policy. if this space not clustered, return value
                    // will null.
                    // if we have cluster_schema attribute as part of the passed
                    // SpaceURL
                    // we use different ClusterXML constructor
                    // OR
                    // if the spaceAttr.m_clusterConfigURL != null

                    // Evgeny, 9.08.2005: comment checking for _clusterSchema
                    // because that causes to bug when
                    // attempt to create space under container that was started with
                    // spaceURL and
                    // _clusterSchema value for example from one of our examples
                    if (spaceAttr.getClusterConfigURL() != null
                            && !spaceAttr.getClusterConfigURL()
                            .equals(CLUSTER_CONFIG_URL_DEFAULT) /*
                             * ||
                             * !JSpaceUtilities.isEmpty(
                             * _clusterSchema )
                             */) {
                        ClusterPolicy clusterPolicy = createClusterPolicy(spaceName, spaceAttr.getClusterConfigURL());
                        spaceAttr.setClustered(true);
                        spaceAttr.setClusterPolicy(clusterPolicy);

                        spaceProperties.put(fullSpaceName + "."
                                        + Constants.SPACE_CONFIG_PREFIX
                                        + IS_CLUSTER_SPACE_PROP,
                                String.valueOf(spaceAttr.isClustered()));

                        spaceProperties.put(fullSpaceName + "."
                                        + Constants.SPACE_CONFIG_PREFIX
                                        + CLUSTER_CONFIG_URL_PROP,
                                spaceAttr.getClusterConfigURL());

                        JProperties.setSpaceProperties(fullSpaceName, spaceProperties);
                    }

                    // clone spaceAttr, for creating Space with converted spaceAttr system properties
                    JSpaceAttributes attr = (JSpaceAttributes) spaceAttr.clone();
                    spaceImpl = createSpaceImpl(spaceName, attr);

                    _containerEntry = new ContainerEntry(spaceImpl, spaceAttr);

                    // if false the space will registering with Directory Services
                    if (!Boolean.valueOf(spaceAttr.isPrivate()).booleanValue())
                        m_LookupMng.register(_containerEntry.getClusteredSpaceProxy(), _containerName);

                    // if there is no container XML file, i.e. invoked via spaceFinder
                    // if( m_rootSpaceElement != null )
                    // Updating Container XML tree //TODO don't do it if schema used and
                    // with only 1 space..
                    // updateContainerXMLTree(spaceName, spaceAttr, false);

                    // Update container XML
                    try {
                        updateContainerXML();
                    } catch (Exception ex) {
                        if (_logger.isWarnEnabled())
                            _logger.warn("Fail to update " + _containerName + " container XML", ex);
                    }

                    if (isJMXEnabled())
                        com.j_spaces.jmx.JMXProvider.registerSpaceMBean(spaceName,
                                spaceImpl);

                    // if space is private returns embedded space, otherwise remote space
                    if (spaceImpl.getClusterPolicy() != null)
                        return this.getClusteredSpace(spaceName);
                    return this.getSpace(spaceName);
                } catch (Exception ex) {
                    // remove space properties from JProperties and delete created spaceName.xml file
                    JProperties.removeSpaceProperties(fullSpaceName);

                    // delete XML configuration file of space ( spaceName.xml), if
                    // isSpaceFileAreadyExists = false
                    if (!isSpaceFileAreadyWasExists) {
                        File sf = new File(m_configDirectory + File.separator + spaceName + ".xml");
                        sf.delete();
                    }

                    if (_logger.isErrorEnabled())
                        _logger.error("Failed to create <" + spaceName + "> space", ex);

                    throw new CreateException("Failed to create <" + spaceName + "> space", ex);
                }
            } else {
                if (spaceName.equalsIgnoreCase(_containerName))
                    throw new CreateException("Could not create space <" + spaceName + "> since its name is the same name as container.");

                throw new CreateException("Space <" + spaceName + "> already exists in <" + _containerName + "> container.");
            }
        }//end of synchronized block
    } // createSpace()

    /**
     * Destroying certain space in JSpace container and close connection with Storage Adapters(if
     * opened). This destroyed space will be unregistered with all Lookup Services on which was
     * registered. Destroyed space will removed from XML file.
     *
     * @param spaceName JSpace name.
     * @throws DestroyedFailedException During destroying JSpace in JSpace container.
     * @throws RemoteException          Rmi remote exception.
     */
    public void destroySpace(String spaceName) throws DestroyedFailedException,
            RemoteException {
        if (spaceName == null)
            throw new IllegalArgumentException("The spaceName parameter can not be null.");

        synchronized (_lock) {
            String fullSpaceName = JSpaceUtilities.createFullSpaceName(_containerName, spaceName);

            if (_containerEntry != null && _spaceName.equals(spaceName)) {
                try {
                    // close properly space and connection with storage adapter if
                    // opened
                    _containerEntry.getSpaceImpl().shutdown(true, false);
                } catch (RemoteException ex) {
                    if (_logger.isWarnEnabled())
                        _logger.warn("Failed to destroy <" + spaceName + "> space", ex);

                    throw new DestroyedFailedException("Failed to destroy <" + spaceName + "> space", ex);
                }

                // Destroy distributed cache configuration if not equals default
                // String sourceDCacheConfigFile =
                // JProperties.getSpaceProperty(spaceName,
                // Constants.DCache.CONFIG_FILE_URL_PROP, "");
                String sourceDCacheConfigName = JProperties.getSpaceProperty(fullSpaceName, Constants.DCache.CONFIG_NAME_PROP, "");
                // String defaultDCacheConfigFile = m_configDirectory + File.separator
                // + DCACHE_CONFIG_FILE_DEFAULT;
                String fileName = sourceDCacheConfigName + FILE_SUFFIX_EXTENTION;
                String sourceDCacheConfigFile = m_configDirectory + File.separator + fileName;
                if (!fileName.equals(DCACHE_CONFIG_FILE_DEFAULT))
                    new File(sourceDCacheConfigFile).delete();

                // remove space properties from JProperties class
                JProperties.removeSpaceProperties(fullSpaceName);

                // if working with container xml file, i.e. m_rootContainerElement !=
                // null
                if (m_rootContainerElement != null) {
                    // Remove all space-nodes from XML file
                    NodeList spaceNodeList = m_rootContainerElement.getElementsByTagName(spaceName);
                    if (spaceNodeList.getLength() != 0) {
                        for (int i = 0; i < spaceNodeList.getLength(); ) {
                            Node parent = spaceNodeList.item(i).getParentNode();
                            parent.removeChild(spaceNodeList.item(i));
                        }
                    }
                }

                // delete XML configuration file of space ( spaceName.xml).
                File sf = new File(m_configDirectory + File.separator + spaceName + ".xml");
                sf.delete();

                if (_logger.isInfoEnabled())
                    _logger.info("The <" + spaceName + "> space of <" + _containerName + "> container was destroyed successfully.");

                // Update container XML
                try {
                    updateContainerXML();
                } catch (Exception ex) {
                    if (_logger.isWarnEnabled())
                        _logger.warn("Fail to update " + _containerName + " container XML", ex);
                }

                if (isJMXEnabled())
                    com.j_spaces.jmx.JMXProvider.unregisterSpaceMBean(spaceName, _containerName);
            } else
                throw new DestroyedFailedException("Space <" + spaceName + "> does not exist in <" + _containerName + "> container.");
        }
    }

    /**
     * Returns array of all JSpace names that exists in this JSpace container.
     *
     * @return String[] All JSpace names that exists in this JSpace container.
     * @throws RemoteException Rmi remote exception.
     */
    public String[] getSpaceNames() throws RemoteException {
        synchronized (_lock) {
            return _containerEntry != null ? new String[]{_containerEntry.getName()} : new String[0];
        }
    }

    /**
     * Returns name of container.
     */
    public String getName() {
        return _containerName;
    }

    @Override
    public IJSpace getSpace(String spaceName) throws NoSuchNameException {
        return getContainerEntry(spaceName).getNonClusteredSpaceProxy();
    }

    @Override
    public IJSpace getClusteredSpace(String spaceName) throws NoSuchNameException {
        return getContainerEntry(spaceName).getClusteredSpaceProxy();
    }

    /**
     * Returns the SpaceURL object which initialized that specific container instance. <br> Note
     * that the SpaceURL object contains importent information on the space and container
     * configuration/setup such as the space url used, the space/container/cluster schema used and
     * other attributes. The IJSpace keeps also its reference of the SpaceURL which loanched the
     * space.
     *
     * @return SpaceURL Returns the SpaceURL object which initialized that specific space instance.
     * @see com.j_spaces.core.IJSpaceContainer#getURL()
     */
    public SpaceURL getURL() throws java.rmi.RemoteException {
        return _url;
    }

    @Override
    public void ping() {
    }

    public IRemoteJSpaceAdmin getSpaceAdmin(String spaceName)
            throws NoSuchNameException {
        return getContainerEntry(spaceName).getSpaceImpl();
    }

    @Override
    public void shutdown() throws RemoteException {
        if (!LRMIJSpaceContainer.isEmbeddedShutdownInvocation.get() && !Boolean.getBoolean("com.gs.enabled-backward-space-shutdown"))
            throw new UnsupportedOperationException("shutting down a space is only allowed from the collocated proxy");

        shutdownInternal();
    }

    public void shutdownInternal() {
        _allSpaceSchemasMap = null;
        // set shutdown container status
        synchronized (this) {
            if (getContainerStatus() == SHUTDOWN_STATUS)
                return;

            m_containerStatus = SHUTDOWN_STATUS;
        }

        try {
            synchronized (_lock) {
                if (_containerEntry != null) {
                    _containerEntry.getSpaceImpl().shutdown(true/*isDestroy*/, false/*shutdowContainer*/);
                    if (isJMXEnabled())
                        com.j_spaces.jmx.JMXProvider.unregisterSpaceMBean(_containerEntry.getSpaceImpl().getName(), _containerName);
                }
            }

            // unregistering all space from lookup services.
            if (m_LookupMng != null)
                m_LookupMng.terminate();

            // Unregister container MBean if JMX is enabled.
            if (isJMXEnabled()) {
                try {
                    com.j_spaces.jmx.JMXProvider.unregisterContainerMBean(this);
                } catch (Exception ex) {
                    if (_logger.isErrorEnabled()) {
                        _logger.error(ex.toString(), ex);
                    }
                }
            }
            try {
                com.j_spaces.jmx.JMXProvider.unregisterTransportMBean(_containerName);
            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(ex.toString(), ex);
                }
            }

            unregister();

            // Terminate a running Reggie service
            destroyIfNeeded(_reggieCreatedRef);
            // Terminate a running Mahalo service
            destroyIfNeeded(_mahaloCreatedRef);

            // If container/space constructed through jini ServiceStarter
            // invoke destroy method to inform the JSpaceService object
            // that it can release any resources associated with the server.
            // if (m_jservice != null) m_jservice.destroyService();

            // set shutdown container status
            synchronized (this) {
                notifyAll();
            }

            if (_logger.isInfoEnabled())
                _logger.info("Container <" + _containerName + "> shutdown completed");

        } catch (Exception ex) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to shutdown <" + _containerName + "> container", ex);
        } finally {
            /** Write to the disk indication that container was close successfully. */
            if (Boolean.getBoolean(GracefulContainerShutDown.GRACEFUL_SHUTDOWN))
                GracefulContainerShutDown.gracefulShutDown(_containerName);

            try {
                //De-registers a previously-registered virtual-machine shutdown hook.
                if (_shutdownHook != null)
                    Runtime.getRuntime().removeShutdownHook(_shutdownHook);
            } catch (Throwable t) {
                if (_logger.isWarnEnabled())
                    _logger.warn("Failed to remove ShutdownHook for <" + getName() + "> container: ", t);
            }
        }
    }

    private static void destroyIfNeeded(NonActivatableServiceDescriptor.Created serviceRef)
            throws RemoteException {
        if (serviceRef != null && serviceRef.proxy != null
                && (serviceRef.proxy instanceof Administrable)) {
            Object adminObject = ((Administrable) serviceRef.proxy).getAdmin();
            if (adminObject instanceof DestroyAdmin)
                ((DestroyAdmin) adminObject).destroy();
        }
    }

    public int getContainerStatus() {
        synchronized (this) {
            return m_containerStatus;
        }
    }

    /**
     * Create (if not exists) and update the container XML, that will contain all present spaces and
     * their relevant attributes.
     */
    private synchronized void updateContainerXML() throws IOException, ParserConfigurationException {
        synchronized (_lock) {
            if (_containerEntry == null)
                return;
        }

        // create if the file does not exist (and we do not come from SpaceFinder
        // because the
        // _spaceName is null as well), we create this file
        if (m_ContainerFile != null && !m_ContainerFile.exists()) {
            m_ContainerFile.createNewFile();

            if (_logger.isDebugEnabled()) {
                _logger.debug("Created new " + m_ContainerFile + " XML file for "
                        + _containerName + " container.");
            }
        }

        // Obtaining a org.w3c.dom.Document from XML
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();

        // Build main container XML tree
        m_containerFileDocument = builder.newDocument();
        m_rootContainerElement = m_containerFileDocument.createElement(_containerName);

        m_rootSpaceElement = m_containerFileDocument.createElement(SPACE_TAG);
        m_rootContainerElement.appendChild(m_rootSpaceElement);

        synchronized (_lock) {
            if (_containerEntry != null)
                updateContainerXMLTree(_containerEntry.getName(), _containerEntry.getSpaceAttributes());
        }

        // Save state of container XML files
        saveState();
    }

    /**
     * Building XML space tree inside [container-name].xml file.
     *
     * @param spaceName JSpace name.
     * @param spaceAttr JSpace attributes.
     */
    synchronized public void updateContainerXMLTree(String spaceName,
                                                    JSpaceAttributes spaceAttr) {
        if (m_rootSpaceElement == null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Fail to update the " + _containerName
                        + " container XML file for " + spaceName
                        + " space; the DOM element for spaces is null");
            }

            return;
        }

        spaceAttr = (spaceAttr == null) ? new JSpaceAttributes() : spaceAttr;

        /**
         * Check if this space name exists in XML, if exists remove the existing
         * space Node. This situation can occurs only if space didn't load from
         * XML and application trying create the space yet time.
         */
        NodeList spaceNL = m_rootSpaceElement.getElementsByTagName(spaceName);
        for (int i = 0; i < spaceNL.getLength(); i++)
            m_rootSpaceElement.removeChild(spaceNL.item(i));

        // Create space node and append to root <code>SPACE_TAG</code>
        Element jspaceElement = m_containerFileDocument.createElement(spaceName);
        m_rootSpaceElement.appendChild(jspaceElement);

        // Appending space to container node
        m_rootSpaceElement.appendChild(jspaceElement);

        // Creates Node for every Element
        Element propertiesElem = m_containerFileDocument.createElement(SpaceURL.PROPERTIES_FILE_NAME);
        Element schemaElem = m_containerFileDocument.createElement(SCHEMA_ELEMENT);

        // Set Node-value for schema Element
        Text schemaText = null;
        if (spaceAttr.getSchemaName() != null)
            schemaText = m_containerFileDocument.createTextNode(spaceAttr.getSchemaName());
        else
            schemaText = m_containerFileDocument.createTextNode("");

        // Set Node-value for schema Element
        Text propertiesText = null;
        // Check if a properties file name was passed by the client, if so we add
        // its name to the container <properties> attribute.
        // Otherwise, the system itself have passed a new Properties object which
        // its purpose is to overwrite current configuration values.
        if (spaceAttr.getCustomProperties() != null
                && spaceAttr.getCustomProperties()
                .getProperty(SpaceURL.PROPERTIES_FILE_NAME) != null)
            propertiesText = m_containerFileDocument.createTextNode(spaceAttr.getCustomProperties()
                    .getProperty(SpaceURL.PROPERTIES_FILE_NAME));
        else
            propertiesText = m_containerFileDocument.createTextNode("");

        // Append TEXT-Node to space and Space to Container Node
        jspaceElement.appendChild(schemaElem).appendChild(schemaText);
        jspaceElement.appendChild(propertiesElem).appendChild(propertiesText);
        Element isLoadOnStartupElem = m_containerFileDocument.createElement(Constants.IS_SPACE_LOAD_ON_STARTUP);
        Text isLoadOnStartupText = m_containerFileDocument.createTextNode(String.valueOf(spaceAttr.isLoadOnStartup()));

        jspaceElement.appendChild(isLoadOnStartupElem)
                .appendChild(isLoadOnStartupText);

        // Save state of container XML files
        saveState();
    }

    // return the node value
    private String getNodeValueIfExists(Element parentNode, String nodeName) {
        String value = null;
        NodeList nl = parentNode.getElementsByTagName(nodeName);

        if (nl.getLength() > 0) {
            Node child = nl.item(0).getFirstChild();
            if (child != null)
                value = child.getNodeValue().trim();
        }

        return value;
    }

    /**
     * Load container XML tree from container XML file.
     *
     * @throws RemoteException Rmi remote exception.
     * @throws CreateException During create space in container.
     */
    private void loadSpacesFromXMLFile(DocumentBuilder builder) throws Exception {
        JSpaceAttributes spaceAttr = null;
        String spaceSchemaName = null;
        String customSchemaProperties = null;
        boolean isLoadOnStartup = false;
        FileInputStream is = new FileInputStream(m_ContainerFile);
        /**
         * Parse the content of the given InputStream as an XML document and
         * return a new DOM Document object.
         */
        m_containerFileDocument = builder.parse(is);

        // Get container tag
        m_rootContainerElement = m_containerFileDocument.getDocumentElement();

        /*************************************************************************
         * Reading <code>SPACE_TAG</code> *
         ************************************************************************/
        // remove white space between tags
        JSpaceUtilities.normalize(m_rootContainerElement);

        // Get root space tag <code>SPACE_TAG</code>
        m_rootSpaceElement = (Element) m_rootContainerElement.getFirstChild();
        Element spaceElement = (Element) m_rootSpaceElement.getFirstChild();

        // Reading space attributes
        while (spaceElement != null) {
            spaceAttr = null;
            spaceSchemaName = null;
            customSchemaProperties = null;
            String spaceName = spaceElement.getNodeName();
            /**
             * Check if this space contain attributes, if not contains so will be
             * created only Space-name node without attributes
             */
            if (spaceElement.getFirstChild() != null) {
                // get the space schema template if exist
                spaceSchemaName = getNodeValueIfExists(spaceElement, SCHEMA_ELEMENT);
                String isLoadOnStartupStr = getNodeValueIfExists(spaceElement,
                        Constants.IS_SPACE_LOAD_ON_STARTUP);
                // try to get property as system var
                isLoadOnStartupStr = JProperties.getPropertyFromSystem(isLoadOnStartupStr,
                        Boolean.FALSE.toString());

                isLoadOnStartup = Boolean.valueOf(isLoadOnStartupStr)
                        .booleanValue();
                // get the schema properties file name if exist
                customSchemaProperties = getNodeValueIfExists(spaceElement,
                        SpaceURL.PROPERTIES_FILE_NAME);

                // if loaded from SpaceURL and this is no required space name and
                // its loadOnStartup flag is false
                // in such case don't load space, therefore that will prevent
                // loading spaces
                // with loadOnStartup flag is false if there is no space to load
                if ((_spaceName != null
                        && !_spaceName.equals(spaceName) && !isLoadOnStartup)) {
                    // Goes to next space attributes
                    spaceElement = (Element) spaceElement.getNextSibling();
                    continue;
                }

                spaceAttr = new JSpaceAttributes(spaceSchemaName, isLoadOnStartup);

                // we check if each space section, contains a <properties> name for
                // the Properties
                // file name we will use for overwriting the space default schema
                // values.
                if (customSchemaProperties != null) {
                    spaceAttr.setCustomProperties(ResourceLoader.findCustomPropertiesObj(customSchemaProperties));

                    if (spaceAttr.getCustomProperties() != null)
                        spaceAttr.getCustomProperties()
                                .setProperty(SpaceURL.PROPERTIES_FILE_NAME,
                                        customSchemaProperties);// Setting the
                    // properties
                    // file name
                }
            }

            // Get space name of this node

            try {
                if (spaceAttr == null) {
                    spaceAttr = new JSpaceAttributes();
                    // if the container has custom properties (and no space
                    // properties) passed to it, we pass it
                    // forward to the spaces
                    if (_customProperties != null) {
                        spaceAttr.setCustomProperties(_customProperties);
                    }
                }

                loadSpace(spaceName, spaceAttr);
            } catch (Exception ex) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn("Exception occurs during loading "
                            + spaceName + " from xml.", ex);
                }
            }

            // Goes to next space attributes
            spaceElement = (Element) spaceElement.getNextSibling();
        }
        // Registering on Lookup Services all spaces with exists ServiceID
        // Registering all spaces with all discovered lookup services
        JSpaceAttributes spaceAttributes = _containerEntry.getSpaceAttributes();
        boolean isPrivate = spaceAttributes != null && spaceAttributes.isPrivate();
        if (!isPrivate)
            m_LookupMng.register(_containerEntry.getClusteredSpaceProxy(), _containerName);

        // update XML tree after loading all JSpaces from XML file
        // updateContainerXMLTree();
    }

    // Create new cluster policy
    private ClusterPolicy createClusterPolicy(String spaceName, String clusterConfigUrl) throws CreateException {
        ClusterPolicy clusterPolicy = null;

        try {
            // starting to create cluster policy
            ClusterXML clusXml = new ClusterXML(_url,
                    clusterConfigUrl,
                    spaceName);
            clusterConfigOutput = clusXml.getClusterConfigDebugOutput();

            if (clusterConfigOutput != null && _logger.isDebugEnabled()) {
                _logger.debug("Cluster Config for space <" + spaceName + " > \n"
                        + clusterConfigOutput.toString());
            }

            clusterPolicy = clusXml.createClusterPolicy(_containerName + ":"
                    + spaceName);
        } catch (Exception ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error(ex.toString(), ex);
            }
            throw new CreateException("Failed to create cluster policy", ex);
        }

        if (_logger.isDebugEnabled()) {
            _logger.debug(clusterPolicy.toString());
        }

        return clusterPolicy;
    }

    /**
     * Load space from certain storage adapter ([space-name].xml file). If storage adapter not
     * determined, this space will be created. Space xml file will not be created if the call comes
     * from SpaceFinder (and specific space was requested to be loaded) and then we use a schema for
     * this space.
     *
     * @param spaceName Name of this space.
     * @param spaceAttr Attributes of this space.
     * @return SpaceInternal J-Space space.
     */
    private SpaceImpl loadSpace(String spaceName, JSpaceAttributes spaceAttr)
            throws ClassNotFoundException,
            IllegalAccessException, InstantiationException, CreateException,
            IOException, ConfigurationException,
            com.gigaspaces.config.ConfigurationException {
        /**
         * Fixed bug 26/06/06 Gershon - http://62.90.11.164:8080/browse/APP-108
         * Validate space name when creating the space through SpaceFinder and not
         * when calling createSpace() where it was been validated before.
         */
        try {
            ValidationUtils.checkServiceNameForValidation(spaceName, "Space name");
        } catch (InvalidServiceNameException exc) {
            throw new CreateException("Invalid space name.",
                    new InvalidServiceNameException(exc.getMessage()));

        }

        // Evgeny 3.11.2005
        // set properties that will be used for db name evaluation from space
        // schema
        System.setProperty(SystemProperties.DB_CONTAINER_NAME, _containerName);
        System.setProperty(SystemProperties.DB_SPACE_NAME, spaceName);

        // create configuration XML file for this space
        createSpaceXML(spaceName, spaceAttr);

        // if spaceNameToLoad != null , meaning SpaceFinder requests to load
        // specific
        // space, we do not care if the load-on-startup flag is false, any way we
        // load it.
        if ((JSpaceUtilities.isEmpty(_spaceName)) // the default needs to be false-not to load-on-startup
                && !Boolean.valueOf(spaceAttr.isLoadOnStartup()).booleanValue())
            return null;

        final String fullSpaceName = JSpaceUtilities.createFullSpaceName(_containerName, spaceName);
        final SpaceConfigReader configReader = new SpaceConfigReader(fullSpaceName);

        // check if this space is clustered, if true, the ClusterPolicy object
        // will be created
        spaceAttr.setClustered(configReader.getBooleanSpaceProperty(
                IS_CLUSTER_SPACE_PROP,
                configReader.getSpaceProperty(IS_CLUSTER_SPACE_PROP, IS_CLUSTER_SPACE_DEFAULT)));

        spaceAttr.setClusterConfigURL(configReader.getSpaceProperty(
                CLUSTER_CONFIG_URL_PROP,
                configReader.getSpaceProperty(Constants.Cluster.CLUSTER_CONFIG_URL_PROP, CLUSTER_CONFIG_URL_DEFAULT)));

        // if we have cluster_schema attribute as part of the passed SpaceURL
        // we use different ClusterXML constructor
        // OR
        // if the spaceAttr.m_isClustered == true
        if (Boolean.valueOf(spaceAttr.isClustered()).booleanValue()
                || (!isFirstSpaceCreated && !JSpaceUtilities.isEmpty(_clusterSchema))) {
            // clustered proxy
            spaceAttr.setClustered(true);

            if (spaceAttr.getClusterConfigURL().trim().equals(""))
                throw new CreateException("Cluster config URL not found in "
                        + spaceName + ".xml file.");

            // get cluster policy
            ClusterPolicy clusterPolicy = createClusterPolicy(spaceName, spaceAttr.getClusterConfigURL());
            spaceAttr.setClusterPolicy(clusterPolicy);
        } // if clustered

        spaceAttr.setPrivate(configReader.getBooleanSpaceProperty(
                LOOKUP_IS_PRIVATE_PROP, LOOKUP_IS_PRIVATE_DEFAULT, false));
        spaceAttr.setPersistent(configReader.getBooleanSpaceProperty(
                PERSISTENT_ENABLED_PROP, PERSISTENT_ENABLED_DEFAULT, false));

        // External Data Source
        spaceAttr.setDataSourceClass(configReader.getSpaceProperty(
                DATA_SOURCE_CLASS_PROP, DATA_SOURCE_CLASS_DEFAULT, false));
        spaceAttr.setDataClass(configReader.getSpaceProperty(
                DATA_CLASS_PROP, DATA_CLASS_DEFAULT, false));
        spaceAttr.setQueryBuilderClass(configReader.getSpaceProperty(
                QUERY_BUILDER_PROP, QUERY_BUILDER_PROP_DEFAULT, false));
        spaceAttr.setDataPropertiesFile(configReader.getSpaceProperty(
                DATA_PROPERTIES, DATA_PROPERTIES_DEFAULT, false));
        spaceAttr.setUsage(configReader.getSpaceProperty(
                USAGE, USAGE_DEFAULT, false));
        spaceAttr.setSupportsInheritanceEnabled(configReader.getBooleanSpaceProperty(
                SUPPORTS_INHERITANCE_PROP, SUPPORTS_INHERITANCE_DEFAULT, false));
        spaceAttr.setSupportsVersionEnabled(configReader.getBooleanSpaceProperty(
                SUPPORTS_VERSION_PROP, SUPPORTS_VERSION_DEFAULT, false));
        spaceAttr.setSupportsPartialUpdateEnabled(configReader.getBooleanSpaceProperty(
                SUPPORTS_PARTIAL_UPDATE_PROP, SUPPORTS_PARTIAL_UPDATE_DEFAULT, false));
        spaceAttr.setSupportsRemoveByIdEnabled(configReader.getBooleanSpaceProperty(
                SUPPORTS_REMOVE_BY_ID_PROP, SUPPORTS_REMOVE_BY_ID_DEFAULT, false));

        spaceAttr.setDataSourceSharedIteratorMode(configReader.getBooleanSpaceProperty(
                DATA_SOURCE_SHARE_ITERATOR_ENABLED_PROP, DATA_SOURCE_SHARE_ITERATOR_ENABLED_DEFAULT, false));

        spaceAttr.setExpirationTimeInterval(configReader.getSpaceProperty(LM_EXPIRATION_TIME_INTERVAL_PROP,
                String.valueOf(LM_EXPIRATION_TIME_INTERVAL_DEFAULT)));
        spaceAttr.setExpirationTimeRecentDeletes(configReader.getSpaceProperty(LM_EXPIRATION_TIME_RECENT_DELETES_PROP,
                String.valueOf(LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT)));
        spaceAttr.setExpirationTimeRecentUpdate(configReader.getSpaceProperty(LM_EXPIRATION_TIME_RECENT_UPDATES_PROP,
                String.valueOf(LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT)));
        spaceAttr.setExpirationStaleReplicas(configReader.getSpaceProperty(LM_EXPIRATION_TIME_STALE_REPLICAS_PROP,
                String.valueOf(LM_EXPIRATION_TIME_STALE_REPLICAS_DEFAULT)));

        String dataSourceShareIteratorTTLDefault = DataAdaptorIterator.getDataSourceShareIteratorTTLDefault(Long.parseLong(spaceAttr.getExpirationTimeRecentDeletes()),
                Long.parseLong(spaceAttr.getExpirationTimeRecentUpdates()));

        spaceAttr.setDataSourceSharedIteratorTimeToLive(configReader.getLongSpaceProperty(
                DATA_SOURCE_SHARE_ITERATOR_TTL_PROP, dataSourceShareIteratorTTLDefault, false));

        // Query Processor
        // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        spaceAttr.setQPAutoCommit(configReader.getBooleanSpaceProperty(
                QueryProcessorInfo.QP_AUTO_COMMIT_PROP, QueryProcessorInfo.QP_AUTO_COMMIT_DEFAULT, false));
        spaceAttr.setQPParserCaseSensetivity(configReader.getBooleanSpaceProperty(
                QueryProcessorInfo.QP_PARSER_CASE_SENSETIVITY_PROP, QueryProcessorInfo.QP_PARSER_CASE_SENSETIVITY_DEFAULT, false));
        spaceAttr.setQPTraceExecTime(configReader.getBooleanSpaceProperty(
                QueryProcessorInfo.QP_TRACE_EXEC_TIME_PROP, QueryProcessorInfo.QP_TRACE_EXEC_TIME_DEFAULT, false));
        spaceAttr.setQpTransactionTimeout(configReader.getIntSpaceProperty(
                QueryProcessorInfo.QP_TRANSACTION_TIMEOUT_PROP, QueryProcessorInfo.QP_TRANSACTION_TIMEOUT_DEFAULT, false));
        spaceAttr.setQpSpaceReadLeaseTime(configReader.getIntSpaceProperty(
                QueryProcessorInfo.QP_SPACE_READ_LEASE_TIME_PROP, QueryProcessorInfo.QP_SPACE_READ_LEASE_TIME_DEFAULT, false));
        spaceAttr.setQpSpaceWriteLeaseTime(configReader.getLongSpaceProperty(
                QueryProcessorInfo.QP_SPACE_WRITE_LEASE_PROP, QueryProcessorInfo.QP_SPACE_WRITE_LEASE_DEFAULT, false));
        spaceAttr.setQpDateFormat(configReader.getSpaceProperty(
                QueryProcessorInfo.QP_DATE_FORMAT_PROP, QueryProcessorInfo.QP_DATE_FORMAT_DEFAULT, false));
        spaceAttr.setQpDateTimeFormat(configReader.getSpaceProperty(
                QueryProcessorInfo.QP_DATETIME_FORMAT_PROP, QueryProcessorInfo.QP_DATETIME_FORMAT_DEFAULT, false));
        spaceAttr.setQpTimeFormat(configReader.getSpaceProperty(
                QueryProcessorInfo.QP_TIME_FORMAT_PROP, QueryProcessorInfo.QP_TIME_FORMAT_DEFAULT, false));
        // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

        spaceAttr.setMirrorServiceEnabled(configReader.getBooleanSpaceProperty(
                MIRROR_SERVICE_ENABLED_PROP, MIRROR_SERVICE_ENABLED_DEFAULT, false));

        spaceAttr.setMaxClassLoaders(configReader.getIntSpaceProperty(
                REMOTE_CODE_MAX_CLASS_LOADERS, REMOTE_CODE_MAX_CLASS_LOADERS_DEFAULT));

        spaceAttr.setSupportCodeChange(configReader.getBooleanSpaceProperty(
                REMOTE_CODE_SUPPORT_CODE_CHANGE, REMOTE_CODE_SUPPORT_CODE_CHANGE_DEFAULT));

        spaceAttr.setZookeeperConnectionTimeout(configReader.getIntSpaceProperty(
                CONNECTION_TIMEOUT, CONNECTION_TIMEOUT_DEFAULT));

        spaceAttr.setZookeeperSessionTimeout(configReader.getIntSpaceProperty(
                SESSION_TIMEOUT, SESSION_TIMEOUT_DEFAULT));

        spaceAttr.setZookeeperRetryTimeout(configReader.getIntSpaceProperty(
                RETRY_TIMEOUT, RETRY_TIMEOUT_DEFAULT));

        spaceAttr.setZookeeperRetryInterval(configReader.getIntSpaceProperty(
                RETRY_INTERVAL, RETRY_INTERVAL_DEFAULT));

        // clone spaceAttr, for creating Space with converted spaceAttr system
        // properties
        JSpaceAttributes attr = (JSpaceAttributes) spaceAttr.clone();

        // Creating space with certain space-name.
        SpaceImpl spaceImpl = createSpaceImpl(spaceName, attr);
        ContainerEntry containerEntry = new ContainerEntry(spaceImpl, spaceAttr);
        synchronized (_lock) {
            _containerEntry = containerEntry;
        }

        if (isJMXEnabled())
            com.j_spaces.jmx.JMXProvider.registerSpaceMBean(spaceName, spaceImpl);

        // if !spaceAttr.m_isPrivate - the space will registrering with Directory
        // Services
        // if it has a clustered proxy, we register it, otherwise we register a
        // regular space proxy.
        if (!Boolean.valueOf(spaceAttr.isPrivate()).booleanValue())
            m_LookupMng.register(containerEntry.getClusteredSpaceProxy(), _containerName);

        // proxy
        spaceAttr.setProxyConnectionMode(configReader.getSpaceProperty(Constants.SpaceProxy.OldRouter.CONNECTION_MONITOR_FULL, Constants.SpaceProxy.OldRouter.CONNECTION_MONITOR_DEFAULT));
        spaceAttr.setProxyMonitorFrequency(Long.parseLong(configReader.getSpaceProperty(Constants.SpaceProxy.OldRouter.MONITOR_FREQUENCY_FULL, Constants.SpaceProxy.OldRouter.MONITOR_FREQUENCY_DEFAULT)));
        spaceAttr.setProxyDetectorFrequency(Long.parseLong(configReader.getSpaceProperty(Constants.SpaceProxy.OldRouter.DETECTOR_FREQUENCY_FULL, Constants.SpaceProxy.OldRouter.DETECTOR_FREQUENCY_DEFAULT)));
        spaceAttr.setProxyConnectionRetries(Integer.parseInt(configReader.getSpaceProperty(Constants.SpaceProxy.OldRouter.CONNECTION_RETRIES_FULL, Constants.SpaceProxy.OldRouter.CONNECTION_RETRIES_DEFAULT)));

        isFirstSpaceCreated = true;
        return spaceImpl;
    }

    private SpaceImpl createSpaceImpl(String spaceName, JSpaceAttributes attr)
            throws CreateException, IOException, ConfigurationException {
        return new SpaceImpl(spaceName, _containerName, this, _url, attr, _customProperties,
                false, SecurityResolver.isSecurityEnabled(_url), _lifeCycle);
    }

    /**
     * Verify if JMX feature is enabled. Return <code>true<code> in case of
     * <tt>com.gs.jmx.enabled<tt> system property has <code>true<code> value.
     */
    private boolean isJMXEnabled() {
        return Boolean.parseBoolean(System.getProperty(SystemProperties.JMX_ENABLED_PROP,
                SystemProperties.JMX_ENABLED_DEFAULT_VALUE));
    }

    /**
     * Create XML configuration file for this space if not exists. The XML file name will be
     * spaceName.xml, also creates <code>Properties</code> object for this space. First we check if
     * space schema needs to be used. If it is defined (in the container config file [schema] tag),
     * we check if the schema xml file exists, load/parse it and then add to it the space name tag.
     *
     * @param spaceName Space name
     * @param spaceAttr JSpaceAttributes of this space
     */
    private void createSpaceXML(String spaceName, JSpaceAttributes spaceAttr)
            throws com.gigaspaces.config.ConfigurationException {
        try {
            // File selectedSchemaFile = null;
            // String schemaFile;
            InputStream spaceSchemaInputStream = null;
            boolean schemaFileSetAndExists = false;// if schema needs to be used
            String schemaName = spaceAttr.getSchemaName();                                                    // and was successfully located
            String schemaFilePath = null;
            if (schemaName != null) {
                // look for the requested space schema file in the resource bundle,
                // first in disk at
                // <home dir>/config/schemas/[requested_schema_name]-space-schema.xml
                // then in the JSpaces.jar at,
                // config/schemas/[requested_schema_name]-space-schema.xml
                SchemaProperties schemaProperties = ResourceLoader.findSpaceSchema(schemaName);
                spaceSchemaInputStream = schemaProperties.getInputStream();
                schemaFilePath = schemaProperties.getFullPath();

                if (spaceSchemaInputStream != null)
                    schemaFileSetAndExists = true;
                else {
                    if (_logger.isWarnEnabled()) {
                        _logger.info("The requested space schema <"
                                + schemaName
                                + "> does not exist in the resource bundle");
                    }
                }
            }

            String fullSpaceName =
                    JSpaceUtilities.createFullSpaceName(_containerName, spaceName);


            // create space.xml under config directory
            File spaceFile = new File(m_configDirectory + File.separator
                    + spaceName + ".xml");

            // if spaceName.xml already exists load only Properties object
            if (spaceFile.exists()) {
                if (schemaFileSetAndExists) {
                    // set <code>Properties</code> object of created space
                    // using the requested space schema file as a template for this
                    // space
                    // and just adding (in memory only) the tag of the space name as
                    // the parent of the schema.
                    // e.g. to be
                    // [JavaSpaces]
                    // [space-config]
                    JProperties.setUrlWithSchema(fullSpaceName, spaceAttr.getCustomProperties(), spaceSchemaInputStream);
                    // JProperties.setDefaultSpaceProperties( spaceSchemaInputStream
                    // );
                    Logger logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONFIG);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Loaded the requested space schema < "
                                + schemaName
                                + " > to be used for the < " + spaceName
                                + " > space configuration.");
                    }
                } else {
                    JProperties.setUrlWithoutSchema(fullSpaceName, spaceAttr.getCustomProperties(), spaceFile.getPath());
                }
            }
            // if the space config xml files does not exist
            // e.g when using schema or dynamic clustering....
            else {
                // if NO schema should be used AND the space config node exist in
                // the container config xml
                // BACKWARDS SUPPORT
                if (m_spaceConfigNode != null && !schemaFileSetAndExists) {
                    // if SPACE_CONFIG tag exists in container XML file, the XML file
                    // of space will created otherwise not.
                    // create <spaceName> tag and append SPACE_CONFIG tag
                    Node spaceNode = m_containerConfigFileDocument.createElement(spaceName);

                    JSpaceAttributes attr = (JSpaceAttributes) spaceAttr.clone();
                    Node spaceConfigNode = m_spaceConfigNode.cloneNode(true);
                    NodeList nList = spaceConfigNode.getChildNodes();
                    boolean perstNotExist = true;

                    for (int i = 0; i < nList.getLength() && perstNotExist; i++) {
                        perstNotExist = nList.item(i)
                                .getNodeName()
                                .equals("persistent") ? false : true;
                    }

                    if (perstNotExist && attr.isPersistent()) {
                        Node persist = m_containerConfigFileDocument.createElement("persistent");
                        Node perstEnabledClassNode = m_containerConfigFileDocument.createElement(Constants.StorageAdapter.PERSISTENT_ENABLED);
                        perstEnabledClassNode.appendChild(m_containerConfigFileDocument.createTextNode(Boolean.FALSE.toString()));

                        persist.appendChild(perstEnabledClassNode);
                        spaceConfigNode.appendChild(persist);
                    }

                    spaceNode.appendChild(spaceConfigNode);

                    // create spaceName.xml file and flush entire Node to this file
                    PrintStream propStream = new PrintStream(new FileOutputStream(spaceFile.getPath()));
                    JSpaceUtilities.domWriter(spaceNode, propStream, " ");
                    propStream.flush();
                    propStream.close();

                    // URL to space-name.xml file
                    String spaceFileURL = spaceFile.getPath();

                    // update space config only if, spaceAttr not default
                    if (spaceAttr != null) {
                        // change slash for platform capability
                        if (spaceAttr.getClusterConfigURL() != null
                                && !spaceAttr.getClusterConfigURL()
                                .equals(CLUSTER_CONFIG_URL_DEFAULT))
                            spaceAttr.setClusterConfigURL(spaceAttr.getClusterConfigURL()
                                    .replace('\\', '/'));

                        // create space XML file
                        SpaceImpl.setConfig(spaceName, _containerName, spaceAttr, spaceFileURL);
                    }

                    // set <code>Properties</code> object of created space
                    JProperties.setUrlWithoutSchema(fullSpaceName, spaceAttr.getCustomProperties(), spaceFileURL);

                    if (_logger.isInfoEnabled()) {
                        _logger.info("XML configuration file " + spaceName
                                + ".xml created successfully");
                    }
                }
                /**
                 * if the space config xml file does not exist AND the selected
                 * space schema xml file do exist, we do not attempt to create a new
                 * space xml file but to use the template space schema requested by
                 * the client. We load and parse the space schema xml file and just
                 * add to it the space name tag.
                 */
                else if (schemaFileSetAndExists) {
                    // set <code>Properties</code> object of created space
                    JProperties.setUrlWithSchema(fullSpaceName, spaceAttr.getCustomProperties(), spaceSchemaInputStream);
                    Properties spaceProps =
                            JProperties.getSpaceProperties(fullSpaceName);

                    // if space was started from SpaceURL and clusterSchema name
                    // is not null so update isClustered flag
                    if (_url != null && _clusterSchema != null
                            && !isFirstSpaceCreated)
                        spaceProps.setProperty(fullSpaceName + '.' + SPACE_CONFIG
                                        + '.'
                                        + IS_CLUSTER_SPACE_PROP,
                                Boolean.TRUE.toString());

                    Logger logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONFIG);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Loaded the requested space schema < "
                                + spaceAttr.getSchemaName()
                                + " > to be used for the < " + spaceName
                                + " > space configuration.");
                    }
                } else
                    // set dummy properties
                    JProperties.setSpaceProperties(fullSpaceName, new Properties());
            } // if space.xml not exists

            if (schemaFilePath != null) {
                Properties spaceProperties = JProperties.getSpaceProperties(fullSpaceName);
                spaceProperties.put(Constants.Schemas.SCHEMA_FILE_PATH, schemaFilePath);
                JProperties.setSpaceProperties(fullSpaceName, spaceProperties);
            }

            // retriveSpaceResourceFiles(spaceName);
            putDCacheProperites(spaceName);
        } catch (com.gigaspaces.config.ConfigurationException sce) {
            throw sce;
        } catch (Exception ex) {
            if (_logger.isWarnEnabled()) {
                _logger.warn(ex.toString()
                        + " exception occurred during < " + spaceName
                        + " > space configuration setup.", ex);
            }

        }
    }

    private void putDCacheProperites(String spaceName) {
        String fullSpaceName =
                JSpaceUtilities.createFullSpaceName(_containerName, spaceName);

        String dCacheConfigName = JProperties.getSpaceProperty(fullSpaceName,
                CONFIG_NAME_PROP,
                DCACHE_CONFIG_NAME_DEFAULT);
        Properties spProp = JProperties.getSpaceProperties(fullSpaceName);
        if (spProp != null)
            spProp.setProperty(Constants.SPACE_CONFIG_PREFIX + CONFIG_NAME_PROP,
                    dCacheConfigName);
    }

    /**
     * Every change in container the container xml file will be updated using this method. On every
     * update current file will be renamed to <%filename%>.old and the new one will be
     * <%filename%>.xml
     *
     * @Exception FileNotFoundException - if the file exists but is a directory rather than a
     * regular file, does not exist but cannot be created, or cannot be opened for any other
     * reason.
     */
    synchronized private void saveState() {
        try {
            /**
             * Renaming xxx.xml file to xxx.old file. If xxx.old file exists, the
             * xxx.old file will be deleted for successful renaming.
             */
            File oldFile = new File(m_FileNameWithoutExtention + ".old");
            if (oldFile.exists())
                oldFile.delete();
            else if (m_ContainerFile != null)
                m_ContainerFile.renameTo(oldFile);

            // check if working with container xml file
            if (m_rootContainerElement != null) {
                // creating new xxx.xml file
                PrintStream attrStream = new PrintStream(new FileOutputStream(m_FileNameWithoutExtention
                        + ".xml"));
                JSpaceUtilities.domWriter(m_rootContainerElement, attrStream, "");
                attrStream.close();
            }

            if (_logger.isDebugEnabled()) {
                _logger.debug("The " + m_ContainerFile + " XML file for "
                        + _containerName + " container has been updated");
            }
        } catch (FileNotFoundException ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error("FileNotFoundException: ", ex);
            }
        } /* catch */
    }

    // get container configuration depends XML configuration file
    public ContainerConfig getConfig() throws RemoteException {
        if (m_containerConfig == null) {
            // create structure object of container
            ContainerConfig containerConfig = new ContainerConfig();
            Map<String, SpaceConfig> spaceSchemasMap = createSpaceSchemasMap();

            // m_containerConfig.defautSpaceConfig = createDefaultSpaceConfig();
            containerConfig.setSpaceSchemasMap(spaceSchemasMap);

            /** ** BUILD CONTAINER CONFIGURATION <containerName>-config.xml file *** */
            // m_containerConfig.updateModeEnabled = new
            // File(JProperties.getURL()).canRead();
            containerConfig.containerName = _containerName;
            containerConfig.homeDir = SystemLocations.singleton().home().toString();
            containerConfig.containerHostName = _hostname;

            containerConfig.setJndiEnabled(
                    Boolean.valueOf(JProperties.getContainerProperty(_containerName, LOOKUP_JNDI_ENABLED_PROP,
                            LOOKUP_JNDI_ENABLED_DEFAULT)).booleanValue());

            containerConfig.setJiniLusEnabled(
                    Boolean.valueOf(JProperties.getContainerProperty(_containerName, LOOKUP_ENABLED_PROP,
                            LOOKUP_ENABLED_DEFAULT)).booleanValue());

            containerConfig.unicastEnabled = Boolean.valueOf(
                    JProperties.getContainerProperty(_containerName, LOOKUP_UNICAST_ENABLED_PROP,
                            LOOKUP_UNICAST_ENABLED_DEFAULT))
                    .booleanValue();
            containerConfig.unicastURL = JProperties.getContainerProperty(
                    _containerName, LOOKUP_UNICAST_URL_PROP,
                    LOOKUP_UNICAST_URL_DEFAULT,
                    true);

            containerConfig.jndiUrl = JProperties.getContainerProperty(_containerName, LOOKUP_JNDI_URL_PROP,
                    null,
                    false);
            if(SystemInfo.singleton().network().isPublicHostConfigured()){
               //ugly ugly. build the new public jmx url
                JmxUrlData jmxUrlData = retrieveUrlData(containerConfig.jndiUrl);
                int  port = jmxUrlData.getPort();
                containerConfig.setJndiPublicURL( SystemInfo.singleton().network().getPublicHostId() + port);
            }


            containerConfig.lookupGroups = JProperties.getContainerProperty(_containerName,
                    LOOKUP_GROUP_PROP,
                    null,
                    true);

            SpaceConfig defaultSpaceConfig = containerConfig.getSpaceSchemasMap().get(DEFAULT_SCHEMA);

            // JMS settings
            containerConfig.jmsEnabled = Boolean.valueOf(JProperties.getContainerProperty(
                    _containerName, LOOKUP_JMS_ENABLED_PROP, LOOKUP_JMS_ENABLED_DEFAULT)).booleanValue();

            containerConfig.jmsInternalJndiEnabled = Boolean.valueOf(JProperties.getContainerProperty(
                    _containerName, LOOKUP_JMS_INTERNAL_ENABLED_PROP, LOOKUP_JMS_INTERNAL_ENABLED_DEFAULT)).booleanValue();

            containerConfig.jmsExtJndiEnabled = Boolean.valueOf(JProperties.getContainerProperty(
                    _containerName, LOOKUP_JMS_EXT_ENABLED_PROP, LOOKUP_JMS_EXT_ENABLED_DEFAULT)).booleanValue();

            containerConfig.setSchemaName(_schemaName);

            containerConfig.setShutdownHook(Boolean.valueOf(JProperties.getContainerProperty(
                    _containerName, CONTAINER_SHUTDOWN_HOOK_PROP, CONTAINER_SHUTDOWN_HOOK_PROP_DEFAULT)).booleanValue());

            containerConfig.setJndiEnabled(Boolean.valueOf(JProperties.getContainerProperty(
                    _containerName, LOOKUP_JNDI_ENABLED_PROP, LOOKUP_JNDI_ENABLED_DEFAULT)).booleanValue());

            containerConfig.setJiniLusEnabled(Boolean.valueOf(JProperties.getContainerProperty(
                    _containerName, LOOKUP_ENABLED_PROP, LOOKUP_ENABLED_DEFAULT)).booleanValue());

            containerConfig.setStartEmbeddedJiniLus(Boolean.valueOf(JProperties.getContainerProperty(
                    _containerName, START_EMBEDDED_LOOKUP_PROP, START_EMBEDDED_LOOKUP_DEFAULT)).booleanValue());

            containerConfig.setStartEmbeddedJiniMahalo(Boolean.valueOf(JProperties.getContainerProperty(
                    _containerName, CONTAINER_EMBEDDED_MAHALO_ENABLED_PROP, CONTAINER_EMBEDDED_MAHALO_ENABLED_DEFAULT)).booleanValue());
            containerConfig.setJMXEnabled(isJMXEnabled());

            m_containerConfig = containerConfig;
        }

        return m_containerConfig;
    } // getConfig()

    /**
     * Create and return map of SpaceConfig instances. Key in this map is schema name, value is
     * appropriate SpaceConfig instance. At the beginning map is filled with all schema files from
     * resource bundle, after this all still not loaded schemas are loaded from folder
     * [GIGASPACES_HOME]\config\schemas on disc.
     *
     * @return map of available SpaceConfig instances
     */
    private Map<String, SpaceConfig> createSpaceSchemasMap() {
        if (_allSpaceSchemasMap == null) {

            final HashMap<String, SpaceConfig> allSpaceSchemasMap = new HashMap<String, SpaceConfig>();
            try {

                // get all predefined schemas from resource bundle
                for (String schema : getAllSchemas()) {
                    SchemaProperties schemaProperties = ResourceLoader.findSpaceSchema(schema);
                    InputStream predefinedSchemaIs = schemaProperties.getInputStream();
                    Properties props = JProperties.convertXML(predefinedSchemaIs);
                    SpaceConfig spaceConfig = null;
                    try {
                        spaceConfig = SpaceConfigFactory.createSpaceConfig(props,
                                null,
                                _containerName,
                                true, schemaProperties.getFullPath());
                    } catch (SpaceConfigurationException exc) {
                        if (_logger.isErrorEnabled()) {
                            _logger.error(
                                    "An exception occurred while loading space configuration for container: "
                                            + _containerName,
                                    exc);
                        }

                    }
                    if (spaceConfig != null)
                        allSpaceSchemasMap.put(spaceConfig.getSchemaName(), spaceConfig);
                }

                // get schema folder path
                String schemasDirPath = SystemLocations.singleton().config(SCHEMAS_FOLDER).toString();

                File schemasDir = new File(schemasDirPath);
                String[] allSchemaFiles = schemasDir.list();

                if (allSchemaFiles != null) {
                    int filesNum = allSchemaFiles.length;

                    for (int i = 0; i < filesNum; i++) {
                        String fileName = allSchemaFiles[i];
                        if (fileName.endsWith(SPACE_SCHEMA_FILE_SUFFIX)) {
                            // get schema name from schema file name
                            String schemaName = fileName.substring(0,
                                    fileName.indexOf(SPACE_SCHEMA_FILE_SUFFIX));
                            if (!allSpaceSchemasMap.containsKey(schemaName)) {
                                String schemaFilePath = schemasDirPath + File.separator + fileName;
                                String fullSpaceSchemaLocation =
                                        "[" + SchemaProperties.getHostName() + "] " + schemaFilePath;
                                // create file
                                File schemaFile = new File(schemaFilePath);
                                FileInputStream fis = new FileInputStream(schemaFile);
                                Properties props = JProperties.convertXML(fis,
                                        true,
                                        _customProperties);// we
                                // pass
                                // customProperties
                                SpaceConfig spaceConfig = null;
                                try {
                                    // get instance of SpaceConfig
                                    spaceConfig = SpaceConfigFactory.createSpaceConfig(props,
                                            null,
                                            _containerName,
                                            true, fullSpaceSchemaLocation);
                                } catch (SpaceConfigurationException exc) {
                                    if (_logger.isErrorEnabled()) {
                                        _logger.error(
                                                "An exception occurred while loading space configuration for container: "
                                                        + _containerName,
                                                exc);
                                    }

                                }
                                // set right schema name, fix bug if schema file was
                                // created manually and its name is not compatible with
                                // name defined in <name> tag
                                if (spaceConfig != null) {
                                    spaceConfig.setSchemaName(schemaName);
                                    allSpaceSchemasMap.put(schemaName, spaceConfig);
                                }
                            }
                        }
                    }// end of for()
                } // end of if ( allSchemaFiles != null )

            } catch (Exception ex) {
                if (_logger.isErrorEnabled()) {
                    _logger.error(ex.toString(), ex);
                }
            }
            _allSpaceSchemasMap = allSpaceSchemasMap;
        }
        return _allSpaceSchemasMap;
    }


    private static String[] getAllSchemas() {
        return new String[] {
                Constants.Schemas.DEFAULT_SCHEMA, Constants.Schemas.JAVASPACE_SCHEMA,
                Constants.Schemas.CACHE_SCHEMA,
                Constants.Schemas.PERSISTENT_SCHEMA,
                Constants.Schemas.MIRROR_SCHEMA};
    }

    // set new container configuration and make update of XML file
    synchronized public void setConfig(ContainerConfig config)
            throws RemoteException {
        try {
            String containerConfigURL = JProperties.getURL();
            // the url can be null, if schema is used and therefore fetched from
            // resource bundle
            // and not necessarily from the disk, there for we write to disk the
            // schema.
            if (containerConfigURL == null) {
                String schemasFolderPath = Environment.createSchemasFolderIfNotExists();
                containerConfigURL = schemasFolderPath + File.separator
                        + getSchemaName() + CONTAINER_SCHEMA_FILE_SUFFIX;

                File schemaConfigFile = new File(containerConfigURL);
                // check if schema file exists under schemas folder,
                // if not create this one
                if (!schemaConfigFile.exists()) {
                    ContainerConfigFactory.createContainerSchemaFile(getSchemaName(),
                            containerConfigURL);
                }
            }

            if (new File(containerConfigURL).exists()) {
                ContainerConfigFactory.updateFile(config,
                        containerConfigURL,
                        getSchemaName());
                String schemaName = config.getSchemaName();
                InputStream schemaInputStream = ResourceLoader.findContainerSchema(schemaName);

                JProperties.setInputStream(_containerName, schemaInputStream, null);
            } // if no containerUpdate
            else if (_logger.isWarnEnabled()) {
                _logger.warn("Remote Access Denied. Unable to update the configuration file: "
                        + containerConfigURL + " via FTP/HTTP protocol.");
            }

            //set configuration equals null in order to take all parameters from JProiperties()
            //in method getConfig() next time
            m_containerConfig = null;
        } catch (Exception ex) {
            throw new RemoteException(ex.toString(), ex);
        } // catch
    }

    /**
     * @return The requested schema name, to be used as the space/container configuration template.
     */
    public String getSchemaName() {
        return _schemaName;
    }

    private class JmxUrlData{
        private String _jmxUrl;
        private int _port;
        private JmxUrlData(String url, int port){
            _jmxUrl=url;
            _port=port;
        }
        private int getPort(){
            return  _port;
        }
        private String getUrl(){
            return _jmxUrl;
        }
    }

    private JmxUrlData retrieveUrlData(String hostAndPort){
        if ( hostAndPort != null && hostAndPort.indexOf(':') != -1){
            int port=0;
            int portSeparator = hostAndPort.lastIndexOf(COLON);
            String hostName = hostAndPort.substring(0, portSeparator);
            int jndiPort = Integer.parseInt(hostAndPort.substring(portSeparator + 1));
            return new JmxUrlData(hostName,jndiPort);
        }
        else{
            return null;
        }
    }

    /**
     * Run RMIRegistry Parse valid JNDI port. If JNDI port really valid, trying to startRMIRegistry
     * on this port. Any exception during start is ignored.
     *
     * If the XPATH com.j_spaces.core.container.directory_services.jndi.enabled=false them we will
     * not start RMIReg
     *
     * @param hostAndPort retrieved from the container configuration. Default value is:
     *                    localhost:10098
     */
    private String loadRMIRegistry(String hostAndPort) throws Exception {
        String jndiFlag = _url.getCustomProperties().getProperty(XPathProperties.CONTAINER_JNDI_ENABLED);
        if (!JSpaceUtilities.isEmpty(jndiFlag) && jndiFlag.equalsIgnoreCase("false")) {
            if (_rmiHostAndPort == null)
                return hostAndPort;

            return _rmiHostAndPort;
        }

        if (_rmiHostAndPort != null)
            return _rmiHostAndPort;

        _rmiHostAndPort = hostAndPort;
        JmxUrlData jmxData = retrieveUrlData(hostAndPort);
        if (jmxData != null) {
            try {
                int jndiPort = 0;
                // ignore jndi host
                String hostName = jmxData.getUrl();
                jndiPort = jmxData.getPort();

                String registryPortStr = System.getProperty(CommonSystemProperties.REGISTRY_PORT);
                boolean isStartedRMIRegistry = false;
                boolean isPortOccupiedByAnotherAppl = false;

                //check if jndi port was passed as system variable when container started from
                //gsInstance script or from GSC
                if (registryPortStr != null) {
                    jndiPort = Integer.parseInt(registryPortStr);
                    try {
                        //try to create RMI registry, if succeed that means port was not busy
                        //and we have now RMI Registry running on that port
                        LocateRegistry.createRegistry(jndiPort);
                        isStartedRMIRegistry = true;
                    } catch (RemoteException re) {
                        //check now if port is occupied by any another application
                        //that is not RMI Registry
                        try {
                            Registry registry = LocateRegistry.getRegistry(jndiPort);
                            registry.list();
                        } catch (RemoteException exc) {
                            isPortOccupiedByAnotherAppl = true;
                            if (_logger.isInfoEnabled()) {
                                _logger.info("Failed to run RMI registry on " + jndiPort +
                                        ".Port is busy.");
                            }
                        }

                        isStartedRMIRegistry = false;
                    }
                }

                //if jndi port was not passed as system variable and did not succeed to run RMIRegistry
                //on specific port OR
                //any another application is running on that port
                if ((!isStartedRMIRegistry && registryPortStr == null) ||
                        isPortOccupiedByAnotherAppl) {
                    //check if port is not busy
                    while (IOUtils.isPortBusy(jndiPort, null)) {   //find another free port
                        jndiPort = IOUtils.getAnonymousPort();
                    }

                    LocateRegistry.createRegistry(jndiPort);
                    System.setProperty(CommonSystemProperties.REGISTRY_PORT, String.valueOf(jndiPort));
                }

                _rmiHostAndPort = hostName + ":" + jndiPort;

                if (!isStartedRMIRegistry && registryPortStr == null) {


                    String jmxServiceURL = JMXUtilities.createJMXUrl(_rmiHostAndPort);
                    System.setProperty(CommonSystemProperties.JMX_SERVICE_URL, jmxServiceURL);
                    if (_logger.isDebugEnabled())
                        _logger.debug("Created RMIRegistry on: < " + _rmiHostAndPort + " >");
                } else {
                    if (_logger.isDebugEnabled())
                        _logger.debug("Using an already running RMIRegistry on: < " + _rmiHostAndPort + " >");
                }

            } catch (RemoteException ex) {
                if (_logger.isDebugEnabled()) {
                    _logger.debug("RMIRegistry did not start on: < " + _rmiHostAndPort + " >. It might be already running.");
                }
                // in case the rmi-registry is already running, we do nothing
            }
        }// if...

        return _rmiHostAndPort;
    }


    /**
     * Strong references to transient reggie, this prevents the transient service from getting
     * garbage collected.
     */
    private NonActivatableServiceDescriptor.Created _reggieCreatedRef = null;

    private static NonActivatableServiceDescriptor.Created createLookupServiceIfNeeded(String containerName) {
        if (System.getProperty(SystemProperties.LUS_FILE_PATH) != null)
            return null;
        // starting embedded Jini Lookup Service if the [start-embedded-lus] set to true in the container schema
        boolean isEmbeddedReggieEnabled = Boolean.parseBoolean(JProperties.getContainerProperty(containerName,
                START_EMBEDDED_LOOKUP_PROP, START_EMBEDDED_LOOKUP_DEFAULT, true));

        if (!isEmbeddedReggieEnabled)
            return null;

        if (RegistrarFactory.isActive()) {
            _logger.info("Creation of an embedded Lookup Service was skipped - another Lookup Service is already active in this process");
            return null;
        }

        try {
            Object instance = RegistrarFactory.createRegistrar();
            Object proxy = ((ServiceProxyAccessor) instance).getServiceProxy();
            NonActivatableServiceDescriptor.Created result = new NonActivatableServiceDescriptor.Created(instance, proxy);

            // enabling the Jini Unicast Discovery even if its disabled (by
            // default) in the container configuration.
            JProperties.getContainerProperties(containerName).setProperty(LOOKUP_UNICAST_ENABLED_PROP, String.valueOf(true));
            if (_logger.isDebugEnabled()) {
                _logger.debug("Enabled the Jini Unicast Discovery on: < " + JProperties.getContainerProperty(
                        containerName, LOOKUP_UNICAST_URL_PROP, LOOKUP_UNICAST_URL_DEFAULT, true) + " >  ");
            }
            return result;
        } catch (Throwable ex) {
            String error = ex.getMessage();
            if (error == null) {
                Throwable throwable = ex.getCause();
                if (throwable != null)
                    error = throwable.getMessage();
            }
            if (_logger.isWarnEnabled()) {
                _logger.warn("Failed to start an embedded Jini Lookup Service - " + error, ex);
            }
            return null;
        }
    }


    /**
     * Strong references to transient mahalo, this prevents the transient service from getting
     * garbage collected.
     */
    private NonActivatableServiceDescriptor.Created _mahaloCreatedRef = null;

    /**
     * Run an embedded Mahalo Jini Transaction Manager.
     */
    private static NonActivatableServiceDescriptor.Created createMahaloIfNeeded(String containerName) {
        // starting embedded Mahalo TX service if the [start-embedded-mahalo] set to true in the container schema
        boolean isEmbeddedMahaloEnabled = Boolean.parseBoolean(JProperties.getContainerProperty(
                containerName, CONTAINER_EMBEDDED_MAHALO_ENABLED_PROP,
                CONTAINER_EMBEDDED_MAHALO_ENABLED_DEFAULT, true));
        if (!isEmbeddedMahaloEnabled)
            return null;

        try {
            Object instance = MahaloFactory.createMahalo();
            Object proxy = ((TxnManager) instance).getLocalProxy();
            return new NonActivatableServiceDescriptor.Created(instance, proxy);
        } catch (Throwable ex) {
            String error = ex.getMessage();
            if (error == null) {
                Throwable throwable = ex.getCause();
                if (throwable != null)
                    error = throwable.getMessage();
            }
            if (_logger.isWarnEnabled()) {
                _logger.warn("Failed to start an embedded Mahalo Jini Transaction Manager - " + error, ex);
            }
            return null;
        }
    }

    private static String setLookupgroups(String value) {
        if (!JSpaceUtilities.isEmpty(value, true)) {
            return SystemInfo.singleton().lookup().setGroups(value);
        }

        String oldValue = SystemInfo.singleton().lookup().groups();
        if (oldValue == null)
            SystemInfo.singleton().lookup().setGroups(SystemInfo.singleton().lookup().defaultGroups());
        return oldValue;
    }

    private static String setLookupLocators(String value) {
        if (!JSpaceUtilities.isEmpty(value, true)) {
            return SystemInfo.singleton().lookup().setLocators(value);
        }

        String oldValue = SystemInfo.singleton().lookup().locators();
        if (oldValue == null)
            SystemInfo.singleton().lookup().setLocators(SystemProperties.JINI_LUS_LOCATORS_DEFAULT);
        return oldValue;
    }
}
