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

package com.j_spaces.start;

import com.gigaspaces.internal.utils.ReplaceInFileUtils;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.JSpaceAttributes;
import com.j_spaces.core.JSpaceContainerImpl;
import com.j_spaces.core.NoSuchNameException;
import com.j_spaces.core.client.SpaceFinder;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.service.AbstractService;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.log.JProperties;
import com.sun.jini.start.LifeCycle;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;
import net.jini.config.ConfigurationProvider;
import net.jini.config.NoSuchEntryException;
import net.jini.export.ProxyAccessor;
import net.jini.lookup.JoinManager;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.rmi.RemoteException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
@com.gigaspaces.api.InternalApi
public class JSpaceServiceImpl extends AbstractService implements ProxyAccessor {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ADMIN);

    /**
     * Configuration component name.
     */
    public static final String CONFIG_COMPONENT = "com.gigaspaces.start";

    private Object _spaceProxy;
    private JSpaceAttributes _spaceAttr;
    private JSpaceContainerImpl _spaceContainer;
    /**
     * Published service name.
     */
    private String _spaceName;
    /**
     * space/container schema name to be used as a template for space/container configuration.
     */
    private String _schemaName;
    private String _customPropertiesFileName;
    private String _spaceUrl;

    private String _containerName;
    private String _clusterConfigURL;
    private String _licenseKey;
    private String _homeDirectory;
    private String _initialMemberGroups;

    /**
     * Specifies the jini lookup locators to be used for unicast discovery and registration
     */
    private String _initialLookupLocators;

    /** JNDI host name should be initialize RMI registry through SpaceFinder utility. */
//    private String _jndiHostname; // Currently doesn't in use
    /**
     * The hostname and port via which the client tries to obtain the required classes through HTTP
     * daemon.
     */
    private String _downloadHost;
    private Boolean _startEmbeddedLus;

    /**
     * Constructs a transient/persistent JavaSpace based on a configuration obtained using the
     * provided arguments. If lifeCycle is non-null, then its unregister method is invoked during
     * service shutdown.
     */
    public JSpaceServiceImpl(String[] configArgs, LifeCycle lifeCycle)
            throws Exception {
        super(lifeCycle);
        final Configuration config = ConfigurationProvider.getInstance(
                configArgs, getClass().getClassLoader());

        // Initialize configuration entries and set system properties
        init(config);

        // Initialize cache instance through CacheFinder
        String cacheUrl = System.getProperty(SpaceURL.CACHE_URL_PROP, _spaceUrl);
        if (cacheUrl != null) {
            _spaceProxy = SpaceFinder.find(cacheUrl, null, lifeCycle, null);
            // This service should returns a proxy(serializable) object for this remote
            // object. Not embedded proxy, as SpaceFinder will return by java:// protocol.
            if (_spaceProxy instanceof IJSpace) {
                _spaceProxy = ((IJSpace) _spaceProxy).getDirectProxy().getNonClusteredProxy();
            }
        }
        // TODO Otherwise, initialize space instance through SpaceFinder
        else {
            Properties _customPropsObj = null;
            if (_customPropertiesFileName != null)
                _customPropsObj = ResourceLoader.findCustomPropertiesObj(_customPropertiesFileName);

            _spaceAttr = new JSpaceAttributes( /*(_storageAdapterURL != null ? Constants.StorageAdapter.DEFAULT_STORAGE_ADAPTER_CLASS : null),
                    _storageAdapterURL, */_schemaName, /*_isPrivate.booleanValue(),*/ _clusterConfigURL);

            if (_customPropsObj != null) {
                _customPropsObj.setProperty(SpaceURL.PROPERTIES_FILE_NAME, _customPropertiesFileName);//setting the properties file name
                _spaceAttr.setCustomProperties(_customPropsObj);
            }

            // TODO it's temporary, update the space XML if clustered
            String spaceFileURL = SystemLocations.singleton().config(_spaceName + ".xml").toString();
            if (new File(spaceFileURL).exists()) {
                ReplaceInFileUtils updateFile = new ReplaceInFileUtils(spaceFileURL);
                updateFile.xmlReplace(Constants.Cluster.IS_CLUSTER_SPACE_PROP, _clusterConfigURL != null ? "true" : "false");
                if (_clusterConfigURL != null)
                    updateFile.xmlReplace(Constants.Cluster.CLUSTER_CONFIG_URL_PROP, _clusterConfigURL);
                updateFile.close();
            }

            //TODO DO WE SUPPORT properties ??

            // initialize JSpaceContainer
            _spaceContainer = initContainer(lifeCycle, _containerName, _schemaName, _customPropsObj);

            // Check if specified space exists
            try {
                _spaceProxy = _spaceContainer.getClusteredSpace(_spaceName);
            } catch (NoSuchNameException ex) {
                // Otherwise try to create desirable space
                try {
                    _spaceProxy = _spaceContainer.createSpace(_spaceName, _spaceAttr);
                } catch (Exception ex1) {
                    if (_logger.isErrorEnabled())
                        _logger.error(ex1.toString(), ex1);
                }
            }
        }
    }

    /**
     * Initialize configuration entries.
     */
    private void init(Configuration config) {
        String localhostName = SystemInfo.singleton().network().getHost().getHostAddress();

        try {
            _containerName = getConfigString(config, "containerName", localhostName);
            _spaceName = getConfigString(config, "spaceName", "JavaSpaces");
            _clusterConfigURL = getConfigString(config, "clusterConfigURL", null);
            _licenseKey = getConfigString(config, "licenseKey", null);
            _homeDirectory = getConfigString(config, "homeDirectory", null);
            _initialMemberGroups = getConfigString(config, "initialMemberGroups", null);
            _initialLookupLocators = getConfigString(config, "initialLookupLocators", null);
            _downloadHost = getConfigString(config, "downloadHost", localhostName + ":9010");
            _schemaName = getConfigString(config, "schema", null);
            _customPropertiesFileName = getConfigString(config, "properties", null);
            _spaceUrl = getConfigString(config, "spaceURL", null);
            _startEmbeddedLus = getConfigBoolean(config, "startEmbeddedLus", Boolean.FALSE);
        } catch (ConfigurationException e) {
            if (_logger.isErrorEnabled())
                _logger.error("Error while loading JSpaceService. " + e.toString()
                        + " Please check the Jini configuration files.");
        }
        
		/*if (localhostName != null)
            System.setProperty("com.gs.localhost.name", localhostName);*/
        if (_containerName != null)
            System.setProperty(SystemProperties.DB_CONTAINER_NAME, _containerName);
        if (_licenseKey != null)
            System.setProperty(SystemProperties.LICENSE_KEY, _licenseKey);
        if (_homeDirectory != null)
            System.setProperty(SystemProperties.GS_HOME, _homeDirectory);
        if (_initialMemberGroups != null) {
            //System.setProperty("com.gs.member.groups", _initialMemberGroups);
            SystemInfo.singleton().lookup().setGroups(JSpaceUtilities.removeInvertedCommas(_initialMemberGroups));
        }
        if (_initialLookupLocators != null)
            SystemInfo.singleton().lookup().setLocators(JSpaceUtilities.removeInvertedCommas(_initialLookupLocators));
        if (_downloadHost != null)
            System.setProperty("com.gs.downloadhost", _downloadHost);
        if (_startEmbeddedLus != null)
            System.setProperty(SystemProperties.START_EMBEDDED_LOOKUP, _startEmbeddedLus.toString());
    }

    /**
     * Initialize JProperties class. Using <code>containerName</code> this method If com.gs.home
     * System.property is not defined the current directory will used to find
     * [containerName]-config.xml file and initialize <code>JProperties</code> class.
     *
     * If schema name is passed we look for it in resource bundle (disk, jar according to the
     * classpath) and load it as the space/container configuration template. If spaceName is passed,
     * we load only this space, even if more spaces are defined part of the container persistency
     * file, [container-name].xml, if it exists.
     *
     * @param containerName        Container name for initialize [containerName]-config.xml
     * @param schemaName           The container schema name to be used to load this container
     * @param spaceName            The space name to be loaded
     * @param su                   SpaceFinder instance, passed later to the JSpaceContainerImpl
     * @param schemaPropertiesName if passed we use it to overwrite the space/container
     *                             JProperties.
     * @throws Exception Failed to initialize <code>JProperties</code> class.
     **/
    private JSpaceContainerImpl initContainer(LifeCycle lifeCycle,
                                              String containerName, String schemaName,
                                              Properties schemaProperties)
            throws Exception {
// TODO later call SpaceFinder with url to avoid duplicated code

        //TODO replace the host/container name vars in the selected schema file

        // check if the config file exists and readable, otherwise thrown exception
        //Used for backward compatability.
        String contConfFile = SystemLocations.singleton().config(containerName + "-config.xml").toString();
        File configFile = new File(contConfFile);

        //Go and find the container schema file if schema is requested AND
        //no regular <container name>-config.xml exists on disk. We try to find it in the resource bundles.
        if (!configFile.canRead() && !JSpaceUtilities.isEmpty(schemaName)) {
            if (_logger.isInfoEnabled()) {
                _logger.info("Couldn't find the required " + containerName + "-config.xml file: "
                        + contConfFile + ".\n About to load the default container schema file from the ResourceBundle, to be used for the container configuration.");
            }
            InputStream schemaInputStream = ResourceLoader.findContainerSchema(schemaName);

            JProperties.setInputStream(containerName, schemaInputStream, schemaProperties);
        } else {
            // set container configuration
            JProperties.setURL(containerName, contConfFile);
        }
        JProperties.getContainerProperties(containerName).
                setProperty(Constants.Container.CONTAINER_NAME_PROP, containerName);

        // init and return container instance
        return new JSpaceContainerImpl(lifeCycle, schemaName, null, schemaProperties);
    }

    @Override
    public String getServiceName() {
        return _spaceName;
    }

    // Don't affect because on LUS adv. proxy of SpaceInternal class
    @Override
    public void shutdown() {
    }

    @Override
    public JoinManager getJoinManager() {
        return null;
    }

    /**
     * Implements ServiceProxyAccessor interface.
     */
    public Object getServiceProxy() throws RemoteException {
        return _spaceProxy;
    }

    /**
     * Implements ProxyAccessor interface.
     */
    public Object getProxy() {
        return _spaceProxy;
    }

    @Override
    public String getContainerName() {
        //return _spaceContainer.getName();
        throw new UnsupportedOperationException();
    }

    private static String getConfigString(Configuration config, String name, String defaultValue)
            throws ConfigurationException {
        return (String) getConfigEntry(config, CONFIG_COMPONENT, name, String.class, defaultValue);
    }

    private static Boolean getConfigBoolean(Configuration config, String name, Boolean defaultValue)
            throws ConfigurationException {
        return (Boolean) getConfigEntry(config, CONFIG_COMPONENT, name, Boolean.class, defaultValue);
    }

    private static Object getConfigEntry(Configuration config, String component, String name, Class<?> type, Object defaultValue)
            throws ConfigurationException {
        try {
            return config.getEntry(component, name, type);
        } catch (NoSuchEntryException e) {
            return defaultValue;
        }
    }
}
