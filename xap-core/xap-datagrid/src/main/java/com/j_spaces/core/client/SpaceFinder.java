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

package com.j_spaces.core.client;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.extension.XapExtensions;
import com.gigaspaces.internal.remoting.routing.clustered.LookupType;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.GSLogConfigLoader;
import com.gigaspaces.security.SecurityException;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.directory.CredentialsProviderHelper;
import com.gigaspaces.security.directory.DefaultCredentialsProvider;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.start.SystemLocations;
import com.j_spaces.core.Constants;
import com.j_spaces.core.JSpaceContainerImpl;
import com.j_spaces.core.NoSuchNameException;
import com.j_spaces.core.service.Service;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.log.JProperties;
import com.sun.jini.proxy.DefaultProxyPivot;
import com.sun.jini.start.LifeCycle;

import java.io.*;
import java.net.MalformedURLException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <pre>
 * This utility class provides accessing to Container proxy
 * or Space proxy ({@link com.j_spaces.core.IJSpace IJSpace}).
 *
 *  The SpaceFinder is designed to provide a unified interface for finding a
 * space/container in each of the mode specified.
 * In addition to the lookup option there is another option of finding the space
 * via its container.
 *  To provide a unified manager of accessing the space in any of the above modes
 * we will use a url based interface which will provide the information of the
 * protocol and the address of the space that should be found.
 *
 * The general format for this URL is as follows:
 *
 * <code>Protocol://[host]:[port]/[container_name]/[space_name]?[query_string]</code>
 *
 * Protocol: [ rmi | jini | java ]
 *           The "java" protocol enables working with an embedded space. When choosing so
 *           one should specify in the query string one of three space operations:
 *
 *           > create - to create a new space
 *            example of "mySpace" space creation:
 *
 *            <code>java://myHost/myContinaer/mySpace?create</code>
 *
 *           > open - To open  an existing space in an embedded mode:
 *            example of opening "mySpace" space:
 *
 *            <code>java://myHost/myContinaer/mySpace?open</code>
 *
 *           > destroy - for destroy  an existing space:
 *            example of destroying "mySpace" space:
 *
 *            <code>java://myHost/myContinaer/mySpace?destroy</code>
 *
 * Host: The host name. Can be '*', when JINI is used as a protocol, the host
 *       value determines whether to use Unicast or Multicast.
 *
 * Port: the registry or lookup port. If the port is  not specified,
 *       the default port 10098 will be used.
 *
 * Container Name: The name of the container, which holds the space.
 *                 If the container name is '*' then the container attribute will be ignored and
 * the
 *                 space will be looked for directly regardless of the container that holds it .
 *
 * Examples of space url's:
 *
 * 1. Looking for a space using JINI Unicast protocol.
 *    <code>jini://mylookuphost/mycontainername/myspace</code>
 *  Or
 *    <code>jini://<tt>*</tt>/<tt>*</tt>/myspace</code>
 *
 * 2. Looking for a space using the JINI multicast protocol.
 *    <code>jini://<tt>*</tt>/containername/myspace</code>
 *  Or
 *    <code>jini://<tt>*</tt>/<tt>*</tt>/myspace</code>
 *
 * 3. looking for a container regardless of the space it contains in any of the above methods:
 *    <code>jini://mylookuphost/mycontainername</code>
 *    <code>jini://<tt>*</tt>/mycontainername</code>
 *
 * <b>For more details, examples and options see {@link com.j_spaces.core.client.SpaceURL
 * SpaceURL}</b>
 * </pre>
 *
 * @author Igor Goldenberg
 * @version 1.0
 * @see com.j_spaces.core.client.SpaceURL
 * @deprecated Use {@link org.openspaces.core.space.UrlSpaceConfigurer} instead.
 */
@Deprecated
@com.gigaspaces.api.InternalApi
public class SpaceFinder {
    private static final Map<SpaceURL, SpaceImpl> _embeddedSpaces = new WeakHashMap<SpaceURL, SpaceImpl>();

    private static final SpaceFinder spaceFinder = new SpaceFinder();

    protected final Logger _logger;

    /**
     * Singleton.
     */
    protected SpaceFinder() {
        GSLogConfigLoader.getLoader();

        //011205 Gershon:
        //check if we use default xml parsers implementations or override it.
        // We do this check here, since the com.gs.use-default-xml-parser sys prop might have been
        //set in the m_customProperties
        JSpaceUtilities.setXMLImplSystemProps();

        this._logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACEFINDER);

        // Check if the RMI GC system properties are been set, if not we set it to the recommended values.
        try {
            if (System.getProperty(SystemProperties.RMI_CLIENT_GC_INTERVAL) == null)
                System.setProperty(SystemProperties.RMI_CLIENT_GC_INTERVAL, SystemProperties.RMI_GC_DEFAULT_INTERVAL);
            if (System.getProperty(SystemProperties.RMI_SERVER_GC_INTERVAL) == null)
                System.setProperty(SystemProperties.RMI_SERVER_GC_INTERVAL, SystemProperties.RMI_GC_DEFAULT_INTERVAL);
        } catch (java.lang.SecurityException e) {
            if (_logger.isWarnEnabled())
                _logger.warn("Failed to set sun.rmi.dgc.xxx system properties. \n", e);
        }
    }

    /**
     * Verifies for validation if the specified <code>url</code> is valid and returns
     * <code>SpaceURL</code> object that contains all information about specified <code>url</code>.
     * This method also support multiple URLs separate by ";"
     *
     * @param url The <code>url</code> to verify.
     * @return Returns <code>SpaceURL</code> object that contains all information about specified
     * <code>url</code>.
     * @throws MalformedURLException Throws exception if specified <code>url</code> is not valid.
     **/
    public static SpaceURL verifyURL(String url)
            throws MalformedURLException {
        return parseURL(url, null);
    }


    /**
     * SpaceFinder.find multiple URL provides ability to define space search order. It is useful
     * when Jini URL for locating services on the network is not available. If the first space URL
     * is unavailable, SpaceFinder will try the next one until a live proxy will be found. If all
     * URLs are unavailable this method throws FinderException
     *
     * @param urls Array of SpaceFinder URLs.
     * @return Returns single found proxy.
     * @throws FinderException Failed to found proxy.All URLs are unavailable.
     **/
    public static Object find(String[] urls)
            throws FinderException {
        return spaceFinder.findService(urls);
    }

    /**
     * <pre>
     * The general format for this URL is as follows:
     * Protocol://[host]:[port]/[container_name]/[space_name]?[query_string]
     * Protocol: [ RMI | JINI | JAVA ]
     * This method also supports multiple URLs separate by ";",
     * e.i. jini://localhost/containerName/SpaceName;jini://'*'/containerName/SpaceName;
     * It is useful when Jini URL for locating services on the network is not available.
     * 	If the first space URL is unavailable, SpaceFinder will try the next one until a live proxy
     * will be found.
     * If all URLs are unavailable this method throws FinderException
     * </pre>
     *
     * @param urls Space URL
     * @return Returns single Space or Container proxy.
     * @throws FinderException During finding space or container.
     **/
    public static Object find(String urls)
            throws FinderException {
        return spaceFinder.findService(urls, null, null, null);
    }

    /**
     * <pre>
     * The general format for this URL is as follows:
     * Protocol://[host]:[port]/[container_name]/[space_name]?[query_string]
     * Protocol: [ RMI | JINI | JAVA ]
     * This method also supports multiple URLs separate by ";",
     * e.i. jini://localhost/containerName/SpaceName;jini://'*'/containerName/SpaceName;
     * It is useful when Jini URL for locating services on the network is not available.
     *    If the first space URL is unavailable, SpaceFinder will try the next one until a live
     * proxy will be found.
     * If all URLs are unavailable this method throws FinderException
     * </pre>
     *
     * @param urls             Space URL
     * @param customProperties Custom Properties object which overwrites the space/container/cluster
     *                         configurations.
     * @return Returns single Space or Container proxy.
     * @throws FinderException During finding space or container.
     **/
    public static Object find(String urls, Properties customProperties)
            throws FinderException {
        return spaceFinder.findService(urls, customProperties, null, null);
    }

    /**
     * Finds or creates an instance of a GigaSpace based on the specified spaceURL.
     *
     * @param urls                The general format for this URL is as follows:
     *                            Protocol://[host]:[port]/[container_name]/[space_name]. Must not
     *                            be null
     * @param customProperties    Optional custom {@link java.util.Properties} object which
     *                            overwrites the space/container/cluster configurations, used when
     *                            creating a new instance
     * @param lifeCycle           Optional reference for hosting environment. This parameter is
     *                            passed as the {@link com.sun.jini.start.LifeCycle} argument to the
     *                            implementation's constructor if the GigaSpaces instance is
     *                            created. If this argument is <code>null</code>, then a default,
     *                            no-op LifeCycle object will be assigned. If the GigaSpaces
     *                            instance already exists, this parameter is ignored.
     * @param credentialsProvider credentials to be used for authentication against a remote secured
     *                            space; may be <code>null</code>.
     * @return The proxy to a newly created GigaSpaces instance or to an instance that matches the
     * specification of the <code>spaceURL</code> parameter.
     * @throws FinderException      If there are exception creating or finding the GigaSpaces
     *                              instance
     * @throws NullPointerException if the spaceURL parameter is null
     */
    public static Object find(String urls, Properties customProperties, LifeCycle lifeCycle,
                              CredentialsProvider credentialsProvider)
            throws FinderException {
        return spaceFinder.findService(urls, customProperties, lifeCycle, credentialsProvider);
    }

    /**
     * <pre>
     * The general format for this URL is as follows:
     * Protocol://[host]:[port]/[container_name]/[space_name]?[query_string]
     * Protocol: [ RMI | JINI | JAVA ]
     * This method also supports multiple URLs separate by ";",
     * e.i. jini://localhost/containerName/SpaceName;jini://'*'/containerName/SpaceName;
     * It is useful when Jini URL for locating services on the network is not available.
     * 	If the first space URL is unavailable, SpaceFinder will try the next one until a live proxy
     * will be found.
     * If all URLs are unavailable this method throws FinderException
     * </pre>
     *
     * @param spaceURL Space URL
     * @return Returns single Space or Container proxy.
     * @throws FinderException During finding space or container.
     **/
    public static Object find(SpaceURL spaceURL)
            throws FinderException {
        return find(spaceURL, null);
    }

    /**
     * <pre>
     * The general format for this URL is as follows:
     * Protocol://[host]:[port]/[container_name]/[space_name]?[query_string]
     * Protocol: [ RMI | JINI | JAVA ]
     * This method also supports multiple URLs separate by ";",
     * e.i. jini://localhost/containerName/SpaceName;jini://'*'/containerName/SpaceName;
     * It is useful when Jini URL for locating services on the network is not available.
     * 	If the first space URL is unavailable, SpaceFinder will try the next one until a live proxy
     * will be found.
     * If all URLs are unavailable this method throws FinderException
     * </pre>
     *
     * @param credentialsProvider credentials to be used for authentication against a remote secured
     *                            space; may be <code>null</code>.
     * @return Returns single Space or Container proxy.
     * @throws FinderException During finding space or container.
     */
    public static Object find(SpaceURL spaceURL, CredentialsProvider credentialsProvider)
            throws FinderException {
        if (spaceURL == null)
            throw new FinderException(new SpaceURLValidationException("Invalid space url - SpaceURL value must be supplied to this method."));
        long timeout = Long.parseLong(spaceURL.getProperty(SpaceURL.TIMEOUT, "-1"));
        return spaceFinder.findService(spaceURL, spaceURL.getCustomProperties(), null /*lifeCycle*/, credentialsProvider, timeout, null /* lookupType */);
    }

    public static Object find(SpaceURL spaceURL, CredentialsProvider credentialsProvider, long timeout,
                              LookupType lookupType)
            throws FinderException {
        if (spaceURL == null)
            throw new FinderException(new SpaceURLValidationException("Invalid space url - SpaceURL value must be supplied to this method."));
        return spaceFinder.findService(spaceURL, spaceURL.getCustomProperties(), null /*lifeCycle*/, credentialsProvider, timeout,
                lookupType);
    }

    public static Object find(SpaceURL[] spaceURLs, CredentialsProvider credentialsProvider)
            throws FinderException {
        if (spaceURLs == null || spaceURLs.length == 0)
            throw new IllegalArgumentException("At least one urls must be passed to space finder");

        return spaceFinder.findService(spaceURLs, spaceURLs[0].getCustomProperties(), null, credentialsProvider);
    }

    private static SpaceURL[] prepareUrl(String[] urls, Properties customProperties)
            throws FinderException {
        //011205 Gershon - moved only to embedded space scenario and that is after loading
        //potential sys props from the custom properties.
        //JSpaceUtilities.setXMLImplSystemProps();
        if (urls == null)
            throw new FinderException(new SpaceURLValidationException("Invalid space url - SpaceURL value must be supplied to this method."));
        if (urls.length == 0)
            throw new FinderException(new SpaceURLValidationException("Invalid space url - Invalid array size of SpaceURLs " + urls.length));

        SpaceURL[] spaceUrls = new SpaceURL[urls.length];

        for (int i = 0; i < urls.length; i++) {
            try {
                spaceUrls[i] = parseURL(urls[i], customProperties);
            } catch (Exception ex) {
                if (i == urls.length - 1)
                    throw new FinderException(ex.getMessage(), ex);
            }
        }

        return spaceUrls;
    }

    private static SpaceURL parseURL(String url, Properties customProperties)
            throws MalformedURLException {
        if (url == null)
            throw new SpaceURLValidationException("Invalid space url - SpaceURL value must be supplied to this method.");

        return SpaceURLParser.parseURL(url, customProperties);
    }

    protected Object findService(String url, Properties customProperties, LifeCycle lifeCycle,
                                 CredentialsProvider credentialsProvider)
            throws FinderException {
        if (url == null)
            throw new FinderException(new SpaceURLValidationException("Invalid space url - SpaceURL value must be supplied to this method."));

        StringTokenizer st = new StringTokenizer(url, ";");
        String[] urls = new String[st.countTokens()];
        for (int i = 0; st.hasMoreTokens(); i++)
            urls[i] = st.nextToken();

        SpaceURL[] spaceURLs = prepareUrl(urls, customProperties);
        Properties customProps = spaceURLs[spaceURLs.length - 1].getCustomProperties();
        return findService(spaceURLs, customProps, lifeCycle, credentialsProvider);
    }

    protected Object findService(String[] urls)
            throws FinderException {
        SpaceURL[] spaceURLs = prepareUrl(urls, null);
        return findService(spaceURLs, null, null, null);
    }

    protected Object findService(SpaceURL[] spaceURLs, Properties customProperties,
                                 LifeCycle lifeCycle, CredentialsProvider credentialsProvider)
            throws FinderException {
        if (spaceURLs == null || spaceURLs.length == 0)
            throw new FinderException(new SpaceURLValidationException("Invalid space url - SpaceURL value must be supplied to this method."));

        Properties actualCustomProperties;
        for (int i = 0; i < spaceURLs.length; i++) {
            try {
                actualCustomProperties = customProperties != null ? customProperties : spaceURLs[i].getCustomProperties();
                long timeout = Long.parseLong(spaceURLs[i].getProperty(SpaceURL.TIMEOUT, "-1"));
                return findService(spaceURLs[i], actualCustomProperties, lifeCycle, credentialsProvider, timeout, null /* lookupType */);
            } catch (FinderException ex) {
                if (i == spaceURLs.length - 1)
                    throw ex;
            }
        }

        // Should not happen - Satisfy the compiler
        throw new FinderException("find finished without returning result.");
    }

    /**
     * The main method of SpaceFinder to find single Space or Container proxy. If the proxy is not
     * found throws <code>FinderException</code>.
     *
     * @param url                 The valid SpaceFinder URL to Container or Space.
     * @param credentialsProvider security credentials to be used for authentication in case of a
     *                            secured space; may be <code>null</code>.
     * @return Returns single found proxy.
     * @throws FinderException Failed to find proxy of desired <code>url</code>.
     **/
    protected Object findService(SpaceURL url, Properties customProperties,
                                 LifeCycle lifeCycle, CredentialsProvider credentialsProvider, long timeout,
                                 LookupType lookupType)
            throws FinderException {
        try {
            // If user details are not provided, search custom properties:
            if (credentialsProvider == null && customProperties != null) {
                final String username = (String) customProperties.remove(Constants.Security.USERNAME);
                final String password = (String) customProperties.remove(Constants.Security.PASSWORD);
                if (StringUtils.hasLength(username) || StringUtils.hasLength(password))
                    credentialsProvider = new DefaultCredentialsProvider(username, password);
            }

            Object result;
            if (url.isEmbeddedProtocol())
                result = findEmbeddedSpace(url, customProperties, credentialsProvider, lifeCycle);
            else if (url.isJiniProtocol()) {
                //IRemoteSpace remoteSpace = findJiniSpace(url, customProperties, timeout, lookupType);
                //result = remoteSpace.getServiceProxy();
                result = findJiniSpace(url, null, customProperties, timeout, lookupType, true);
            } else if (url.isFileProtocol()) {
                result = findFileSpace(url);
            } else
                throw new SpaceURLValidationException("Unsupported url protocol: " + url.getProtocol());

            if (result instanceof ISpaceProxy)
                result = initSpaceProxy((ISpaceProxy) result, url, customProperties, credentialsProvider);

            return result;
        } catch (FinderException e) {
            throw e;
        } catch (Exception e) {
            throw new FinderException("Failed to find: " + url, e);
        }
    }

    private ISpaceProxy initSpaceProxy(ISpaceProxy spaceProxy, SpaceURL spaceURL, Properties customProperties,
                                       CredentialsProvider credentialsProvider)
            throws RemoteException, FinderException {
        // If this is a "sink" proxy set a flag
        // Should be set before performing login on the space since the flag is cached with
        // a SpaceContext instance.
        if (spaceProxy instanceof SpaceProxyImpl) {
            SpaceProxyImpl directProxy = (SpaceProxyImpl) spaceProxy;
            directProxy.setFinderURL(spaceURL);
            String property = customProperties.getProperty(Constants.Replication.GATEWAY_PROXY);
            if (Boolean.parseBoolean(property))
                directProxy.setGatewayProxy();
        }

        //remote proxy login with userDetails
        if (credentialsProvider != null && spaceURL.isRemoteProtocol()) {
            if (!spaceProxy.isServiceSecured())
                throw new SecurityException("Can't provide security credentials to a non-secured space");
            spaceProxy.login(credentialsProvider);
        }

        // fill spaceURL properties for desired space proxy
        spaceURL.setPropertiesForSpaceProxy(spaceProxy);

        final SpaceFinderListener spaceFinderListener = XapExtensions.getInstance().getSpaceFinderListener();
        if (spaceFinderListener != null) {
            spaceProxy = spaceFinderListener.onFindSpace(spaceProxy, spaceURL, customProperties);
        }
        return spaceProxy;
    }

    private SpaceImpl createEmbeddedSpace(String containerName, SpaceURL url, Properties customProperties, LifeCycle lifeCycle)
            throws Exception {
        String schemaName = url.getProperty(SpaceURL.SCHEMA_NAME);
        if (JSpaceUtilities.isEmpty(schemaName))
            schemaName = SpaceURL.DEFAULT_SCHEMA_NAME;

        //TODO replace the host/container name vars in the selected schema file

        // check if the config file exists and readable, otherwise thrown exception
        //Used for backward compatability.
        String contConfFile = SystemLocations.singleton().config(containerName + "-config.xml").toString();
        File configFile = new File(contConfFile);

        //Go and find the container schema file if schema is requested AND
        //no regular <container name>-config.xml exists on disk. We try to find it in the resource bundles.
        if (!configFile.canRead() && !JSpaceUtilities.isEmpty(schemaName)) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("Could not find the " + containerName + "-config.xml file: "
                        + contConfFile + ".\n Loading the default container schema file from the ResourceBundle, for the container configuration.");
            }
            InputStream schemaInputStream = ResourceLoader.findContainerSchema(schemaName);
            JProperties.setInputStream(containerName, schemaInputStream, customProperties);
        } else {
            // set container configuration
            JProperties.setURL(containerName, contConfFile);
        }
        JProperties.getContainerProperties(containerName).setProperty(Constants.Container.CONTAINER_NAME_PROP, containerName);

        // init and return container instance
        JSpaceContainerImpl container = new JSpaceContainerImpl(lifeCycle, schemaName, url, customProperties);
        return container.getContainerEntry().getSpaceImpl();
    }

    /**
     * Create new instance of JSpaceContainerImpl and trying Create/Destroy/Open - embedded space.
     *
     * @param url SpaceURL parameters
     * @return Embedded Space.
     **/
    private ISpaceProxy findEmbeddedSpace(SpaceURL url, Properties customProperties,
                                          CredentialsProvider credentialsProvider, LifeCycle lifeCycle)
            throws Exception {
        final String spaceName = url.getSpaceName();
        if (JSpaceUtilities.isEmpty(spaceName))
            throw new FinderException("Using java:// protocol SpaceName should be defined in SpaceFinder URL");
        final String containerName = url.getContainerName();
        if (JSpaceUtilities.isEmpty(containerName))
            throw new FinderException("Using java:// protocol ContainerName should be defined in SpaceFinder URL");

        if (credentialsProvider != null)
            CredentialsProviderHelper.appendCredentials(customProperties, credentialsProvider);

        synchronized (_embeddedSpaces) {
            SpaceImpl space = _embeddedSpaces.get(url);
            if (space != null && space.getContainer().getContainerStatus() == JSpaceContainerImpl.SHUTDOWN_STATUS) {
                _embeddedSpaces.remove(url);
                space = null;
            }
            if (space == null) {
                if (url.getProperty(SpaceURL.CREATE, "").equalsIgnoreCase("false"))
                    throw new NoSuchNameException("Space <" + spaceName + "> could not be found using space url: " + url.getURL());
                space = createEmbeddedSpace(containerName, url, customProperties, lifeCycle);
                _embeddedSpaces.put(url, space);
            }

            if (_logger.isDebugEnabled())
                _logger.debug("java:// protocol. Get <" + spaceName + "> space from <" + containerName + "> container");
            return (ISpaceProxy) space.getContainer().getContainerEntry().getClusteredSpaceProxy();
        }
    }

    /**
     * Returns Either LRMISpaceImpl or SpaceProxyImpl.
     */
    public static IRemoteSpace findJiniSpace(SpaceURL url, String spaceUuid, Properties customProperties, long timeout, LookupType lookupType)
            throws FinderException {
        return (IRemoteSpace) findJiniSpace(url, spaceUuid, customProperties, timeout, lookupType, false);
    }

    private static Object findJiniSpace(SpaceURL url, String spaceUuid, Properties customProperties, long timeout, LookupType lookupType, boolean postProcess)
            throws FinderException {
        // NOTE, the Service.class should be used here as there is a special cache aside in GigaRegistrar based on it
        LookupRequest request = new LookupRequest(Service.class)
                .setLocators(url.getHost())
                .setGroups(url.getProperty(SpaceURL.GROUPS))
                .setCustomProperties(customProperties)
                .setTimeout(timeout)
                .setLookupInterval(url.getLookupIntervalTimeout())
                .setLookupType(lookupType)
                .setSpaceUuid(spaceUuid);

        if(spaceUuid == null) request= request.setServiceAttributes(url.getLookupAttributes());

        boolean previousLazyAccessValue = DefaultProxyPivot.updateLazyAccess(!postProcess);

        try {
            Object result = LookupFinder.find(request);
            if (postProcess)
                ((SpaceProxyImpl) result).setFinderURL(url);
            return result;
        } finally {
            DefaultProxyPivot.updateLazyAccess(previousLazyAccessValue);
        }
    }

    private Object findFileSpace(SpaceURL url) throws IOException, ClassNotFoundException {
        String path = System.getProperty(SystemProperties.LUS_FILE_PATH);
        if (path == null)
            throw new IllegalArgumentException("file-lus path was not defined: set it using " + SystemProperties.LUS_FILE_PATH);
        File serviceFile = new File(path, url.getSpaceName() + ".service");
        IRemoteSpace remoteSpace = readRemoteService(serviceFile.getAbsolutePath());
        return remoteSpace.getServiceProxy();
    }

    private static <T extends Remote> T readRemoteService(String path) throws IOException, ClassNotFoundException {
        try(ObjectInputStream is = new ObjectInputStream(new FileInputStream(path))){
            return (T) is.readObject();
        }
    }

    /**
     * This main provides the following options: 1. Starts container or container with an embedded
     * space. i.e. java SpaceFinder java://localhost:10098/containerName or
     * java://localhost:10098/containerName/mySpace or /./mySpace (which translates to
     * java://localhost:10098/containerName/mySpace?schema=default) or /./mySpace?schema=cache
     * (which translates to java://localhost:10098/containerName/mySpace?schema=cache) For more
     * options see usage printouts.
     * <p>
     * 2. Verify if a Space/Container running. i.e. java SpaceFinder rmi://hostname/containerName/SpaceName
     *
     * @param args args[0] should be the space url
     * @throws FinderException During finding space or container.
     **/
    public static void main(String[] args) throws FinderException {
        if (args.length <= 0) {
            SpaceURLParser.printUsage();
            System.exit(-1);
        }

        Object space = find(args[0]);
        if (space != null) {
            Logger logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACEFINDER);
            if (logger.isDebugEnabled()) {
                if (space instanceof ISpaceProxy)
                    logger.debug("Found space: " + space + " using this SpaceURL: " + ((ISpaceProxy) space).getFinderURL() + ". \n The found space initialized using this SpaceURL: " + ((ISpaceProxy) space).getURL());
                else
                    logger.debug("Found space: " + space);
            }
        }
    }
}
