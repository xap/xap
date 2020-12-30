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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.admin.demote.DemoteFailedException;
import com.gigaspaces.admin.quiesce.DefaultQuiesceToken;
import com.gigaspaces.admin.quiesce.QuiesceException;
import com.gigaspaces.admin.quiesce.QuiesceState;
import com.gigaspaces.admin.quiesce.QuiesceStateChangedEvent;
import com.gigaspaces.annotation.SupportCodeChange;
import com.gigaspaces.attribute_store.AttributeStore;
import com.gigaspaces.client.ReadTakeByIdResult;
import com.gigaspaces.client.ReadTakeByIdsException;
import com.gigaspaces.client.WriteMultipleException;
import com.gigaspaces.client.protective.ProtectiveModeException;
import com.gigaspaces.client.transaction.xa.GSServerTransaction;
import com.gigaspaces.cluster.ClusterFailureDetector;
import com.gigaspaces.cluster.activeelection.LeaderSelector;
import com.gigaspaces.cluster.activeelection.*;
import com.gigaspaces.cluster.activeelection.core.ActiveElectionException;
import com.gigaspaces.cluster.replication.ConsistencyLevelViolationException;
import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.executor.SpaceTaskWrapper;
import com.gigaspaces.grid.zone.ZoneHelper;
import com.gigaspaces.internal.classloader.ClassLoaderCache;
import com.gigaspaces.internal.client.spaceproxy.DirectSpaceProxyFactoryImpl;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.executors.SystemTask;
import com.gigaspaces.internal.client.spaceproxy.operations.SpaceConnectRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.SpaceConnectResult;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.cluster.ClusterTopologyState;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencyBackupSyncIteratorHandler;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncListBatch;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IReplicationConnectionProxy;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyReplicaState;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeResult;
import com.gigaspaces.internal.document.DocumentObjectConverterInternal;
import com.gigaspaces.internal.exceptions.BatchQueryException;
import com.gigaspaces.internal.exceptions.ChunksMapGenerationException;
import com.gigaspaces.internal.exceptions.WriteResultImpl;
import com.gigaspaces.internal.extension.XapExtensions;
import com.gigaspaces.internal.jvm.HeapUsageEstimator;
import com.gigaspaces.internal.jvm.JVMDetails;
import com.gigaspaces.internal.jvm.JVMHelper;
import com.gigaspaces.internal.jvm.JVMStatistics;
import com.gigaspaces.internal.lrmi.stubs.LRMISpaceImpl;
import com.gigaspaces.internal.lrmi.stubs.LRMIStubHandlerImpl;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.naming.LookupNamingService;
import com.gigaspaces.internal.os.OSDetails;
import com.gigaspaces.internal.os.OSHelper;
import com.gigaspaces.internal.os.OSStatistics;
import com.gigaspaces.internal.query.explainplan.SingleExplainPlan;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.broadcast_table.BroadcastTableHandler;
import com.gigaspaces.internal.server.space.demote.DemoteHandler;
import com.gigaspaces.internal.server.space.executors.SpaceActionExecutor;
import com.gigaspaces.internal.server.space.iterator.ServerIteratorRequestInfo;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsExecutor;
import com.gigaspaces.internal.server.space.operations.WriteEntriesResult;
import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.gigaspaces.internal.server.space.quiesce.QuiesceHandler;
import com.gigaspaces.internal.server.space.recovery.RecoveryManager;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyRecoveryException;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyRecoveryHelper;
import com.gigaspaces.internal.server.space.recovery.strategy.SpaceRecoverStrategy;
import com.gigaspaces.internal.server.space.suspend.SuspendTypeChangedInternalListener;
import com.gigaspaces.internal.service.ServiceRegistrationException;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.internal.transaction.DefaultTransactionUniqueId;
import com.gigaspaces.internal.transaction.XATransactionUniqueId;
import com.gigaspaces.internal.transport.AbstractProjectionTemplate;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.ITransportPacket;
import com.gigaspaces.internal.utils.ReplaceInFileUtils;
import com.gigaspaces.internal.utils.concurrent.GSThreadFactory;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.internal.version.PlatformVersion;
import com.gigaspaces.lrmi.*;
import com.gigaspaces.lrmi.classloading.LRMIClassLoadersHolder;
import com.gigaspaces.lrmi.nio.*;
import com.gigaspaces.lrmi.nio.async.IFuture;
import com.gigaspaces.lrmi.nio.async.LRMIFuture;
import com.gigaspaces.lrmi.nio.info.NIODetails;
import com.gigaspaces.lrmi.nio.info.NIOInfoHelper;
import com.gigaspaces.lrmi.nio.info.NIOStatistics;
import com.gigaspaces.management.space.LocalCacheDetails;
import com.gigaspaces.management.space.LocalViewDetails;
import com.gigaspaces.management.transport.ITransportConnection;
import com.gigaspaces.metadata.StorageType;
import com.gigaspaces.security.SecurityException;
import com.gigaspaces.security.authorities.GridAuthority;
import com.gigaspaces.security.authorities.Privilege;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.gigaspaces.security.directory.CredentialsProviderHelper;
import com.gigaspaces.security.service.SecurityInterceptor;
import com.gigaspaces.server.space.suspend.SuspendType;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.time.SystemTime;
import com.gigaspaces.utils.Pair;
import com.j_spaces.core.*;
import com.j_spaces.core.Constants.LookupManager;
import com.j_spaces.core.LeaseManager;
import com.j_spaces.core.Constants.*;
import com.j_spaces.core.admin.*;
import com.j_spaces.core.client.*;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.cluster.ClusterXML;
import com.j_spaces.core.cluster.startup.ReplicationStartupManager;
import com.j_spaces.core.exception.*;
import com.j_spaces.core.filters.*;
import com.j_spaces.core.server.processor.UpdateOrWriteBusPacket;
import com.j_spaces.core.service.AbstractService;
import com.j_spaces.core.service.Service;
import com.j_spaces.jdbc.IQueryProcessor;
import com.j_spaces.jdbc.QueryProcessor;
import com.j_spaces.jdbc.QueryProcessorFactory;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.ResourceLoader;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.log.JProperties;
import com.j_spaces.kernel.weaklistener.WeakDiscoveryListener;
import com.j_spaces.lookup.entry.*;
import com.j_spaces.sadapter.datasource.DataAdaptorIterator;
import com.j_spaces.worker.WorkerManager;
import com.sun.jini.mahalo.ExtendedPrepareResult;
import com.sun.jini.start.LifeCycle;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.*;
import net.jini.discovery.DiscoveryEvent;
import net.jini.discovery.DiscoveryListener;
import net.jini.discovery.LookupDiscoveryManager;
import net.jini.id.Uuid;
import net.jini.lookup.LookupCache;
import net.jini.lookup.ServiceDiscoveryEvent;
import net.jini.lookup.ServiceDiscoveryListener;
import net.jini.lookup.entry.Name;
import net.jini.lookup.entry.ServiceInfo;
import org.jini.rio.boot.CodeChangeClassLoadersManager;
import org.jini.rio.boot.ServiceClassLoader;
import org.jini.rio.boot.SpaceInstanceRemoteClassLoaderInfo;
import org.jini.rio.boot.SupportCodeChangeAnnotationContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.j_spaces.core.Constants.CacheManager.*;
import static com.j_spaces.core.Constants.DirectPersistency.ZOOKEEPER.ATTRIBUET_STORE_HANDLER_CLASS_NAME;
import static com.j_spaces.core.Constants.DirectPersistency.ZOOKEEPER.ZOOKEEPER_CLIENT_CLASS_NAME;
import static com.j_spaces.core.Constants.LeaderSelector.LEADER_SELECTOR_HANDLER_CLASS_NAME;
import static com.j_spaces.core.Constants.LeaseManager.*;


@com.gigaspaces.api.InternalApi
public class SpaceImpl extends AbstractService implements IRemoteSpace, IInternalRemoteJSpaceAdmin, DiscoveryListener,
        TransactionParticipant, StatisticsAdmin {
    // Configuration component name
    protected static final String COMPONENT = "com.gigaspaces.javaspace";

    private final String _spaceName;
    private final String _containerName;
    private final JSpaceContainerImpl _container;
    private final SpaceURL _url;
    private final JSpaceAttributes _jspaceAttr;
    private final Properties _customProperties;
    private final ClusterPolicy _clusterPolicy;
    private final boolean _secondary;

    private final String _instanceId;
    private final String _nodeName;
    private final Logger _logger;
    private final Logger _operationLogger;
    private final String _spaceMemberName;
    private final String _deployPath;
    private final SpaceConfigReader _configReader;
    private final boolean _isLusRegEnabled;
    private final JSpaceState _spaceState;
    private final int _serializationType;
    //unused embedded mahalo async clear
    public final boolean _cleanUnusedEmbeddedGlobalXtns;

    private final SpaceOperationsExecutor _operationsExecutor;
    private final IJSpaceContainer _containerProxyRemote;
    private final SecurityInterceptor _securityInterceptor;
    private final CredentialsProvider _embeddedCredentialsProvider;
    private final QuiesceHandler _quiesceHandler;
    private final Object _cleanedLock = new Object();
    private final PrimarySpaceModeListeners primarySpaceModeListeners = new PrimarySpaceModeListeners();
    private final CompositeSpaceModeListener _internalSpaceModesListeners = new CompositeSpaceModeListener();
    private final ClusterInfoChangedListeners clusterInfoChangedListeners = new ClusterInfoChangedListeners();
    private final IStubHandler _stubHandler;
    private final DemoteHandler _demoteHandler;
    private final HttpServer _httpServer;
    private final HeapUsageEstimator _heapUsageEstimator = new HeapUsageEstimator();

    private final String _puName;
    private final boolean _scalable;
    private SpaceClusterInfo _clusterInfo;
    private SpaceConfig _spaceConfig;
    private SpaceEngine _engine;
    private SpaceProxyImpl _embeddedProxy;
    private SpaceProxyImpl _clusteredProxy;
    private SpaceProxyImpl _taskProxy;
    private IRemoteSpace _spaceStub;
    private boolean _isCleaned;
    private JSpaceStatistics _statistics;
    private AtomicBoolean _recovering = new AtomicBoolean();
    private WorkerManager _workerManager;
    private LeaderSelectorHandler _leaderSelector;
    private ReplicationStartupManager _startupManager;
    private SpaceComponentManager _componentManager;
    private RecoveryManager _recoveryManager;
    private QueryProcessor _qp;

    private volatile ClusterFailureDetector _clusterFailureDetector;

    //direct-persistency recovery
    private volatile DirectPersistencyRecoveryHelper _directPersistencyRecoveryHelper;

    private final ZKCollocatedClientConfig zkConfig;
    private final AttributeStore attributeStore;
    private ZookeeperClient zookeeperClient;
    private final ZookeeperLastPrimaryHandler zookeeperLastPrimaryHandler;
    private ZookeeperTopologyHandler zookeeperTopologyHandler;

    private final Map<Class<? extends SystemTask>, SpaceActionExecutor> executorMap = XapExtensions.getInstance().getActionExecutors();
    private BroadcastTableHandler _broadcastTableHandler;

    public SpaceImpl(String spaceName, String containerName, JSpaceContainerImpl container, SpaceURL url,
                     JSpaceAttributes spaceConfig, Properties customProperties,
                     boolean isSecondary, boolean securityEnabled, LifeCycle lifeCycle)
            throws CreateException, RemoteException {
        super(lifeCycle);

        this._spaceName = spaceName;
        this._containerName = containerName;
        this._spaceMemberName = JSpaceUtilities.createFullSpaceName(containerName, spaceName);
        this._container = container;
        this._url = url;
        this._jspaceAttr = spaceConfig != null ? spaceConfig : new JSpaceAttributes();
        this._customProperties = new Properties();
        this._customProperties.putAll(customProperties);
        this._clusterPolicy = _jspaceAttr.getClusterPolicy();
        this._instanceId = extractInstanceIdFromContainerName(containerName);
        this._nodeName = _instanceId.equals("0") ? spaceName : spaceName + "." + _instanceId;
        this._logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACE + "." + _nodeName);
        this._operationLogger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ENGINE_OPERATIONS + "." + _nodeName);
        this._configReader = new SpaceConfigReader(_spaceMemberName);

        boolean hasZk = useZooKeeper();
        this.zkConfig = hasZk ? createZKCollocatedClientConfig() : null;
        this.attributeStore = hasZk ? createZooKeeperAttributeStore() : null;
        this._puName = customProperties.getProperty("clusterInfo.name");
        this._scalable = !isMirror() && Boolean.parseBoolean(customProperties.getProperty("clusterInfo.supportsDynamicRepartitioning"));
        this._clusterInfo = new SpaceClusterInfo(_jspaceAttr, _spaceMemberName);
        if (_scalable) {
            zookeeperTopologyHandler = new ZookeeperTopologyHandler(_puName, attributeStore);
            try {
                ClusterTopologyState state = zookeeperTopologyHandler.getClusterTopologyState();
                ClusterTopology topology = zookeeperTopologyHandler.getClusterTopology(state.getGenerationForPartition(getPartitionIdOneBased()));
                _clusterInfo.setTopology(topology);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to find chunks map in zk - disabling chunks space routing", e);
            }
        }

        this._secondary = isSecondary;

        this._deployPath = _customProperties.getProperty("deployPath");

        this._isLusRegEnabled = Boolean.valueOf(JProperties.getContainerProperty(
                containerName, Constants.LookupManager.LOOKUP_ENABLED_PROP,
                Constants.LookupManager.LOOKUP_ENABLED_DEFAULT)).booleanValue();
        this._spaceState = new JSpaceState();
        this._serializationType = loadSerializationType();
        this._cleanUnusedEmbeddedGlobalXtns = _configReader.getBooleanSpaceProperty(
                Engine.ENGINE_CLEAN_UNUSED_EMBEDDED_GLOBAL_XTNS_PROP, Engine.ENGINE_CLEAN_UNUSED_EMBEDDED_GLOBAL_XTNS_DEFAULT);

        this._httpServer = initWebServerIfNeeded();
        this._operationsExecutor = new SpaceOperationsExecutor();
        this._containerProxyRemote = container != null ? container.getContainerProxy() : null;
        this._securityInterceptor = securityEnabled ? new SecurityInterceptor(spaceName, customProperties, true) : null;
        this._embeddedCredentialsProvider = securityEnabled ? extractCredentials(customProperties, spaceConfig) : null;
        this._demoteHandler = new DemoteHandler(this);
        this._stubHandler = new LRMIStubHandlerImpl();

        if(spaceConfig != null){
            initClassLoadersManager(spaceConfig.getSupportCodeChange(), spaceConfig.getMaxClassLoaders());
        }

        zookeeperLastPrimaryHandler = hasZk ? new ZookeeperLastPrimaryHandler(this, attributeStore, _logger) : null;


        //init quiesce handler before startInternal to ensure no operations will arrive before handler is initialized
        this._quiesceHandler = new QuiesceHandler(this, getQuiesceStateChangedEvent(customProperties));

        startInternal();

        // TODO RMI connections are not blocked
        if (_clusterPolicy != null && _clusterPolicy.isPersistentStartupEnabled())
            initSpaceStartupStateManager();

        _broadcastTableHandler = new BroadcastTableHandler(this);
    }

    private ZKCollocatedClientConfig createZKCollocatedClientConfig() {
        Properties props = JProperties.getSpaceProperties(_configReader.getFullSpaceName());
        return new ZKCollocatedClientConfig(props);
    }

    public ZKCollocatedClientConfig getZkConfig() {
        return zkConfig;
    }

    private AttributeStore createZooKeeperAttributeStore() {
        try {
            //noinspection unchecked
            Constructor constructor = ClassLoaderHelper.loadLocalClass(ATTRIBUET_STORE_HANDLER_CLASS_NAME)
                    .getConstructor(ZKCollocatedClientConfig.class);
            return (AttributeStore) constructor.newInstance(zkConfig);

        } catch (Exception e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to create attribute store ",e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + _spaceName
                    + "] Failed to create attribute store.");
        }
    }

    private ZookeeperClient createZooKeeperClient() {
        try {
            //noinspection unchecked
            Constructor constructor = ClassLoaderHelper.loadLocalClass(ZOOKEEPER_CLIENT_CLASS_NAME)
                    .getConstructor(ZKCollocatedClientConfig.class);
            return (ZookeeperClient) constructor.newInstance(zkConfig);

        } catch (Exception e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to create zookeeper client",e);
            throw new DirectPersistencyRecoveryException("Failed to start [" + (_spaceName)
                    + "] Failed to create zookeeper client.");
        }
    }

    private HttpServer initWebServerIfNeeded() throws CreateException {
        final String prefix = "com.gs.space.rest";
        boolean enabled = Boolean.getBoolean(prefix + ".enabled");
        if (!enabled)
            return null;

        int port = Integer.getInteger(prefix + ".port", 8089);
        int socketBacklog = Integer.getInteger(prefix + ".socket-backlog", 0);
        final Logger restLogger = LoggerFactory.getLogger(prefix);
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), socketBacklog);
            server.createContext("/probes/alive", new HttpHandler() {
                @Override
                public void handle(HttpExchange context) throws IOException {
                    try {
                        assertAvailable();
                        httpResponse(context, 200, "OK");
                        if (restLogger.isDebugEnabled())
                            restLogger.debug("/probes/alive result: 200 (OK)");
                    } catch (SpaceUnavailableException e) {
                        httpResponse(context, 503, e.getMessage());
                        restLogger.warn("/probes/alive result: 503 (" + e.getMessage() + ")");
                    }
                }
            });
            server.createContext("/probes/ready", new HttpHandler() {
                @Override
                public void handle(HttpExchange context) throws IOException {
                    try {
                        assertReady();
                        httpResponse(context, 200, "OK");
                        if (restLogger.isDebugEnabled())
                            restLogger.debug("/probes/ready result: 200 (OK)");
                    } catch (Exception e) {
                        httpResponse(context, 503, e.getMessage());
                        restLogger.warn("/probes/ready result: 503 (" + e.getMessage() + ")");
                    }
                }
            });
            server.setExecutor(Executors.newSingleThreadExecutor(new GSThreadFactory("WEB", true)));
            server.start();
            restLogger.info("Started rest server at " + server.getAddress());
            return server;

        } catch (IOException e) {
            throw new CreateException("Failed to start web server", e);
        }
    }

    private static void httpResponse(HttpExchange context, int responseCode, String response) throws IOException {
        OutputStream os = context.getResponseBody();
        if (response == null ) {
            context.sendResponseHeaders(responseCode, -1);
        } else {
            byte[] data = response.getBytes("UTF-8");
            context.sendResponseHeaders(responseCode, data.length);
            os.write(data);
        }
        os.close();
    }

    public ZookeeperLastPrimaryHandler getZookeeperLastPrimaryHandler() {
        return zookeeperLastPrimaryHandler;
    }

    private void initClassLoadersManager(String enableTaskReloadingStr, String maxClassLoadersStr) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if(contextClassLoader instanceof ServiceClassLoader){
            ((ServiceClassLoader)contextClassLoader).initCodeChangeClassLoadersManager(Boolean.parseBoolean(enableTaskReloadingStr), Integer.parseInt(maxClassLoadersStr));
        }
        else {
            CodeChangeClassLoadersManager.initInstance(contextClassLoader, Boolean.parseBoolean(enableTaskReloadingStr),Integer.parseInt(maxClassLoadersStr));
        }
    }

    public String getContainerName() {
        return _containerName;
    }

    public String getInstanceId() {
        return _instanceId;
    }

    public String getNodeName() {
        return _nodeName;
    }

    /**
     * Returns the SpaceURL object which initialized that specific space instance. Note that the
     * SpaceURL object contains important information on the space and container configuration/setup
     * such as the space url used, the space/container/cluster schema used and other attributes.
     *
     * The IJSpaceContainer keeps also its reference of the SpaceURL which launched the container.
     *
     * @return SpaceURL Returns the SpaceURL object which initialized that specific space instance.
     **/
    public SpaceURL getURL() {
        return _url;
    }

    public Properties getCustomProperties() {
        return _customProperties;
    }

    public JSpaceAttributes getJspaceAttr() {
        return _jspaceAttr;
    }

    public JSpaceContainerImpl getContainer() {
        return _container;
    }

    public Logger getOperationLogger() {
        return _operationLogger;
    }

    public boolean isPrivate() {
        return _jspaceAttr != null ? _jspaceAttr.isPrivate() : false;
    }

    @Override
    public String getServiceName() {
        return _spaceMemberName;
    }

    @Override
    protected String getServiceTypeDescription() {
        return "Space";
    }

    @Override
    public synchronized Object getAdmin() {
        try {
            if (m_adminImpl == null)
                m_adminImpl = new JSpaceAdminImpl(this, m_adminExporter);
        } catch (Exception ex) {
            _logger.error("Fail to create admin object", ex);
        }
        return (m_adminImpl != null ? m_adminImpl.getProxy() : null);
    }

    private QuiesceStateChangedEvent getQuiesceStateChangedEvent(Properties customProperties) {
        QuiesceStateChangedEvent result = null;
        if (customProperties.containsKey("quiesce.token")) {
            String token = customProperties.getProperty("quiesce.token");
            String description = customProperties.getProperty("quiesce.description");
            result = new QuiesceStateChangedEvent(QuiesceState.QUIESCED, new DefaultQuiesceToken(token), description);
        }
        return result;
    }

    public static String extractInstanceIdFromContainerName(String containerName) {
        final String containerSuffix = "_container";
        final int pos = containerName.lastIndexOf(containerSuffix);
        final String suffix = pos != -1 ? containerName.substring(pos + containerSuffix.length()) : containerName;
        return suffix.length() == 0 ? "0" : suffix;
    }

    public static String extractContainerName(String fullName) {
        final int pos = fullName.indexOf(":");
        return pos != -1 ? fullName.substring(0, pos) : fullName;
    }

    public static String extractInstanceId(String fullName) {
        return extractInstanceIdFromContainerName(extractContainerName(fullName));
    }

    /**
     * Extract the embedded user-details used to load this space. Use them to login to the embedded
     * single and clustered proxies. Can be null if no user details were provided. Note: Remove
     * these from the properties object so that they don't get passed around.
     */
    private CredentialsProvider extractCredentials(Properties properties, JSpaceAttributes spaceAttr) {
        CredentialsProvider credentialsProvider = CredentialsProviderHelper.extractCredentials(properties);
        //need to remove it from space attributes as well
        if (credentialsProvider != null)
            CredentialsProviderHelper.clearCredentialsProperties(spaceAttr);
        return credentialsProvider;
    }

    public boolean isSecuredSpace() {
        return _securityInterceptor != null;
    }

    /**
     * Creates new startup manager and configures it
     */
    private void initSpaceStartupStateManager() throws CreateException {
        // Create replication startup state manager
        List<String> targetNames = _clusterPolicy.m_ReplicationPolicy.m_ReplicationGroupMembersNames;
        String spaceName = _clusterPolicy.m_ReplicationPolicy.m_OwnMemberName;

        _startupManager = new ReplicationStartupManager(spaceName);

        // Register for connect events
        for (String name : targetNames) {
            if (name.equals(spaceName))
                continue;
        }

        // Check if this space is allowed to join the cluster
        // or wait for the last/first space to start
        try {
            if (_startupManager.shouldWait(targetNames)) {
                _logger.info("Waiting for the first space in cluster to start");
                _startupManager.waitForLastSpace();
            }
        } catch (InterruptedException e) {
            throw new CreateException("ReplicationStartupManager was interrupted", e);
        }
    }

    public SpaceEngine getEngine() {
        return _engine;
    }

    public QuiesceHandler getQuiesceHandler() {
        return _quiesceHandler;
    }

    public DirectPersistencyRecoveryHelper getDirectPersistencyRecoveryHelper() {
        return _directPersistencyRecoveryHelper;
    }

    public void beforeOperation(boolean isCheckForStandBy, boolean checkQuiesceMode, SpaceContext sc)
            throws RemoteException {
        if (isCheckForStandBy) {
            /** only if stand by enabled */
            if (_leaderSelector != null && !_leaderSelector.isPrimary() && !SpaceInitializationIndicator.isInitializer())
                throwInactiveSpaceException();
        }

        assertAvailable();

        // check if space finished initializing.
        // this is relevant for split brain when primary is moving to backup.
        // only workers and filters init() is allowed at this stage.
        if (_spaceState.getState() == ISpaceState.STARTING && _leaderSelector != null && !SpaceInitializationIndicator.isInitializer())
            throwInactiveSpaceException();

        //block until clean operation is done
        try {
            blockIfNecessaryUntilCleaned();
        } catch (InterruptedException e) {
            throw new RemoteException("interrupted.", e);
        }
        try {
            if (checkQuiesceMode && getQuiesceHandler() != null)
                getQuiesceHandler().checkAllowedOp(sc != null ? sc.getQuiesceToken() : null);
        } catch (RuntimeException ex) {
            throw logException(ex);
        }

        if(sc != null && this.getClusterInfo().isChunksRouting()) {
            int spaceGeneration = getClusterInfo().getTopology().getGeneration();
            if (sc.getChunksMapGeneration() != spaceGeneration) {
                ChunksMapGenerationException exception = new ChunksMapGenerationException("chunks map generation of client is " + sc.getChunksMapGeneration()
                        + " but partition " + getPartitionId() + " is at generation " + spaceGeneration);
                exception.setNewMap(getClusterInfo().getTopology());
                throw exception;
            }
        }

    }

    private void assertAvailable()
            throws SpaceUnavailableException {
        /** Checks that the space is not stopped or aborted. */
        if (_spaceState.isAborted())
            throw new SpaceUnavailableException(getServiceName(), "Space [" + getServiceName() + "] is not available. Shutdown or other abort operation in process.");
        if (_spaceState.isStopped() || _engine == null)
            throw new SpaceStoppedException(getServiceName(), "Space [" + getServiceName() + "] is in stopped state.");
    }

    private void assertReady() throws Exception{
        /** Checks that the space is ready. */
        if(!_spaceState.isStarted()){
            throw new Exception("Space is NOT ready yet");
        }
    }

    public void beforeTypeOperation(boolean isCheckForStandBy, SpaceContext sc, Privilege privilege, String className)
            throws RemoteException {
        beforeOperation(isCheckForStandBy, true /*checkQuiesceMode*/, sc);

        // check Type Access Privileges:
        if (_securityInterceptor != null && privilege != SpacePrivilege.NOT_SET)
            _securityInterceptor.intercept(SpaceContextHelper.getSecurityContext(sc), privilege, className);
    }

    public void beginPacketOperation(boolean isCheckForStandBy, SpaceContext sc, Privilege privilege, ITransportPacket packet)
            throws RemoteException {
        beforeOperation(isCheckForStandBy, true /*checkQuiesceMode*/, sc);

        // check Type Access Privileges:
        if (_securityInterceptor != null)
            _securityInterceptor.intercept(SpaceContextHelper.getSecurityContext(sc), privilege,
                    packet != null ? packet.getTypeName() : null);

        //_operationsCoordinator.beginConcurrentOperation();
    }

    private void beginBatchOperation(SpaceContext sc, Privilege privilege, ITransportPacket[] packets)
            throws RemoteException {
        beforeOperation(true, true /*checkQuiesceMode*/, sc);

        // check Packets Access Privileges:
        if (_securityInterceptor != null) {
            com.gigaspaces.security.service.SecurityContext securityContext = SpaceContextHelper.getSecurityContext(sc);
            for (ITransportPacket packet : packets)
                _securityInterceptor.intercept(securityContext, privilege,
                        packet != null ? packet.getTypeName() : null);
        }

        //_operationsCoordinator.beginConcurrentOperation();
    }

    private void endPacketOperation() {
        //_operationsCoordinator.endConcurrentOperation();
    }

    /**
     * Check if a user's has sufficient privileges to perform this operation.
     *
     * @param sc        Holds the security context
     * @param privilege The privilege required to perform this operation.
     * @param packet    The packet applied to this operation (can be <code>null</code>).
     */
    public void checkPacketAccessPrivileges(SpaceContext sc, Privilege privilege, ITransportPacket packet) {
        if (_securityInterceptor != null)
            _securityInterceptor.intercept(SpaceContextHelper.getSecurityContext(sc), privilege,
                    packet != null ? packet.getTypeName() : null);
    }

    private void close() {
        // close by proper way all generic workers
        if (_workerManager != null) {
            _workerManager.shutdown();
            _workerManager = null;
        }

        // close the engine and all engine's resources
        if (_engine != null)
            _engine.close();

        if (_componentManager != null)
            _componentManager.clear();

        if (_leaderSelector != null) {
            _leaderSelector.removeListener(_engine);
        }
    }

    private void destroy() {
        try {
            if (_leaderSelector != null) {
                _leaderSelector.terminate();
            }


            if(zookeeperClient != null){
                try{
                    zookeeperClient.close();
                } catch (Exception e) {
                    _logger.warn("Failed to close zookeeperClient", e);
                }
            }


            if (zookeeperTopologyHandler != null) {
                zookeeperTopologyHandler.close();
            }


            if (attributeStore != null) {
                try {
                    attributeStore.close();
                } catch (Exception e) {
                    _logger.warn("Failed to close ZooKeeperAttributeStore", e);
                }
            }

            if(!clusterInfoChangedListeners.isEmpty()){
                clusterInfoChangedListeners.clear();
            }

            if (_qp != null)
                _qp.close();

            cleanOnDestroy();
            if (_clusterFailureDetector != null)
                _clusterFailureDetector.terminate();
        } catch (Exception ex) {
            JSpaceUtilities.throwInternalSpaceException(ex.toString(), ex);
        }

        // Force the garbage collector
        _engine = null;
        Runtime.getRuntime().gc();
    }

    /**
     * Throw inactive space exception. Called when user tries to execute space operation on backup
     * space
     */
    private void throwInactiveSpaceException() throws InactiveSpaceException {
        String primaryMemberName = _leaderSelector.getPrimaryMemberName();
        /* this space undergoing election process */
        if (primaryMemberName == null)
            throw new ElectionInProcessException(getServiceName());

        /* backup space, reroute this operation to the active primary */
        throw new InactiveSpaceException(getServiceName(), primaryMemberName);
    }

    /**
     * Blocks concurrent calls to Space while clean is being performed. When clean operation will
     * finish, a notifyAll() will be invoked awakening all waiting concurrent operations.
     *
     * @throws SpaceCleanedException when clean operation times out.
     * @throws InterruptedException  if interrupted while waiting.
     */
    private void blockIfNecessaryUntilCleaned() throws SpaceCleanedException, InterruptedException {
        if (_isCleaned) {
            //block only if not held by current thread
            if (Thread.holdsLock(_cleanedLock))
                return;

            synchronized (_cleanedLock) {
                long CLEAN_TIMEOUT = 10 * 1000; // 10 sec
                while (_isCleaned) {
                    _cleanedLock.wait(CLEAN_TIMEOUT);

                    if (_isCleaned)    //if timeout has elapsed, m_isCleaned is still true
                        throw new SpaceCleanedException(getServiceName(), "Clean operation timed out after " + CLEAN_TIMEOUT + " ms. for space: " + getServiceName());
                }
            }
        }
    }

    public void internalClean(boolean isInternalUse, boolean isRestart, boolean isWarmInit) {
        // clean still in process
        if (_isCleaned)
            return;

        // invoke before_clean filter
        if (!isInternalUse)
            _engine.getFilterManager().invokeFilters(FilterOperationCodes.BEFORE_CLEAN_SPACE, null, null);

        synchronized (_cleanedLock) {
            try {
                // clean in process
                _isCleaned = true;

                // close engine
                close();

                if (_embeddedProxy != null)
                    _embeddedProxy.directClean();
                if (_clusteredProxy != null)
                    _clusteredProxy.directClean();

                if (_engine == null)
                    return;

                _engine = new SpaceEngine(this);
                initReplicationStateBasedOnActiveElection();
                _engine.init(isWarmInit, isRestart);
                _statistics = (JSpaceStatistics) getFilterObject(Constants.Statistics.STATISTICS_FILTER_NAME);


                /**
                 * TODO Igor.G 14/03/2006 BugId: GS1338
                 * Fix start/internalClean/close method, make abstraction logic.
                 **/
                if (!isInternalUse) {

                    // restart the worker manager
                    try {
                        _workerManager = createWorkerManager();

                    } catch (RemoteException ex) {
                        JSpaceUtilities.throwInternalSpaceException("Failed to get embedded space-proxy for worker manager.", ex);
                    }

                    if (_qp != null) {
                        _qp.clean();
                    }


                    // restart space components
                    try {
                        _componentManager.restartSpaceComponents();
                    } catch (SpaceComponentsInitializeException e) {
                        JSpaceUtilities.throwInternalSpaceException("Failed to restart space components.", e);
                    }
                }

                LRMIClassLoadersHolder.dropAllClasses();
            } catch (Exception ex) {
                // Log internal error...
                if (_logger.isErrorEnabled())
                    _logger.error(ex.toString(), ex);
                JSpaceUtilities.throwInternalSpaceException("internal error upon clean operation", ex);
            } finally {
                // clean finished (either successful or not)
                _isCleaned = false;
                _cleanedLock.notifyAll(); //notify all waiting on this lock
            }
        }

        // Force the Garbage Collector
        Runtime.getRuntime().gc();
    }

    public void cleanOnDestroy() {
        // clean still in process
        if (_isCleaned)
            return;

        synchronized (_cleanedLock) {
            try {
                // clean in process
                _isCleaned = true;

                // close engine
                close();

                // clean the proxies
                if (_embeddedProxy != null)
                    _embeddedProxy.directClean();
                if (_clusteredProxy != null)
                    _clusteredProxy.directClean();
                if (_taskProxy != null)
                    _taskProxy.directClean();

                LRMIClassLoadersHolder.dropAllClasses();
            } catch (Exception ex) {
                // Log internal error...
                if (_logger.isErrorEnabled())
                    _logger.error(ex.toString(), ex);
                JSpaceUtilities.throwInternalSpaceException("internal error upon clean operation", ex);
            } finally {
                // clean finished (either successful or not)
                _isCleaned = false;
                _cleanedLock.notifyAll(); //notify all waiting on this lock
            }
        }
    }

    /**
     * An atomic update-or-write (called when update/updateMultiple is used in conjuction with
     * UPDATE_OR_WRITE modifier).
     *
     * @return AnswerPacket constructed according to the following semantics: <ul> <li>EntryPacket
     * contains previous value - on successful update, <li>EntryPacket is null if matching entry was
     * locked - on failed update, <li>LeaseProxy contains lease - on successful write <li>null if
     * NoWriteLease or called from updateMultiple - on successful write </ul>
     */
    AnswerPacket updateOrWrite(UpdateOrWriteContext ctx, boolean newRouter)
            throws RemoteException, UnusableEntryException, UnknownTypeException, TransactionException, InterruptedException {
        // mast be called hare to make sure that MarshalledExternalEntry will be unmarshed.
        _engine.getTypeManager().loadServerTypeDesc(ctx.packet);

        /*
         * determine first operation:
         * If pure pojo (no UID supplied), perform write.
         * Choose update if version was set, or if the entry exists in pure cache.
         * Otherwise, choose write.
         */
        if (isPurePojo(ctx.packet)) {
            ctx.isUpdateOperation = false;
        } else {
            ctx.isUpdateOperation = (ctx.packet.getVersion() > 0) || _engine.getCacheManager().isEntryInPureCache(ctx.packet.getUID());
        }

        IResponseContext responseContext = ResponseContext.getResponseContext();
        if (responseContext != null && !ctx.hasConcentratingTemplate())
            responseContext.setResponseHandler(new UpdateOrWriteResponseHandler(_engine, ctx));

        // semantics: when update-or-write is called by updateMultiple, no lease should be returned.
        // otherwise, preserve NoWriteLease indicator
        if (ctx.fromUpdateMultiple)
            ctx.operationModifiers |= Modifiers.NO_WRITE_LEASE;
        ExtendedAnswerHolder holder = _engine.updateOrWrite(ctx, newRouter);
        return holder != null ? holder.m_AnswerPacket : null;
    }

    private boolean isPurePojo(IEntryPacket packet) {
        return packet.getUID() == null;
    }

    public PrimarySpaceModeListeners getPrimarySpaceModeListeners() {
        return primarySpaceModeListeners;
    }

    public int getPartitionIdOneBased() {
        return _clusterInfo.getPartitionOfMember(getServiceName()) + 1 ;
    }


    public int getPartitionId() {
        return _clusterInfo.getPartitionOfMember(getServiceName()) ;
    }

    public void registerToClusterInfoChangedEvent(IClusterInfoChangedListener listener) {
        this.clusterInfoChangedListeners.addListener(listener);
    }

    private boolean isMirror() {
        return _configReader.getBooleanSpaceProperty(Mirror.MIRROR_SERVICE_ENABLED_PROP, Mirror.MIRROR_SERVICE_ENABLED_DEFAULT);
    }

    public void removeClusterInfoChangedListener(IClusterInfoChangedListener listener) {
        this.clusterInfoChangedListeners.removeListener(listener);
    }

    /**
     * this class handles the special case of retires due to update/write failures. if case of an
     * EntryNotInSpaceException exception the thread is being prepared for a new call to
     * update/write so that consequent update will not block but rather delegate to another
     * processor thread. if the update/write returned immediately, the answer is sent back to the
     * client. if the (not first) update should wait the response is not sent to the client and the
     * processor thread/TemplateExpirationManager thread will send the response.
     */
    private static class UpdateOrWriteResponseHandler extends DefaultResponseHandler {
        private final SpaceEngine engine;
        private final UpdateOrWriteContext ctx;

        private UpdateOrWriteResponseHandler(SpaceEngine engine, UpdateOrWriteContext ctx) {
            this.engine = engine;
            this.ctx = ctx;
        }

        @Override
        public void handleResponse(IResponseContext respContext, ReplyPacket respPacket) {
            final long current = SystemTime.timeMillis();
            long expTime = LeaseManager.toAbsoluteTime(ctx.timeout, current);
            boolean expired = current > expTime;

            final Exception ex = respContext.isInvokedFromNewRouter() ? ((RemoteOperationResult) respPacket.getResult()).getExecutionException()
                    : respPacket.getException();

            if (ex != null && ex instanceof EntryNotInSpaceException && !expired) {
                UpdateOrWriteBusPacket packet = new UpdateOrWriteBusPacket(ctx.packet.getOperationID(), respContext, ctx, respPacket);
                engine.getProcessorWG().enqueueBlocked(packet);
            } else {
                super.handleResponse(respContext, respPacket);
            }
        }
    }

    /**
     * Return the internal instance of space proxy.
     *
     * @return If cluster policy contains LB or FO, then return clustered proxy,otherwise regular
     * proxy
     */
    public IJSpace getSpaceProxy() throws RemoteException {
        return isClusteredSpace() ? getClusteredProxy() : getSingleProxy();
    }

    /**
     * Return the internal instance of embedded not-clustered space proxy.
     */
    public IDirectSpaceProxy getSingleProxy() throws RemoteException {
        if (_embeddedProxy == null)
            _embeddedProxy = createProxy(false /*clustered*/, true /*UseEmbeddedCredentials*/);
        return _embeddedProxy;
    }

    private IJSpace getClusteredProxy() throws RemoteException {
        if (_clusteredProxy == null)
            _clusteredProxy = createProxy(true /*clustered*/, true /*UseEmbeddedCredentials*/);
        return _clusteredProxy;
    }

    protected IJSpace getTaskProxy() throws RemoteException {
        if (_taskProxy == null)
            _taskProxy = (SpaceProxyImpl) createProxy(true, true).getNonClusteredProxy();
        return _taskProxy;
    }

    private SpaceProxyImpl createProxy(boolean isClustered) throws RemoteException {
        return createProxy(isClustered, false);
    }

    protected SpaceProxyImpl createProxy(boolean isClustered, boolean useEmbeddedCredentials) throws RemoteException {
        DirectSpaceProxyFactoryImpl factory = new DirectSpaceProxyFactoryImpl(getSpaceStub(), createSpaceSettings(isClustered), isClustered);
        SpaceProxyImpl spaceProxy = factory.createSpaceProxy();
        spaceProxy.setFinderURL(getURL());
        if (useEmbeddedCredentials && _embeddedCredentialsProvider != null)
            spaceProxy.login(_embeddedCredentialsProvider);
        return spaceProxy;
    }

    private SpaceSettings createSpaceSettings(boolean isClustered) {
        SpaceConfig spaceConfig = getConfig();
        if (!isClustered) {
            spaceConfig = (SpaceConfig) spaceConfig.clone();
            spaceConfig.setClusterPolicy(null);
        }

        return new SpaceSettings(_containerName,
                _containerProxyRemote,
                getSpaceUuid(),
                _secondary,
                _serializationType,
                spaceConfig,
                _spaceName,
                _url,
                _stubHandler,
                isSecuredSpace(),
                _stubHandler.getTransportConfig(),
                _cleanUnusedEmbeddedGlobalXtns);
    }

    private SpaceProxyImpl createSecuredProxy() throws RemoteException {
        SpaceProxyImpl proxy = createProxy(false);
        secureProxy(proxy);
        return proxy;
    }

    private void secureProxy(IJSpace spaceProxy) throws RemoteException {
        if (isSecuredSpace())
            _securityInterceptor.trustProxy(spaceProxy);
    }


    public boolean isClusteredSpace() {
        return _clusterPolicy != null
                && (_clusterPolicy.m_FailOverPolicy != null || _clusterPolicy.m_LoadBalancingPolicy != null);
    }

    public IRemoteSpace getSpaceStub() throws RemoteException {
        if (_spaceStub == null)
            _spaceStub = new LRMISpaceImpl(this, (IRemoteSpace) _stubHandler.exportObject(this));
        return _spaceStub;
    }

    @Override
    public String toString() {
        return this.getName();
    }

    public static void setConfig(String spaceName, String containerName, JSpaceAttributes config, String spaceFileURL)
            throws RemoteException {
        if (config == null) return;

        try {
            ReplaceInFileUtils updateFile = new ReplaceInFileUtils(spaceFileURL);

            for (Map.Entry entry : config.entrySet()) {
                String key = (String) entry.getKey();
                String value = (String) entry.getValue();

                //take off space name from key
                key = key.substring(key.indexOf('.') + 1);

                //do not change cluster enable definition, they are read only
                if (!key.equals(Constants.Cluster.FULL_IS_CLUSTER_SPACE_PROP)) {
                    updateFile.xmlReplace(key, value);
                }
            }

            /** Distributed Cache */
            if (!config.isClustered() && config.getDCacheConfigName() != null) {
                updateFile.xmlReplace(DCache.CONFIG_NAME_PROP, config.getDCacheConfigName());
                if (config.getDCacheProperties() != null) {
                    String spDir = null;
                    //get path to "config" directory
                    if (!JSpaceUtilities.isEmpty(config.getSchemaName()) /*config.m_schema != null*/)
                        spDir = new File(spaceFileURL).getParentFile().getParent();
                    else
                        spDir = new File(spaceFileURL).getParent();

                    String dCacheConfigFileName =
                            spDir + File.separator +
                                    config.getDCacheConfigName() + DCache.FILE_SUFFIX_EXTENTION;
                    String fullSpaceName = JSpaceUtilities.createFullSpaceName(containerName, spaceName);
                    String oldDCacheConfigName = JProperties.getSpaceProperty(
                            fullSpaceName, DCache.CONFIG_NAME_PROP, DCache.DCACHE_CONFIG_NAME_DEFAULT);
                    String oldDCacheConfigFileName = spDir + File.separator + oldDCacheConfigName + DCache.FILE_SUFFIX_EXTENTION;


                    //always write on disk, whatever configuration changed or no
                    //if( !config.m_dcacheConfigName.equals( DCache.DCACHE_CONFIG_NAME_DEFAULT )  ){
                    if ((new File(oldDCacheConfigFileName)).exists())
                        new File(oldDCacheConfigFileName).renameTo(new File(dCacheConfigFileName));
                    else {
                        InputStream dcacheConfigInputStream = ResourceLoader.findConfigDCache(DCache.DCACHE_CONFIG_NAME_DEFAULT);
                        if (dcacheConfigInputStream != null)
                            SpaceConfigFactory.writeDOMToFile(dcacheConfigInputStream, new File(dCacheConfigFileName));
                    }

                    ReplaceInFileUtils dcacheFile = new ReplaceInFileUtils(dCacheConfigFileName);
                    for (Enumeration enum_ = config.getDCacheProperties().keys(); enum_.hasMoreElements(); ) {
                        String key = (String) enum_.nextElement();
                        dcacheFile.xmlReplace(key, config.getDCacheProperties().getProperty(key));
                    }
                    dcacheFile.close();
                }
            }

            // save and close the file
            updateFile.close();

            // build/update Filters structure
            if (config.getFiltersInfo() != null && config.getFiltersInfo().length > 0)
                updateFilterXMLNodes(spaceFileURL, config.getFiltersInfo());
        } catch (Exception ex) {
            Logger logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_COMMON);
            if (logger.isErrorEnabled())
                logger.error(ex.toString(), ex);
            throw new RemoteException(ex.getMessage(), ex.getCause());
        }
    }

    /**
     * Invoking by setConfig() method. Create/Update Filter structure in spaceName.xml file
     */
    static void updateFilterXMLNodes(String spaceConfigFile, FiltersInfo[] filtersInfo)
            throws Exception {
        // Obtaining a org.w3c.dom.Document from XML
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(spaceConfigFile);
        NodeList nodeList = doc.getElementsByTagName(Filter.FILTERS_TAG_NAME);
        if (nodeList.getLength() <= 0)
            return;

        // remove old filters nodes
        Element filterElem = (Element) nodeList.item(0);
        Node filterNamesNode = filterElem.getElementsByTagName(Filter.FILTER_NAMES_TAG_NAME).item(0);
        String fnames = filterNamesNode.getFirstChild().getNodeValue();
        StringTokenizer st = new StringTokenizer(fnames, ",");
        while (st.hasMoreElements()) {
            String filterName = st.nextToken().trim();
            NodeList filNL = filterElem.getElementsByTagName(filterName);
            for (int c = 0; c < filNL.getLength(); c++)
                filterElem.removeChild(filNL.item(c));
        }// while...

        String filterNames = "";
        for (int i = 0; i < filtersInfo.length; i++) {
            filterNames += filtersInfo[i].filterName + ", ";
            Element filElem = doc.createElement(filtersInfo[i].filterName);
            filElem.appendChild(createXMLTextNode(
                    doc, Filter.FILTER_ENABLED_TAG_NAME, String.valueOf(filtersInfo[i].enabled)));
            filElem.appendChild(createXMLTextNode(
                    doc, Filter.FILTER_CLASS_TAG_NAME, filtersInfo[i].filterClassName));
            filElem.appendChild(createXMLTextNode(
                    doc, Filter.FILTER_URL_TAG_NAME, filtersInfo[i].paramURL));
            filElem.appendChild(createXMLTextNode(
                    doc, Filter.FILTER_PRIORITY_TAG_NAME, String.valueOf(i)));

            // build operation code value
            String operCode = "";
            if (filtersInfo[i].beforeAuthentication)
                operCode += String.valueOf(FilterOperationCodes.BEFORE_AUTHENTICATION) + ", ";
            if (filtersInfo[i].afterRemove)
                operCode += String.valueOf(FilterOperationCodes.AFTER_REMOVE) + ", ";
            if (filtersInfo[i].afterWrite)
                operCode += String.valueOf(FilterOperationCodes.AFTER_WRITE) + ", ";
            if (filtersInfo[i].beforeClean)
                operCode += String.valueOf(FilterOperationCodes.BEFORE_CLEAN_SPACE) + ", ";
            if (filtersInfo[i].beforeNotify)
                operCode += String.valueOf(FilterOperationCodes.BEFORE_NOTIFY) + ", ";
            if (filtersInfo[i].beforeRead)
                operCode += String.valueOf(FilterOperationCodes.BEFORE_READ) + ", ";
            if (filtersInfo[i].afterRead)
                operCode += String.valueOf(FilterOperationCodes.AFTER_READ) + ", ";
            if (filtersInfo[i].beforeTake)
                operCode += String.valueOf(FilterOperationCodes.BEFORE_TAKE) + ", ";
            if (filtersInfo[i].afterTake)
                operCode += String.valueOf(FilterOperationCodes.AFTER_TAKE) + ", ";
            if (filtersInfo[i].beforeWrite)
                operCode += String.valueOf(FilterOperationCodes.BEFORE_WRITE) + ", ";

            if (filtersInfo[i].beforeGetAdmin)
                operCode += String.valueOf(FilterOperationCodes.BEFORE_GETADMIN) + ", ";
            if (filtersInfo[i].beforeUpdate)
                operCode += String.valueOf(FilterOperationCodes.BEFORE_UPDATE) + ", ";
            if (filtersInfo[i].afterUpdate)
                operCode += String.valueOf(FilterOperationCodes.AFTER_UPDATE) + ", ";

            // create operation code, if no operation code, don't remove the last comma
            if (!operCode.equals(""))
                operCode = operCode.substring(0, operCode.lastIndexOf(','));
            filElem.appendChild(createXMLTextNode(doc, Filter.FILTER_OPERATION_CODE_TAG_NAME, operCode));

            // append the filter node
            filterElem.appendChild(filElem);
        }// for...

        // update the filterName info
        Node filterNameNode = filterElem.getElementsByTagName(Filter.FILTER_NAMES_TAG_NAME).item(0);
        filterNameNode.getFirstChild().setNodeValue(filterNames.substring(0, filterNames.lastIndexOf(',')));

        // store changes in xml
        JSpaceUtilities.domWriter(doc.getDocumentElement(),
                new PrintStream(new FileOutputStream(spaceConfigFile)),
                " ");
    }// updateFilterXMLNodes()

    // create XML Text Node
    static private Node createXMLTextNode(Document rootDoc, String tagName, String tagValue) {
        Element tag = rootDoc.createElement(tagName);
        Text tagText = rootDoc.createTextNode(tagValue);
        tag.appendChild(tagText);

        return tag;
    }

    public SpaceConfigReader getConfigReader() {
        return _configReader;
    }

    @Override
    public Uuid getSpaceUuid() {
        return getUuid();
    }

    private ISpaceFilter getFilterObject(String filterId) {
        return (_engine != null ? _engine.getFilterManager().getFilterObject(filterId) : null);
    }

    /**
     * Shutdown this space, do not destroy it.
     *
     * @see com.j_spaces.core.service.ServiceAdminImpl#destroy()
     */
    @Override
    public void shutdown() throws RemoteException {
        shutdown(false, true);
    }

    /**
     * Attempt to shutdown this service .
     *
     * @param isDestroy        specify that space should be destroyed.
     * @param shutdowContainer If has <code>true</code> value and space container contains one
     *                         space, it will be also shutdown.
     */
    public void shutdown(boolean isDestroy, boolean shutdowContainer) throws RemoteException {
        if (_spaceState.isAborted()) {
            _logger.debug("Shutdown already in progress...");
            return;
        }

        int previousState = _spaceState.getState();
        _spaceState.setState(ISpaceState.ABORTING);
        if (isPrivate())
            _logger.debug("Beginning shutdown...");
        else
            _logger.info("Beginning shutdown...");

        beforeShutdown();

        if (isDestroy)
            destroy();
        else if (previousState != ISpaceState.STOPPED)
            stopInternal();

        _spaceState.setState(ISpaceState.ABORTED);

        if (_logger.isDebugEnabled())
            _logger.debug("Life cycle is " + getLifeCycle());

        if (getLifeCycle() != null)
            getLifeCycle().unregister(SpaceImpl.this);

        if (shutdowContainer && _container != null)
            _container.shutdownInternal();

        unregister();


        if (_qp != null)
            _qp.close();

        /*
         * finally, release references
         */
        if (_embeddedProxy != null)
            _embeddedProxy.close();

        if (_clusteredProxy != null)
            _clusteredProxy.close();

        if (_taskProxy != null)
            _taskProxy.close();

        if (_customProperties != null)
            _customProperties.clear();

        if (_jspaceAttr != null)
            _jspaceAttr.clear();

        if (_httpServer != null)
            _httpServer.stop(1);

        if (isPrivate())
            _logger.debug("Shutdown complete");
        else
            _logger.info("Shutdown complete");
    }

    private void beforeShutdown() {
        SpaceEngine engine = _engine;
        if (engine != null)
            engine.waitForConsistentState();
    }

    public void unregister() throws RemoteException {
        unregisterFromLookupService();

        if (m_adminImpl != null)
            m_adminImpl.unexport(true);

        disableStub();
    }

    private void initReplicationStateBasedOnActiveElection()
            throws RemoteException {
        if (_clusterPolicy != null) {
            // First check if primary selection should be activated
            // and initialize the primary selector
            if (_clusterPolicy.isPrimaryElectionAvailable()) {
                _leaderSelector.addListenerAndNotify(_engine);
            } else {
                //This is active active topology
                _engine.getReplicationNode().getAdmin().setActive();
            }
        } else if (_engine.isMirrorService()) {
            //activate mirror replication
            _engine.getReplicationNode().getAdmin().setActive();

        }
    }

    private QueryProcessor createQueryProcessor() throws Exception {
        IJSpace singleProxy = createProxy(false);
        secureProxy(singleProxy);

        IJSpace clusterProxy;
        if (isClusteredSpace()) {
            if (isActive())
                clusterProxy = createProxy(true);
            else {
                SpaceURL remoteUrl = (SpaceURL) getURL().clone();
                remoteUrl.setProperty(SpaceURL.PROTOCOL_NAME, SpaceURL.JINI_PROTOCOL);
                remoteUrl.setProperty(SpaceURL.HOST_NAME, SpaceURL.ANY);
                Long queryProcessorSpaceLookupTimeout = Long.getLong(   SystemProperties.QUERY_PROCESSOR_CLUSTER_LOOKUP_TIMEOUT,
                                                                        SystemProperties.QUERY_PROCESSOR_CLUSTER_LOOKUP_TIMEOUT_DEFAULT);
                remoteUrl.setProperty(SpaceURL.TIMEOUT, String.valueOf(queryProcessorSpaceLookupTimeout));
                remoteUrl.refreshUrlString();
                clusterProxy = (IJSpace) SpaceFinder.find(remoteUrl);
            }
            secureProxy(clusterProxy);
        } else {
            clusterProxy = singleProxy;
        }

        QueryProcessor qp = (QueryProcessor) QueryProcessorFactory.newLocalInstance(clusterProxy, singleProxy, null, _securityInterceptor);
        qp.initStub();
        return qp;
    }

    /**
     * Perform any actions that need to be executed after space recovery and before it is started
     */
    private void postRecoveryActions(ISpaceSynchronizeReplicaState recoveryState) throws Exception {
        // change the state to replicable
        changeSpaceState(ISpaceState.STARTING, true, true);

        if (recoveryState != null) {
            int replicationSynchronizationTimeout = 5 * 60;
            try {
                long syncStartTime = SystemTime.timeMillis();
                ISpaceSynchronizeResult synchronizeResult = recoveryState.waitForSynchronizeCompletion(replicationSynchronizationTimeout, TimeUnit.SECONDS);
                if (synchronizeResult != null) {
                    if (synchronizeResult.isFailed()) {
                        if (_logger.isWarnEnabled())
                            _logger.warn("Synchronization failed: " + synchronizeResult.getFailureReason());
                        throw synchronizeResult.getFailureReason();
                    }
                    if (getEngine().getReplicationNode() != null && getEngine().getReplicationNode().getDirectPesistencySyncHandler() != null) {
                        // clear backup sync list after recovery & set sync iterator handler to null
                        getEngine().getReplicationNode().getDirectPesistencySyncHandler().afterRecovery();
                        getEngine().getReplicationNode().setDirectPersistencyBackupSyncIteratorHandler(null);
                    }
                    // mark backup as finished recovery
                    if (getDirectPersistencyRecoveryHelper() != null && isBackup()) {
                        getDirectPersistencyRecoveryHelper().setPendingBackupRecovery(false);
                    }
                    if (_logger.isInfoEnabled()) {
                        long duration = SystemTime.timeMillis() - syncStartTime;
                        _logger.info("Synchronization completed [duration=" + JSpaceUtilities.formatMillis(duration) + "]");
                    }
                }
            } catch (TimeoutException e) {
                if (getEngine().getCacheManager().isBlobStoreCachePolicy()) {
                    if (_logger.isErrorEnabled())
                        _logger.error("Timeout occurred [" + replicationSynchronizationTimeout + " seconds] while waiting for replication to synchronize. Will shut down space since blobstore inconsistent space can't be started.");
                    throw e;
                }
                if (_logger.isWarnEnabled())
                    _logger.warn("Timeout occurred [" + replicationSynchronizationTimeout + " seconds] while waiting for replication to synchronize. Starting the space without complete synchronization.");
            } catch (Exception e) {
                //Restore non replicable state for next iteration
                changeSpaceState(ISpaceState.STARTING, true, false);
                throw e;
            }
        }
    }

    /**
     * @throws Exception
     * @throws RemoteException
     */
    private void initAndStartPrimaryBackupSpace() throws Exception {
        // in case of failure - retry

        for (int retries = 1; retries <= RecoveryManager.RECOVERY_RETRIES; retries++) {
            try {
                SpaceMode spaceMode = _leaderSelector.getSpaceMode();

                SpaceRecoverStrategy strategy = _recoveryManager.getRecoveryStrategy(spaceMode);
                ISpaceSynchronizeReplicaState recoveryState = strategy.recover();

                // perform post recovery actions
                postRecoveryActions(recoveryState);

                _internalSpaceModesListeners.beforeSpaceModeChange(spaceMode);

                // check that the election state wasn't changed and register for election events
                if (!_leaderSelector.compareAndRegister(spaceMode, _internalSpaceModesListeners)) {
                    throw new SpaceRecoveryException("Space state changed during recovery");
                }

                changeSpaceState(ISpaceState.STARTED, true, true);

                _internalSpaceModesListeners.afterSpaceModeChange(spaceMode);

                return;
            } catch (Exception e) {
                handleRecoveryFailure(e, retries);
            }

        }

    }

    /**
     * Handle primary backup recovery failure
     */
    private void handleRecoveryFailure(Exception e, int retries) throws Exception {
        if (_logger.isWarnEnabled()) {
            _logger.warn("Space recovery failure.", e);
        }

        if (retries == RecoveryManager.RECOVERY_RETRIES
                || getEngine().getCacheManager().isBlobStoreCachePolicy()
                    || _spaceState.getState() == ISpaceState.STOPPED)
            throw e;

        close();

        _engine = new SpaceEngine(this);

        // reset space election state and check that lookup service is not down
        if (_leaderSelector != null) {
            _logger.info("Space recovery failed - selecting primary");
            _leaderSelector.select();
        }

        initReplicationStateBasedOnActiveElection();
    }

    /**
     * Initialize and start space without primary election
     */
    private void initAndStartRegularSpace()
            throws Exception, RemoteException {
        changeSpaceState(ISpaceState.STARTING, true, false);

        // in case of failure - retry
        for (int retries = 1; retries <= RecoveryManager.RECOVERY_RETRIES; retries++) {
            try {
                SpaceRecoverStrategy strategy = _recoveryManager.getRecoveryStrategy(SpaceMode.NONE);
                ISpaceSynchronizeReplicaState recoveryState = strategy.recover();

                // perform post recovery actions
                postRecoveryActions(recoveryState);

                _componentManager.initComponents();

                changeSpaceState(ISpaceState.STARTED, true, true);

                _componentManager.startComponents();

                break;
            } catch (Exception e) {
                handleRecoveryFailure(e, retries);

            }
        }
    }

    /**
     * Initialize main space components
     *
     * @param isWarm if set to true - the space will recover from database
     */
    public void initAndRecoverFromDataStorage(boolean isWarm)
            throws RemoteException, CreateException, SpaceComponentsInitializeException {
        _engine.init(isWarm, true);
        _statistics = (JSpaceStatistics) getFilterObject(Constants.Statistics.STATISTICS_FILTER_NAME);

        // Create WorkerManager
        _workerManager = createWorkerManager();

        // Create a  manager for the space components
        // Any components that can be initialized before recovery(Filter/ReplicationFilters)
        // are initialized here
        _componentManager = new SpaceComponentManager(this);
    }

    public boolean useZooKeeper() {
        return !SystemInfo.singleton().getManagerClusterInfo().isEmpty();
    }

    private boolean isLookupServiceEnabled() {
        return _isLusRegEnabled && !isPrivate();
    }

    public boolean restartLeaderSelectorHandler() {
        try {
            _leaderSelector.terminate();
            _leaderSelector = initLeaderSelectorHandler(isLookupServiceEnabled());
        } catch (RemoteException e) {
            _logger.error("Unable to reinitialize leader selector ", e);
            return false;
        } catch (ActiveElectionException e) {
            _logger.error("Unable to reinitialize leader selector ", e);
            return false;
        }
        return true;
    }

    public LeaderSelectorHandler getLeaderSelector() {
        return _leaderSelector;
    }

    private LeaderSelectorHandler initLeaderSelectorHandler(boolean isLookupServiceEnabled) throws RemoteException, ActiveElectionException {
        LeaderSelectorHandler leaderSelectorHandler = null;
        if (_clusterPolicy != null && _clusterPolicy.isPrimaryElectionAvailable()) {
            if (!isLookupServiceEnabled)
                throw new ActiveElectionException("This cluster configuration requires a running Jini Lookup Service. " +
                        " Make sure this space is registering/joining a running Jini Lookup Service (i.e. verify the <jini_lus><enabled>true<enabled> element in the relevant container schema).");

            //TODO - Should we change the SpaceState to Starting in case we are moving a primary to be a backup ?
            if (_spaceState.getState() != ISpaceState.STARTED)
                changeSpaceState(ISpaceState.STARTING, true, false);

            boolean useZooKeeper = useZooKeeper();
            try {
                LeaderSelectorHandlerConfig leaderSelectorHandlerConfig = new LeaderSelectorHandlerConfig();
                leaderSelectorHandlerConfig.setSpace(this);
                if (!useZooKeeper) {
                    leaderSelectorHandler = new LusBasedSelectorHandler(createSecuredProxy());
                    leaderSelectorHandler.initialize(leaderSelectorHandlerConfig);
                } else {
                    leaderSelectorHandler = createZooKeeperLeaderSelector();
                    leaderSelectorHandler.initialize(leaderSelectorHandlerConfig);
                }

                leaderSelectorHandler.select();

                final CountDownLatch latch;
                final ServiceTemplate participantSrvTemplate;
                if (leaderSelectorHandler.isPrimary()) {
                    latch = new CountDownLatch(1);
                    participantSrvTemplate = buildServiceTemplate(_clusterPolicy, true);
                } else {
                    latch = new CountDownLatch(2);
                    participantSrvTemplate = buildServiceTemplate(_clusterPolicy, false);
                }

                LookupNamingService namingService = new LookupNamingService((LookupDiscoveryManager) getJoinManager().getDiscoveryManager(), getJoinManager().getLeaseRenewalManager());
                LookupCache lookupCache = namingService.notify(participantSrvTemplate, null, new LeaderSelectorServiceDiscoveryListener(latch));
                latch.await();
                lookupCache.terminate();
            } catch (Exception e) {
                if (leaderSelectorHandler != null) {
                    leaderSelectorHandler.terminate();
                }
                throw new ActiveElectionException("Failed to initialize Leader Selector handler", e);
            }
        }
        return leaderSelectorHandler;
    }


    public ClusterFailureDetector getClusterFailureDetector() {
        return _clusterFailureDetector;
    }

    /**
     * Update specified configuration entry.
     */
    public void updateXmlConfigEntry(String configEntry, String configValue)
            throws IOException {
        String spaceConfigURL = JProperties.getURL(getServiceName());
        // TODO Add get schema name method
        /*if (spaceConfigURL == null) {
            spaceConfigURL = CONTAINER_CONFIG_DIRECTORY + File.separator
						+ SCHEMAS_FOLDER + File.separator + getSchemaName()
						+ SPACE_SCHEMA_FILE_SUFFIX;
		}*/
        ReplaceInFileUtils updateFile = new ReplaceInFileUtils(spaceConfigURL);
        updateFile.xmlReplace(configEntry, configValue);
        updateFile.close();
    }

    private final StatisticsNotAvailable _statNotAvailableEx =
            new StatisticsNotAvailable("Statistics are not available, because a space statistics filter is disabled");

    public ServerTransaction createServerTransaction(TransactionManager mgr, Object id, int numOfParticipants) {
        try {
            if (id instanceof Long) {
                long txId = ((Long) id).longValue();
                TransactionParticipantDataImpl serverTransactionMetaData = new TransactionParticipantDataImpl(new DefaultTransactionUniqueId(mgr.getTransactionManagerId(),
                        txId),
                        _engine.getPartitionIdOneBased(),
                        numOfParticipants);
                return new ServerTransaction(mgr, txId, serverTransactionMetaData);
            } else {
                TransactionParticipantDataImpl serverTransactionMetaData = new TransactionParticipantDataImpl(new XATransactionUniqueId(mgr.getTransactionManagerId(),
                        id),
                        _engine.getPartitionIdOneBased(),
                        numOfParticipants);
                return new GSServerTransaction(mgr, id, serverTransactionMetaData);
            }
        } catch (RemoteException e) {
            // mgr.getTransactionManagerId should never throw this exception since
            // there isn't a remote call involved.
        }
        return null;
    }

    private ServerTransaction createServerTransaction(TransactionManager mgr, Object id) {
        return createServerTransaction(mgr, id, 1);

    }

    /**
     * Change space state to given state - lookup attributes are updated as well
     */
    public void changeSpaceState(int state, boolean electable, boolean replicable) {
        _spaceState.setState(state);

        modifyLookupAttributes(new Entry[]{new State()},
                new Entry[]{new State(state, electable, replicable)});
    }

    /**
     * Create a list of all space component handlers
     *
     * @return list of all space component handlers
     */
    public LinkedList<ISpaceComponentsHandler> getSpaceComponentHandlers() {
        LinkedList<ISpaceComponentsHandler> componentsHandlers = new LinkedList<>();
        componentsHandlers.clear();
        componentsHandlers.addAll(_engine.getComponentsHandlers());

        if (_workerManager != null)
            componentsHandlers.add(_workerManager);

        return componentsHandlers;
    }

    /**
     * set serialization type according to property- if no property preset, the default will be set
     * the first time the getProxy method will be called.
     */
    private int loadSerializationType() {
        String propertyValue = _configReader.getSpaceProperty(Engine.ENGINE_SERIALIZATION_TYPE_PROP, Engine.ENGINE_SERIALIZATION_TYPE_DEFAULT).trim();

        try {
            int result = Integer.parseInt(propertyValue);
            StorageType storageType = StorageType.fromCode(result);
            if (storageType != StorageType.OBJECT && _logger.isWarnEnabled())
                _logger.warn(Engine.FULL_ENGINE_SERIALIZATION_TYPE_PROP + "=" + propertyValue + " is deprecated - use POJO annotations or gs.xml instead.");
            return result;
        } catch (NumberFormatException e) {
            String msg = "Invalid serialization-type specified " + propertyValue;
            if (_logger.isErrorEnabled())
                _logger.error(msg, e);
            throw new RuntimeException(msg, e);
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage().replace("StorageType", "serialization-type");
            if (_logger.isErrorEnabled())
                _logger.error(msg);
            throw new RuntimeException(msg);
        }
    }

    public SpaceHealthStatus getLocalSpaceHealthStatus() {
        ArrayList<Throwable> errors = new ArrayList<>();
        return getSpaceHealthStatusHelper(errors);
    }

    private SpaceHealthStatus getSpaceHealthStatusHelper(ArrayList<Throwable> errors) {
        SpaceMode spaceMode = getSpaceMode();

        final SpaceEngine engine = _engine;
        //engine can be null due to concurrent shutdown/restart
        if (engine != null) {
            if (spaceMode == SpaceMode.BACKUP) {
                try {
                    //If space is in backmode, if it reached memory shortage, it is considered unhealthy
                    engine.getMemoryManager().monitorMemoryUsage(true);
                } catch (Exception e) {
                    errors.add(e);
                }
            }
            errors.addAll(engine.getUnhealthyReasons());
        }

        LeaderSelector leaderElector = _leaderSelector;
        if (leaderElector != null && leaderElector.getLastError() != null)
            errors.add(leaderElector.getLastError());

        return new SpaceHealthStatus(errors.toArray(new Throwable[errors.size()]));
    }

    @Override
    public com.gigaspaces.security.service.SecurityContext login(
            com.gigaspaces.security.service.SecurityContext securityContext) throws RemoteException {

        if (!isSecuredSpace()) {
            throw new SecurityException("Trying to login to a non-secured space [" + getServiceName() + "]");
        }

        //invoke before-authentication filter
        if (_engine != null && !_securityInterceptor.shouldBypassFilter(securityContext)) {
            _engine.beforeAuthentication(securityContext);
        }

        return _securityInterceptor.authenticate(securityContext);
    }

    public void addInternalSpaceModeListener(ISpaceModeListener listener) {
        _internalSpaceModesListeners.addListener(listener);
    }

    public void removeInternalSpaceModeListener(ISpaceModeListener listener) {
        _internalSpaceModesListeners.removeListener(listener);
    }

    @Override
    public SpaceProxyImpl getServiceProxy() throws RemoteException {
        return createProxy(isClusteredSpace(), false);
    }

    @Override
    public SpaceConnectResult connect(SpaceConnectRequest request) throws RemoteException {
        assertAvailable();
        SpaceConnectResult result = new SpaceConnectResult();
        boolean serializeClusterPolicy = request.getVersion().lessThan(PlatformLogicalVersion.v11_0_0);
        result.setSpaceSettings(createSpaceSettings(serializeClusterPolicy));
        result.setClustered(_clusterInfo.isClustered());
        return result;
    }

    public boolean isAborted() {
        return _spaceState.isAborted();
    }

    private <T extends Throwable> T logException(T e) {
        if (!(e instanceof ProtectiveModeException) && (!(e instanceof ConsistencyLevelViolationException)) && (!(e instanceof QuiesceException))
                && _logger.isErrorEnabled())
            _logger.error(e.toString(), e);
        return e;
    }

    @Override
    public DirectPersistencySyncListBatch getSynchronizationListBatch() throws RemoteException {
        // get iterator of backup's sync list which is used in direct persistency sync list memory recovery
        DirectPersistencyBackupSyncIteratorHandler directPersistencyBackupSyncIteratorHandler = getEngine().getReplicationManager().getReplicationNode().getDirectPersistencyBackupSyncIteratorHandler();
        return directPersistencyBackupSyncIteratorHandler.getNextBatch();
    }

    @Override
    public String getName() {
        return _spaceName;
    }

    @Override
    public boolean isEmbedded() {
        return false;
    }

    ////////////////////////////////////////
    // Admin Operations
    ////////////////////////////////////////

    @Override
    public void ping() throws RemoteException {
        if (_spaceState.isTerminated())
            throw new SpaceUnavailableException(getServiceName(), "Space [" + getServiceName() + "] is not available. Shutdown or other abort operation in process.");

        if (_spaceState.isStopped() || _engine == null)
            throw new SpaceStoppedException(getServiceName(), "Space [" + getServiceName() + "] is in stopped state.");
    }

    @Override
    public SpaceHealthStatus getSpaceHealthStatus() throws RemoteException {
        ArrayList<Throwable> errors = new ArrayList<>();

        try {
            ping();
        } catch (Exception e) {
            errors.add(e);
        }

        return getSpaceHealthStatusHelper(errors);
    }

    @Override
    public IQueryProcessor getQueryProcessor() {
        return _qp.getStub();
    }

    public IStubHandler getStubHandler() {
        return _stubHandler;
    }

    /**
     * Generates and returns a unique id
     */
    @Override
    public String getUniqueID() throws RemoteException {
        beforeOperation(false, false /*checkQuiesceMode*/, null);
        return _engine.generateUid();
    }

    @Override
    public Class<?> loadRemoteClass(String className) throws ClassNotFoundException {
        return ClassLoaderHelper.loadClass(className, true);
    }

    public SpaceResponseInfo executeAction(SystemTask systemTask) {
        // TODO: Handle common aspects:
        // 		Security
        // 		Memory Manager
        // 		Filters
        // 		etc.
        final SpaceActionExecutor executor = getActionExecutor(systemTask);
        return executor.execute(this, systemTask.getSpaceRequestInfo());
    }

    private SpaceActionExecutor getActionExecutor(SystemTask<?> systemTask) {
        SpaceActionExecutor result = executorMap.get(systemTask.getClass());
        if (result == null)
            throw new UnsupportedOperationException("Could not find executor for request of type " + systemTask.getClass().getName());
        return result;
    }

    @Override
    public boolean isActive() throws RemoteException {
        assertAvailable();
        return isPrimary();
    }

    @Override
    public boolean isActiveAndNotSuspended() throws RemoteException {
        return isActive() && !_quiesceHandler.isOn();
    }

    @Override
    public Boolean isActiveAsync() throws RemoteException {
        return isActive() ? Boolean.TRUE : Boolean.FALSE;
    }

    @Override
    public ClusterTopology checkChunkMapGeneration(int clientGeneration) {
        return this._clusterInfo.getTopology().getGeneration() != clientGeneration ?
                this._clusterInfo.getTopology() : null;

    }

    @Override
    public <T extends RemoteOperationResult> T executeOperation(RemoteOperationRequest<T> request)
            throws RemoteException {
        return _operationsExecutor.executeOperation(request, this, false);
    }

    @Override
    public <T extends RemoteOperationResult> T executeOperationAsync(RemoteOperationRequest<T> request)
            throws RemoteException {
        return executeOperation(request);
    }

    @Override
    public void executeOperationOneway(RemoteOperationRequest<?> request)
            throws RemoteException {
        _operationsExecutor.executeOperation(request, this, true);
    }

    ////////////////////////////////////////
    // CRUD entry Operations
    ////////////////////////////////////////

    public WriteEntryResult write(IEntryPacket entry, Transaction txn, long lease, int modifiers, boolean fromReplication, SpaceContext sc)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException {
        if (UpdateModifiers.isPotentialUpdate(modifiers))
            beginPacketOperation(true, sc, SpacePrivilege.WRITE, entry);
        else
            beginPacketOperation(true, sc, SpacePrivilege.CREATE, entry);

        try {
            if (txn != null && !_engine.isLocalCache() && !fromReplication)
                _engine.getTransactionHandler().checkTransactionDisconnection(entry.getOperationID(), (ServerTransaction) txn);
            if(performBroadcastOperation(entry, sc)){
                checkReplicationIsHealthy();
                WriteEntryResult result = _engine.write(entry, txn, lease, modifiers, fromReplication, true/*origin*/, sc);
                pushBroadcastEntry(entry, lease, false, Long.MAX_VALUE, modifiers);
                return result;
            }
            return _engine.write(entry, txn, lease, modifiers, fromReplication, true/*origin*/, sc);
        } catch (EntryAlreadyInSpaceException e) {
            throw e;
        } catch (RuntimeException e) {
            throw logException(e);
        } finally {
            endPacketOperation();
        }
    }

    public LeaseContext<?>[] writeOld(IEntryPacket[] entries, Transaction txn, long lease, long[] leases, SpaceContext sc, int modifiers)
            throws TransactionException, UnknownTypesException, RemoteException {
        if (UpdateModifiers.isPotentialUpdate(modifiers))
            beginBatchOperation(sc, SpacePrivilege.WRITE, entries);
        else
            beginBatchOperation(sc, SpacePrivilege.CREATE, entries);

        try {
            WriteEntriesResult result = write(entries, txn, lease, leases, sc, 0L, modifiers, false);
            return convertWriteMultipleResult(result, entries, modifiers);
        } catch (WriteMultipleException e) {
            throw e;
        } catch (RuntimeException e) {
            throw logException(e);
        } finally {
            endPacketOperation();
        }
    }

    public WriteEntriesResult write(IEntryPacket[] entries, Transaction txn, long lease, long[] leases, SpaceContext sc, long timeout, int modifiers,
                                    boolean newRouter)
            throws TransactionException, UnknownTypesException, RemoteException {
        if (UpdateModifiers.isPotentialUpdate(modifiers))
            beginBatchOperation(sc, SpacePrivilege.WRITE, entries);
        else
            beginBatchOperation(sc, SpacePrivilege.CREATE, entries);

        try {
            if (txn != null && !_engine.isLocalCache() && entries.length >0)
                _engine.getTransactionHandler().checkTransactionDisconnection(entries[0].getOperationID(), (ServerTransaction) txn);
            if(performBroadcastOperation(entries[0], sc)) {
                checkReplicationIsHealthy();
                WriteEntriesResult writeEntriesResult = _engine.write(entries, txn, lease, leases, modifiers, sc, timeout, newRouter);
                pushBroadcastEntries(entries, lease, leases, timeout, modifiers);
                return writeEntriesResult;
            }
            return _engine.write(entries, txn, lease, leases, modifiers, sc, timeout, newRouter);
        } catch (WriteMultipleException e) {
            throw e;
        } catch (RuntimeException e) {
            throw logException(e);
        } finally {
            endPacketOperation();
        }
    }

    private LeaseContext<?>[] convertWriteMultipleResult(WriteEntriesResult result, IEntryPacket[] entries, int modifiers)
            throws WriteMultipleException {
        WriteEntryResult[] results = result.getResults();
        Exception[] errors = result.getErrors();

        if (errors != null) {
            WriteResultImpl[] partialFailureResults = new WriteResultImpl[errors.length];
            for (int i = 0; i < results.length; i++) {
                if (errors[i] != null)
                    partialFailureResults[i] = WriteResultImpl.createErrorResult(errors[i]);
                else {
                    boolean noWriteLease = _engine.isNoWriteLease(entries[i], modifiers, false /*fromReplication*/);
                    LeaseContext<?> lease = results[i] == null ? null : results[i].createLease(entries[i].getTypeName(), this, noWriteLease);
                    partialFailureResults[i] = WriteResultImpl.createLeaseResult(lease);
                }
            }
            throw new WriteMultipleException(partialFailureResults);
        }

        LeaseContext<?>[] leases = new LeaseContext[entries.length];
        boolean noReturnValue = UpdateModifiers.isNoReturnValue(modifiers);
        for (int i = 0; i < leases.length; i++) {
            boolean noWriteLease = _engine.isNoWriteLease(entries[i], modifiers, false /*fromReplication*/);
            if (results[i] != null) {
                if (!noReturnValue)
                    leases[i] = results[i].createLease(entries[i].getTypeName(), this, noWriteLease);
                else if (entries[i].getUID() == null)
                    leases[i] = LeaseInitializer.createDummyLease(results[i].getUid(), results[i].getVersion());
            }
        }
        return leases;
    }

    public AnswerPacket update(IEntryPacket entry, Transaction txn, long lease, long timeout,
                               SpaceContext sc, int modifiers, boolean newRouter)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException, InterruptedException {
        if (UpdateModifiers.isPotentialUpdate(modifiers))
            beginPacketOperation(true, sc, SpacePrivilege.WRITE, entry);
        else
            beginPacketOperation(true, sc, SpacePrivilege.CREATE, entry);

        try {
            if (txn != null && !_engine.isLocalCache())
                _engine.getTransactionHandler().checkTransactionDisconnection(entry.getOperationID(), (ServerTransaction) txn);
            boolean broadcastOperation = performBroadcastOperation(entry, sc);
            if(broadcastOperation)
                checkReplicationIsHealthy();
            AnswerPacket result;
            if (UpdateModifiers.isUpdateOrWrite(modifiers)) {
                UpdateOrWriteContext ctx = new UpdateOrWriteContext(entry, lease, timeout, txn, sc, modifiers, false, true, false);
                result = updateOrWrite(ctx, newRouter);
            } else if (UpdateModifiers.isWriteOnly(modifiers)) {
                boolean fromReplication = false;
                WriteEntryResult writeResult = write(entry, txn, lease, modifiers, fromReplication, sc);
                if (UpdateModifiers.isNoReturnValue(modifiers))
                    result = null;
                else if (newRouter)
                    result = new AnswerPacket(writeResult);
                else {
                    LeaseContext<?> leaseResult = writeResult.createLease(entry.getTypeName(), this,
                            _engine.isNoWriteLease(entry, modifiers, fromReplication));
                    result = new AnswerPacket(leaseResult);
                }
            } else {
                ExtendedAnswerHolder aHolder = _engine.update(entry, txn, lease, timeout, sc, false, true/*origin*/, newRouter, modifiers);
                result = aHolder != null ? aHolder.m_AnswerPacket : null;
            }
            if(broadcastOperation)
                pushBroadcastEntry(entry, lease, true, timeout, modifiers);
            if (newRouter)
                return result;
            return UpdateModifiers.isNoReturnValue(modifiers) ? null : result;
        } catch (RuntimeException e) {
            throw logException(e);
        } finally {
            endPacketOperation();
        }
    }

    private Object[] updateMultiple(IEntryPacket[] entries, Transaction txn, long[] leases,
                                    SpaceContext sc, int modifiers)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException {
        if (UpdateModifiers.isPotentialUpdate(modifiers))
            beginBatchOperation(sc, SpacePrivilege.WRITE, entries);
        else
            beginBatchOperation(sc, SpacePrivilege.CREATE, entries);

        if (UpdateModifiers.isUpdateOrWrite(modifiers))
            return _engine.updateOrWrite(entries, txn, leases, sc, modifiers, false, false);
        if (UpdateModifiers.isWriteOnly(modifiers)) //need to loop and so each entry will get its lease.
        {
            Object[] res = new Object[entries.length];
            boolean fromReplication = false;
            for (int i = 0; i < entries.length; ++i) {
                WriteEntryResult writeResult = _engine.write(entries[i], txn, leases[i], modifiers, fromReplication, true, sc);
                LeaseContext<?> lease = writeResult.createLease(entries[i].getTypeName(), this,
                        _engine.isNoWriteLease(entries[i], modifiers, fromReplication));
                // update API should return null for new objects, returning only UID at this point will result in null to the end user
                // where returning LeaseContext at this point will return this object to the user.
                res[i] = lease.getUID();
            }
            return res;
        }


        try {
            return _engine.updateMultiple(entries, txn, leases, sc, modifiers, false /*newRouter*/);
        } catch (RuntimeException e) {
            throw logException(e);
        } finally {
            endPacketOperation();
        }
    }

    public Pair<Integer,SingleExplainPlan> count(ITemplatePacket template, Transaction txn, SpaceContext sc, int modifiers)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException {
        beginPacketOperation(true, sc, SpacePrivilege.READ, template);

        try {
            return _engine.count(template, txn, sc, modifiers);
        } catch (RuntimeException e) {
            throw logException(e);
        } finally {
            endPacketOperation();
        }
    }

    public Pair<Integer,SingleExplainPlan> clear(ITemplatePacket template, Transaction txn, int modifiers, SpaceContext sc)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException {
        beginPacketOperation(true, sc, SpacePrivilege.TAKE, template);

        try {
            if(txn != null && !_engine.isLocalCache())
                _engine.getTransactionHandler().checkTransactionDisconnection(template.getOperationID(), (ServerTransaction) txn);
            return _engine.clear(template, txn, sc, modifiers);
        } catch (RuntimeException e) {
            throw logException(e);
        } finally {
            endPacketOperation();
        }
    }

    public AnswerHolder readNew(ITemplatePacket template, Transaction txn, long timeout, boolean ifExists,
                                boolean take, IJSpaceProxyListener listener, SpaceContext sc, boolean returnOnlyUid, int modifiers)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException, InterruptedException {
        beginPacketOperation(true, sc, (take ? SpacePrivilege.TAKE : SpacePrivilege.READ), template);

        try {
            if (txn != null && take && !_engine.isLocalCache())
                _engine.getTransactionHandler().checkTransactionDisconnection(template.getOperationID(), (ServerTransaction) txn);

            AnswerHolder answerHolder = _engine.read(template, txn, timeout, ifExists, take, sc, returnOnlyUid, false/*fromRepl*/,
                    true/*origin*/, modifiers);

            AnswerPacket answerPacket = answerHolder != null ? answerHolder.getAnswerPacket() : null;
            if (answerPacket != null && answerPacket.m_EntryPacket != null)
                applyEntryPacketOutFilter(answerPacket.m_EntryPacket, modifiers, template.getProjectionTemplate());

            if( !take && answerHolder != null) {
                _engine.updateObjectTypeReadCounts(answerHolder.getServerTypeDesc(), template);
            }

            return answerHolder;
        } catch (RuntimeException e) {
            throw logException(e);
        } finally {
            endPacketOperation();
        }
    }

    public AnswerPacket read(ITemplatePacket template, Transaction txn, long timeout, boolean ifExists,
                             boolean take, IJSpaceProxyListener listener, SpaceContext sc, boolean returnOnlyUid, int modifiers)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException, InterruptedException {
        AnswerHolder answerHolder = readNew(template, txn, timeout, ifExists, take, listener, sc, returnOnlyUid, modifiers);
        return answerHolder != null ? answerHolder.getAnswerPacket() : null;
    }

    public Object asyncRead(ITemplatePacket template, Transaction txn, long timeout, boolean ifExists,
                            boolean take, IJSpaceProxyListener listener, SpaceContext sc, boolean returnOnlyUid, int modifiers,
                            final IFuture result)
            throws UnusableEntryException, TransactionException, UnknownTypeException, RemoteException, InterruptedException {
        boolean isEmbedded = (result != null);
        if (isEmbedded) {
            IResponseContext respContext = new AbstractResponseContext(null, OperationPriority.REGULAR, null, null) {
                @Override
                public void sendResponseToClient(ReplyPacket<?> respPacket) {
                    //Do nothing this method should be called by this sequence
                }

            };
            respContext.setResponseHandler(new IResponseHandler() {

                @Override
                public void handleResponse(IResponseContext responseContext,
                                           ReplyPacket<?> respPacket) {
                    ((LRMIFuture) result).setResultPacket(respPacket);
                }

            });
        }

        Object res = read(template, txn, timeout, ifExists, take, listener, sc,
                returnOnlyUid, modifiers);

        if (isEmbedded) {
            if (res != null) {
                result.setResult(res);
            }

            ResponseContext.clearResponseContext();
        }

        return res;
    }

    public ServerIteratorAnswerHolder getNextBatchFromServerIterator(ITemplatePacket template,
                                                                     SpaceContext sc, int modifiers, ServerIteratorRequestInfo serverIteratorRequestInfo)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException, InterruptedException {
        BatchQueryOperationContext operationContext = new ReadMultipleContext(template, serverIteratorRequestInfo.getBatchSize(), serverIteratorRequestInfo.getBatchSize());
        return (ServerIteratorAnswerHolder) _engine.readMultiple(template, null,0,false,false, sc, false, modifiers, operationContext, null, serverIteratorRequestInfo);
    }

    public void closeServerIterator(UUID uuid){
        _engine.closeServerIterator(uuid);
    }

    public void renewServerIteratorLease(UUID uuid){
        _engine.renewServerIteratorLease(uuid);
    }

    public IEntryPacket[] readMultiple(ITemplatePacket template, Transaction txn, boolean take,
                                       int maxEntries, SpaceContext sc, boolean returnOnlyUid, int modifiers)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException {
        AnswerHolder answerHolder = readMultiple(template, txn, take,
                maxEntries, sc, returnOnlyUid, modifiers, maxEntries /*minEntries*/, 0L /*timeout*/, false /* isIfExist*/);
        return answerHolder != null ? answerHolder.getEntryPackets() : null;
    }


    public AnswerHolder readMultiple(ITemplatePacket template, Transaction txn, boolean take,
                                     int maxEntries, SpaceContext sc, boolean returnOnlyUid, int modifiers, int minEntries, long timeout, boolean isIfExist)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException {
        beginPacketOperation(true, sc, (take ? SpacePrivilege.TAKE : SpacePrivilege.READ), template);

        try {
            BatchQueryOperationContext operationContext = take
                    ? new TakeMultipleContext(template, maxEntries, minEntries)
                    : new ReadMultipleContext(template, maxEntries, minEntries);
            AnswerHolder ah = null;
            if (take && txn != null && !_engine.isLocalCache()) {
                    _engine.getTransactionHandler().checkTransactionDisconnection(template.getOperationID(),(ServerTransaction)txn);

            }
            ah = _engine.readMultiple(template, txn, timeout, isIfExist,
                 take, sc, returnOnlyUid, modifiers, operationContext, null /*aggregatorContext*/, null);

            if( !take && ah != null) {
                _engine.updateObjectTypeReadCounts(ah.getServerTypeDesc(), template);
            }

            if (ah == null)
                 return null;
            if (ah.getException() != null) {
                Exception  retex=ah.getException();
                if (retex instanceof RuntimeException)
                    throw (RuntimeException) retex;
                if (retex instanceof InterruptedException)
                    throw (InterruptedException) retex;
                if (retex instanceof TransactionException)
                    throw (TransactionException) retex;
                if (retex instanceof UnusableEntryException)
                    throw (UnusableEntryException) retex;
                if (retex instanceof UnknownTypeException)
                    throw (UnknownTypeException) retex;
                if (retex instanceof RemoteException)
                    throw (RemoteException) retex;
            }
            if (ah.getEntryPackets() == null)
                return null;

            IEntryPacket[] results = ah.getEntryPackets();

            for (IEntryPacket packet : results) {
                applyEntryPacketOutFilter(packet, modifiers, template.getProjectionTemplate());
            }
            return ah;
        } catch (BatchQueryException batchEx) {
            if (batchEx.getResults() != null) {
                for (Object result : batchEx.getResults()) {
                    if (result != null && result instanceof IEntryPacket)
                        applyEntryPacketOutFilter((IEntryPacket) result,
                                modifiers, template.getProjectionTemplate());
                }
            }
            throw batchEx;

        } catch (RuntimeException e) {
            throw logException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(logException(e));
        } finally {
            endPacketOperation();
        }
    }

    public static void applyEntryPacketOutFilter(IEntryPacket entryPacket, int modifiers, AbstractProjectionTemplate projectionTemplate) {
        if (projectionTemplate != null)
            projectionTemplate.filterOutNonProjectionProperties(entryPacket);

        if (Modifiers.contains(modifiers, Modifiers.RETURN_STRING_PROPERTIES))
            DocumentObjectConverterInternal.instance().convertNonPrimitivePropertiesToStrings(entryPacket);
        else if (Modifiers.contains(modifiers, Modifiers.RETURN_DOCUMENT_PROPERTIES))
            DocumentObjectConverterInternal.instance().convertNonPrimitivePropertiesToDocuments(entryPacket);
    }

    public IEntryPacket[] readByIds(ITemplatePacket template, Transaction txn, boolean take,
                                    SpaceContext spaceContext, int modifiers)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException, InterruptedException {
        try {
            beginPacketOperation(true, spaceContext, (take ? SpacePrivilege.TAKE : SpacePrivilege.READ), template);

            ReadByIdsContext readByIdsContext = new ReadByIdsContext(template, true);

            if (take && txn != null && !_engine.isLocalCache())
                _engine.getTransactionHandler().checkTransactionDisconnection(template.getOperationID(), (ServerTransaction) txn);

            _engine.readByIds((AbstractIdsQueryPacket) template, txn, take, spaceContext, modifiers, readByIdsContext);

            for (IEntryPacket packet : readByIdsContext.getResults()) {
                if (packet != null)
                    applyEntryPacketOutFilter(packet, modifiers, template.getProjectionTemplate());
            }

            return readByIdsContext.getResults();
        } catch (ReadTakeByIdsException e) {
            for (ReadTakeByIdResult result : e.getResults()) {
                if (result.isError())
                    continue;

                Object entry = result.getObject();
                if (entry instanceof IEntryPacket)
                    applyEntryPacketOutFilter((IEntryPacket) entry, modifiers, template.getProjectionTemplate());
            }
            throw e;
        } catch (RuntimeException e) {
            throw logException(e);
        } finally {
            endPacketOperation();
        }
    }

    public GSEventRegistration notify(ITemplatePacket template, Transaction txn, long lease, SpaceContext sc,
                                      NotifyInfo info)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException {
        beginPacketOperation(true, sc, SpacePrivilege.READ, template);

        try {
            if (txn != null)
                throw new UnsupportedOperationException("Notification registration with transactions is no longer supported.");
            info.applyDefaults(_clusterPolicy);

            // When using guaranteed notifications we need "two-way" notifications
            if (info.isGuaranteedNotifications()) {
                if (!getEngine().supportsGuaranteedNotifications())
                    throw new IllegalArgumentException("Guaranteed notifications are supported only on primary-backup topology.");

                ILRMIProxy listener = (ILRMIProxy) info.getListener();
                Map<String, LRMIMethodMetadata> methodsMetadata = null;
                final boolean oneWay = false;
                methodsMetadata = new HashMap<>();
                methodsMetadata.put("notifyBatch(Lcom/gigaspaces/events/batching/BatchRemoteEvent;)V", new LRMIMethodMetadata(oneWay));
                methodsMetadata.put("notify(Lnet/jini/core/event/RemoteEvent;)V", new LRMIMethodMetadata(oneWay));
                listener.overrideMethodsMetadata(methodsMetadata);
            }

            return _engine.notify(template, lease, false /*fromReplication*/,
                    info.getTemplateUID(), sc, info);
        } catch (RuntimeException e) {
            throw logException(e);
        } finally {
            endPacketOperation();
        }
    }

    public void snapshot(ITemplatePacket template)
            throws UnusableEntryException, RemoteException {
        beforeOperation(false, true /*checkQuiesceMode*/, null);

        try {
            _engine.snapshot(template);
        } catch (RuntimeException e) {
            throw logException(e);
        }
    }

    public void cancel(String entryUID, String classname, int objectType, boolean isFromGateway)
            throws UnknownLeaseException, RemoteException {
        beforeOperation(true, true /*checkQuiesceMode*/, null);

        try {
            _engine.getLeaseManager().cancel(entryUID, classname, objectType,
                    false /*fromReplication*/, true /*origin*/, isFromGateway);
        } catch (RuntimeException e) {
            throw logException(e);
        }
    }

    @Override
    public void cancel(String entryUID, String classname, int objectType)
            throws UnknownLeaseException, RemoteException {
        cancel(entryUID, classname, objectType, false /* isFromGateway */);
    }

    public long renew(String entryUID, String classname, int objectType, long duration, boolean isFromGateway)
            throws UnknownLeaseException, RemoteException {
        beforeOperation(true, true /*checkQuiesceMode*/, null);

        try {
            return _engine.getLeaseManager().renew(entryUID, classname,
                    objectType, duration, false /*fromReplication*/, true /*origin*/, isFromGateway);
        } catch (RuntimeException e) {
            throw logException(e);
        }
    }

    @Override
    public long renew(String entryUID, String classname, int objectType, long duration)
            throws LeaseDeniedException, UnknownLeaseException, RemoteException {
        return renew(entryUID, classname, objectType, duration, false);
    }

    @Override
    public Exception[] cancelAll(String[] entryUIDs, String[] classnames, int[] objectTypes)
            throws RemoteException {
        beforeOperation(true, true /*checkQuiesceMode*/, null);

        try {
            return _engine.getLeaseManager().cancelAll(entryUIDs, classnames, objectTypes);
        } catch (RuntimeException e) {
            throw logException(e);
        }
    }

    @Override
    public Object[] renewAll(String[] entryUIDs, String[] classnames, int[] objectTypes, long[] durations)
            throws RemoteException {
        beforeOperation(true, false /*checkQuiesceMode*/, null);

        try {
            return _engine.getLeaseManager().renewAll(entryUIDs, classnames, objectTypes, durations);
        } catch (RuntimeException e) {
            throw logException(e);
        }
    }

    @Override
    public void abort(TransactionManager mgr, long id)
            throws UnknownTransactionException, RemoteException {
        abort(mgr, Long.valueOf(id));
    }

    @Override
    public void abort(TransactionManager mgr, Object id)
            throws UnknownTransactionException, RemoteException {
        abortImpl(mgr, id, false, null);
    }

    public void abortImpl(TransactionManager mgr, Object id, boolean supportsTwoPhaseReplication, OperationID operationID)
            throws RemoteException, UnknownTransactionException {
        beforeOperation(true, false /*checkQuiesceMode*/, null);

        try {
            _engine.abort(mgr, createServerTransaction(mgr, id), supportsTwoPhaseReplication, operationID);
        } catch (RuntimeException e) {
            throw logException(e);
        }
    }

    @Override
    public void commit(TransactionManager mgr, long id)
            throws UnknownTransactionException, RemoteException {
        commit(mgr, Long.valueOf(id), 1);
    }

    @Override
    public void commit(TransactionManager mgr, Object id)
            throws UnknownTransactionException, RemoteException {
        commit(mgr, id, 1);
    }

    @Override
    public void commit(TransactionManager mgr, long id, int numOfParticipants)
            throws UnknownTransactionException, RemoteException {
        commit(mgr, Long.valueOf(id), numOfParticipants);
    }

    @Override
    public void commit(TransactionManager mgr, Object id, int numOfParticipants)
            throws UnknownTransactionException, RemoteException {
        commitImpl(mgr, id, numOfParticipants, false, null, true);
    }

    public void commitImpl(TransactionManager mgr, Object id,
                           int numOfParticipants, boolean supportsTwoPhaseReplication, OperationID operationID, boolean mayBeFromReplication) throws RemoteException,
            UnknownTransactionException {
        beforeOperation(true, false /*checkQuiesceMode*/, null);

        try {
            _engine.commit(mgr, createServerTransaction(mgr, id, numOfParticipants), supportsTwoPhaseReplication, operationID, mayBeFromReplication);
        } catch (RuntimeException e) {
            throw logException(e);
        }
    }

    @Override
    public int prepare(TransactionManager mgr, long id)
            throws UnknownTransactionException, RemoteException {
        return prepare(mgr, Long.valueOf(id), 1);
    }

    @Override
    public int prepare(TransactionManager mgr, Object id)
            throws UnknownTransactionException, RemoteException {
        return prepare(mgr, id, 1);
    }

    @Override
    public int prepare(TransactionManager mgr, Object id, int numOfParticipants)
            throws UnknownTransactionException, RemoteException {
        return prepareImpl(mgr, id, numOfParticipants, false);
    }

    private int prepareImpl(TransactionManager mgr, Object id,
                            int numOfParticipants, boolean supportsTwoPhaseReplication) throws RemoteException,
            UnknownTransactionException {
        beforeOperation(true, true /*checkQuiesceMode*/, null);

        try {
            final ServerTransaction transaction = createServerTransaction(mgr, id, numOfParticipants);
            if (supportsTwoPhaseReplication && _operationLogger.isTraceEnabled())
                _operationLogger.trace("preparing transaction [" + _engine.createTransactionDetailsString(transaction, null) + "]");
            final int prepareResult = _engine.prepare(mgr, transaction, false /*singleParticipant*/, supportsTwoPhaseReplication, null /*OperationID*/);
            if (supportsTwoPhaseReplication && _operationLogger.isTraceEnabled())
                _operationLogger.trace("prepared transaction [" + _engine.createTransactionDetailsString(transaction, null) + "] result=" + prepareResult);

            return prepareResult;
        } catch (RuntimeException e) {
            throw logException(e);
        }
    }

    public Object prepare(TransactionManager mgr, long id, boolean needClusteredProxy)
            throws UnknownTransactionException, RemoteException {
        int vote = prepareImpl(mgr, Long.valueOf(id), 1, true);
        return needClusteredProxy ? new ExtendedPrepareResult(vote, (IDirectSpaceProxy) getSpaceProxy()) : new ExtendedPrepareResult(vote, null);
    }


    public Object prepare(TransactionManager mgr, Object parm2, boolean needClusteredProxy)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        int vote = prepareImpl(mgr, parm2, 1, true);
        return needClusteredProxy ? new ExtendedPrepareResult(vote, (IDirectSpaceProxy) getSpaceProxy()) : new ExtendedPrepareResult(vote, null);
    }

    public Object prepare(TransactionManager mgr, Object parm2, int numOfParticipants, boolean needClusteredProxy)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        int vote = prepareImpl(mgr, parm2, numOfParticipants, true);
        return needClusteredProxy ? new ExtendedPrepareResult(vote, (IDirectSpaceProxy) getSpaceProxy()) : new ExtendedPrepareResult(vote, null);
    }

    @Override
    public int prepareAndCommit(TransactionManager mgr, long id)
            throws UnknownTransactionException, RemoteException {
        return prepareAndCommit(mgr, Long.valueOf(id));
    }

    @Override
    public int prepareAndCommit(TransactionManager mgr, Object id)
            throws UnknownTransactionException, RemoteException {
        return prepareAndCommitImpl(mgr, id, null);
    }

    public int prepareAndCommitImpl(TransactionManager mgr, Object id, OperationID operationID)
            throws RemoteException, UnknownTransactionException {
        beforeOperation(true, true /*checkQuiesceMode*/, null);

        ServerTransaction st = createServerTransaction(mgr, id);
        try {
            return _engine.prepareAndCommit(mgr, st, operationID);
        } catch (RuntimeException e) {
            logException(e);
            throw e;
        }
    }

    private void renewLocalXtn(TransactionManager mgr, Object id, long time)
            throws LeaseDeniedException, UnknownLeaseException, RemoteException {
        beforeOperation(true, true /*checkQuiesceMode*/, null);

        try {
            _engine.renewXtn(createServerTransaction(mgr, id), time);
        } catch (RuntimeException e) {
            throw logException(e);
        }
    }

    @Override
    public void renewLease(TransactionManager mgr, long id, long time)
            throws LeaseDeniedException, UnknownLeaseException, RemoteException {
        renewLocalXtn(mgr, id, time);
    }

    public Object executeTask(SpaceTask task, Transaction tx, SpaceContext sc, boolean newRouter)
            throws RemoteException, ExecutionException {
        final String typeName = task instanceof SpaceTaskWrapper
                ? ((SpaceTaskWrapper) task).getWrappedTask().getClass().getName()
                : task.getClass().getName();

        boolean isSystemTask = task instanceof SystemTask;
        boolean supportsBackup = false;
        SpacePrivilege privilege = SpacePrivilege.EXECUTE;
        if (isSystemTask) {
            SystemTask<?> systemTask = (SystemTask<?>) task;
            SpaceActionExecutor executor = getActionExecutor(systemTask);
            supportsBackup = executor.supportsBackup();
            privilege = executor.getPrivilege();
            systemTask.getSpaceRequestInfo().setSpaceContext(sc);
        }

        beforeTypeOperation(!supportsBackup, sc, privilege, typeName);

        _engine.getMemoryManager().monitorMemoryUsage(true);

        if (!isSystemTask)
            _engine.invokeFilters(sc, FilterOperationCodes.BEFORE_EXECUTE, task);

        IDirectSpaceProxy spaceProxy = getTaskProxy().getDirectProxy();
        SpaceContext prevContext = null;
        try {
            if (tx != null && newRouter) {
                // Register transaction in space - in case the task does not effect any objects in space
                XtnEntry xtnEntry = _engine.getTransactionHandler().attachToXtnGranular((ServerTransaction) tx, false);

                // Set the transaction as operated upon so it wont be cleaned
                xtnEntry.setOperatedUpon();
            }
            if (isSecuredSpace())
                prevContext = spaceProxy.getSecurityManager().setThreadSpaceContext(_securityInterceptor.trustContext(sc));
            Object result = task.execute(spaceProxy, tx);
            if (!isSystemTask && _engine.getFilterManager()._isFilter[FilterOperationCodes.AFTER_EXECUTE])
                _engine.invokeFilters(sc, FilterOperationCodes.AFTER_EXECUTE, result);
            return result;
        } catch (RemoteException e) {
            throw e;
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally {
            if (isSecuredSpace())
                spaceProxy.getSecurityManager().setThreadSpaceContext(prevContext);

            // Unload the task class.
            // See org.openspaces.core.executor.internal.InternalSpaceTaskWrapper#readTaskInNewClassLoader(ObjectInput in).
            Class<?> oneTimeClass = getOneTimeClassIfExists(task);
            if (oneTimeClass != null) {
                if (_logger.isTraceEnabled()) {
                    _logger.trace("Dropping class of OneTime task " + oneTimeClass.getName());
                }
                ClassLoaderCache.getCache().removeClassLoader(oneTimeClass.getClassLoader());
            }
        }
    }

    private Class<?> getOneTimeClassIfExists(SpaceTask task) {
        if (task instanceof SpaceTaskWrapper) {
            SpaceTaskWrapper wrapper = (SpaceTaskWrapper) task;
            SupportCodeChangeAnnotationContainer supportCodeChange = wrapper.getSupportCodeChangeAnnotationContainer();
            return supportCodeChange != null && supportCodeChange.getVersion().isEmpty() ? wrapper.getWrappedTask().getClass() : null;
        }
        else { // "pure" spaceTask
            boolean isOneTime = false;
            if(task.getClass().isAnnotationPresent(SupportCodeChange.class)){
                SupportCodeChange annotation = task.getClass().getAnnotation(SupportCodeChange.class);
                if (annotation.id().isEmpty()){
                    isOneTime = true;
                }
            }
            return  isOneTime ? task.getClass() : null;
        }
    }

    @Override
    public IReplicationConnectionProxy getReplicationRouterConnectionProxy() throws RemoteException {
        SpaceEngine engine = _engine;
        if (engine == null)
            throw new RemoteException("engine is being created");
        return (IReplicationConnectionProxy) engine.getReplicationManager().getReplicationRouter().getMyStubHolder().getStub();
    }

    @Override
    public NIODetails getNIODetails() {
        return NIOInfoHelper.getDetails();
    }

    @Override
    public NIOStatistics getNIOStatistics() {
        return NIOInfoHelper.getNIOStatistics();
    }

    @Override
    public void enableLRMIMonitoring() throws RemoteException {
        NIOInfoHelper.enableMonitoring();
    }

    @Override
    public void disableLRMIMonitoring() throws RemoteException {
        NIOInfoHelper.disableMonitoring();
    }

    @Override
    public LRMIMonitoringDetails fetchLRMIMonitoringDetails() throws RemoteException {
        return NIOInfoHelper.fetchMonitoringDetails();
    }

    @Override
    public long getCurrentTimestamp() {
        return System.currentTimeMillis();
    }

    @Override
    public OSDetails getOSDetails() {
        return OSHelper.getDetails();
    }

    @Override
    public OSStatistics getOSStatistics() {
        return OSHelper.getStatistics();
    }

    @Override
    public JVMDetails getJVMDetails() {
        return JVMHelper.getDetails();
    }

    @Override
    public JVMStatistics getJVMStatistics() {
        return JVMHelper.getStatistics();
    }

    @Override
    public void runGc() {
        System.gc();
    }

    @Override
    public String[] getZones() {
        return ZoneHelper.getSystemZones();
    }

    @Override
    public boolean isStatisticsAvailable() {
        return _statistics != null;
    }

    @Override
    public String[] getStatisticsStringArray() throws StatisticsNotAvailable {
        assertStatisticsAvailable();
        return _statistics.toStringArray();
    }

    @Override
    public long getStatisticsSamplingRate() throws StatisticsNotAvailable {
        assertStatisticsAvailable();
        return _statistics.getPeriod();
    }

    @Override
    public void setStatisticsSamplingRate(long rate) throws StatisticsNotAvailable {
        assertStatisticsAvailable();
        _statistics.setPeriod(rate);
    }

    @Override
    public Map<Integer, StatisticsContext> getStatistics() throws StatisticsNotAvailable {
        assertStatisticsAvailable();
        return _statistics.getStatistics();
    }

    @Override
    public StatisticsContext getStatistics(int operationCode) throws StatisticsNotAvailable {
        assertStatisticsAvailable();
        return _statistics.getStatistics(operationCode);
    }

    @Override
    public Map<Integer, StatisticsContext> getStatistics(Integer[] operationCodes) throws StatisticsNotAvailable {
        assertStatisticsAvailable();
        Map<Integer, StatisticsContext> result = new HashMap<>(operationCodes.length);
        for (Integer code : operationCodes)
            result.put(code, _statistics.getStatistics().get(code));

        return result;
    }

    @Override
    public StatisticsHolder getHolder() {
        // _engine might be not initialized
        SpaceEngine engine = _engine;
        if (engine == null) {
            return new StatisticsHolder(new long[]{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1});
        }
        if (engine.isMirrorService()) {
            //create mirror specific statistics
            StatisticsHolder mirrorStat = new StatisticsHolder(new long[]{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1});
            mirrorStat.setMirrorStatistics(_engine.getReplicationManager().getMirrorService().getMirrorStatistics());
            return mirrorStat;
        }

        if (!isStatisticsAvailable())
            return new StatisticsHolder(new long[]{-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1});

        long[] totalCounts = new long[StatisticsHolder.getOperationCodes().length];
        for (int i = 0; i < totalCounts.length; i++)
            totalCounts[i] = _statistics.getStatistics(StatisticsHolder.getOperationCodes()[i]).getCurrentCount();

        StatisticsHolder stat = new StatisticsHolder(totalCounts);

        if (engine.isReplicated())
            stat.setReplicationStatistics(engine.getReplicationNode().getAdmin().getStatistics());

        stat.setProcessorQueueSize(engine.getProcessorQueueSize());
        stat.setNotifierQueueSize(engine.getNotifierQueueSize());

        try {
            final long numOfEntries = engine.getCacheManager().getNumberOfEntries();
            final long numOTemplates = engine.getCacheManager().getNumberOfNotifyTemplates();
            final int numOfConnections = countIncomingConnections();
            final int transactions = countTransactions(TransactionInfo.Types.ALL, TransactionConstants.ACTIVE);
            stat.setRuntimeStatisticsHolder(new RuntimeStatisticsHolder(numOfEntries, numOTemplates, numOfConnections, transactions));
        } catch (RemoteException e) {
            //should not happen since this is called locally
            throw new RuntimeException(e);
        }

        stat.setBlobStoreStatistics(engine.getCacheManager().getBlobStoreStatistics());

        return stat;
    }

    @Override
    public Map<String,Object> getMetricSnapshots( Collection<String> prefixes ) throws RemoteException {
        if (_engine == null) return new HashMap<>(0); //return empty map on un-deploy
        Map<String, Object> metricsSnapshotByPrefix = _engine.getMetricsSnapshotByPrefix(prefixes);
        return metricsSnapshotByPrefix;
    }

    private void assertStatisticsAvailable() throws StatisticsNotAvailable {
        if (_statistics == null)
            throw _statNotAvailableEx;

    }

    @Override
    public SpaceRuntimeInfo getRuntimeInfo() throws RemoteException {
        beforeOperation(false, false /*checkQuiesceMode*/, null);
        return _engine.getRuntimeInfo();
    }

    @Override
    public SpaceRuntimeInfo getRuntimeInfo(String className) throws RemoteException {
        beforeOperation(false, false /*checkQuiesceMode*/, null);
        return _engine.getRuntimeInfo(className);
    }

    @Override
    public SpaceConfig getConfig() {
        synchronized (this) {
            //FIX for GS-11826
            //Cause to SpaceConfig initialization if cachePolicy is BlobStore and devices still were not initialized
            if (_spaceConfig != null
                    && _spaceConfig.getCachePolicy().equals(CACHE_POLICY_BLOB_STORE)
                    && _spaceConfig.getBlobStoreDevices() == null) {
                _spaceConfig = null;
            }

            if (_spaceConfig != null)
                return _spaceConfig;

            final Properties spaceProps = JProperties.getSpaceProperties(_configReader.getFullSpaceName());
            _spaceConfig = new SpaceConfig(_spaceName, spaceProps, _containerName, "");
            _spaceConfig.setClusterPolicy(_clusterPolicy);
            _spaceConfig.setClusterInfo(_clusterInfo);
            _spaceConfig.setLoadOnStartup(_jspaceAttr.isLoadOnStartup());
            _spaceConfig.setSpaceState(String.valueOf(_spaceState.getState()));
            final String schemaFilePath = spaceProps.getProperty(Constants.Schemas.SCHEMA_FILE_PATH);
            if (schemaFilePath != null)
                _spaceConfig.setSchemaPath(schemaFilePath);
            initSpaceConfig(_spaceConfig, _configReader, schemaFilePath);
            this.registerToClusterInfoChangedEvent(_spaceConfig);
        }

        return _spaceConfig;
    }

    private void initSpaceConfig(SpaceConfig spaceConfig, SpaceConfigReader configReader, String schemaFilePath) {

        final String schemaName = configReader.getSpaceProperty(Schemas.SCHEMA_ELEMENT, null);

        spaceConfig.setPrivate(configReader.getBooleanSpaceProperty(LookupManager.LOOKUP_IS_PRIVATE_PROP, Boolean.FALSE.toString()));//m_JSpaceAttr.m_isPrivate;

        //this check was added by Evgeny
        //in order to prevent NullPointerException in the case of usage in useLocalCache
        if (schemaName != null)
            spaceConfig.setSchemaName(schemaName);

        spaceConfig.setConnectionRetries(configReader.getSpaceProperty(SpaceProxy.OldRouter.RETRY_CONNECTION, SpaceProxy.OldRouter.RETRY_CONNECTION_DEFAULT));
        spaceConfig.setNotifyRetries(configReader.getSpaceProperty(Engine.ENGINE_NOTIFIER_TTL_PROP, Engine.ENGINE_NOTIFIER_RETRIES_DEFAULT));
        spaceConfig.setExpirationTimeInterval(configReader.getSpaceProperty(LM_EXPIRATION_TIME_INTERVAL_PROP, String.valueOf(LM_EXPIRATION_TIME_INTERVAL_DEFAULT)));

        spaceConfig.setExpirationTimeRecentDeletes(configReader.getSpaceProperty(LM_EXPIRATION_TIME_RECENT_DELETES_PROP,
                String.valueOf(LM_EXPIRATION_TIME_RECENT_DELETES_DEFAULT)));
        spaceConfig.setExpirationTimeRecentUpdate(configReader.getSpaceProperty(LM_EXPIRATION_TIME_RECENT_UPDATES_PROP,
                String.valueOf(LM_EXPIRATION_TIME_RECENT_UPDATES_DEFAULT)));
        spaceConfig.setExpirationStaleReplicas(configReader.getSpaceProperty(LM_EXPIRATION_TIME_STALE_REPLICAS_PROP,
                String.valueOf(LM_EXPIRATION_TIME_STALE_REPLICAS_DEFAULT)));

        spaceConfig.setEngineMemoryUsageEnabled(configReader.getSpaceProperty(Engine.ENGINE_MEMORY_USAGE_ENABLED_PROP, Engine.ENGINE_MEMORY_USAGE_ENABLED_DEFAULT));

        spaceConfig.setEngineMemoryUsageHighPercentageRatio(configReader.getSpaceProperty(
                Engine.ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_PROP, Engine.ENGINE_MEMORY_USAGE_HIGH_PERCENTAGE_RATIO_DEFAULT));

        spaceConfig.setEngineMemoryUsageLowPercentageRatio(configReader.getSpaceProperty(
                Engine.ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_PROP, Engine.ENGINE_MEMORY_USAGE_LOW_PERCENTAGE_RATIO_DEFAULT));

        spaceConfig.setEngineMemoryUsageWriteOnlyBlockPercentageRatio(configReader.getSpaceProperty(
                Engine.ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_PROP, Engine.ENGINE_MEMORY_USAGE_WR_ONLY_BLOCK_PERCENTAGE_RATIO_DEFAULT));

        spaceConfig.setEngineMemoryWriteOnlyCheckPercentageRatio(configReader.getSpaceProperty(
                Engine.ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_PROP, Engine.ENGINE_MEMORY_USAGE_WR_ONLY_CHECK_PERCENTAGE_RATIO_DEFAULT));

        spaceConfig.setEngineMemoryUsageEvictionBatchSize(configReader.getSpaceProperty(
                Engine.ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP, Engine.ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_DEFAULT));

        spaceConfig.setEngineMemoryUsageRetryCount(configReader.getSpaceProperty(
                Engine.ENGINE_MEMORY_USAGE_RETRY_COUNT_PROP, Engine.ENGINE_MEMORY_USAGE_RETRY_COUNT_DEFAULT));

        spaceConfig.setEngineMemoryExplicitGSEnabled(configReader.getSpaceProperty(
                Engine.ENGINE_MEMORY_EXPLICIT_GC_PROP, Engine.ENGINE_MEMORY_EXPLICIT_GC_DEFAULT));

        spaceConfig.setEngineMemoryGCBeforeShortageEnabled(configReader.getSpaceProperty(
                Engine.ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_PROP, Engine.ENGINE_MEMORY_GC_BEFORE_MEMORY_SHORTAGE_DEFAULT));

        setOrUpdateBlobstoreRocksDBAllowDuplicateUIDs(spaceConfig, configReader);

        // Serialization type
        String serilType = configReader.getSpaceProperty(Engine.ENGINE_SERIALIZATION_TYPE_PROP, Engine.ENGINE_SERIALIZATION_TYPE_DEFAULT);
        if (serilType != null)
            spaceConfig.setSerializationType(Integer.parseInt(serilType));

        spaceConfig.setEngineMinThreads(configReader.getSpaceProperty(Engine.ENGINE_MIN_THREADS_PROP, Engine.ENGINE_MIN_THREADS_DEFAULT));
        spaceConfig.setEngineMaxThreads(configReader.getSpaceProperty(Engine.ENGINE_MAX_THREADS_PROP, Engine.ENGINE_MAX_THREADS_DEFAULT));

        //db properties
        boolean isPersitent = configReader.getBooleanSpaceProperty(StorageAdapter.PERSISTENT_ENABLED_PROP, StorageAdapter.PERSISTENT_ENABLED_DEFAULT);
        spaceConfig.setPersistent(isPersitent);
        spaceConfig.setMirrorServiceEnabled(configReader.getBooleanSpaceProperty(
                Mirror.MIRROR_SERVICE_ENABLED_PROP, Mirror.MIRROR_SERVICE_ENABLED_DEFAULT));
        // External Data Source
        //
        spaceConfig.setDataSourceClass(configReader.getSpaceProperty(DataAdapter.DATA_SOURCE_CLASS_PROP, DataAdapter.DATA_SOURCE_CLASS_DEFAULT));
        spaceConfig.setDataClass(configReader.getSpaceProperty(DataAdapter.DATA_CLASS_PROP, DataAdapter.DATA_CLASS_DEFAULT));
        spaceConfig.setQueryBuilderClass(configReader.getSpaceProperty(Constants.DataAdapter.QUERY_BUILDER_PROP, Constants.DataAdapter.QUERY_BUILDER_PROP_DEFAULT));
        spaceConfig.setDataPropertiesFile(configReader.getSpaceProperty(DataAdapter.DATA_PROPERTIES, DataAdapter.DATA_PROPERTIES_DEFAULT));
        spaceConfig.setUsage(configReader.getSpaceProperty(DataAdapter.USAGE, DataAdapter.USAGE_DEFAULT));
        spaceConfig.setSupportsInheritanceEnabled(configReader.getBooleanSpaceProperty(DataAdapter.SUPPORTS_INHERITANCE_PROP, DataAdapter.SUPPORTS_INHERITANCE_DEFAULT));
        spaceConfig.setSupportsVersionEnabled(configReader.getBooleanSpaceProperty(DataAdapter.SUPPORTS_VERSION_PROP, DataAdapter.SUPPORTS_VERSION_DEFAULT));
        spaceConfig.setSupportsPartialUpdateEnabled(configReader.getBooleanSpaceProperty(Constants.DataAdapter.SUPPORTS_PARTIAL_UPDATE_PROP, Constants.DataAdapter.SUPPORTS_PARTIAL_UPDATE_DEFAULT));
        spaceConfig.setSupportsRemoveByIdEnabled(configReader.getBooleanSpaceProperty(Constants.DataAdapter.SUPPORTS_REMOVE_BY_ID_PROP, Constants.DataAdapter.SUPPORTS_REMOVE_BY_ID_DEFAULT));

        spaceConfig.setDataSourceSharedIteratorMode(configReader.getBooleanSpaceProperty(
                DataAdapter.DATA_SOURCE_SHARE_ITERATOR_ENABLED_PROP, DataAdapter.DATA_SOURCE_SHARE_ITERATOR_ENABLED_DEFAULT));

        String dataSourceShareIteratorTTLDefault = DataAdaptorIterator.getDataSourceShareIteratorTTLDefault(Long.parseLong(spaceConfig.getExpirationTimeRecentDeletes()),
                Long.parseLong(spaceConfig.getExpirationTimeRecentUpdates()));

        spaceConfig.setDataSourceSharedIteratorTimeToLive(configReader.getLongSpaceProperty(
                DataAdapter.DATA_SOURCE_SHARE_ITERATOR_TTL_PROP, dataSourceShareIteratorTTLDefault));

        //Query Processor ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        spaceConfig.setQPAutoCommit(configReader.getBooleanSpaceProperty(
                QueryProcessorInfo.QP_AUTO_COMMIT_PROP, QueryProcessorInfo.QP_AUTO_COMMIT_DEFAULT));
        spaceConfig.setQPParserCaseSensetivity(configReader.getBooleanSpaceProperty(
                QueryProcessorInfo.QP_PARSER_CASE_SENSETIVITY_PROP, QueryProcessorInfo.QP_PARSER_CASE_SENSETIVITY_DEFAULT));
        spaceConfig.setQPTraceExecTime(configReader.getBooleanSpaceProperty(
                QueryProcessorInfo.QP_TRACE_EXEC_TIME_PROP, QueryProcessorInfo.QP_TRACE_EXEC_TIME_DEFAULT));
        spaceConfig.setQpTransactionTimeout(configReader.getIntSpaceProperty(
                QueryProcessorInfo.QP_TRANSACTION_TIMEOUT_PROP, QueryProcessorInfo.QP_TRANSACTION_TIMEOUT_DEFAULT));
        spaceConfig.setQpSpaceReadLeaseTime(configReader.getIntSpaceProperty(
                QueryProcessorInfo.QP_SPACE_READ_LEASE_TIME_PROP, QueryProcessorInfo.QP_SPACE_READ_LEASE_TIME_DEFAULT));
        spaceConfig.setQpSpaceWriteLeaseTime(configReader.getLongSpaceProperty(
                QueryProcessorInfo.QP_SPACE_WRITE_LEASE_PROP, QueryProcessorInfo.QP_SPACE_WRITE_LEASE_DEFAULT));
        spaceConfig.setQpDateFormat(configReader.getSpaceProperty(
                QueryProcessorInfo.QP_DATE_FORMAT_PROP, QueryProcessorInfo.QP_DATE_FORMAT_DEFAULT));
        spaceConfig.setQpDateTimeFormat(configReader.getSpaceProperty(
                QueryProcessorInfo.QP_DATETIME_FORMAT_PROP, QueryProcessorInfo.QP_DATETIME_FORMAT_DEFAULT));
        spaceConfig.setQpTimeFormat(configReader.getSpaceProperty(
                QueryProcessorInfo.QP_TIME_FORMAT_PROP, QueryProcessorInfo.QP_TIME_FORMAT_DEFAULT));
        // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


        spaceConfig.setCacheManagerSize(configReader.getSpaceProperty(CacheManager.CACHE_MANAGER_SIZE_PROP, CacheManager.CACHE_MANAGER_SIZE_DEFAULT));

        final String defaultCachePolicyValue = isPersitent ? String.valueOf(
                CacheManager.CACHE_POLICY_LRU) : String.valueOf(CacheManager.CACHE_POLICY_ALL_IN_CACHE);

        spaceConfig.setCachePolicy(configReader.getSpaceProperty(CACHE_POLICY_PROP, defaultCachePolicyValue));

        spaceConfig.setClusterConfigURL(configReader.getSpaceProperty(Cluster.CLUSTER_CONFIG_URL_PROP, Cluster.CLUSTER_CONFIG_URL_DEFAULT, false));
        spaceConfig.setClustered(configReader.getBooleanSpaceProperty(Cluster.IS_CLUSTER_SPACE_PROP, Cluster.IS_CLUSTER_SPACE_DEFAULT));

                    /* Distributed Cache */
        JSpaceAttributes spaceAttr = (JSpaceAttributes) JProperties.getSpaceProperties(configReader.getFullSpaceName());
        if (spaceAttr != null) {
            spaceConfig.setDCacheConfigName(spaceAttr.getDCacheConfigName());
            spaceConfig.setDCacheProperties(spaceAttr.getDCacheProperties());
        }

        // build filter information
        int filterCounter = 0;
        String fnames = configReader.getSpaceProperty(Constants.Filter.FILTER_NAMES_PROP, "");
        StringTokenizer st = new StringTokenizer(fnames, ",");
        spaceConfig.setFiltersInfo(new FiltersInfo[st.countTokens()]);
        while (st.hasMoreElements()) {
            String filterName = st.nextToken().trim();
            FiltersInfo info = new FiltersInfo();
            spaceConfig.setFilterInfoAt(info, filterCounter++);

            // get full filter information
            info.filterName = filterName;
            info.enabled = configReader.getBooleanSpaceProperty(Filter.FILTERS_TAG_NAME + "." + filterName + "." + Filter.FILTER_ENABLED_TAG_NAME, Filter.DEFAULT_FILTER_ENABLE_VALUE);
            info.filterClassName = configReader.getSpaceProperty(Filter.FILTERS_TAG_NAME + "." + filterName + "." + Filter.FILTER_CLASS_TAG_NAME, "");
            info.paramURL = configReader.getSpaceProperty(Filter.FILTERS_TAG_NAME + "." + filterName + "." + Filter.FILTER_URL_TAG_NAME, "");
            String operationsCode = configReader.getSpaceProperty(
                    Filter.FILTERS_TAG_NAME + "." + filterName + "." + Filter.FILTER_OPERATION_CODE_TAG_NAME,
                    Filter.DEFAULT_OPERATION_CODE_VALUE);
            StringTokenizer operStrToken = new StringTokenizer(operationsCode, ",");
            while (operStrToken.hasMoreTokens()) {
                int operCode = Integer.parseInt(operStrToken.nextToken().trim());
                switch (operCode) {
                    case FilterOperationCodes.AFTER_REMOVE:
                        info.afterRemove = true;
                        break;
                    case FilterOperationCodes.AFTER_WRITE:
                        info.afterWrite = true;
                        break;
                    case FilterOperationCodes.BEFORE_CLEAN_SPACE:
                        info.beforeClean = true;
                        break;
                    case FilterOperationCodes.BEFORE_NOTIFY:
                        info.beforeNotify = true;
                        break;
                    case FilterOperationCodes.BEFORE_READ:
                        info.beforeRead = true;
                        break;
                    case FilterOperationCodes.BEFORE_TAKE:
                        info.beforeTake = true;
                        break;
                    case FilterOperationCodes.BEFORE_WRITE:
                        info.beforeWrite = true;
                        break;
                    case FilterOperationCodes.AFTER_READ:
                        info.afterRead = true;
                        break;
                    case FilterOperationCodes.AFTER_TAKE:
                        info.afterTake = true;
                        break;

                    case FilterOperationCodes.BEFORE_GETADMIN:
                        info.beforeGetAdmin = true;
                        break;
                    case FilterOperationCodes.BEFORE_AUTHENTICATION:
                        info.beforeAuthentication = true;
                        break;
                    case FilterOperationCodes.BEFORE_UPDATE:
                        info.beforeUpdate = true;
                        break;
                    case FilterOperationCodes.AFTER_UPDATE:
                        info.afterUpdate = true;
                        break;

                }// switch()
            }// while operCode...

            if (filterName.equalsIgnoreCase(Filter.DEFAULT_FILTER_SECURITY_NAME)) {
                String schemaRef = schemaFilePath != null ? " [" + schemaFilePath + "] " : " ";
                _logger.warn("The filter [" + Filter.DEFAULT_FILTER_SECURITY_NAME
                        + "] defined in the space schema file" + schemaRef + "is no longer in use since 7.0.1; Please remove it.");
            }
        }// while filters...

        // proxy
        spaceConfig.setProxyConnectionMode(configReader.getSpaceProperty(SpaceProxy.OldRouter.CONNECTION_MONITOR, Constants.SpaceProxy.OldRouter.CONNECTION_MONITOR_DEFAULT));
        spaceConfig.setProxyMonitorFrequency(configReader.getLongSpaceProperty(Constants.SpaceProxy.OldRouter.MONITOR_FREQUENCY, Constants.SpaceProxy.OldRouter.MONITOR_FREQUENCY_DEFAULT));
        spaceConfig.setProxyDetectorFrequency(configReader.getLongSpaceProperty(Constants.SpaceProxy.OldRouter.DETECTOR_FREQUENCY, Constants.SpaceProxy.OldRouter.DETECTOR_FREQUENCY_DEFAULT));
        spaceConfig.setProxyConnectionRetries(configReader.getIntSpaceProperty(Constants.SpaceProxy.OldRouter.CONNECTION_RETRIES, Constants.SpaceProxy.OldRouter.CONNECTION_RETRIES_DEFAULT));
    }

    @Override
    public void start() throws RemoteException {
        if (!Boolean.getBoolean("com.gs.enabled-backward-space-lifecycle-admin"))
            throw new UnsupportedOperationException("Invoking start on a space has reached end of life and it is no longer supported");

        startInternal();
    }

    public void startInternal() throws RemoteException {
        if (_logger.isDebugEnabled())
            _logger.debug("Starting space [url=" + getURL() + "]...");

        if (_spaceState.getState() == ISpaceState.STARTED)
            throw new SpaceAlreadyStartedException("Space [" + getServiceName() + "] already started");

        final boolean isLookupServiceEnabled = isLookupServiceEnabled();
        try {
            _clusterFailureDetector = initClusterFailureDetector(_clusterPolicy);
            _engine = new SpaceEngine(this);

            if (zookeeperTopologyHandler != null && _clusterInfo.isChunksRouting()) {
                zookeeperClient = createZooKeeperClient();
                zookeeperTopologyHandler.addListener(zookeeperClient, _spaceConfig, getPartitionIdOneBased());
            }
            if (isLookupServiceEnabled)
                registerLookupService();
            _directPersistencyRecoveryHelper = initDirectPersistencyRecoveryHelper(_clusterPolicy);
            _leaderSelector = initLeaderSelectorHandler(isLookupServiceEnabled);
            initReplicationStateBasedOnActiveElection();
            recover();
            _qp = createQueryProcessor();

            if (_logger.isInfoEnabled() && !isPrivate()) {
                long duration = System.currentTimeMillis() - _container.getStartTime();
                _logger.info("Space started [duration=" + duration / 1000d + "s, " +
                        "url=" + getURL() + ", " + _engine.getCacheManager().getConfigInfo() + "]");
            }
        } catch (Throwable ex) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to start space with url [" + getURL() + "]", ex);

            shutdown(false/*isDestroy*/, true/*shutdowContainer*/);
            throw new RemoteException(ex.getMessage(), ex);
        }

    }

    private ServiceTemplate buildServiceTemplate(ClusterPolicy clusterPolicy, boolean isPrimary) {
        Entry clusterName = new ClusterName(clusterPolicy.m_ClusterName);
        ClusterGroup clusterGroup = new ClusterGroup(clusterPolicy.m_FailOverPolicy.getElectionGroupName(),
                null, /* repl */
                null/* load-balance */);

        com.j_spaces.lookup.entry.State state = new com.j_spaces.lookup.entry.State();
        state.setElectable(Boolean.TRUE);

        ContainerName containerName = new ContainerName(_containerName);
        Entry[] spaceTemplAttr;
        if (isPrimary) {
            spaceTemplAttr = new Entry[]{clusterName, clusterGroup, state, containerName};
        } else {
            spaceTemplAttr = new Entry[]{clusterName, clusterGroup, state};
        }
        Class[] serviceTypes = new Class[]{Service.class};

        return new ServiceTemplate(null, serviceTypes, spaceTemplAttr);
    }

    private void registerLookupService() throws CreateException {
        try {
            registerLookupService(new WeakDiscoveryListener(this), false);
        } catch (Throwable e) {
            throw new CreateException("Failed to create Join Manager for [" + getServiceName() + "]", e);
        }
    }

    private DirectPersistencyRecoveryHelper initDirectPersistencyRecoveryHelper(ClusterPolicy clusterPolicy) {
        DirectPersistencyRecoveryHelper result = null;
        boolean isBlobStore = _configReader.getIntSpaceProperty(CACHE_POLICY_PROP, "-1") == CACHE_POLICY_BLOB_STORE;
        if (clusterPolicy != null && clusterPolicy.isPrimaryElectionAvailable() && isBlobStore && _engine.getCacheManager().isPersistentBlobStore()) {
            //set state to starting to allow cleaning in case beforePrimaryElectionProcess throws exception
            _spaceState.setState(JSpaceState.STARTING);
            result = new DirectPersistencyRecoveryHelper(this, _logger);
            result.beforePrimaryElectionProcess();
        }
        return result;
    }

    private void recover() throws Exception {
        _recovering.set(true);

        // create a recovery manager
        _recoveryManager = new RecoveryManager(this);

        _broadcastTableHandler = new BroadcastTableHandler(this);

        // Perform space recovery according to election state
        if (_leaderSelector != null)
            initAndStartPrimaryBackupSpace();
        else
            initAndStartRegularSpace();

        _recovering.set(false);
    }

    private static ClusterFailureDetector initClusterFailureDetector(ClusterPolicy clusterPolicy) {
        ClusterFailureDetector result = null;
        if (clusterPolicy != null) {
            // First check if primary selection should be activated and initialize the primary selector
            if (clusterPolicy.isPrimaryElectionAvailable()) {
                result = new ClusterFailureDetector(
                        clusterPolicy.m_FailOverPolicy.getActiveElectionConfig().getFDHConfig());

            }
            // initialize the failure detector with default configuration used to detect replication channel disconnection
            else if (clusterPolicy.m_ReplicationPolicy != null) {
                result = new ClusterFailureDetector();
            }
        }

        return result;
    }

    @Override
    public void stop() throws RemoteException {
        if (!Boolean.getBoolean("com.gs.enabled-backward-space-lifecycle-admin"))
            throw new UnsupportedOperationException("Invoking stop on a space has reached end of life and it is no longer supported");

        stopInternal();

    }

    public void stopInternal() throws RemoteException {
        if (!_spaceState.isAborted() && _spaceState.isStopped())
            throw new SpaceAlreadyStoppedException(getServiceName(), "Space [" + getServiceName() + "] already stopped");


        int previousState = _spaceState.getState();
        if (!_spaceState.isAborted())
            _spaceState.setState(ISpaceState.STOPPED);

        try {
            if (_leaderSelector != null)
                _leaderSelector.terminate();

            try {
                close();
            } catch(Exception e) {
                _logger.info("Ignoring caught exception: " + e + " while stopping Space");
            }

            // Close the query processor
            if (_qp != null) {
                try {
                    _qp.close();
                } catch (RemoteException e) {
                    // Nothing to do...
                }
            }

            //storeState(JSpaceState.STOPPED);

            modifyLookupAttributes(
                    new Entry[]{new State()},
                    new Entry[]{new State(ISpaceState.STOPPED, Boolean.FALSE, Boolean.FALSE)});

            _logger.info("Space Stopped successfully");
        } catch (Exception ex) {
            _logger.warn("Failed to stop space", ex);
            if (!_spaceState.isAborted())
                _spaceState.setState(previousState);
            throw new RemoteException(ex.getMessage(), ex);
        }
    }

    @Override
    public void restart() throws RemoteException {
        if (!Boolean.getBoolean("com.gs.enabled-backward-space-lifecycle-admin"))
            throw new UnsupportedOperationException("Invoking restart on a space has reached end of life and it is no longer supported");

        beforeOperation(false, false /*checkQuiesceMode*/, null);

        // TODO make FILTER-OPERATION ===> BEFORE RESTART
        try {

            //String getSpaceSchemaPath( schemaName );

            String spaceConfigFileURL = JProperties.getURL(getServiceName());
            if (spaceConfigFileURL != null)
                JProperties.setUrlWithoutSchema(getServiceName(), null, spaceConfigFileURL);
            else {
                //System.out.println( "> m_JSpaceAttr=" + m_JSpaceAttr );
                String schemaName = _jspaceAttr.getSchemaName();
                InputStream inputStream = ResourceLoader.findSpaceSchema(schemaName).getInputStream();
                JProperties.setUrlWithSchema(getServiceName(), _customProperties, inputStream);
            }
            //set null because configuration was
            //changed and next time when getConfig() will be invoked
            //SpaceConfig instance will be filled
            _spaceConfig = null;//m_NewSpaceConfig;


            _spaceConfig = getConfig();

            //Evgeny 3.11.2005
            //set properties that will be used for db name evaluation from space schema
            System.setProperty(SystemProperties.DB_CONTAINER_NAME, _containerName);
            System.setProperty(SystemProperties.DB_SPACE_NAME, _spaceName);

            Properties spaceProperties = JProperties.getSpaceProperties(getServiceName());

            //check if space was created via space URL and it is clustered
            if (_url != null && _url.getClusterSchema() != null) {
                spaceProperties.setProperty(
                        getServiceName() + "." + Constants.SPACE_CONFIG_PREFIX + "." + Cluster.IS_CLUSTER_SPACE_PROP, Boolean.TRUE.toString());
                _spaceConfig.setClustered(true);
            }
        } catch (Exception ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error(ex.toString(), ex);
            }
            throw new RemoteException(ex.getMessage(), ex.getCause());
        }


        internalClean(false/*isInternalUse*/, true/*isRestart*/, true/*isWarmInit*/);

        /** Update lookup attributes */
        String clusterName = "NONE";
        if (getClusterPolicy() != null)
            clusterName = getClusterPolicy().m_ClusterName;

        modifyLookupAttributes(
                new Entry[]{new ClusterName(), new State()},
                new Entry[]{new ClusterName(clusterName), new State(getState(), Boolean.TRUE, Boolean.TRUE)});
    }

    @Override
    public SpaceInstanceRemoteClassLoaderInfo getSpaceInstanceRemoteClassLoaderInfo() {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        if(contextClassLoader instanceof ServiceClassLoader){
            CodeChangeClassLoadersManager codeChangeClassLoadersManager = ((ServiceClassLoader) contextClassLoader).getCodeChangeClassLoadersManager();
            return codeChangeClassLoadersManager.createSpaceInstanceRemoteClassLoaderInfo();
        }
        throw new UnsupportedOperationException("not ServiceClassLoader");
    }

    /**
     * return the replication status of all my replication group returns an array of remote member
     * names and an array of replication status to them.
     */
    @Override
    public Object[] getReplicationStatus() throws RemoteException {
        beforeOperation(false, false /*checkQuiesceMode*/, null);
        return _engine.getReplicationStatus();
    }

    @Override
    public ClusterPolicy getClusterPolicy() {
        return _clusterPolicy;
    }

    public SpaceClusterInfo getClusterInfo() {
        return _clusterInfo;
    }

    @Override
    public BasicTypeInfo getClassTypeInfo(String className) throws RemoteException {
        ITypeDesc typeDesc = getClassDescriptor(className);
        if (typeDesc.isInactive())
            return null;

        return new BasicTypeInfo(typeDesc);
    }

    /**
     * Drop all Class entries and all its templates from the space. Calling this method will remove
     * all internal meta data related to this class stored in the space. When using persistent
     * spaced the relevant RDBMS table will be dropped. It is the caller responsibility to ensure
     * that no entries from this class are written to the space while this method is called. This
     * method is protected through the space Default Security Filter. Admin permissions required to
     * execute this request successfully.
     *
     * @param className - name of class to delete.
     **/
    @Override
    public void dropClass(String className, SpaceContext sc)
            throws RemoteException, DropClassException {
        beforeTypeOperation(false, sc, SpacePrivilege.ALTER, className);

        _engine.dropClass(className);

        //handle cleanings in proxies
        if (_embeddedProxy != null)
            _embeddedProxy.directDropClass(className);

        if (_clusteredProxy != null)
            _clusteredProxy.directDropClass(className);

        if (_taskProxy != null)
            _taskProxy.directDropClass(className);

        LRMIClassLoadersHolder.dropClass(className);
    }

    @Override
    public int getState() {
        return _spaceState.getState();
    }

    ////////////////////////////////////////
    // IInternalRemoteJSpaceAdmin methods
    ////////////////////////////////////////

    @Override
    public void disableStub() throws RemoteException {
        try {
            if (_spaceStub != null) {
                _stubHandler.unexportObject(this);
                _spaceStub = null;
            }
        } catch (Exception e) {
            throw new RemoteException(e.getMessage(), e);
        }
    }

    @Override
    public RuntimeHolder getRuntimeHolder() throws RemoteException {
        beforeOperation(false, false /*checkQuiesceMode*/, null);
        Object[] replicationStatus = null;
        if (_engine.isReplicated())
            replicationStatus = _engine.getReplicationNode().getAdmin().getStatus();

        return new RuntimeHolder(getSpaceMode(), replicationStatus, getState(), getQuiesceHandler().getSuspendInfo());
    }

    @Override
    public boolean isStartedWithinGSC() {
        return System.getProperty("com.gigaspaces.gsc.running", "false").equals("true");
    }

    /**
     * Returns the deploy path of this space instance, when deployed in the service grid. NOTE: When
     * not deployed in the service grid, this method may return null.
     */
    public String getDeployPath() {
        return _deployPath;
    }

    @Override
    public SpaceCopyStatus spaceCopy(String remoteUrl, ITemplatePacket template,
                                     boolean includeNotifyTemplates, int chunkSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SpaceCopyStatus spaceCopy(String remoteUrl, ITemplatePacket template,
                                     boolean includeNotifyTemplates, int chunkSize, SpaceContext sc)
            throws RemoteException {
        beginPacketOperation(false, sc, SpacePrivilege.WRITE, template);

        try {
            final int findTimeout = 3 * 1000;
            SpaceURL remoteSpaceUrl = SpaceURLParser.parseURL(remoteUrl);
            /** get remote space object */
            remoteSpaceUrl.setProperty(SpaceURL.TIMEOUT, String.valueOf(findTimeout));
            //GERSHON sourceRemoteUrl = SpaceURL.concatAttrIfNotExist( sourceRemoteUrl, SpaceURL.TIMEOUT, String.valueOf( findTimeout ) );
            IDirectSpaceProxy abstractSpaceProxy;
            try {
                abstractSpaceProxy = (IDirectSpaceProxy) SpaceFinder.find(remoteSpaceUrl);
            } catch (FinderException ex) {
                SpaceCopyStatusImpl copyStatus = new SpaceCopyStatusImpl(SpaceCopyStatusImpl.COPY_TYPE, getServiceName());
                copyStatus.setCauseException(ex);

                if (_logger.isDebugEnabled())
                    _logger.debug(ex.toString(), ex);

                return copyStatus;
            }

            String sourceMemberName = abstractSpaceProxy.getRemoteMemberName();
            IRemoteSpace remoteJSpace = abstractSpaceProxy.getRemoteJSpace();

            //we assume remote security context is the same as local security context; otherwise should use spaceCopy(..,remoteJSpace,..)
            return spaceCopy(remoteJSpace, remoteUrl, sourceMemberName, template, includeNotifyTemplates, chunkSize, sc, sc);

        } catch (Exception ex) {
            throw new RemoteException(ex.getMessage(), ex);
        } finally {
            endPacketOperation();
        }
    }

    @Override
    public SpaceCopyStatus spaceCopy(IRemoteSpace remoteProxy, String remoteSpaceUrl,
                                     String remoteSpaceName, ITemplatePacket template, boolean includeNotifyTemplates,
                                     int chunkSize, SpaceContext sc, SpaceContext remoteSpaceContext)
            throws RemoteException {
        checkPacketAccessPrivileges(sc, SpacePrivilege.WRITE, template);
        try {
            SpaceURL url = SpaceURLParser.parseURL(remoteSpaceUrl);
            ISpaceCopyReplicaState spaceCopyReplica = _engine.spaceCopyReplica(url,
                    remoteSpaceName, remoteProxy, template, includeNotifyTemplates,
                    chunkSize, sc, remoteSpaceContext);

            return spaceCopyReplica.getCopyResult().toOldResult(SpaceCopyStatusImpl.COPY_TYPE, remoteSpaceName);
        } catch (MalformedURLException e) {
            throw new RemoteException(e.getMessage(), e);
        }
    }

    @Override
    public TransactionInfo[] getTransactionsInfo(int type, int status) throws RemoteException {
        return _engine.getTransactionsInfo(type, status);
    }

    @Override
    public int countTransactions(int type, int status) throws RemoteException {
        return _engine.countTransactions(type, status);
    }

    @Override
    public List<TemplateInfo> getTemplatesInfo(String className) throws RemoteException {
        return _engine.getTemplatesInfo(className);
    }

    @Override
    public List<ITransportConnection> getConnectionsInfo() throws RemoteException {
        if (_engine.isLocalCache())
            return Collections.emptyList();
        IRemoteSpace dynamicProxy = ((LRMISpaceImpl) getSpaceStub()).getDynamicProxy();
        long remoteObjID = TransportProtocolHelper.getRemoteObjID(dynamicProxy);
        return LRMIRuntime.getRuntime().getRemoteObjectConnectionsList(remoteObjID);
    }

    @Override
    public int countIncomingConnections() throws RemoteException {
        return _engine.countIncomingConnections();
    }

    /**
     * getLockedObjects is called by the GUI. it returns Info about all the locked Object by the
     * given Transaction.
     *
     * @param txn - the Transaction that locks the Objects.
     * @return List of UnderTxnLockedObject
     */
    @Override
    public List<UnderTxnLockedObject> getLockedObjects(Transaction txn) throws RemoteException {
        return _engine.getLockedObjects(txn);
    }

    @Override
    public SpaceMode getSpaceMode() {
        return _leaderSelector != null ? _leaderSelector.getSpaceMode() : SpaceMode.PRIMARY;
    }

    public boolean isPrimary() {
        return getSpaceMode() == SpaceMode.PRIMARY;
    }

    public boolean isBackup() {
        return getSpaceMode() == SpaceMode.BACKUP;
    }

    @Override
    public SpaceMode addSpaceModeListener(ISpaceModeListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("Callback listener supplied for space mode changes is <null>.");

        primarySpaceModeListeners.addListener(listener);

        return getSpaceMode();
    }

    @Override
    public void removeSpaceModeListener(ISpaceModeListener listener) {
        primarySpaceModeListeners.removeListener(listener);
    }

    @Override
    public ITypeDesc getClassDescriptor(String className) throws RemoteException {
        return getClassDescriptor(className, false);
    }

    @Override
    public ITypeDesc getClassDescriptor(String className, boolean asVirtualType) throws RemoteException {
        beforeOperation(false, false /*checkQuiesceMode*/, null);
        try {
            return _engine.getClassTypeInfo(className, asVirtualType);
        } catch (Exception ex) {
            throw new RemoteException(ex.getMessage(), ex);
        }
    }

    @Override
    public void forceMoveToPrimary() throws RemoteException {
        if (_leaderSelector == null)
            throw new IllegalArgumentException("Space doesn't support primary/backup mode. ");

        _leaderSelector.forceMoveToPrimary();
    }

    @Override
    public String getReplicationDump() {
        return _engine.getReplicationNode().getAdmin().dumpState();
    }

    public void assertAuthorizedForType(String typeName, SpacePrivilege privilege, SpaceContext spaceContext) {
        if (_securityInterceptor != null) {
            _securityInterceptor.intercept(SpaceContextHelper.getSecurityContext(spaceContext), privilege, typeName);
        }
    }

    public void assertAuthorizedForPrivilege(Privilege privilege, SpaceContext spaceContext) {
        if (_securityInterceptor != null) {
            _securityInterceptor.intercept(SpaceContextHelper.getSecurityContext(spaceContext), privilege, null);
        }
    }

    private WorkerManager createWorkerManager() throws RemoteException {
        return new WorkerManager(createSecuredProxy(), getServiceName());
    }

    public boolean isRecovering() {
        return _recovering.get();
    }

    public AtomicBoolean getRecoveryIndicator() {
        return _recovering;
    }

    @Override
    public Map<String, LocalCacheDetails> getLocalCacheDetails() throws RemoteException {
        return isActive() ? _engine.getCacheManager().getLocalCaches() : null;
    }

    @Override
    public Map<String, LocalViewDetails> getLocalViewDetails() throws RemoteException {
        return isActive() ? _engine.getLocalViewRegistrations().get() : null;
    }

    public boolean isLocalCache() {
        return getConfigReader().getBooleanSpaceProperty(Engine.ENGINE_LOCAL_CACHE_MODE_PROP, Engine.ENGINE_LOCAL_CACHE_MODE_DEFAULT);
    }

    /**
     * Igor.G 13/3/2005 ver 5.01. This method will never invoked! We have an abstraction problem
     * with IRemoteJSpaceAdmin, JSpaceAdminProxy and SpaceInternal, so added this non-impl method
     * only to make compiler happy :(
     **/
    @Override
    public SpaceCopyStatus spaceCopy(String remoteUrl, Object template, boolean includeNotifyTemplates, int chunkSize) {
        throw new UnsupportedOperationException("shouldn't of come here ...");
    }

    @Override
    public SpaceCopyStatus spaceCopy(IJSpace remoteSpace, Object template, boolean includeNotifyTemplates, int chunkSize) {
        throw new UnsupportedOperationException("shouldn't of come here ...");
    }

    @Override
    public void discarded(DiscoveryEvent event) {
    }

    @Override
    public void discovered(DiscoveryEvent event) {
        try {
            ServiceRegistrar[] registrars = event.getRegistrars();
            for (int i = 0; i < registrars.length; i++)
                if (_logger.isDebugEnabled()) {
                    _logger.debug("Directory Service (Jini Lookup Service registrar): \n" +
                            "[  Registrar <" + registrars[i].getLocator().getHost() + ':' +
                            registrars[i].getLocator().getPort() + ">  ]" +
                            "[  Member of " + Arrays.asList(registrars[i].getGroups()) +
                            " has been discovered by the <" + getServiceName() + "> space.  ]");
                }
        } catch (Exception ex) {
            if (_logger.isErrorEnabled())
                _logger.error(ex.toString(), ex);
        }
    }

    @Override
    public byte[] getClusterConfigFile() throws RemoteException {
        try {
            String clusterConfigURL = _configReader.getSpaceProperty(Constants.Cluster.CLUSTER_CONFIG_URL_PROP, null);

            ClusterXML clusterXml = new ClusterXML(_url, clusterConfigURL, _spaceName);
            StringBuilder clusterConfigOutput = clusterXml.getClusterConfigDebugOutput();
            if (_logger.isDebugEnabled())
                _logger.debug("Cluster Configuration:\n" + clusterConfigOutput);
            ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
            JSpaceUtilities.domWriter(
                    clusterXml.getXMLRootDocument().getDocumentElement(),
                    new PrintStream(byteArray), "");
            byteArray.flush();
            byteArray.close();
            return byteArray.toByteArray();
        } catch (Exception ex) {
            throw new RemoteException("Fail to obtain a cluster configuration file", ex);
        }
    }

    @Override
    public void setClusterConfigFile(byte[] config) throws RemoteException {
        throw new UnsupportedOperationException("Currently doesn't implemented");
    }

    @Override
    protected void AppendInitialLookupAttributes(List<Entry> lookupAttributes) throws ServiceRegistrationException {
        super.AppendInitialLookupAttributes(lookupAttributes);

        // Get name of this space and build attributes for this proxy service
        lookupAttributes.add(new Name(getName()));

        ClusterName clusterName = null;
        ClusterGroup clusterGroup = null;

        // build cluster information for LookupService entries
        ClusterPolicy clusterPolicy = getClusterPolicy();
        if (clusterPolicy != null) {
            clusterName = new ClusterName(clusterPolicy.m_ClusterName);
            clusterGroup = new ClusterGroup();
            clusterGroup.electionGroup = "";
            clusterGroup.replicationGroup = "";
            clusterGroup.loadBalancingGroup = "";
            if (clusterPolicy.m_FailOverPolicy != null)
                clusterGroup.electionGroup = clusterPolicy.m_FailOverPolicy.getElectionGroupName();
            if (clusterPolicy.m_ReplicationPolicy != null)
                clusterGroup.replicationGroup = clusterPolicy.m_ReplicationPolicy.m_ReplicationGroupName;
            if (clusterPolicy.m_LoadBalancingPolicy != null)
                clusterGroup.loadBalancingGroup = clusterPolicy.m_LoadBalancingPolicy.m_GroupName;
        } else {
            clusterName = new ClusterName("NONE");
            clusterGroup = new ClusterGroup("NONE");
        }

        lookupAttributes.add(clusterName);
        lookupAttributes.add(clusterGroup);
        lookupAttributes.add(new ContainerName(_container.getName()));
        lookupAttributes.add(new ServiceInfo("JavaSpace", LookupManager.MANUFACTURE, LookupManager.VENDOR, PlatformVersion.getOfficialVersion(), "", ""));
        lookupAttributes.add(new HostName(SystemInfo.singleton().network().getHostId()));
        lookupAttributes.add(new State(getState()));
    }

    private LeaderSelectorHandler createZooKeeperLeaderSelector() throws ActiveElectionException {
        try {
            Class clazz = ClassLoaderHelper.loadLocalClass(LEADER_SELECTOR_HANDLER_CLASS_NAME);
            return (LeaderSelectorHandler) clazz.newInstance();
        } catch (Exception e) {
            if (_logger.isErrorEnabled())
                _logger.error("Failed to initialize Leader Selector handler");
            throw new ActiveElectionException("Failed to start [" + (getEngine().getFullSpaceName())
                    + "] Failed to initialize Leader Selector handler.");
        }
    }

    private class LeaderSelectorServiceDiscoveryListener implements ServiceDiscoveryListener {
        CountDownLatch latch;

        public LeaderSelectorServiceDiscoveryListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void serviceAdded(ServiceDiscoveryEvent event) {
            latch.countDown();
        }

        @Override
        public void serviceRemoved(ServiceDiscoveryEvent event) {

        }

        @Override
        public void serviceChanged(ServiceDiscoveryEvent event) {

        }
    }

    @Override
    public void demote(long maxSuspendTime, TimeUnit unit, SpaceContext sc) throws DemoteFailedException, RemoteException {
        assertAuthorizedForPrivilege(GridAuthority.GridPrivilege.MANAGE_PU, sc);
        _demoteHandler.demote(maxSuspendTime, unit);
    }

    public SuspendType addSpaceSuspendTypeListener(SuspendTypeChangedInternalListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Callback listener supplied for space suspend type changes is <null>.");
        }
        _quiesceHandler.addSpaceSuspendTypeListener(listener);

        return _quiesceHandler.getSuspendInfo().getSuspendType();
    }

    public void removeSpaceSuspendTypeListener(SuspendTypeChangedInternalListener listener) {
        _quiesceHandler.removeSpaceSuspendTypeListener(listener);
    }

    @Override
    public void updateClusterInfo() {
        if (_scalable) {
            synchronized (this){
                try {
                    ClusterTopologyState state = zookeeperTopologyHandler.getClusterTopologyState();
                    ClusterTopology topology = zookeeperTopologyHandler.getClusterTopology(state.getGenerationForPartition(getPartitionIdOneBased()));
                    this._clusterInfo = this._clusterInfo.cloneAndUpdate(topology);
                    this.clusterInfoChangedListeners.afterClusterInfoChange(this._clusterInfo);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        } else {
            throw new IllegalStateException("Nothing to update, CHUNKS_SPACE_ROUTING is disabled");
        }
    }

    private void setOrUpdateBlobstoreRocksDBAllowDuplicateUIDs(SpaceConfig spaceConfig, SpaceConfigReader spaceConfigReader) {
        String rocksDBStoreHandlerClassName = "com.gigaspaces.blobstore.rocksdb.RocksDBBlobStoreHandler";
        String isDuplicateUIDsAllowed = "false";

        Object blobstoreDataPolicy = getCustomProperties().get(CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_PROP);
        if (blobstoreDataPolicy != null){
            if (blobstoreDataPolicy.getClass().getName().equals(rocksDBStoreHandlerClassName)){
                String blobstoreRocksDBAllowDuplicateUIDs = spaceConfigReader.getSpaceProperty(Engine.ENGINE_BLOBSTORE_ROCKSDB_ALLOW_DUPLICATE_UIDS, Engine.ENGINE_BLOBSTORE_ROCKSDB_ALLOW_DUPLICATE_UIDS_DEFAULT);
                if (!blobstoreRocksDBAllowDuplicateUIDs.equals(Engine.ENGINE_BLOBSTORE_ROCKSDB_ALLOW_DUPLICATE_UIDS_DEFAULT)){
                    isDuplicateUIDsAllowed = blobstoreRocksDBAllowDuplicateUIDs;
                } else if (PlatformVersion.isInsightEdgeAnalytics()) {
                    isDuplicateUIDsAllowed = "true";
                }
            }
        }
        spaceConfig.setBlobstoreRocksDBAllowDuplicateUIDs(isDuplicateUIDsAllowed);
    }

    public boolean pullBroadcastTables(){
        List<String> typeNames = getEngine().getTypeManager().getSafeTypeTable().values().stream()
                .map(IServerTypeDesc::getTypeDesc)
                .filter(ITypeDesc::isBroadcast)
                .map(ITypeDesc::getTypeName)
                .collect(Collectors.toList());
        return pullBroadcastTables(new LinkedList<>(typeNames));
    }

    private boolean pullBroadcastTables(LinkedList<String> types) {
        LinkedList<Integer> targetPartitions = new LinkedList<>();
        for (int i = 0; i < getEngine().getNumberOfPartitions(); i++) {
            if (getEngine().getPartitionIdZeroBased() == i)
                continue;
            targetPartitions.add(i);
        }
        while (!targetPartitions.isEmpty()) {
            Integer partitionId = targetPartitions.removeFirst();
            String type;
            while(!types.isEmpty()) {
                type = types.removeFirst();
                if(_broadcastTableHandler.pullEntries(type, partitionId)){
                    if (_logger.isDebugEnabled()) {
                        _logger.debug("Space [" + getServiceName() + "] successfully pulled broadcast table type: " + type);
                    }
                }
                else {
                    if (_logger.isWarnEnabled()) {
                        _logger.warn("Space [" + getServiceName() + "]" +
                                " failed to pull broadcast table type: " + type +
                                " from partition " + partitionId + ". Attempting to pull remaining types from the next partition");
                    }
                    break;
                }
            }
            if (_logger.isDebugEnabled()) {
                _logger.debug("Space [" + getServiceName() + "] successfully pulled all broadcast table types");
            }
            return true;
        }
        if(_logger.isErrorEnabled()){
            _logger.error("Space [" + getServiceName() + "]" +
                    " failed to pull broadcast table types: " + String.join(", ", types) +
                    " from all available partitions");
        }
        return false;
    }

    private void pushBroadcastEntry(IEntryPacket entryPacket, long lease, boolean isUpdate, long timeout, int modifiers) throws RemoteException {
        _broadcastTableHandler.pushEntry(entryPacket, lease, isUpdate, timeout, modifiers);
    }

    private void pushBroadcastEntries(IEntryPacket[] entryPacket, long lease, long[] leases, long timeout, int modifiers) throws RemoteException {
        _broadcastTableHandler.pushEntries(entryPacket, lease ,leases, timeout, modifiers);
    }

    private boolean performBroadcastOperation(IEntryPacket entryPacket, SpaceContext sc) {
        ITypeDesc typeDesc = entryPacket.getTypeDescriptor() != null ? entryPacket.getTypeDescriptor() : _engine.getClassTypeInfo(entryPacket.getTypeName());
        boolean broadcast = typeDesc != null && typeDesc.isBroadcast();
        boolean isClustered = sc == null || sc.isClustered();
        return broadcast && _engine.getNumberOfPartitions() > 1 && _engine.getPartitionIdZeroBased() == 0 && isClustered;
    }

    private void checkReplicationIsHealthy() {
        List<ReplicationStatistics.OutgoingChannel> backupChannels = getEngine().getReplicationNode().getAdmin().getStatistics().getOutgoingReplication().getChannels(ReplicationStatistics.ReplicationMode.BACKUP_SPACE);
        for (ReplicationStatistics.OutgoingChannel channel: backupChannels){
            if(!channel.getChannelState().equals(ReplicationStatistics.ChannelState.ACTIVE)){
                throw new RuntimeException("Broadcast table operation failure. Backup replication channel " + channel.getTargetMemberName() + " is not active, state is " + channel.getChannelState());
            }
        }
    }
}
