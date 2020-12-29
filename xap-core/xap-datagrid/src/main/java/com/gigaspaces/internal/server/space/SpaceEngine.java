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

import com.gigaspaces.client.ClearByIdsException;
import com.gigaspaces.client.ReadTakeByIdsException;
import com.gigaspaces.client.WriteMultipleException;
import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.client.protective.ProtectiveMode;
import com.gigaspaces.client.protective.ProtectiveModeException;
import com.gigaspaces.client.transaction.local.LocalTransactionManagerImpl;
import com.gigaspaces.client.transaction.xa.GSServerTransaction;
import com.gigaspaces.cluster.DynamicPartitionInfo;
import com.gigaspaces.cluster.PartitionRoutingInfo;
import com.gigaspaces.cluster.activeelection.ISpaceComponentsHandler;
import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.cluster.replication.ConsistencyLevelCompromisedException;
import com.gigaspaces.datasource.SpaceDataSource;
import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.events.GSEventRegistration;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.TypeDescFactory;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.cluster.DynamicPartitionInfoImpl;
import com.gigaspaces.internal.cluster.PartitionRoutingInfoImpl;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.cluster.node.IReplicationNode;
import com.gigaspaces.internal.cluster.node.IReplicationNodeAdmin;
import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.FailedSyncSpaceReplicateState;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig.LimitReachedPolicy;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogMemberLimitationConfig;
import com.gigaspaces.internal.cluster.node.impl.backlog.OperationWeightInfo;
import com.gigaspaces.internal.cluster.node.impl.backlog.OperationWeightInfoFactory;
import com.gigaspaces.internal.cluster.node.impl.backlog.WeightInfoOperationType;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfigBuilder;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencyBackupSyncIteratorHandler;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncListFetcher;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.AsyncChannelConfig;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceSynchronizeReplicaRequestContext;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;
import com.gigaspaces.internal.cluster.node.impl.view.ViewDynamicSourceGroupMemberLifeCycle;
import com.gigaspaces.internal.cluster.node.impl.view.ViewReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.replica.*;
import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters.ReplicaType;
import com.gigaspaces.internal.datasource.EDSAdapter;
import com.gigaspaces.internal.lrmi.stubs.LRMISpaceImpl;
import com.gigaspaces.internal.metadata.EntryType;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.internal.metadata.converter.ConversionException;
import com.gigaspaces.internal.query.EntryHolderAggregatorContext;
import com.gigaspaces.internal.query.explainplan.SingleExplainPlan;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils;
import com.gigaspaces.internal.server.metadata.AddTypeDescResult;
import com.gigaspaces.internal.server.metadata.AddTypeDescResultType;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.events.SpaceDataEventManager;
import com.gigaspaces.internal.server.space.iterator.ServerIteratorInfo;
import com.gigaspaces.internal.server.space.iterator.ServerIteratorRequestInfo;
import com.gigaspaces.internal.server.space.iterator.ServerIteratorsManager;
import com.gigaspaces.internal.server.space.metadata.ServerTypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.space.operations.WriteEntriesResult;
import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.StorageConsistencyModes;
import com.gigaspaces.internal.server.space.replication.SpaceReplicationInitializer;
import com.gigaspaces.internal.server.space.replication.SpaceReplicationManager;
import com.gigaspaces.internal.server.storage.*;
import com.gigaspaces.internal.sync.SynchronizationStorageAdapter;
import com.gigaspaces.internal.sync.hybrid.SyncHybridSAException;
import com.gigaspaces.internal.sync.hybrid.SyncHybridTransactionException;
import com.gigaspaces.internal.transport.*;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.IAddOnlySet;
import com.gigaspaces.logger.LogLevel;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.lrmi.TransportProtocolHelper;
import com.gigaspaces.lrmi.nio.IResponseContext;
import com.gigaspaces.lrmi.nio.ResponseContext;
import com.gigaspaces.management.space.SpaceQueryDetails;
import com.gigaspaces.metrics.*;
import com.gigaspaces.query.aggregators.SpaceEntriesAggregator;
import com.gigaspaces.security.authorities.SpaceAuthority.SpacePrivilege;
import com.gigaspaces.server.blobstore.BlobStoreException;
import com.gigaspaces.server.filter.NotifyAcknowledgeFilter;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;
import com.gigaspaces.time.SystemTime;
import com.gigaspaces.utils.Pair;
import com.j_spaces.core.*;
import com.j_spaces.core.Constants.SpaceProxy;
import com.j_spaces.core.admin.SpaceRuntimeInfo;
import com.j_spaces.core.admin.TemplateInfo;
import com.j_spaces.core.cache.*;
import com.j_spaces.core.cache.blobStore.BlobStoreEntryHolder;
import com.j_spaces.core.cache.blobStore.BlobStoreRefEntryCacheInfo;
import com.j_spaces.core.cache.blobStore.IBlobStoreEntryHolder;
import com.j_spaces.core.cache.blobStore.optimizations.BlobStoreOperationOptimizations;
import com.j_spaces.core.cache.blobStore.storage.bulks.BlobStoreBulkInfo;
import com.j_spaces.core.cache.blobStore.storage.preFetch.BlobStorePreFetchIteratorBasedHandler;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.*;
import com.j_spaces.core.cluster.*;
import com.j_spaces.core.exception.internal.EngineInternalSpaceException;
import com.j_spaces.core.exception.internal.ProxyInternalSpaceException;
import com.j_spaces.core.fifo.FifoBackgroundRequest;
import com.j_spaces.core.filters.FilterManager;
import com.j_spaces.core.filters.FilterOperationCodes;
import com.j_spaces.core.filters.FilterProvider;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;
import com.j_spaces.core.sadapter.*;
import com.j_spaces.core.server.processor.*;
import com.j_spaces.core.transaction.TransactionHandler;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.*;
import com.j_spaces.kernel.list.CircularNumerator;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.locks.ILockObject;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.Lease;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.core.transaction.server.TransactionConstants;
import net.jini.core.transaction.server.TransactionManager;
import net.jini.space.InternalSpaceException;
import net.jini.space.JavaSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.j_spaces.core.Constants.CacheManager.*;
import static com.j_spaces.core.Constants.Engine.*;

@com.gigaspaces.api.InternalApi
public class SpaceEngine implements ISpaceModeListener , IClusterInfoChangedListener {


    private final Logger _logger;
    private final Logger _operationLogger;
    private final Logger _loggerConfig;

    //---------------  constant exceptions ---------------
    private final static TemplateDeletedException TEMPLATE_DELETED_EXCEPTION = new TemplateDeletedException(null);
    private final static EntryDeletedException ENTRY_DELETED_EXCEPTION = new EntryDeletedException();
    private final static NoMatchException NO_MATCH_EXCEPTION = new NoMatchException(null);
    final static TransactionConflictException TX_CONFLICT_EXCEPTION = new TransactionConflictException(null, null);
    private final FifoException _fifoException = new FifoException();
    //---------------  constant exceptions ---------------

    private final SpaceImpl _spaceImpl;
    private final String _spaceName;
    private final String _containerName;
    private final String _fullSpaceName;
    private final ClusterPolicy _clusterPolicy;
    private volatile SpaceClusterInfo _clusterInfo;

    // Independent components
    private final Random _random;
    private final EntryArrivedPacketsFactory _entryArrivedFactory;
    private final LocalViewRegistrations _localViewRegistrations;
    private final MetricManager _metricManager;
    private final InternalMetricRegistrator _metricRegistrator;
    private final Map<String,MetricRegistrator> _dataTypesMetricRegistrators = new ConcurrentHashMap<>();

    // Components which depend only on spaceImpl and configuration
    private final SpaceConfigReader _configReader;
    private final SpaceUidFactory _uidFactory;
    private final IDirectSpaceProxy _directProxy;
    private final SpaceTypeManager _typeManager;
    // Uncategorized components
    private final SpaceReplicationManager _replicationManager;
    private final TransactionHandler _transactionHandler;
    private final CacheManager _cacheManager;
    private final FilterManager _filterManager;
    private final SpaceDataEventManager _dataEventManager;
    private final TemplateScanner _templateScanner;
    private final FifoGroupsHandler _fifoGroupsHandler;
    private LeaseManager _leaseManager;
    private MemoryManager _memoryManager;
    private ServerIteratorsManager _serverIteratorsManager;

    /*--------- Working Groups ---------*/
    private final WorkingGroup<BusPacket<Processor>> _processorWG;
    private final Processor _coreProcessor; //only for SA
    /*--------- end of Working Groups ---------*/

    /**
     * volatile memory synchronization
     */
    private volatile long _lastEntryTimestamp;

    // if this boolean is true, use dirty read (i.e. entries will be
    // read if if they are under write-lock)
    private final boolean _useDirtyRead;
    // client retries
    private final int _TTL;

    //---------------  local-cache mode -----------------
    /**
     * when the space is in local cache mode it acts differently: 1. write, (the entry might be
     * there already) 2. update (optimistic lock takes a greater version as-is) 3. take (versionID
     * ignored) other diff' may be added.
     */
    private final boolean _isLocalCache;
    private final EntryDataType _entryDataType;

    /**
     * <code>true</code> if this space serves as MirrorService to receive async bulk operations to
     * be saved into persistent cache adaptor.
     **/
    private final boolean _isReplicated;
    private final boolean _isReplicatedPersistentBlobstore;

    /**
     * the following attribute are relevant if the space is a part of a fixed partition, relevant
     * for non-dynamic cluster -1 if not relevant
     **/
    private final int _partitionId;

    private final boolean _allowNonBlockingRead;
    private boolean _memoryRecoveryEnabled;
    /**
     * Synchronize replication, if true synchronize replication enabled.
     */
    private final boolean _isSyncReplication;
    private final IDuplicateOperationFilter _duplicateOperationIDFilter;
    private boolean _coldStart;

    private volatile Exception _replicationUnhealthyReason;
    private volatile Exception _unhealthyReason;
    private volatile boolean _failOverDuringRecovery;

    //befoe/after remove filters are not only for lease expiration/cancel but
    //for all remove operations
    @Deprecated
    private final boolean _general_purpose_remove_filters;

    private final int _resultsSizeLimit;
    private final int _resultsSizeLimitMemoryCheckBatchSize;

    public SpaceEngine(SpaceImpl spaceImpl) throws CreateException, RemoteException {
        _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ENGINE + "." + spaceImpl.getNodeName());
        _operationLogger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_ENGINE_OPERATIONS + "." + spaceImpl.getNodeName());
        _loggerConfig = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CONFIG + "." + spaceImpl.getNodeName());

        _spaceImpl = spaceImpl;
        _spaceName = spaceImpl.getName();
        _containerName = spaceImpl.getContainerName();
        _fullSpaceName = spaceImpl.getServiceName();
        _clusterPolicy = spaceImpl.getClusterPolicy();
        _clusterInfo = spaceImpl.getClusterInfo();
        _configReader = spaceImpl.getConfigReader();

        // Read properties:
        _isLocalCache = spaceImpl.isLocalCache();
        _allowNonBlockingRead = _configReader.getBooleanSpaceProperty(ENGINE_NON_BLOCKING_READ_PROP, ENGINE_NON_BLOCKING_READ_DEFAULT);
        _useDirtyRead = _configReader.getBooleanSpaceProperty(ENGINE_DIRTY_READ_PROP, ENGINE_DIRTY_READ_DEFAULT);
        _TTL = _configReader.getIntSpaceProperty(SpaceProxy.OldRouter.RETRY_CONNECTION, SpaceProxy.OldRouter.RETRY_CONNECTION_DEFAULT);
        _entryDataType = initEntryDataType(_configReader, _isLocalCache);

        //purpose
        _general_purpose_remove_filters = Boolean.parseBoolean(System.getProperty(SystemProperties.ENGINE_GENERAL_PURPOSE_REMOVE_FILTERS, "false"));

        // ********** Start initializing independent components **********
        _random = new Random();
        _entryArrivedFactory = new EntryArrivedPacketsFactory();
        _localViewRegistrations = new LocalViewRegistrations(getFullSpaceName());
        _metricManager = MetricManager.acquire();

        _metricRegistrator = initSpaceMetricRegistrator();
        // ********** Finished initializing independent components **********

        // ********** Start initializing components which depend only on spaceImpl and configuration **********
        _uidFactory = new SpaceUidFactory(extractMemberIdFromContainer(spaceImpl));

        _directProxy = spaceImpl.getSingleProxy();

        final TypeDescFactory typeDescFactory = new TypeDescFactory(_directProxy);
        _typeManager = new SpaceTypeManager(typeDescFactory, _configReader);

        _partitionId = _clusterInfo.getPartitionOfMember(_fullSpaceName);

        // ********** Finished initializing components which depend only on spaceImpl and configuration **********

        // Initialize Spaghetti components (TODO: de-Spaghetti components):
        _transactionHandler = new TransactionHandler(_configReader, this);

        final IStorageAdapter storageAdapter = initStorageAdapter(spaceImpl, this);
        verifySystemTime(storageAdapter);

        if (isBlobStorePersistent() && getClusterInfo().getNumberOfBackups() > 1) {
            throw new CreateException("BlobStore persistency is not allowed with more then a single backup");
        }

        if (isBlobStorePersistent()) {
            blobStoreOverrideConfig(spaceImpl);
        }

        // Initialize replication components
        _replicationManager = new SpaceReplicationManager(new SpaceReplicationInitializer(
                _configReader, _spaceImpl, _typeManager, storageAdapter, this));
        _isReplicated = _replicationManager.isReplicated();
        _isReplicatedPersistentBlobstore = _replicationManager.isReplicatedPersistentBlobstore();
        _isSyncReplication = _replicationManager.isSyncReplication();

        _cacheManager = new CacheManager(_configReader, _clusterPolicy, _typeManager,
                _replicationManager.getReplicationNode(), storageAdapter, this, _spaceImpl.getCustomProperties());

        // create and start working groups
        final int minThreads = _configReader.getIntSpaceProperty(ENGINE_MIN_THREADS_PROP, ENGINE_MIN_THREADS_DEFAULT);
        final int maxThreads = _configReader.getIntSpaceProperty(ENGINE_MAX_THREADS_PROP, ENGINE_MAX_THREADS_DEFAULT);
        final boolean isHighPriority = _configReader.getBooleanSpaceProperty(ENGINE_THREADS_HIGHER_PRIORITY_PROP, "false");
        final int threadPriority = isHighPriority ? Thread.MAX_PRIORITY : Thread.NORM_PRIORITY;
        final long timeout = 60 * 1000;

        _coreProcessor = new Processor(this);
        _processorWG = new WorkingGroup<BusPacket<Processor>>(_coreProcessor,
                threadPriority, "Processor", minThreads, maxThreads, timeout);

        _processorWG.start();

        // call the filter ON_INIT
        /* filters */
        // GS-12157 - passing direct proxy which can be transformed into clustered proxy on demand
        _filterManager = new FilterManager(_typeManager, spaceImpl.getTaskProxy(), this);
        _dataEventManager = new SpaceDataEventManager(_directProxy, _filterManager,
                spaceImpl, _configReader);

        _templateScanner = new TemplateScanner(_typeManager, _cacheManager, _dataEventManager, this);
        _fifoGroupsHandler = new FifoGroupsHandler(this);
        _duplicateOperationIDFilter = createDuplicateOperationIDFilter();
        _resultsSizeLimit = _configReader.getIntSpaceProperty(ENGINE_QUERY_RESULT_SIZE_LIMIT, ENGINE_QUERY_RESULT_SIZE_LIMIT_DEFAULT);
        _resultsSizeLimitMemoryCheckBatchSize = _configReader.getIntSpaceProperty(ENGINE_QUERY_RESULT_SIZE_LIMIT_MEMORY_CHECK_BATCH_SIZE, ENGINE_QUERY_RESULT_SIZE_LIMIT_MEMORY_CHECK_BATCH_SIZE_DEFAULT);
        if (!_isLocalCache)
            registerSpaceMetrics(_metricRegistrator);
        _serverIteratorsManager = new ServerIteratorsManager(_spaceImpl.getPartitionId());
        spaceImpl.registerToClusterInfoChangedEvent(this);
    }

    private void blobStoreOverrideConfig(SpaceImpl spaceImpl) {
        String replicationPolicyPath = ClusterXML.CLUSTER_CONFIG_TAG + "." + ClusterXML.GROUPS_TAG + "." + ClusterXML.GROUP_TAG + "." + ClusterXML.REPL_POLICY_TAG + ".";

        String redoLogCapacityFromCustomProperties = spaceImpl.getCustomProperties().getProperty(replicationPolicyPath + ClusterXML.REPL_REDO_LOG_CAPACITY_TAG);
        String redoLogMemoryCapacityFromCustomProperties = spaceImpl.getCustomProperties().getProperty(replicationPolicyPath + ClusterXML.REPL_REDO_LOG_MEMORY_CAPACITY_TAG);
        String redoLogCompactionFromCustomProperties = spaceImpl.getCustomProperties().getProperty(replicationPolicyPath + ClusterXML.REPL_REDO_LOG_COMPACTION_TAG);
        String onRedoLogCapacityExceededFromCustomProperties = spaceImpl.getCustomProperties().getProperty(replicationPolicyPath + ClusterXML.REPL_REDO_LOG_CAPACITY_EXCEEDED_TAG);

        if (redoLogCapacityFromCustomProperties == null && spaceImpl.getClusterPolicy() != null && spaceImpl.getClusterPolicy().getReplicationPolicy() != null) {
            if (_loggerConfig.isInfoEnabled()) {
                _loggerConfig.info("Override the cluster config property <" + replicationPolicyPath + ClusterXML.REPL_REDO_LOG_CAPACITY_EXCEEDED_TAG + ">. new value: <" + ClusterXML.REPL_PESRISTENT_BLOBSTORE_REDO_LOG_CAPACITY_DEFAULT_VALUE + ">");
            }
            spaceImpl.getClusterPolicy().getReplicationPolicy().setMaxRedoLogCapacity(Integer.valueOf(ClusterXML.REPL_PESRISTENT_BLOBSTORE_REDO_LOG_CAPACITY_DEFAULT_VALUE));
        }
        if (redoLogMemoryCapacityFromCustomProperties == null && spaceImpl.getClusterPolicy() != null && spaceImpl.getClusterPolicy().getReplicationPolicy() != null) {
            if (_loggerConfig.isInfoEnabled()) {
                _loggerConfig.info("Override the cluster config property <" + replicationPolicyPath + ClusterXML.REPL_REDO_LOG_MEMORY_CAPACITY_TAG + ">. new value: <" + ClusterXML.REPL_PESRISTENT_BLOBSTORE_MEMORY_REDO_LOG_CAPACITY_DEFAULT_VALUE + ">");
            }
            spaceImpl.getClusterPolicy().getReplicationPolicy().setMaxRedoLogMemoryCapacity(Integer.valueOf(ClusterXML.REPL_PESRISTENT_BLOBSTORE_MEMORY_REDO_LOG_CAPACITY_DEFAULT_VALUE));
        }
        if (onRedoLogCapacityExceededFromCustomProperties == null && spaceImpl.getClusterPolicy() != null && spaceImpl.getClusterPolicy().getReplicationPolicy() != null) {
            if (_loggerConfig.isInfoEnabled()) {
                _loggerConfig.info("Override the cluster config property <" + replicationPolicyPath + ClusterXML.REPL_REDO_LOG_CAPACITY_EXCEEDED_TAG + ">. new value: <" + RedoLogCapacityExceededPolicy.BLOCK_OPERATIONS + ">");
            }
            spaceImpl.getClusterPolicy().getReplicationPolicy().setOnRedoLogCapacityExceeded(RedoLogCapacityExceededPolicy.BLOCK_OPERATIONS);
        } else {
            if (onRedoLogCapacityExceededFromCustomProperties != null && spaceImpl.getClusterPolicy() != null
                    && spaceImpl.getClusterPolicy().getReplicationPolicy() != null
                    && spaceImpl.getClusterPolicy().getReplicationPolicy().getOnRedoLogCapacityExceeded().equals(RedoLogCapacityExceededPolicy.DROP_OLDEST)) {
                throw new BlobStoreException("property [ " + replicationPolicyPath + ClusterXML.REPL_REDO_LOG_CAPACITY_EXCEEDED_TAG + " ] must configured as [ " + RedoLogCapacityExceededPolicy.BLOCK_OPERATIONS + " ] ");
            }
        }
    }

    public MetricRegistrator extendSpaceMetricRegistrator(String prefix, Map<String, String> extraTags, Map<String, DynamicMetricTag> extraDynamicTags) {
        return _metricRegistrator.extend(prefix, extraTags, extraDynamicTags);
    }

    private InternalMetricRegistrator initSpaceMetricRegistrator() {
        // Create space tags:
        final String prefix = "metrics.";
        final Map<String, String> tags = new HashMap<>();
        for (Map.Entry<Object, Object> property : _spaceImpl.getCustomProperties().entrySet()) {
            String name = (String) property.getKey();
            if (name.startsWith(prefix))
                tags.put(name.substring(prefix.length()), (String) property.getValue());
        }
        tags.put("space_name", _spaceImpl.getName());
        tags.put("space_instance_id", _spaceImpl.getInstanceId());
        // Create space dynamic tags:
        Map<String, DynamicMetricTag> dynamicTags = new HashMap<>();
        dynamicTags.put("space_active", new DynamicMetricTag() {
            @Override
            public Object getValue() {
                boolean active;
                try {
                    active = _spaceImpl.isActive();
                } catch (RemoteException e) {
                    active = false;
                }
                return active;
            }
        });
        return (InternalMetricRegistrator) _metricManager.createRegistrator(MetricConstants.SPACE_METRIC_NAME, tags, dynamicTags);
    }

    private void registerSpaceMetrics(MetricRegistrator registrator) {
        registrator.register(registrator.toPath(MetricConstants.CONNECTIONS_METRIC_NAME, "incoming", "active"), new Gauge<Integer>() {
            @Override
            public Integer getValue() throws Exception {
                return countIncomingConnections();
            }
        });

        registrator.register(registrator.toPath("transactions", "active"), new Gauge<Integer>() {
            @Override
            public Integer getValue() throws Exception {
                return countTransactions(TransactionInfo.Types.ALL, TransactionConstants.ACTIVE);
            }
        });
    }

    private IDuplicateOperationFilter createDuplicateOperationIDFilter() {
        if (isReplicated() && getClusterPolicy().isPrimaryElectionAvailable()) {
            //TODO TXFailover configurable?
            int lastOperationCountToKeep = 200000;
            IAddOnlySet<OperationID> duplicateFilter = new DuplicateOperationIDFilter(lastOperationCountToKeep);
            long duplicateProtectionTimeInterval = 2 * 60 * 1000; //2 minutes
            return new BackupFailoverOperationIDFilter(duplicateFilter, duplicateProtectionTimeInterval);
        }
        return new NullDuplicateOperationIDFilter();
    }

    private static IStorageAdapter initStorageAdapter(SpaceImpl spaceImpl, SpaceEngine spaceEngine) throws CreateException {
        JSpaceAttributes spaceAttributes = spaceImpl.getJspaceAttr();
        if (!spaceAttributes.isPersistent())
            return new MemorySA();

        try {
            final SpaceDataSource spaceDataSourceInstance = getSpaceDataSourceInstance(spaceAttributes);
            final SpaceSynchronizationEndpoint synchronizationEndpointInterceptorInstance = getSynchronizationEndpointInstance(spaceAttributes);
            if (spaceDataSourceInstance != null || synchronizationEndpointInterceptorInstance != null) {
                return new SynchronizationStorageAdapter(spaceEngine,
                        spaceDataSourceInstance,
                        synchronizationEndpointInterceptorInstance);
            }
            return EDSAdapter.create(spaceEngine, spaceAttributes);
        } catch (Exception e) {
            throw new CreateException("Failed to load storage adapter", e);
        }
    }

    private static SpaceSynchronizationEndpoint getSynchronizationEndpointInstance(
            JSpaceAttributes spaceAttributes) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        SpaceSynchronizationEndpoint synchronizationEndpointInstance = spaceAttributes.getSynchronizationEndpointInstance();
        if (synchronizationEndpointInstance == null) {
            String synchronizationEndpointInterceptorClassName = spaceAttributes.getSynchronizationEndpointClassName();
            if (StringUtils.hasText(synchronizationEndpointInterceptorClassName)) {
                Class<SpaceSynchronizationEndpoint> clazz = ClassLoaderHelper.loadClass(synchronizationEndpointInterceptorClassName);
                return clazz.newInstance();
            }
        }
        return synchronizationEndpointInstance;
    }

    private static SpaceDataSource getSpaceDataSourceInstance(
            JSpaceAttributes spaceAttributes) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
        SpaceDataSource spaceDataSourceInstance = spaceAttributes.getSpaceDataSourceInstance();
        if (spaceDataSourceInstance == null) {
            String spaceDataSourceClassName = spaceAttributes.getSpaceDataSourceClassName();
            if (StringUtils.hasText(spaceDataSourceClassName)) {
                Class<SpaceDataSource> clazz = ClassLoaderHelper.loadClass(spaceDataSourceClassName);
                return clazz.newInstance();
            }
        }
        return spaceDataSourceInstance;
    }

    private static EntryDataType initEntryDataType(SpaceConfigReader configReader, boolean isLocalCache) {
        if (!isLocalCache)
            return EntryDataType.FLAT;
        String entryDataTypeProperty = configReader.getSpaceProperty(
                Constants.DCache.STORAGE_TYPE_PROP,
                Constants.DCache.STORAGE_TYPE_DEFAULT);

        return entryDataTypeProperty.equalsIgnoreCase(SpaceURL.LOCAL_CACHE_STORE_REFERENCE)
                ? EntryDataType.USER_TYPE : EntryDataType.FLAT;
    }

    private void verifySystemTime(final IStorageAdapter sa) {
        if (_logger.isInfoEnabled()) {
            Throwable initializedException = null;
            String initializedMsg = null;
            try {
                SystemTime.timeMillis();
                if (SystemInfo.singleton().timeProvider().isRelativeTime() && sa.supportsExternalDB())
                    initializedMsg = "Limitation - Relative time is currently limited to in-memory space.";
            } catch (Throwable t) {
                initializedException = t;
                initializedMsg = String.valueOf(t.getCause());
            } finally {
                boolean initializeError = (initializedMsg != null);

                if (initializedMsg == null && SystemInfo.singleton().timeProvider().isRelativeTime()) // if no error encountered
                    initializedMsg = "<" + SystemInfo.singleton().timeProvider().getTimeProviderName() + "> initialized successfully";

                if (initializedMsg != null)
                    _logger.info("System time-provider:[  " + initializedMsg + "  ] ");

                if (initializeError)
                    throw new RuntimeException(initializedMsg, initializedException);
            }
        }
    }

    //Force memory barrier
    public synchronized void init(boolean loadDataFromDB, boolean considerMemoryRecovery)
            throws CreateException {
        if (_logger.isDebugEnabled())
            _logger.debug("init(loadDataFromDB=" + loadDataFromDB + ", considerMemoryRecovery=" + considerMemoryRecovery + ")");
        try {
            JSpaceAttributes spaceAttr = _spaceImpl.getJspaceAttr();
            if (!spaceAttr.isPersistent() || _replicationManager.isMirrorService())
                loadDataFromDB = false;

            Properties properties = loadSpaceAttributeProperties(spaceAttr);

            _cacheManager.init(properties);
            if (_clusterPolicy != null && _clusterPolicy.getReplicationPolicy() != null && _clusterPolicy.getReplicationPolicy().getConflictingOperationPolicy() == null) {
                //uninitialized, set a default
                _conflictingOperationPolicy = (_cacheManager.isMemorySpace() && _cacheManager.isEvictableCachePolicy()) ? ConflictingOperationPolicy.OVERRIDE : ConflictingOperationPolicy.DEFAULT;
            }

            //Create LeaseManager
            _leaseManager = new LeaseManager(this, _spaceName, _coreProcessor);
            _dataEventManager.setLeaseManager(_leaseManager);
            _cacheManager.setLeaseManager(_leaseManager);
            _memoryManager = new MemoryManager(_spaceName, _containerName, _cacheManager, _leaseManager, _spaceImpl.isPrimary());

            _typeManager.loadSpaceTypes(_spaceImpl.getURL());
            _cacheManager.initCache(loadDataFromDB, properties);

            //init the lease manager
            _leaseManager.init();
            //expiration manager
            _cacheManager.startTemplateExpirationManager();

            // create and start replicator, if this space is replicated
            if (isReplicated()) {
                if (considerMemoryRecovery && _clusterPolicy.m_ReplicationPolicy.m_ReplicationGroupMembersNames.size() > 1) {
                    ReplicationPolicy.ReplicationPolicyDescription pd =
                            _clusterPolicy.m_ReplicationPolicy.m_ReplMemberPolicyDescTable.get(_clusterPolicy.m_ReplicationPolicy.m_OwnMemberName);

                    if (pd.memberRecovery) {
                        _memoryRecoveryEnabled = true;
                        // if the local database is empty or cold init was used
                        // the engine is started in cold start - means that no data was loaded
                        if (hasMirror() || !loadDataFromDB || isEmptyPersistent())
                            _coldStart = true;
                    }
                }

            }

            _dataEventManager.init(isReplicated(), getReplicationNode());
        } catch (Exception ex) {
            String msg = "Failed to init [" + _spaceName + "] space.";

            if (_logger.isErrorEnabled())
                _logger.error(msg, ex);

            // clean all working groups plus LeaseManager
            _processorWG.shutdown();
            _dataEventManager.close();
            if (_leaseManager != null)
                _leaseManager.close();

            throw new CreateException(msg + " " + ex.getMessage(), ex);
        }
    }

    private Properties loadSpaceAttributeProperties(JSpaceAttributes spaceAttr) {
        Properties properties = new Properties();
        if (spaceAttr.isPersistent()) {
            // Add external database properties
            if (_cacheManager.isCacheExternalDB()) {
                Object dataSource = spaceAttr.getCustomProperties().get(Constants.DataAdapter.DATA_SOURCE);
                if (dataSource != null)
                    properties.put(Constants.DataAdapter.DATA_SOURCE, dataSource);

                properties.setProperty(Constants.DataAdapter.DATA_SOURCE_CLASS_PROP, spaceAttr.getDataSourceClass());
                properties.setProperty(Constants.DataAdapter.DATA_CLASS_PROP, spaceAttr.getDataClass());
                properties.setProperty(Constants.DataAdapter.QUERY_BUILDER_PROP, spaceAttr.getQueryBuilderClass());

                properties.setProperty(Constants.DataAdapter.SUPPORTS_INHERITANCE_PROP, String.valueOf(spaceAttr.isSupportsInheritanceEnabled()));
                properties.setProperty(Constants.DataAdapter.SUPPORTS_VERSION_PROP, String.valueOf(spaceAttr.isSupportsVersionEnabled()));
                properties.setProperty(Constants.DataAdapter.SUPPORTS_PARTIAL_UPDATE_PROP, String.valueOf(spaceAttr.isSupportsPartialUpdateEnabled()));
                properties.setProperty(Constants.DataAdapter.SUPPORTS_REMOVE_BY_ID_PROP, String.valueOf(spaceAttr.isSupportsRemoveByIdEnabled()));
                properties.setProperty(Constants.DataAdapter.USAGE, spaceAttr.getUsage());
                properties.setProperty(Constants.DataAdapter.DATA_PROPERTIES, spaceAttr.getDataPropertiesFile());
                properties.setProperty(Constants.DataAdapter.DATA_SOURCE_SHARE_ITERATOR_ENABLED_PROP, String.valueOf(spaceAttr.getDataSourceSharedIteratorMode()));
                properties.setProperty(Constants.DataAdapter.DATA_SOURCE_SHARE_ITERATOR_TTL_PROP, String.valueOf(spaceAttr.getDataSourceSharedIteratorTimeToLive()));
            }
        }

        final Object customEvictionPolicy = spaceAttr.getCustomProperties().get(CACHE_MANAGER_EVICTION_STRATEGY_PROP);
        if (customEvictionPolicy != null)
            properties.put(CACHE_MANAGER_EVICTION_STRATEGY_PROP, customEvictionPolicy);

        final Object blobStoreDataPolicy = spaceAttr.getCustomProperties().get(CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_PROP);
        if (blobStoreDataPolicy != null)
            properties.put(CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_PROP, blobStoreDataPolicy);

        final Object blobStoreDataPolicyClass = spaceAttr.getCustomProperties().get(CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_CLASS_PROP);
        if (blobStoreDataPolicyClass != null)
            properties.put(CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_CLASS_PROP, blobStoreDataPolicyClass);

        final Object blobStoreDataCacheSize = spaceAttr.getCustomProperties().get(FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP);
        if (blobStoreDataCacheSize != null)
            properties.put(FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP, blobStoreDataCacheSize);
        else
            properties.put(FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP, CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_DELAULT);

        final Object blobStorePersistent = spaceAttr.getCustomProperties().get(FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP);
        if (blobStorePersistent != null)
            properties.put(FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP, blobStorePersistent);

        final Object blobStoreCacheFilterQueries = spaceAttr.getCustomProperties().get(FULL_CACHE_MANAGER_BLOBSTORE_CACHE_FILTER_QUERIES_PROP);
        if (blobStoreCacheFilterQueries != null)
            properties.put(FULL_CACHE_MANAGER_BLOBSTORE_CACHE_FILTER_QUERIES_PROP, blobStoreCacheFilterQueries);
        return properties;
    }

    public TemplateScanner getTemplateScanner() {
        return _templateScanner;
    }

    public SpaceImpl getSpaceImpl() {
        return _spaceImpl;
    }

    public Logger getOperationLogger() {
        return _operationLogger;
    }

    public SpaceConfigReader getConfigReader() {
        return _configReader;
    }

    public SpaceTypeManager getTypeManager() {
        return _typeManager;
    }

    public FilterManager getFilterManager() {
        return _filterManager;
    }

    public LeaseManager getLeaseManager() {
        return _leaseManager;
    }

    public EntryArrivedPacketsFactory getEntryArrivedPacketsFactory() {
        return _entryArrivedFactory;
    }

    public EntryDataType getEntryDataType() {
        return _entryDataType;
    }

    public WorkingGroup<BusPacket<Processor>> getProcessorWG() {
        return _processorWG;
    }


    public void invokeFilters(SpaceContext sc, int operationCode, Object subject) {
        if (_filterManager != null && _filterManager._isFilter[operationCode])
            _filterManager.invokeFilters(operationCode, sc, subject);
    }



    /*----------------- JavaSpaces API :beginning -----------------*/

    /**
     * Creates an IEntryHolder object from the specified entry packet and inserts it into all
     * relevant engine tables.
     *
     * if entryPacket.NoWriteLease=true the return value will be Entry UID(String) of Entry,
     * otherwise Lease object.
     **/
    public WriteEntryResult write(IEntryPacket entryPacket, Transaction txn,
                                  long lease, int modifiers, boolean fromReplication, boolean origin,
                                  SpaceContext sc)
            throws TransactionException, UnusableEntryException,
            UnknownTypeException, RemoteException {
        monitorMemoryUsage(true);
        monitorReplicationStateForModifyingOperation(txn, OperationWeightInfoFactory.create(1, WeightInfoOperationType.WRITE));

        Context context = (fromReplication && getReplicationNode().getBlobStoreReplicaConsumeHelper() != null)
                ? getReplicationNode().getBlobStoreReplicaConsumeHelper().getContext() : null;
        // try get context from BlobStoreReplicationBulkConsumeHelper to consume
        if (context == null) {
            context = (fromReplication && getReplicationNode().getBlobStoreReplicationBulkConsumeHelper() != null)
                    ? getReplicationNode().getBlobStoreReplicationBulkConsumeHelper().getContext() : null;
        }

        boolean supplied_context = (context != null);

        try {
            if (!supplied_context)
                context = _cacheManager.getCacheContext();

            return write(context, entryPacket, txn, lease, modifiers, fromReplication, origin,
                    sc, false, false);
        } finally {
            if (!supplied_context) {
                _cacheManager.freeCacheContext(context);
            }
        }
    }

    private boolean isExecutedAlready(OperationID operationID) {
        return _duplicateOperationIDFilter.contains(operationID);
    }

    /**
     * Creates an IEntryHolder object from the specified entry packet and inserts it into all
     * relevant engine tables.
     *
     * if entryPacket.NoWriteLease=true the return value will be Entry UID(String) of Entry,
     * otherwise Lease object. fromWriteMultiple-true if this write is called from writeMultiple
     */
    private WriteEntryResult write(Context context, IEntryPacket entryPacket, Transaction txn, long lease,
                                   int modifiers, boolean fromReplication, boolean origin, SpaceContext sc,
                                   boolean reInsertedEntry, boolean fromWriteMultiple)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException {
        context.setFromReplication(fromReplication);
        context.setOrigin(origin);
        context.setOperationID(entryPacket.getOperationID());
        context.applyOperationContext(sc);

        // handle entry type (can not be null)
        IServerTypeDesc serverTypeDesc = _typeManager.loadServerTypeDesc(entryPacket);

        final String packetUid = entryPacket.getUID();

        if (_isLocalCache) {
            if (packetUid == null)
                throw new IllegalArgumentException("Write operation requires non-null object UID when using local cache. Object=" + entryPacket.toString());

            if (entryPacket.getVersion() <= 0)
                throw new IllegalArgumentException("Write operation requires object version greater than 0 when using local cache. Object=" + entryPacket.toString());
        }

        if (!fromReplication) {
            if (isPartitionedSpace() && ProtectiveMode.isWrongRoutingUsageProtectionEnabled() && !serverTypeDesc.getTypeDesc().isBroadcast()) {
                if (entryPacket.getRoutingFieldValue() == null && serverTypeDesc.getTypeDesc().getRoutingPropertyName() != null && !serverTypeDesc.getTypeDesc().isAutoGenerateRouting())
                    throwNoRoutingProvidedWhenNeeded(serverTypeDesc, "writing");
                else if (entryPacket.getRoutingFieldValue() != null)
                    checkEntryRoutingValueMatchesCurrentPartition(entryPacket, serverTypeDesc, "written");
            }
            if (ProtectiveMode.isTypeWithoutIdProtectionEnabled()
                    && entryPacket.getID() == null
                    && !serverTypeDesc.getTypeDesc().isAutoGenerateId()
                    && serverTypeDesc.getTypeDesc().getObjectType() != EntryType.EXTERNAL_ENTRY
                    && !ServerTypeDesc.isEntry(serverTypeDesc)
                    && !ProtectiveMode.shouldIgnoreTypeWithoutIdProtectiveMode(serverTypeDesc.getTypeName()))
                throw new ProtectiveModeException("Cannot introduce a type named '"
                        + serverTypeDesc.getTypeName()
                        + "' without an id property defined. Having a type without an id property would prevent modifying (update/change) objects of that type after they were written to the space. (you can disable this protection, though it is not recommended, by setting the following system property: "
                        + ProtectiveMode.TYPE_WITHOUT_ID + "=false)");
        }

        final XtnEntry txnEntry = initTransactionEntry(txn, sc, fromReplication);

        /**
         * build Entry Holder .
         * if <code>uid</code> not null create, creates <code>IEntryHolder</code> with
         * existing desired <code>uid</code>, otherwise create new <code>uid</code>.
         **/
        if (!fromReplication && !_isLocalCache && !context.isFromGateway() && !Modifiers.contains(modifiers, Modifiers.OVERRIDE_VERSION)) {
            // always initial version
            entryPacket.setVersion(1);
        }

        final long current = SystemTime.timeMillis();
        final String entryUid = getOrCreateUid(entryPacket);
        IEntryHolder eHolder = EntryHolderFactory.createEntryHolder(serverTypeDesc, entryPacket, _entryDataType,
                entryUid, LeaseManager.toAbsoluteTime(lease, current), txnEntry, current, (_cacheManager.isblobStoreDataSpace() && serverTypeDesc.getTypeDesc().isBlobstoreEnabled() && !UpdateModifiers.isUpdateOnly(modifiers)));

        /** set write lease mode */
        if (!reInsertedEntry && _filterManager._isFilter[FilterOperationCodes.BEFORE_WRITE])
            _filterManager.invokeFilters(FilterOperationCodes.BEFORE_WRITE, sc, eHolder);

        WriteEntryResult writeResult = null;
        EntryAlreadyInSpaceException entryInSpaceEx = null;
        try {
            context.cacheViewEntryDataIfNeeded(eHolder.getEntryData(), entryPacket);
            writeResult = _coreProcessor.handleDirectWriteSA(context, eHolder, serverTypeDesc, fromReplication,
                    origin, reInsertedEntry, packetUid != null,
                    !fromWriteMultiple, modifiers);
        } catch (EntryAlreadyInSpaceException ex) {
            entryInSpaceEx = ex;
        } catch (SAException ex) {
            throw new EngineInternalSpaceException(ex.toString(), ex);
        }

        //check the case of write under txn for an entry which is
        //taken under that xtn, in this case replace it by update
        if (entryInSpaceEx != null && txnEntry != null && packetUid != null && !fromReplication) {
            //we got EntryAlreadyInSpaceException, check if the entry is
            //taken under the same xtn and if so, apply update
            //instead of take
            IEntryHolder currentEh = _cacheManager.getEntryByUidFromPureCache(eHolder.getUID());
            ServerTransaction sv = currentEh != null ? currentEh.getWriteLockTransaction() : null;
            if (sv != null && (currentEh.getWriteLockOperation() == SpaceOperations.TAKE_IE || currentEh.getWriteLockOperation() == SpaceOperations.TAKE)
                    && sv.equals(txnEntry.m_Transaction)) {
                //are both entries from same class?
                if (!eHolder.getClassName().equals(currentEh.getClassName())) {
                    throw new InternalSpaceException("take + write under same transaction: must have the same classname, UID=" + currentEh.getUID() + " originalClass=" +
                            currentEh.getClassName() + " newname=" + eHolder.getClassName(), entryInSpaceEx);
                }

                //try to perform an update under the transaction
                entryPacket.setVersion(0);
                try {
                    AnswerPacket ap = update(context, entryPacket /*updated_entry*/,
                            txn, lease, 0 /*timeout*/,  /*listener*/  sc, fromReplication,
                            false /*newRouter*/, UpdateModifiers.UPDATE_OR_WRITE, null, null).m_AnswerPacket;

                    if (ap.m_EntryPacket == null)
                        throw new InternalSpaceException("take + write under same transaction: update returned no result, UID=" +
                                currentEh.getUID() + " Class=" + currentEh.getClassName(), entryInSpaceEx);
                } catch (EntryNotInSpaceException ex) {
                    throw new TransactionException("Transaction [" + txnEntry.m_Transaction +
                            "] became inactive while operation is performing.", ex);
                } catch (InterruptedException e) {
                    // can't get here cause the call is NO_WAIT
                    // restore interrupt state just in case it does get here
                    Thread.currentThread().interrupt();
                }

                entryInSpaceEx = null;
                writeResult = context.getWriteResult();
            }
        }

        if (entryInSpaceEx != null)
            throw entryInSpaceEx;

        if (!reInsertedEntry && _filterManager._isFilter[FilterOperationCodes.AFTER_WRITE])
            _filterManager.invokeFilters(FilterOperationCodes.AFTER_WRITE, sc, eHolder);

        /** perform sync-replication */
        if (context.isSyncReplFromMultipleOperation()) {
            //We may had a lease expired on one of the currently written entries, this was inserted to the replication backlog and we need
            //to replicate it now even if under transaction
            performReplIfChunkReached(context);
            if (context.getReplicationContext() != null) {
                writeResult.setSyncReplicationLevel(context.getReplicationContext().getCompleted());
            }
        } else if (txn == null) {
            int level = performReplication(context);
            writeResult.setSyncReplicationLevel(level);
        }

        return writeResult;
    }

    private void checkIfConsistencyLevelIsCompromised(boolean fromReplication, int level) {
        if (isSyncReplicationEnabled() && !fromReplication && level + 1 < requiredConsistencyLevel()) {
            throw new ConsistencyLevelCompromisedException(level + 1);
        }
    }

    private int requiredConsistencyLevel() {
        return Integer.getInteger(SystemProperties.REQUIRED_CONSISTENCY_LEVEL, SystemProperties.REQUIRED_CONSISTENCY_LEVEL_DEFAULT);
    }

    public boolean isNoWriteLease(IEntryPacket entryPacket, int modifiers, boolean fromReplication) {
        return entryPacket.isNoWriteLease() || Modifiers.contains(modifiers, Modifiers.NO_WRITE_LEASE) || fromReplication;
    }


    /* Register a notify template in the space */
    public GSEventRegistration notify(ITemplatePacket template, long lease, boolean fromReplication,
                                      String templateUid, SpaceContext sc, NotifyInfo info)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException {
        monitorMemoryUsage(true);
        monitorReplicationStateForModifyingOperation(null/*transaction*/, OperationWeightInfoFactory.create(WeightInfoOperationType.NOTIFY));

        // if template type is not null, handle type
        IServerTypeDesc typeDesc = _typeManager.loadServerTypeDesc(template);

        // create template and answer
        if (templateUid == null)
            templateUid = generateUid();
        info.setTemplateUID(templateUid);

        // Check if FIFO:
        boolean isFifoOperation = template.isFifo() || info.isFifo();
        // Disable FIFO if not required:
        if (template.getUID() != null)
            isFifoOperation = false;

        NotifyTemplateHolder tHolder = TemplateHolderFactory.createNotifyTemplateHolder(
                typeDesc, template, templateUid, LeaseManager.toAbsoluteTime(lease),
                _dataEventManager.generateEventId(), info, isFifoOperation);

        if (info.getFilter() != null) {
            try {
                info.getFilter().init(_directProxy, template.toObject(template.getEntryType()));
            } catch (Throwable e) {
                _logger.debug("initializing user filter caused an exception", e);
            }
        }

        if (_filterManager._isFilter[FilterOperationCodes.BEFORE_NOTIFY])
            _filterManager.invokeFilters(FilterOperationCodes.BEFORE_NOTIFY, sc, tHolder);

        AnswerHolder aHolder = new AnswerHolder();
        _coreProcessor.handleNotifyRegistration(tHolder, fromReplication, aHolder, template.getOperationID());

        // if there is an exception throw it according to its type
        if (aHolder.m_Exception != null) {
            if (aHolder.m_Exception instanceof InternalSpaceException)
                throw (InternalSpaceException) aHolder.m_Exception;

            JSpaceUtilities.throwEngineInternalSpaceException(aHolder.m_Exception.getMessage(), aHolder.m_Exception);
        }

        //  return Event Registration that includes lease
        return aHolder.getEventRegistration();
    }


    void readByIds(AbstractIdsQueryPacket template, Transaction txn, boolean take,
                   SpaceContext spaceContext, int operationModifiers, ReadByIdsContext readByIdsContext)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException, InterruptedException {
        monitorMemoryUsage(false);
        if (take)
            monitorReplicationStateForModifyingOperation(txn, OperationWeightInfoFactory.create(template.getIds().length, WeightInfoOperationType.TAKE));

        // Ignore fifo-groups - irrelevant for this operation.
        if (ReadModifiers.isFifoGroupingPoll(operationModifiers))
            operationModifiers = Modifiers.remove(operationModifiers, ReadModifiers.FIFO_GROUPING_POLL);

        final IServerTypeDesc serverTypeDesc = _typeManager.loadServerTypeDesc(template);
        final ITypeDesc typeDesc = serverTypeDesc.getTypeDesc();
        final boolean uidHandling = (typeDesc.getIdPropertyName() == null || typeDesc.isAutoGenerateId());
        final boolean isPersistent = !typeDesc.getIntrospector(null).isTransient(null);
        final boolean supportsNonBlobStorePrefetch = isPersistent &&
                !ReadModifiers.isMemoryOnlySearch(operationModifiers) &&
                !_cacheManager.isResidentEntriesCachePolicy() &&
                _cacheManager.getStorageAdapter().supportsGetEntries() &&
                !serverTypeDesc.hasSubTypes(); // TODO: Remove sub types limitation.

        final AbstractIdsQueryPacket idsQueryPacket = (AbstractIdsQueryPacket) template;
        final Object[] ids = idsQueryPacket.getIds();

        ReadByIdsInfo rIdsInfo = null;
        if (_cacheManager.isResidentEntriesCachePolicy() && !uidHandling)
            rIdsInfo = new ReadByIdsInfo(ids);

        final ITemplatePacket[] templatePackets = new ITemplatePacket[ids.length];
        for (int i = 0; i < ids.length; i++) {
            if (uidHandling)
                templatePackets[i] = TemplatePacketFactory.createUidPacket(typeDesc, (String) ids[i], idsQueryPacket.getRouting(i), 0, template.getQueryResultType(), null);
            else {
                templatePackets[i] = TemplatePacketFactory.createIdPacket(ids[i], idsQueryPacket.getRouting(i), 0, typeDesc, template.getQueryResultType(), null);
                if (supportsNonBlobStorePrefetch)
                    templatePackets[i].setUID(SpaceUidFactory.generateUid(typeDesc.getTypeUidPrefix(), ids[i].toString()));
            }
            templatePackets[i].setOperationID(template.getOperationID());
        }

        Map<String, IEntryHolder> prefetchedNonBlobStoreEntries = supportsNonBlobStorePrefetch ? prefetchNonBlobStoreReadByIdsEntries(ids,
                templatePackets, serverTypeDesc) : null;
        readByIdsContext.initResultsEntryPackets();
        Exception[] exceptions = null;
        Context context = null;
        try {
            context = _cacheManager.getCacheContext();
            if (take && txn == null && _cacheManager.isBlobStoreCachePolicy() && _cacheManager.useBlobStoreBulks()) {//can we exploit blob-store bulking ?
                context.setBlobStoreBulkInfo(new BlobStoreBulkInfo(_cacheManager, true /*takeMultipleBulk*/));
            }
            for (int i = 0; i < ids.length; i++) {
                try {
                    AnswerHolder answerHolder = unsafeRead_impl(templatePackets[i],
                            txn,
                            0,
                            false,
                            take,
                            spaceContext,
                            false,
                            false,
                            true/* origin */,
                            operationModifiers,
                            rIdsInfo,
                            prefetchedNonBlobStoreEntries, context);

                    readByIdsContext.applyResult(answerHolder.getAnswerPacket(), i);
                } catch (Exception e) {
                    if (exceptions == null)
                        exceptions = new Exception[ids.length];
                    exceptions[i] = e;
                    readByIdsContext.addFailure(ids[i], e);
                }
            }
        } finally {
            RuntimeException ex = null;
            try {

                if (context.isActiveBlobStoreBulk()) {
                    //if off-heap in case of bulk we first need to flush to the blob-store
                    context.getBlobStoreBulkInfo().bulk_flush(context, false/*only_if_chunk_reached*/, true);
                }
            } catch (RuntimeException ex1) {
                ex = ex1;
            }
            context = _cacheManager.freeCacheContext(context);
            if (ex != null)
                throw ex;

        }

        if (exceptions != null) {
            if (readByIdsContext.accumulate())
                throw ReadTakeByIdsException.newException(idsQueryPacket.getIds(), readByIdsContext.getResults(), exceptions, take);
            else
                throw new ClearByIdsException(readByIdsContext);
        }

    }

    private Map<String, IEntryHolder> prefetchNonBlobStoreReadByIdsEntries(Object[] ids, ITemplatePacket[] templatePackets,
                                                                           IServerTypeDesc typeDesc) throws RemoteException {
        Map<String, IEntryHolder> prefetchedEntries = new HashMap<>();
        Context context = null;

        try {
            context = _cacheManager.getCacheContext();

            Map<Object, ITemplatePacket> cacheMissEntries = null;
            for (int i = 0; i < ids.length; i++) {
                IEntryCacheInfo entryCacheInfo = _cacheManager.getPEntryByUid(templatePackets[i].getUID());
                if (entryCacheInfo == null) {
                    if (cacheMissEntries == null)
                        cacheMissEntries = new HashMap<>();
                    cacheMissEntries.put(ids[i], templatePackets[i]);
                } else if (!entryCacheInfo.isRecentDelete()) {
                    IEntryHolder pureCacheEntry = entryCacheInfo.getEntryHolder(_cacheManager);
                    if (pureCacheEntry != null)
                        prefetchedEntries.put(templatePackets[i].getUID(), pureCacheEntry);
                }
            }
            if (cacheMissEntries != null) {
                try {
                    Object[] cacheMissIds = new Object[cacheMissEntries.size()];
                    IEntryHolder[] cacheMissEntryHolders = new IEntryHolder[cacheMissEntries.size()];
                    int index = 0;
                    for (Map.Entry<Object, ITemplatePacket> entry : cacheMissEntries.entrySet()) {
                        Object id = entry.getKey();
                        ITemplatePacket template = entry.getValue();
                        IEntryHolder entryHolder = TemplateHolderFactory.createTemplateHolder(typeDesc, template, template.getUID(), Lease.FOREVER);
                        cacheMissIds[index] = id;
                        cacheMissEntryHolders[index] = entryHolder;
                        index++;
                    }

                    Map<String, IEntryHolder> entries = _cacheManager.getStorageAdapter().getEntries(context, cacheMissIds,
                            typeDesc.getTypeName(), cacheMissEntryHolders);
                    if (entries != null)
                        prefetchedEntries.putAll(entries);
                } catch (SAException e) {
                    throw new RemoteException("Failed prefetching entries for storage adapter", e);
                }
            }

        } finally {
            context = _cacheManager.freeCacheContext(context);
        }

        return prefetchedEntries;
    }

    public AnswerHolder read(ITemplatePacket template, Transaction txn, long timeout, boolean ifExists,
                             boolean take, SpaceContext sc,
                             boolean returnOnlyUid, boolean fromReplication, boolean origin,
                             int operationModifiers)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException, InterruptedException {
        if (Modifiers.contains(operationModifiers, Modifiers.EXPLAIN_PLAN)) {
            SingleExplainPlan.validate(timeout, _cacheManager.isBlobStoreCachePolicy(), operationModifiers, template.getCustomQuery(), getClassTypeInfo(template.getTypeName()).getIndexes());
        }
        monitorMemoryUsage(false);
        if (take)
            monitorReplicationStateForModifyingOperation(txn, OperationWeightInfoFactory.create(1, WeightInfoOperationType.TAKE));

        return unsafeRead(template, txn, timeout, ifExists, take, sc,
                returnOnlyUid, fromReplication, origin, operationModifiers, null, null /* prefetchedEntries */);
    }

    private AnswerHolder unsafeRead(ITemplatePacket template, Transaction txn, long timeout, boolean ifExists,
                                    boolean take, SpaceContext sc,
                                    boolean returnOnlyUid, boolean fromReplication, boolean origin,
                                    int operationModifiers, ReadByIdsInfo readByIdsInfo, Map<String, IEntryHolder> prefetchedEntries)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException, InterruptedException {
        Context context = (fromReplication && getReplicationNode().getBlobStoreReplicaConsumeHelper() != null
                && getReplicationNode().getDirectPersistencyBackupSyncIteratorHandler() != null)
                ? getReplicationNode().getBlobStoreReplicaConsumeHelper().getContext() : null;

        if (context == null) {
            context = (fromReplication && getReplicationNode().getBlobStoreReplicationBulkConsumeHelper() != null)
                    ? getReplicationNode().getBlobStoreReplicationBulkConsumeHelper().getContext() : null;
        }

        boolean suppliedContext = context != null;

        try {
            if (!suppliedContext)
                context = _cacheManager.getCacheContext();
            return unsafeRead_impl(template, txn, timeout, ifExists,
                    take, sc,
                    returnOnlyUid, fromReplication, origin,
                    operationModifiers, readByIdsInfo, prefetchedEntries, context);
        } finally {
            if (!suppliedContext)
                context = _cacheManager.freeCacheContext(context);
        }


    }


    private AnswerHolder unsafeRead_impl(ITemplatePacket template, Transaction txn, long timeout, boolean ifExists,
                                         boolean take, SpaceContext sc,
                                         boolean returnOnlyUid, boolean fromReplication, boolean origin,
                                         int operationModifiers, ReadByIdsInfo readByIdsInfo, Map<String, IEntryHolder> prefetchedNonBlobStoreEntries, Context context)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException, InterruptedException {

        if (take && TakeModifiers.isEvictOnly(operationModifiers)) {
            if (_cacheManager.isResidentEntriesCachePolicy())
                throw new IllegalArgumentException("EVICT modifier is not supported in non-evictable policy.");

            txn = null;  //ignore
            timeout = 0; //ignore
        }
        IServerTypeDesc typeDesc = _typeManager.loadServerTypeDesc(template);

        // Check if FIFO:
        boolean isFifoOperation = template.isFifo() || ReadModifiers.isFifo(operationModifiers);
        // Validate FIFO:
        if (isFifoOperation && !typeDesc.isFifoSupported())
            throw new InvalidFifoTemplateException(template.getTypeName());
        if ((template.getUID() != null || (template.getID() != null && template.getExtendedMatchCodes() == null)) && ReadModifiers.isFifoGroupingPoll(operationModifiers))
            operationModifiers = Modifiers.remove(operationModifiers, ReadModifiers.FIFO_GROUPING_POLL);//ignore it
        if (ReadModifiers.isFifoGroupingPoll(operationModifiers) && !_cacheManager.isMemorySpace() && _cacheManager.isEvictableCachePolicy())
            throw new UnsupportedOperationException(" fifo grouping not supported with persistent-LRU");
        // Disable FIFO if not required:
        if (isFifoOperation && (template.getUID() != null || ReadModifiers.isFifoGroupingPoll(operationModifiers)))
            isFifoOperation = false;

        final XtnEntry txnEntry = initTransactionEntry(txn, sc, fromReplication);

        // create template UID
        String uid = null;
        if (timeout != 0)
            uid = _uidFactory.createUIDFromCounter();

        if (fromReplication && timeout != 0) {
            throw new RuntimeException("operation with timeout = NO_WAIT came from replication");
        }


        // build Template Holder and mark it stable
        IResponseContext respContext = ResponseContext.getResponseContext();

        int templateOperation;
        if (ifExists)
            templateOperation = take ? SpaceOperations.TAKE_IE : SpaceOperations.READ_IE;
        else
            templateOperation = take ? SpaceOperations.TAKE : SpaceOperations.READ;

        final long startTime = SystemTime.timeMillis();
        ITemplateHolder tHolder = TemplateHolderFactory.createTemplateHolder(typeDesc, template,
                uid, LeaseManager.toAbsoluteTime(timeout, startTime) /* expiration time*/,
                txnEntry, startTime, templateOperation, respContext, returnOnlyUid,
                operationModifiers, isFifoOperation, fromReplication);

        tHolder.setAnswerHolder(new AnswerHolder());
        tHolder.getAnswerHolder().setServerTypeDesc(typeDesc);
        tHolder.setNonBlockingRead(isNonBlockingReadForOperation(tHolder));
        tHolder.setID(template.getID());

        if (take) // call  filters for take
        {
            if (_filterManager._isFilter[FilterOperationCodes.BEFORE_TAKE] && !tHolder.isInitiatedEvictionOperation())
                _filterManager.invokeFilters(FilterOperationCodes.BEFORE_TAKE, sc, tHolder);
            //set fields for after filter (if one will be enlisted)
            if (!tHolder.isInitiatedEvictionOperation())
                tHolder.setForAfterOperationFilter(FilterOperationCodes.AFTER_TAKE, sc, _filterManager, null);
        } else  //  filters for read
        {
            if (_filterManager._isFilter[FilterOperationCodes.BEFORE_READ])
                _filterManager.invokeFilters(FilterOperationCodes.BEFORE_READ, sc, tHolder);
            //set fields for after filter (if one will be enlisted)
            tHolder.setForAfterOperationFilter(FilterOperationCodes.AFTER_READ, sc, _filterManager, null);
        }


        boolean answerSetByThisThread = false;
        int numOfEntriesMatched;

        context.setMainThread(true);
        context.setOperationID(template.getOperationID());
        context.setReadByIdsInfo(readByIdsInfo);
        context.setPrefetchedNonBlobStoreEntries(prefetchedNonBlobStoreEntries);

        if(take && Modifiers.contains(operationModifiers,Modifiers.BACKUP_ONLY)){
            context.setBackupOnly();
        }

        if (ifExists)
            _coreProcessor.handleDirectReadIEOrTakeIESA(context, tHolder, fromReplication, origin);
        else
            _coreProcessor.handleDirectReadOrTakeSA(context, tHolder, fromReplication, origin);

        if (context.getReplicationContext() != null) {
            tHolder.getAnswerHolder().setSyncRelplicationLevel(context.getReplicationContext().getCompleted());
        }
        answerSetByThisThread = context.isOpResultByThread();
        numOfEntriesMatched = context.getNumberOfEntriesMatched();

        boolean callBackMode = ResponseContext.isCallBackMode();

        // wait on Answer
        if (!callBackMode && !answerSetByThisThread && !tHolder.hasAnswer())
            waitForBlockingAnswer(timeout, tHolder.getAnswerHolder(), startTime, tHolder);

        if (answerSetByThisThread) {
            tHolder.getAnswerHolder().throwExceptionIfExists();
            tHolder.getAnswerHolder().setNumOfEntriesMatched(numOfEntriesMatched);
            return tHolder.getAnswerHolder();
        }

        if (callBackMode)
            return prepareCallBackModeAnswer(tHolder, true);

        return prepareBlockingModeAnswer(tHolder, true);
    }

    private void waitForBlockingAnswer(long timeout, AnswerHolder aHolder,
                                       final long startTime, ITemplateHolder tHolder)
            throws InterruptedException {
        //if no answer found recheck result with locked template
        //in order to avoid "lost" operations
        long timeToWait = 0;
        long expirationTimeInMillis = getExpirationTimeInMillis(timeout,
                startTime,
                tHolder);
        while (true) {
            if (timeout != Long.MAX_VALUE) {
                timeToWait = expirationTimeInMillis - SystemTime.timeMillis();
            }
            if (timeout == Long.MAX_VALUE || timeToWait > 0) {
                synchronized (aHolder) {
                    if (!tHolder.hasAnswer()) {
                        if (timeout != Long.MAX_VALUE)
                            aHolder.wait(timeToWait);
                        else
                            aHolder.wait();
                    }
                    if (tHolder.hasAnswer())
                        break;
                }//synchronized
            }//if  (timeout == Long.MAX_VALUE || timeToWait > 0)
            else
                break; //no wating any more
        }//while
    }

    private long getExpirationTimeInMillis(long timeout, final long startTime,
                                           ITemplateHolder tHolder) {
        if (timeout == Long.MAX_VALUE)
            return 0;

        return (tHolder.getExpirationTime() - startTime) + SystemTime.timeMillis();
    }

    private AnswerHolder prepareBlockingModeAnswer(ITemplateHolder tHolder, boolean throwError)
            throws TransactionException, UnusableEntryException {
        Context context = null;
        //this code is reached only in blocking mode
        //if no result returned kill template under lock
        //before giving up check that template is not deleted yet,
        //and mark it deleted
        ILockObject templateLock = getTemplateLockObject(tHolder);
        try {
            synchronized (templateLock) {
                if (!tHolder.hasAnswer()) {
                    context = _cacheManager.getCacheContext();
                    context.setOperationID(tHolder.getOperationID());
                    context.setOperationAnswer(tHolder, null, null);
                    if (!tHolder.isDeleted()) {
                        _cacheManager.removeTemplate(context, tHolder, false /*fromReplication*/, true /*origin*/, false);
                    }
                }

                // if there is an exception throw it according to its type
                if (throwError)
                    tHolder.getAnswerHolder().throwExceptionIfExists();
            }//synchronized

            // we have an answer - return it to caller!
            return tHolder.getAnswerHolder();

        } finally {
            if (templateLock != null)
                freeTemplateLockObject(templateLock);

            if (context != null)
                _cacheManager.freeCacheContext(context);
        }
    }

    private AnswerHolder prepareCallBackModeAnswer(ITemplateHolder tHolder, boolean throwError)
            throws TransactionException, UnusableEntryException {
        ILockObject templateLock = getTemplateLockObject(tHolder);
        try {
            synchronized (templateLock) {
                if (!tHolder.hasAnswer()) {
                    ResponseContext.dontSendResponse();
                    moveTemplateToSecondPhase(tHolder);
                    return null;
                }

                if (throwError)
                    tHolder.getAnswerHolder().throwExceptionIfExists();

                return tHolder.getAnswerHolder();
            }
        } finally {
            if (templateLock != null) {
                freeTemplateLockObject(templateLock);
            }
        }
    }

    /**
     * template MUST be under lock.
     */
    private void moveTemplateToSecondPhase(ITemplateHolder template) {
        if (template.isDeleted() || !template.isInCache())
            return;

        template.setSecondPhase();
    }

    //set the values for a specific template
    public boolean isNonBlockingReadForOperation(ITemplateHolder tHolder) {
        return
                _allowNonBlockingRead && /*_cacheManager.getCachePolicy() == CACHE_POLICY_ALL_IN_CACHE && */
                        tHolder.isReadOperation() &&
                        !tHolder.isExclusiveReadLockOperation() &&
                        (tHolder.getXidOriginated() == null || _useDirtyRead || ReadModifiers.isDirtyRead(tHolder.getOperationModifiers()) || ReadModifiers.isReadCommitted(tHolder.getOperationModifiers())) &&
                        (tHolder.getTemplateOperation() != SpaceOperations.READ_IE || tHolder.getExpirationTime() == 0);

    }

    /**
     * Return a snapshot of the given entry. <p> Note: Snapshot doesn't finalize FIFO indicator and
     * thus no InvalidFifoException is thrown.
     */
    public void snapshot(ITemplatePacket entryPacket)
            throws UnusableEntryException {
        ITypeDesc prev = null;
        if (getCacheManager().isBlobStoreCachePolicy())
            prev = _typeManager.getTypeDesc(entryPacket.getTypeName()); //do we need to save in blob-store ?
        try {
            IServerTypeDesc cur = _typeManager.loadServerTypeDesc(entryPacket);
            if (getCacheManager().isBlobStoreCachePolicy() && cur.getTypeDesc() != prev) //need to be stored in case blobStore recovery will be used
                getCacheManager().getStorageAdapter().introduceDataType(cur.getTypeDesc());
        } catch (UnknownTypeException ute) {
            throw new ProxyInternalSpaceException(ute.toString(), ute);
        }
    }

    /*----------------- JavaSpaces API : end -----------------*/

    /*----------------- Extended API : begin -----------------*/

    /**
     * Creates IEntryHolder objects from the specified entry packets and inserts them into all
     * relevant engine tables.
     *
     * if entryPacket.NoWriteLease=true the return value will be Entry UID(String) of Entry,
     * otherwise Lease object.
     */
    public WriteEntriesResult write(IEntryPacket[] entryPackets, Transaction txn, long lease, long leases[], int modifiers, SpaceContext sc, long timeout, boolean newRouter)
            throws TransactionException, RemoteException, UnknownTypesException {
        monitorMemoryUsage(true);
        monitorReplicationStateForModifyingOperation(txn, OperationWeightInfoFactory.create(entryPackets.length, WeightInfoOperationType.WRITE));

        if (UpdateModifiers.isPotentialUpdate(modifiers)) {
            if (newRouter) {
                AnswerHolder ah = newAdaptUpdateMultiple(entryPackets, txn, lease, leases, sc, timeout, modifiers);
                return ah != null ? ah.getUpdateMultipleResult() : null;
            } else
                return oldAdaptUpdateMultiple(entryPackets, txn, lease, leases, sc, modifiers, newRouter);
        }
        // handle entry types. All types must be handled before inserting
        // any entry to engine, because if an UnknownTypeException occurs,
        // the client will retransmit the packets.
        boolean[] shouldReplicateArray = new boolean[entryPackets.length];
        IServerTypeDesc[] types = new IServerTypeDesc[entryPackets.length];
        Context context = null;
        WriteEntriesResult result = new WriteEntriesResult(entryPackets.length);
        List<Integer> unknownTypePositions = null;

        OperationID[] opIDs = new OperationID[entryPackets.length];
        boolean anyFifoClass = false;
        for (int i = 0; i < entryPackets.length; i++) {
            opIDs[i] = entryPackets[i].getOperationID();

            try {
                IServerTypeDesc typeDesc = _typeManager.loadServerTypeDesc(entryPackets[i]);
                types[i] = typeDesc;

                shouldReplicateArray[i] = false;
                if (isReplicated())
                    shouldReplicateArray[i] = shouldReplicate(ReplicationOperationType.WRITE, typeDesc, false, false);
                if (!anyFifoClass && typeDesc.getTypeDesc().isFifoSupported())
                    anyFifoClass = true;

            } catch (UnusableEntryException e) {
                result.setError(i, e);
            } catch (UnknownTypeException e) {
                if (entryPackets[i].getTypeDescriptor() != null)
                    result.setError(i, e);
                else {
                    if (unknownTypePositions == null)
                        unknownTypePositions = new LinkedList<>();
                    unknownTypePositions.add(i);
                }
            }
        }
        if (unknownTypePositions != null) {
            // some of the tte are missing, request from the proxy to repeat the request with full EP for the
            // following indices.
            throw new UnknownTypesException(unknownTypePositions);
        }

        // attach to Xtn
        XtnEntry txnEntry = null;
        if (txn != null) {
            txnEntry = attachToXtn((ServerTransaction) txn, false);
            attachFromGatewayStateToTransactionIfNeeded(sc, txnEntry);
        }


        try {
            context = _cacheManager.getCacheContext();
            if (_isSyncReplication)
                context.setSyncReplFromMultipleOperation(true);

            context.setMultipleOperation();
            context.setOperationIDs(opIDs);

            context.applyOperationContext(sc);

            /**
             * currently- call regular write for each entry since
             * regular write is locking the object
             **/
            if (txnEntry != null) {
                txnEntry.lock();
                try {
                    if (!txnEntry.m_Active)
                        throw new TransactionException("The transaction is not active: " + txnEntry.m_Transaction);

                    context.setTransactionalMultipleOperation(true);

                    result = writeEntryPackets(entryPackets, result, context, txnEntry.m_Transaction,
                            lease, leases, modifiers, sc, false/* reInsertedEntry*/, true /*fromWriteMultiple */);
                } finally {
                    txnEntry.unlock();
                }
            } else {
                if (_cacheManager.isBlobStoreCachePolicy() && _cacheManager.useBlobStoreBulks()) {//can we exploit blob-store bulking ?
                    if (!anyFifoClass || !_cacheManager.isDirectPersistencyEmbeddedtHandlerUsed())
                        context.setBlobStoreBulkInfo(new BlobStoreBulkInfo(_cacheManager, false /*takeMultipleBulk*/));
                }
                result = writeEntryPackets(entryPackets, result, context, null,
                        lease, leases, modifiers, sc, false/* reInsertedEntry*/, true /*fromWriteMultiple */);
            }

            return result;
        } finally {
            RuntimeException ex = null;
            if (txnEntry != null)
                txnEntry.decrementUsed(true /* disable server-side cleanup as unused */);

            try {
                if (context.isActiveBlobStoreBulk()) {
                    //if off-heap in case of bulk we first need to flush to the blob-store
                    context.getBlobStoreBulkInfo().bulk_flush(context, false/*only_if_chunk_reached*/, true);
                }
            } catch (RuntimeException ex1) {
                ex = ex1;
            }

            replicateAndfreeCache(context);
            if (ex != null)
                throw ex;
        }// finally
    }


    public WriteEntriesResult oldAdaptUpdateMultiple(IEntryPacket[] entries, Transaction txn, long lease, long[] leases, SpaceContext sc
            , int modifiers, boolean newRouter)
            throws RemoteException, TransactionException, UnknownTypesException {
        final boolean noReturnValue = UpdateModifiers.isNoReturnValue(modifiers);
        if (leases == null) {
            leases = new long[entries.length];
            for (int i = 0; i < leases.length; ++i)
                leases[i] = lease;
        }

        Object[] updateMultipleResult;
        try {
            if (UpdateModifiers.isUpdateOrWrite(modifiers))
                updateMultipleResult = updateOrWrite(entries, txn, leases, sc, modifiers, true, newRouter);
            else
                updateMultipleResult = updateMultiple(entries, txn, leases, sc, modifiers, newRouter);
        } catch (UnknownTypeException e) {
            List<Integer> positions = new ArrayList<>();
            for (int i = 0; i < entries.length; ++i)
                positions.add(i);
            throw new UnknownTypesException(positions);
        } catch (UnusableEntryException e) {
            throw new WriteMultipleException(entries.length, e);
        }

        WriteEntriesResult result = new WriteEntriesResult(updateMultipleResult.length);
        for (int i = 0; i < updateMultipleResult.length; ++i) {
            if (updateMultipleResult[i] == null) //Update locked under transaction result
                result.setError(i, new OperationTimeoutException());
            else if (updateMultipleResult[i] instanceof Exception)
                result.setError(i, (Exception) updateMultipleResult[i]);
            else if (updateMultipleResult[i] instanceof WriteEntryResult)
                result.setResult(i, (WriteEntryResult) updateMultipleResult[i]);
            else if (updateMultipleResult[i] instanceof LeaseContext<?>) {
                LeaseContext<?> resultLease = (LeaseContext<?>) updateMultipleResult[i];
                IEntryPacket prevObject = (IEntryPacket) resultLease.getObject();
                WriteEntryResult writeResult = new WriteEntryResult(resultLease.getUID(), prevObject == null ? 1 : prevObject.getVersion() + 1, leases[i]);
                result.setResult(i, writeResult);
            } else if (updateMultipleResult[i] instanceof IEntryPacket) {
                if (!noReturnValue)
                    result.setResult(i, new WriteEntryResult((IEntryPacket) updateMultipleResult[i]));
            }
        }

        return result;
    }


    public AnswerHolder newAdaptUpdateMultiple(IEntryPacket[] entries, Transaction txn, long lease, long[] leases, SpaceContext sc
            , long timeout, int modifiers)
            throws RemoteException, TransactionException, UnknownTypesException {
        if (leases == null) {
            leases = new long[entries.length];
            Arrays.fill(leases, lease);
        }

        AnswerHolder ah = null;
        try {
            if (UpdateModifiers.isUpdateOrWrite(modifiers))
                ah = newUpdateOrWriteMultiple(entries, txn, leases, sc, timeout, modifiers);
            else
                ah = newUpdateMultiple(entries, txn, leases, sc, timeout, modifiers);
        } catch (UnknownTypeException e) {
            List<Integer> positions = new ArrayList<>();
            for (int i = 0; i < entries.length; ++i)
                positions.add(i);
            throw new UnknownTypesException(positions);
        } catch (UnusableEntryException e) {
            throw new WriteMultipleException(entries.length, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WriteMultipleException(entries.length, e);
        }

        return ah;
    }


    /**
     * An atomic update-or-write (called when updateMultiple is used in conjuction with
     * UPDATE_OR_WRITE modifier)
     *
     * @param entries            entries each holding UID and updated values of matching entry.
     * @param txn                transaction (if any).
     * @param leases             leases of each entry.
     * @param sc                 space context.
     * @param operationModifiers Operation modifiers - can be combination of modifiers.
     * @return an array of returned values as corresponding to updateMultiple. update result or null
     * on txn timeout or null if write was performed.
     */
    Object[] updateOrWrite(IEntryPacket[] entries, Transaction txn, long[] leases,
                           SpaceContext sc, int operationModifiers, boolean fromWriteMultiple, boolean newRouter)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException {
        // attach to Xtn
        XtnEntry txnEntry = null;
        if (txn != null) {
            txnEntry = attachToXtn((ServerTransaction) txn, false);
        }
        Context context = null;
        try {
            context = _cacheManager.getCacheContext();
            if (_isSyncReplication)
                context.setSyncReplFromMultipleOperation(true);

            context.setMultipleOperation();
            context.applyOperationContext(sc);
            Object[] returnValues = new Object[entries.length];
            for (int i = 0; i < entries.length; ++i) {
                context.setWriteResult(null);
                try {
                    UpdateOrWriteContext ctx = new UpdateOrWriteContext(entries[i], leases[i],
                            0, txn, sc, operationModifiers, !fromWriteMultiple,
                            false, fromWriteMultiple);
                    if (txnEntry != null
                            && context.isFromGateway()
                            && Modifiers.contains(operationModifiers,
                            Modifiers.OVERRIDE_VERSION)) {
                        txnEntry.setGatewayOverrideVersion(entries[i].getUID());
                    }
                    ctx.setCacheContext(context);
                    AnswerPacket answerPacket = _spaceImpl.updateOrWrite(ctx, newRouter);

                    if (answerPacket != null) {
                        //if write was made - assign the lease
                        if (fromWriteMultiple && answerPacket.m_leaseProxy != null)
                            returnValues[i] = answerPacket.m_leaseProxy;
                        else if (fromWriteMultiple && answerPacket.getWriteEntryResult() != null)
                            returnValues[i] = answerPacket.getWriteEntryResult();
                        else
                            returnValues[i] = answerPacket.m_EntryPacket;
                    }
                } catch (UnknownTypeException e) {
                    throw e;
                } catch (InterruptedException e) {
                    throw e;
                } catch (Throwable e) {
                    returnValues[i] = e;
                }

            }

            return returnValues;

        } catch (InterruptedException e) {
            // can't get here cause the call is NO_WAIT
            // restore interrupt state just in case it does get here
            Thread.currentThread().interrupt();
            return null;
        } finally {
            if (txnEntry != null)
                txnEntry.decrementUsed(true /* disable server-side cleanup as unused */);
            replicateAndfreeCache(context);
        }// Finally

    }


    AnswerHolder newUpdateOrWriteMultiple(IEntryPacket[] entries, Transaction txn, long[] leases,
                                          SpaceContext sc, long timeout, int operationModifiers)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException, InterruptedException {
        //no need to check redolog- done is writeMultiple calling this routine

        boolean fromWriteMultiple = true;
        boolean anyFifoClass = false;
        //verify that each entry has a UID and its type is known
        for (int i = 0; i < entries.length; i++) {
            // must be done before getUID() to ensure unmarsh of MarshalledExternalEntry in embedded space.
            IServerTypeDesc std = _typeManager.loadServerTypeDesc(entries[i]);
            if (!anyFifoClass && std.getTypeDesc().isFifoSupported())
                anyFifoClass = true;
        }

        // attach to Xtn
        XtnEntry txnEntry = null;
        if (txn != null) {
            txnEntry = attachToXtn((ServerTransaction) txn, false);
            attachFromGatewayStateToTransactionIfNeeded(sc, txnEntry);
        }

        //create a concentrating template to handle the by ids ops
        // create template UID
        String uid = null;
        if (timeout != 0)
            uid = _uidFactory.createUIDFromCounter();

        IResponseContext respContext = ResponseContext.getResponseContext();
        final long startTime = SystemTime.timeMillis();

        ITemplateHolder tHolder = TemplateHolderFactory.createTemplateHolder(getTypeManager().getServerTypeDesc(IServerTypeDesc.ROOT_TYPE_NAME) /*typeDesc*/, new TemplatePacket(),
                null /*uid*/, Long.MAX_VALUE /* dummy expiration time*/,
                txnEntry, startTime, SpaceOperations.UPDATE, respContext, false /*returnOnlyUid*/,
                operationModifiers, false /*isFifoOperation*/);

        tHolder.setAnswerHolder(new AnswerHolder());
        UpdateOrWriteMultipleContext opContext = new UpdateOrWriteMultipleContext(tHolder, entries, leases, operationModifiers,
                timeout, this, txnEntry);
        tHolder.setMultipleIdsContext(opContext);


        Context context = null;
        try {
            context = _cacheManager.getCacheContext();
            if (_isSyncReplication)
                context.setSyncReplFromMultipleOperation(true);

            context.setMultipleOperation();
            context.applyOperationContext(sc);
            if (txn == null && _cacheManager.isBlobStoreCachePolicy() && timeout == JavaSpace.NO_WAIT && _cacheManager.useBlobStoreBulks()) {//can we exploit blob-store bulking ?
                //SUPPORT FOR TIMEOUT WILL BE ADDED LATTER
                if (!anyFifoClass || !_cacheManager.isDirectPersistencyEmbeddedtHandlerUsed())
                    context.setBlobStoreBulkInfo(new BlobStoreBulkInfo(_cacheManager, false /*takeMultipleBulk*/));
            }

            Context entryContext = null;
            for (int i = 0; i < entries.length; ++i) {
                entryContext = timeout != JavaSpace.NO_WAIT ? null : context;
                context.setWriteResult(null);
                try {
                    UpdateOrWriteContext ctx = new UpdateOrWriteContext(entries[i], leases[i],
                            timeout, txn, sc, operationModifiers, !fromWriteMultiple,
                            false, fromWriteMultiple, tHolder, i);
                    if (txnEntry != null
                            && context.isFromGateway()
                            && Modifiers.contains(operationModifiers,
                            Modifiers.OVERRIDE_VERSION)) {
                        txnEntry.setGatewayOverrideVersion(entries[i].getUID());
                    }
                    ctx.setCacheContext(entryContext);
                    _spaceImpl.updateOrWrite(ctx, true /*newRouter*/);

                } catch (UnknownTypeException e) {//cannot happen- types are checked prior to the loop
                    throw e;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    tHolder.getMultipleIdsContext().setAnswer(e, i);
                    continue;
                } catch (Throwable e) {
                    tHolder.getMultipleIdsContext().setAnswer(e, i);
                    continue;
                }
            }
        } finally {
            RuntimeException ex = null;
            try {
                if (context.isActiveBlobStoreBulk()) {
                    //if off-heap in case of bulk we first need to flush to the blob-store
                    context.getBlobStoreBulkInfo().bulk_flush(context, false/*only_if_chunk_reached*/, true);
                }
            } catch (RuntimeException ex1) {
                ex = ex1;
            }

            int completedSyncReplication = replicateAndfreeCache(context);  //replicate the ops accumulated so far
            if (!(tHolder.getAnswerHolder().getUpdateMultipleResult() == null)) {
                for (WriteEntryResult writeEntryResult : tHolder.getAnswerHolder().getUpdateMultipleResult().getResults()) {
                    if (writeEntryResult != null) {
                        writeEntryResult.setSyncReplicationLevel(completedSyncReplication);
                    }
                }
            }
            if (ex != null)
                throw ex;
        }// Finally

        boolean callBackMode = ResponseContext.isCallBackMode();

        //wait on Answer- from all the update templates into the concentrating template
        if (!callBackMode && !tHolder.hasAnswer())
            waitForBlockingAnswer(timeout, tHolder.getAnswerHolder(), startTime, tHolder);

        if (tHolder.hasAnswer())
            return tHolder.getAnswerHolder();

        if (callBackMode) {
            prepareCallBackModeAnswer(tHolder, false);
            return tHolder.getAnswerHolder();
        }
        prepareBlockingModeAnswer(tHolder, false);
        return tHolder.getAnswerHolder();
    }

    public void updateObjectTypeReadCounts( IServerTypeDesc serverTypeDesc, ITemplatePacket template) {

        if(!this.getMetricManager().getMetricFlagsState().isDataReadCountsMetricEnabled()){
            return;
        }

        if( serverTypeDesc == null ){
            //serverTypeDesc is null when read returns empty result
            return;
        }

        String typeName = template.getTypeName();
        if (typeName != null) {
            serverTypeDesc.getReadCounter().inc(1);
        }
    }

    /**
     * call write for each EP in value, update value with the result
     */
    private WriteEntriesResult writeEntryPackets(IEntryPacket[] entryPackets, WriteEntriesResult values, Context context,
                                                 ServerTransaction transaction, long lease, long[] leases, int modifiers,
                                                 SpaceContext sc, boolean reInsertedEntry, boolean fromWriteMultiple) throws RemoteException, TransactionException {
        for (int i = 0; i < entryPackets.length; ++i) {
            if (values.isError(i)) // already got an error, no need to write this entry
                continue;
            try {
                final long entryLease = leases != null ? leases[i] : lease;
                WriteEntryResult writeResult = write(context, entryPackets[i], transaction,
                        entryLease, modifiers, false /* fromReplication*/, true/*origin*/,
                        sc, false/* reInsertedEntry*/, true /*fromWriteMultiple */);
                values.setResult(i, writeResult);
                //values.setLease(i, writeResult.createLease(entryPackets[i].getTypeName(), getSpaceImpl(), isNoWriteLease(entryPackets[i], modifiers, false/* fromReplication*/)));
            } catch (UnusableEntryException e) {
                values.setError(i, e);
            } catch (UnknownTypeException e) {
                values.setError(i, e);
            } catch (EntryAlreadyInSpaceException e) {
                values.setError(i, e);
            } catch (ProtectiveModeException e) {
                values.setError(i, e);
            } catch (DuplicateIndexValueException e) {
                values.setError(i, e);
            }
        }
        return values;
    }


    public AnswerHolder readMultiple(ITemplatePacket template, Transaction txn, long timeout, boolean ifExists,
                                     boolean take, SpaceContext sc, boolean returnOnlyUid, int operationModifiers,
                                     BatchQueryOperationContext batchOperationContext,
                                     List<SpaceEntriesAggregator> aggregators, ServerIteratorRequestInfo serverIteratorRequestInfo)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException, InterruptedException {
        monitorMemoryUsage(false);
        if (Modifiers.contains(operationModifiers, Modifiers.EXPLAIN_PLAN)) {
            SingleExplainPlan.validate(timeout, _cacheManager.isBlobStoreCachePolicy(), operationModifiers, template.getCustomQuery(), getClassTypeInfo(template.getTypeName()).getIndexes());
        }
        if (take)
            monitorReplicationStateForModifyingOperation(txn, OperationWeightInfoFactory.create(WeightInfoOperationType.TAKE));
        if (take && TakeModifiers.isEvictOnly(operationModifiers)) {
            if (_cacheManager.isResidentEntriesCachePolicy())
                throw new IllegalArgumentException("EVICT modifier is not supported in non-evictable cache policy.");
            txn = null;  //ignore
            timeout = 0; //ignore
        }
        if (batchOperationContext.getMaxEntries() <= 0)
            throw new IllegalArgumentException("Max entries value should be greater then zero.");
        if (batchOperationContext.getMinEntries() < 0 || batchOperationContext.getMinEntries() > batchOperationContext.getMaxEntries())
            throw new IllegalArgumentException("Min entries value should  not  be less then zero or greater than maxEntries.");
        IServerTypeDesc typeDesc = _typeManager.loadServerTypeDesc(template);

        // Check if FIFO:
        boolean isFifoOperation = template.isFifo() || ReadModifiers.isFifo(operationModifiers);
        boolean isServerIteratorRequest = serverIteratorRequestInfo != null;
        // Validate FIFO:
        if (isFifoOperation && !typeDesc.isFifoSupported())
            throw new InvalidFifoTemplateException(template.getTypeName());
        if ((template.getUID() != null || template.getMultipleUIDs() != null || (template.getID() != null && template.getExtendedMatchCodes() == null)) && ReadModifiers.isFifoGroupingPoll(operationModifiers))
            operationModifiers = Modifiers.remove(operationModifiers, ReadModifiers.FIFO_GROUPING_POLL);//ignore it
        if (ReadModifiers.isFifoGroupingPoll(operationModifiers) && !_cacheManager.isMemorySpace() && _cacheManager.isEvictableCachePolicy())
            throw new UnsupportedOperationException(" fifo grouping not supported with persistent-LRU");
        // Disable FIFO if not required:
        if (isFifoOperation && (template.getUID() != null || ReadModifiers.isFifoGroupingPoll(operationModifiers)) || template.getMultipleUIDs() != null)
            isFifoOperation = false;

        if (timeout != 0 && ReadModifiers.isFifoGroupingPoll(operationModifiers))
            timeout = 0;   //f-g not supporting r-t multiple + timeou

        final XtnEntry txnEntry = initTransactionEntry(txn, sc, false /*fromReplication*/);

        // create template UID
        String uid = null;
        if (timeout != 0)
            uid = _uidFactory.createUIDFromCounter();

        IResponseContext respContext = ResponseContext.getResponseContext();

        int templateOperation;
        if (ifExists)
            templateOperation = take ? SpaceOperations.TAKE_IE : SpaceOperations.READ_IE;
        else
            templateOperation = take ? SpaceOperations.TAKE : SpaceOperations.READ;

        final long startTime = SystemTime.timeMillis();
        ITemplateHolder tHolder = TemplateHolderFactory.createTemplateHolder(typeDesc, template,
                uid, LeaseManager.toAbsoluteTime(timeout, startTime) /* expiration time*/,
                txnEntry, startTime, templateOperation, respContext, returnOnlyUid,
                operationModifiers, isFifoOperation);
        if(isServerIteratorRequest) {
            ServerIteratorInfo serverIteratorInfo = _serverIteratorsManager.getOrCreateServerIteratorInfo(serverIteratorRequestInfo);
            if(_logger.isDebugEnabled())
                _logger.debug("Iterator " + serverIteratorRequestInfo.getUuid() + " requested batch " + serverIteratorRequestInfo.getRequestedBatchNumber() + ", stored batch " + serverIteratorInfo.getStoredBatchNumber());
            if(serverIteratorInfo.isBatchRetrialRequest(serverIteratorRequestInfo)){
                ServerIteratorAnswerHolder serverIteratorAnswerHolder = new ServerIteratorAnswerHolder(serverIteratorRequestInfo.getRequestedBatchNumber());
                serverIteratorAnswerHolder.setEntryPackets(serverIteratorInfo.getStoredEntryPacketsBatch());
                return serverIteratorAnswerHolder;
            }
            tHolder.setServerIteratorInfo(serverIteratorInfo);
            tHolder.setAnswerHolder(new ServerIteratorAnswerHolder(serverIteratorRequestInfo.getRequestedBatchNumber()));
        }
        else {
            tHolder.setAnswerHolder(new AnswerHolder());
        }
        tHolder.getAnswerHolder().setServerTypeDesc(typeDesc);
        tHolder.setNonBlockingRead(isNonBlockingReadForOperation(tHolder));
        tHolder.setID(template.getID());
        tHolder.setBatchOperationContext(batchOperationContext);
        if (aggregators != null)
            tHolder.setAggregatorContext(new EntryHolderAggregatorContext(aggregators, tHolder, getPartitionIdZeroBased()));

        if (take) // call  filters for take
        {
            if (_filterManager._isFilter[FilterOperationCodes.BEFORE_TAKE_MULTIPLE] && !tHolder.isInitiatedEvictionOperation())
                _filterManager.invokeFilters(FilterOperationCodes.BEFORE_TAKE_MULTIPLE, sc, tHolder);
            //set fields for after filter (if one will be enlisted)
            if (!tHolder.isInitiatedEvictionOperation())
                tHolder.setForAfterOperationFilter(FilterOperationCodes.AFTER_TAKE_MULTIPLE, sc, _filterManager, null);
        } else  //  filters for read
        {
            if (_filterManager._isFilter[FilterOperationCodes.BEFORE_READ_MULTIPLE])
                _filterManager.invokeFilters(FilterOperationCodes.BEFORE_READ_MULTIPLE, sc, tHolder);
            //set fields for after filter (if one will be enlisted)
            tHolder.setForAfterOperationFilter(FilterOperationCodes.AFTER_READ_MULTIPLE, sc, _filterManager, null);
        }

        Context context = null;
        boolean answerSetByThisThread = false;
        int numOfEntriesMatched;

        try {
            context = _cacheManager.getCacheContext();
            context.setMainThread(true);
            context.setOperationID(template.getOperationID());
            context.applyOperationContext(sc);
            if (take && txn == null && _cacheManager.isBlobStoreCachePolicy() && _cacheManager.useBlobStoreBulks()) {//can we exploit blob-store bulking ?
                context.setBlobStoreBulkInfo(new BlobStoreBulkInfo(_cacheManager, true /*takeMultipleBulk*/));
            }

            if (ifExists)
                _coreProcessor.handleDirectMultipleReadIEOrTakeIESA(context, tHolder);
            else
                _coreProcessor.handleDirectMultipleReadTakeSA(context, tHolder);

            answerSetByThisThread = context.isOpResultByThread();
            numOfEntriesMatched = context.getNumberOfEntriesMatched();
            if (take && (context.getReplicationContext() != null)) {
                tHolder.getAnswerHolder().setSyncRelplicationLevel(context.getReplicationContext().getCompleted());
            }
        } finally {
            context = _cacheManager.freeCacheContext(context);
        }

        boolean callBackMode = ResponseContext.isCallBackMode();

        // wait on Answer
        if (!callBackMode && !answerSetByThisThread && !tHolder.hasAnswer())
            waitForBlockingAnswer(timeout, tHolder.getAnswerHolder(), startTime, tHolder);

        if (answerSetByThisThread) {
            tHolder.getAnswerHolder().setNumOfEntriesMatched(numOfEntriesMatched);
            if(isServerIteratorRequest) {
                updateServerIteratorAnswerHolder(tHolder);
            }
            return tHolder.getAnswerHolder();
        }

        if (callBackMode) {
            if (prepareCallBackModeAnswer(tHolder, true) == null)
                return null;
        } else
            prepareBlockingModeAnswer(tHolder, true);

        if(isServerIteratorRequest) {
            updateServerIteratorAnswerHolder(tHolder);
        }
        return tHolder.getAnswerHolder();
    }

    private void updateServerIteratorAnswerHolder(ITemplateHolder templateHolder){
        ServerIteratorInfo serverIteratorInfo = templateHolder.getServerIteratorInfo();
        ServerIteratorAnswerHolder serverIteratorAnswerHolder = (ServerIteratorAnswerHolder) templateHolder.getAnswerHolder();
        serverIteratorInfo.setStoredEntryPacketsBatch(serverIteratorAnswerHolder.getEntryPackets()).setStoredBatchNumber(serverIteratorAnswerHolder.getBatchNumber());
    }


    protected void attachFromGatewayStateToTransactionIfNeeded(SpaceContext sc,
                                                               XtnEntry txnEntry) {
        if (sc != null && sc.isFromGateway()) {
            txnEntry.setFromGateway();
        }
    }

    public Pair<Integer, SingleExplainPlan> count(ITemplatePacket template, Transaction txn, SpaceContext sc, int operationModifiers)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException {
        monitorMemoryUsage(false);
        if (Modifiers.contains(operationModifiers, Modifiers.EXPLAIN_PLAN)) {
            SingleExplainPlan.validate(0, _cacheManager.isBlobStoreCachePolicy(), operationModifiers, template.getCustomQuery(), getClassTypeInfo(template.getTypeName()).getIndexes());
        }
        IServerTypeDesc typeDesc = _typeManager.loadServerTypeDesc(template);

        XtnEntry txnEntry = null;
        if (txn != null)
            txnEntry = attachToXtn((ServerTransaction) txn, false);

        // create template UID
        String uid = _uidFactory.createUIDFromCounter();

        final long timeMillis = SystemTime.timeMillis();
        // build Template Holder and mark it stable
        ITemplateHolder tHolder = TemplateHolderFactory.createTemplateHolder(typeDesc, template, uid,
                LeaseManager.toAbsoluteTime(0, timeMillis), /* expiration time*/
                txnEntry, timeMillis, SpaceOperations.READ,
                null, false, operationModifiers, false);

        // invoke before_read filter
        if (_filterManager._isFilter[FilterOperationCodes.BEFORE_READ])
            _filterManager.invokeFilters(FilterOperationCodes.BEFORE_READ, sc, tHolder);

        Integer counter = 0;

        Context context = null;

        try {
            context = _cacheManager.getCacheContext();
            context.applyOperationContext(sc);
            context.setOperationID(template.getOperationID());
            counter = _cacheManager.count(context, tHolder, txnEntry);
        } catch (SAException ex) {
            JSpaceUtilities.throwEngineInternalSpaceException(ex.getMessage(), ex);
        } finally {
            if (txnEntry != null)
                txnEntry.decrementUsed();
            _cacheManager.freeCacheContext(context);
        }

        updateObjectTypeReadCounts(typeDesc, template);

        if (tHolder instanceof TemplateHolder && ((TemplateHolder) tHolder).getExplainPlan() != null) {
            return new Pair(counter, ((TemplateHolder) tHolder).getExplainPlan());
        }
        return new Pair(counter, null);
    }

    public Pair<Integer, SingleExplainPlan> clear(ITemplatePacket template, Transaction txn, SpaceContext sc, int operationModifiers)
            throws UnusableEntryException, UnknownTypeException,
            TransactionException, RemoteException {
        if (Modifiers.contains(operationModifiers, Modifiers.EXPLAIN_PLAN)) {
            throw new UnsupportedOperationException("Sql explain plan is not supported for clear operation");
        }
        if (template.getTypeName() != null)
            _typeManager.loadServerTypeDesc(template);
        int res = 0;
        AnswerHolder ah = null;
        // memory management is called from readMultiple and from read.
        if (template.isIdQuery()) {
            try {
                ah = read(template,
                        txn,
                        0,
                        false /* ifExists */,
                        true /* take */,
                        sc,
                        false /* returnUID */,
                        false /* fromReplication */,
                        true /* origin */,
                        operationModifiers);
                AnswerPacket ap = ah.getAnswerPacket();
                res = ap.m_EntryPacket != null ? 1 : 0;
            } catch (InterruptedException e) {
                // can't get here cause the call is NO_WAIT
                // restore interrupt state just in case it does get here
                Thread.currentThread().interrupt();
            }
        } else if (template.isIdsQuery()) {
            try {
                ReadByIdsContext readByIdsContext = new ReadByIdsContext(template, false);
                readByIds((AbstractIdsQueryPacket) template, txn, true, sc, operationModifiers, readByIdsContext);
                res = readByIdsContext.getSuccessCount();
            } catch (InterruptedException e) {
            } //cannot happen since its always no-wait
        } else {
            ClearContext batchOperationContext = new ClearContext(template, Integer.MAX_VALUE,_filterManager);
            try {
                ah = readMultiple(template,
                        txn,
                        0L /*timeout*/,
                        false, /*ifExists*/
                        true /* take */,
                        sc,
                        false,/*returnOnlyUid*/
                        operationModifiers,
                        batchOperationContext,
                        null /* aggregatorContext*/,
                        null);
                if (ah.getException() != null) {
                    Exception retex = ah.getException();
                    if (retex instanceof RuntimeException)
                        throw (RuntimeException) retex;
                    if (retex instanceof TransactionException)
                        throw (TransactionException) retex;
                    if (retex instanceof UnusableEntryException)
                        throw (UnusableEntryException) retex;
                    if (retex instanceof UnknownTypeException)
                        throw (UnknownTypeException) retex;
                    if (retex instanceof RemoteException)
                        throw (RemoteException) retex;
                }
            } catch (InterruptedException ex) {
            }  //cannot happen since its always no-wait
            res = batchOperationContext.getNumResults();
        }
        if (ah != null) {
            return new Pair<>(res, ah.getExplainPlan());
        }
        return new Pair<>(res, null);
    }

    /**
     * perform an update operation- make it atomic NOTE- returnonlyUID is ignored
     */
    public ExtendedAnswerHolder update(IEntryPacket updated_entry, Transaction txn, long lease,
                                       long timeout, SpaceContext sc, boolean fromReplication,
                                       boolean origin, boolean newRouter, int modifiers)
            throws UnusableEntryException, UnknownTypeException,
            TransactionException, RemoteException, InterruptedException {
        if (Modifiers.contains(modifiers, Modifiers.EXPLAIN_PLAN)) {
            throw new UnsupportedOperationException("Sql explain plan is not supported for update operation");
        }
        return
                update(updated_entry, txn, lease,
                        timeout, sc, fromReplication,
                        origin, newRouter, modifiers, null);
    }


    public ExtendedAnswerHolder update(IEntryPacket updated_entry, Transaction txn, long lease,
                                       long timeout, SpaceContext sc, boolean fromReplication,
                                       boolean origin, boolean newRouter, int modifiers, MultipleIdsContext multipleIdsContext)
            throws UnusableEntryException, UnknownTypeException,
            TransactionException, RemoteException, InterruptedException {
        monitorMemoryUsage(true /*writeOp*/);
        monitorReplicationStateForModifyingOperation(txn, OperationWeightInfoFactory.create(1, WeightInfoOperationType.UPDATE));

        Context context = (fromReplication && getReplicationNode().getBlobStoreReplicationBulkConsumeHelper() != null)
                ? getReplicationNode().getBlobStoreReplicationBulkConsumeHelper().getContext() : null;

        boolean suppliedContext = context != null;

        try {
            if (!suppliedContext) {
                context = _cacheManager.getCacheContext();
            }
            context.setFromReplication(fromReplication);
            context.setOrigin(origin);

            return update(context, updated_entry, txn, lease, timeout, sc, fromReplication,
                    newRouter, modifiers, null, multipleIdsContext);
        } finally {
            if (!suppliedContext) {
                _cacheManager.freeCacheContext(context);
            }
        }
    }


    public ExtendedAnswerHolder updateOrWrite(UpdateOrWriteContext ctx, boolean newRouter)
            throws TransactionException, UnusableEntryException, UnknownTypeException, RemoteException, InterruptedException {
        return updateOrWrite(ctx, false, true /*origin*/, newRouter);
    }

    public ExtendedAnswerHolder updateOrWrite(UpdateOrWriteContext ctx,
                                              boolean fromReplication, boolean origin, boolean newRouter)
            throws TransactionException, UnusableEntryException,
            UnknownTypeException, RemoteException, InterruptedException {
        // Memory manager is called from within update and unsafeWrite no need
        // to call here.
        boolean discardWriteResult = !ctx.fromWriteMultiple && (ctx.fromUpdateMultiple || ctx.isNoWriteLease());
        int versionID = ctx.packet.getVersion();

        // avoid race condition between update/write and take
        while (true) {
            try {
                boolean inBlobStoreNewRecovery = fromReplication && getReplicationNode().getDirectPersistencyBackupSyncIteratorHandler() != null
                        && getReplicationNode().getBlobStoreReplicaConsumeHelper() != null;
                if (inBlobStoreNewRecovery && !ctx.fromUpdateMultiple && !ctx.fromWriteMultiple && ctx.getCacheContext() == null) {
                    ctx.setCacheContext(getReplicationNode().getBlobStoreReplicaConsumeHelper().getContext());
                    if (ctx.getCacheContext() == null) {
                        inBlobStoreNewRecovery = false;
                    }
                }
                boolean blobstoreReplicationBackupBulks = fromReplication && getReplicationNode().getBlobStoreReplicationBulkConsumeHelper() != null;
                if (blobstoreReplicationBackupBulks && !inBlobStoreNewRecovery) {
                    ctx.setCacheContext(getReplicationNode().getBlobStoreReplicationBulkConsumeHelper().getContext());
                    if (ctx.getCacheContext() == null) {
                        blobstoreReplicationBackupBulks = false;
                    }
                }

                WriteEntryResult writeResult;
                if (ctx.fromUpdateMultiple || ctx.fromWriteMultiple || inBlobStoreNewRecovery || blobstoreReplicationBackupBulks) {
                    // if cache context was assigned by the multiple operation -
                    // use it
                    Context context = null;
                    try {
                        context = ctx.getCacheContext() != null ? ctx.getCacheContext()
                                : _cacheManager.getCacheContext();
                        context.setFromReplication(fromReplication);
                        context.setOrigin(origin);
                        context.setOrdinalForMultipleIdsOperation(ctx.getOrdinalInMultipleIdsContext());

                        if (ctx.isUpdateOperation) {
                            ctx.packet.setVersion(versionID);
                            return update(context,
                                    ctx.packet,
                                    ctx.tx,
                                    ctx.lease,
                                    ctx.timeout,
                                    ctx.sc,
                                    fromReplication,
                                    newRouter,
                                    ctx.operationModifiers,
                                    ctx,
                                    ctx.hasConcentratingTemplate() ? ctx.getConcentratingTemplate().getMultipleIdsContext() : null);
                        }

                        writeResult = write(context,
                                ctx.packet,
                                ctx.tx,
                                ctx.lease,
                                ctx.operationModifiers,
                                fromReplication,
                                origin,
                                ctx.sc,
                                false,
                                true);
                        if (ctx.hasConcentratingTemplate()) {//multiple by ids. if all ids have been served set final result ready
                            if (ctx.getConcentratingTemplate().getMultipleIdsContext().setAnswer(writeResult, ctx.getOrdinalInMultipleIdsContext())) {//wake up the concentraing templating in case of a timeout op'- use a dummy template for setOperationAnswer
                                if (ctx.timeout != JavaSpace.NO_WAIT) {
                                    ITemplateHolder tHolder = TemplateHolderFactory.createUpdateTemplateHolder(_typeManager.loadServerTypeDesc(ctx.packet), ctx.packet, null,
                                            JavaSpace.NO_WAIT/* dummy expiration time*/,
                                            ctx.getConcentratingTemplate().getMultipleIdsContext().getXtnEntry(), 0, null, ctx.operationModifiers);

                                    tHolder.setAnswerHolder(new ExtendedAnswerHolder());
                                    tHolder.setMultipleIdsContext(ctx.getConcentratingTemplate().getMultipleIdsContext());
                                    tHolder.setOrdinalForEntryByIdMultipleOperation(ctx.getOrdinalInMultipleIdsContext());
                                    tHolder.setUpdateOrWriteContext(ctx);
                                    context.setOperationAnswer(tHolder, null, null);

                                }
                            }
                            return null;  //no need to create AH
                        }
                    } finally {
                        if (ctx.getCacheContext() == null)
                            _cacheManager.freeCacheContext(context);
                    }

                } else {
                    if (ctx.isUpdateOperation) {
                        ctx.packet.setVersion(versionID);
                        return update(ctx.packet,
                                ctx.tx,
                                ctx.lease,
                                ctx.timeout,
                                ctx.sc,
                                fromReplication,
                                origin,
                                newRouter,
                                ctx.operationModifiers);
                    }

                    writeResult = write(ctx.packet,
                            ctx.tx,
                            ctx.lease,
                            ctx.operationModifiers,
                            fromReplication,
                            origin,
                            ctx.sc);
                }

                if (newRouter) {
                    ExtendedAnswerHolder aHolder = new ExtendedAnswerHolder();
                    aHolder.m_AnswerPacket = new AnswerPacket(writeResult);
                    return aHolder;
                }
                // no lease should be returned - AnswerPacket == null
                if (discardWriteResult)
                    return null; // discard returned UID
                LeaseContext<?> lease = writeResult.createLease(ctx.packet.getTypeName(),
                        getSpaceImpl(),
                        isNoWriteLease(ctx.packet,
                                ctx.operationModifiers,
                                fromReplication));
                ExtendedAnswerHolder aHolder = new ExtendedAnswerHolder();
                aHolder.m_AnswerPacket = new AnswerPacket(lease);
                return aHolder;
            } catch (EntryNotInSpaceException enise) {
                if (enise.isDeletedByOwnTxn())
                    throw enise;

                // in this case we can't change the version to 0, since the version is set by the master space.
                if (!_isLocalCache && !Modifiers.contains(ctx.operationModifiers, Modifiers.OVERRIDE_VERSION))
                    versionID = 0; // next updates will be with zero versionID

                ctx.isUpdateOperation = false; // resolve to write
            } catch (EntryAlreadyInSpaceException eais) {
                ctx.isUpdateOperation = true; // resolve to update
            }
        } // while true
    }


    private ExtendedAnswerHolder update(Context context, IEntryPacket updated_entry, Transaction txn,
                                        long lease, long timeout, SpaceContext sc, boolean fromReplication,
                                        boolean newRouter, int modifiers, UpdateOrWriteContext ctx, MultipleIdsContext multipleIdsContext)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException, InterruptedException {
        //verify that each entry has a UID and its type is known
        // must be done before getUID() to ensure unmarsh of MarshalledExternalEntry in embedded space.
        IServerTypeDesc serverTypeDesc = _typeManager.loadServerTypeDesc(updated_entry);

        if (updated_entry.getUID() == null)
            throw new DetailedUnusableEntryException("Update operation requires non-null object UID. Object=" + updated_entry.toString());

        if (_isLocalCache && updated_entry.getVersion() <= 0)
            throw new DetailedUnusableEntryException("Update operation requires object version greater than 0 when using local cache. Object=" + updated_entry.toString());

        boolean isBroadcast = updated_entry.getTypeDescriptor().isBroadcast();

        if (!fromReplication && isPartitionedSpace() && ProtectiveMode.isWrongRoutingUsageProtectionEnabled() && !isBroadcast) {
            if (updated_entry.getRoutingFieldValue() == null)
                throwNoRoutingProvidedWhenNeeded(serverTypeDesc, "updating");
            else
                checkEntryRoutingValueMatchesCurrentPartition(updated_entry, serverTypeDesc, "updated");
        }

        context.setOperationID(updated_entry.getOperationID());
        context.setWriteResult(null);

        final XtnEntry txnEntry = multipleIdsContext != null ? multipleIdsContext.getXtnEntry() : initTransactionEntry(txn, sc, fromReplication);

        // create template UID
        String uid = null;
        if (timeout != 0)
            uid = _uidFactory.createUIDFromCounter();

        if (fromReplication && timeout != 0) {
            throw new RuntimeException("operation with timeout = NO_WAIT came from replication");
        }

        String entryId = updated_entry.getUID();
        final long startTime = SystemTime.timeMillis();
        long expiration_time = lease != UPDATE_NO_LEASE ? LeaseManager.toAbsoluteTime(lease, startTime) : UPDATE_NO_LEASE;

        //create an update template that will contain the updated_entry
        // with the UID of the original entry. Note that it is assumed that primary key fields
        //are not changed and are used to get the original
        IEntryPacket template = updated_entry;

        IResponseContext respContext = ResponseContext.getResponseContext();
        if (respContext != null)
            respContext.setInvokedFromNewRouter(newRouter);
        ITemplateHolder tHolder = TemplateHolderFactory.createUpdateTemplateHolder(serverTypeDesc, template, uid,
                LeaseManager.toAbsoluteTime(timeout, startTime) /* expiration time*/,
                txnEntry, startTime, respContext, modifiers);

        tHolder.setAnswerHolder(new ExtendedAnswerHolder());
        if (multipleIdsContext != null) {
            tHolder.setMultipleIdsContext(multipleIdsContext);
            tHolder.setOrdinalForEntryByIdMultipleOperation(context.getOrdinalForMultipleIdsOperation());
        }
        if (ctx != null)
            tHolder.setUpdateOrWriteContext(ctx);
        tHolder.setReRegisterLeaseOnUpdate(lease != UPDATE_NO_LEASE);

        context.setMainThread(true);
        context.setOpResultByThread(false);

        int versionID;
        //try to guess the versionID of this op;
        if (_isLocalCache || context.isFromReplication())
            versionID = tHolder.getEntryData().getVersion(); // local cache mode, template must contain version-id
        else
            versionID = Math.max(tHolder.getEntryData().getVersion() + 1, 2);

        IEntryHolder updated_eh = EntryHolderFactory.createEntryHolder(serverTypeDesc, updated_entry, _entryDataType,
                entryId, expiration_time, (XtnEntry) null, SystemTime.timeMillis(), versionID, true /*keepExpiration*/, _cacheManager.isblobStoreDataSpace() && serverTypeDesc.getTypeDesc().isBlobstoreEnabled() && !UpdateModifiers.isUpdateOnly(modifiers));
        tHolder.setUpdatedEntry(updated_eh);

        // invoke before_update filter
        if (_filterManager._isFilter[FilterOperationCodes.BEFORE_UPDATE]) {
            _filterManager.invokeFilters(FilterOperationCodes.BEFORE_UPDATE, sc, updated_eh);
        }
        //set fields for after filter (if one will be enlisted)
        tHolder.setForAfterOperationFilter(FilterOperationCodes.AFTER_UPDATE, sc, _filterManager, updated_entry);

        _coreProcessor.handleDirectUpdateSA(context, tHolder);
        if (context.getReplicationContext() != null && tHolder.getAnswerHolder().getAnswerPacket().getWriteEntryResult() != null) {
            tHolder.getAnswerHolder().getAnswerPacket().getWriteEntryResult().setSyncReplicationLevel(context.getReplicationContext().getCompleted());
        }

        boolean answerSetByThisThread = context.isOpResultByThread();

        if (multipleIdsContext != null) {
            //should we retry for this entry (done in case of EntryNotInSpaceException)
            if (answerSetByThisThread && tHolder.getMultipleIdsContext().isUpdateOrWriteMultiple() && tHolder.getAnswerHolder().getException() != null && !tHolder.isInCache()
                    && (tHolder.getAnswerHolder().getException() instanceof EntryNotInSpaceException) && !tHolder.getMultipleIdsContext().isAnswerSetForOrdinal(tHolder.getOrdinalForEntryByIdMultipleOperation()))
                throw (EntryNotInSpaceException) tHolder.getAnswerHolder().getException();
            return null;     //dont wait- the calling routine will wait after launching all updates
        }

        boolean callBackMode = ResponseContext.isCallBackMode();
        if (!callBackMode) {
            if (!answerSetByThisThread && !tHolder.hasAnswer()) {
                waitForBlockingAnswer(timeout, tHolder.getAnswerHolder(), startTime, tHolder);
            }
        }

        if (answerSetByThisThread) {
            // if there is an exception throw it according to its type
            tHolder.getAnswerHolder().throwExceptionIfExists();
            return (ExtendedAnswerHolder) tHolder.getAnswerHolder();
        }


        if (callBackMode) {
            ILockObject templateLock = getTemplateLockObject(tHolder);
            try {
                synchronized (templateLock) {
                    if (!tHolder.hasAnswer()) {
                        ResponseContext.dontSendResponse();
                        moveTemplateToSecondPhase(tHolder);
                        return null;
                    }
                    tHolder.getAnswerHolder().throwExceptionIfExists();
                    // we have an answer - return it to caller!
                    return (ExtendedAnswerHolder) tHolder.getAnswerHolder();
                }
            } finally {
                if (templateLock != null)
                    freeTemplateLockObject(templateLock);
            }
        }

        //this code is reached only in blocking mode
        //if no result returned kill template under lock
        //before giving up check that template is not deleted yet,
        //and mark it deleted
        ILockObject templateLock = getTemplateLockObject(tHolder);
        try {
            synchronized (templateLock) {
                if (!tHolder.hasAnswer()) {
                    tHolder.getAnswerHolder().m_AnswerPacket = new AnswerPacket((IEntryPacket) null);
                    if (!tHolder.isDeleted()) {
                        _cacheManager.removeTemplate(context, tHolder, false, true /*origin*/, false);
                    }
                }

                // if there is an exception throw it according to its type
                tHolder.getAnswerHolder().throwExceptionIfExists();
            }//synchronized (templateLock)
            // we have an answer - return it to caller!
            return (ExtendedAnswerHolder) tHolder.getAnswerHolder();
        } finally {
            if (templateLock != null)
                freeTemplateLockObject(templateLock);
        }
    }

    private XtnEntry initTransactionEntry(Transaction txn, SpaceContext sc, boolean fromReplication)
            throws TransactionException, RemoteException {
        if (txn == null)
            return null;

        XtnEntry txnEntry = attachToXtn((ServerTransaction) txn, fromReplication);
        txnEntry.setFromReplication(fromReplication);
        attachFromGatewayStateToTransactionIfNeeded(sc, txnEntry);
        return txnEntry;
    }

    /**
     * perform an update in place using mutators
     */
    public ExtendedAnswerHolder change(ITemplatePacket template, Transaction txn, long lease, long timeout,
                                       SpaceContext sc, boolean fromReplication, boolean origin,
                                       Collection<SpaceEntryMutator> mutators, int operationModifiers, boolean returnOnlyUid)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException, InterruptedException {
        monitorMemoryUsage(true /*writeOp*/);
        if (Modifiers.contains(operationModifiers, Modifiers.EXPLAIN_PLAN)) {
            throw new UnsupportedOperationException("Sql explain plan is not supported for change operation");
        }
        if (!fromReplication && txn != null && !isLocalCache())
            getTransactionHandler().checkTransactionDisconnection(template.getOperationID(), (ServerTransaction) txn);

        monitorReplicationStateForModifyingOperation(txn, OperationWeightInfoFactory.create(WeightInfoOperationType.CHANGE));
        IServerTypeDesc typeDesc = _typeManager.loadServerTypeDesc(template);

        boolean byId = (template.getUID() != null || (template.getID() != null && (template.getExtendedMatchCodes() == null && template.getCustomQuery() == null)));
        if (!byId)
            return changeMultiple(template, txn, lease, timeout,
                    sc, fromReplication, origin,
                    mutators, operationModifiers, returnOnlyUid, typeDesc);

        // Check if FIFO:
        boolean isFifoOperation = template.isFifo() || ReadModifiers.isFifo(operationModifiers);
        // Validate FIFO:
        if (isFifoOperation && !typeDesc.isFifoSupported())
            throw new InvalidFifoTemplateException(template.getTypeName());
        // Disable FIFO if not required:
        if (isFifoOperation && template.getID() != null)
            isFifoOperation = false;

        final XtnEntry txnEntry = initTransactionEntry(txn, sc, fromReplication);

        // create template UID
        String uid = null;
        if (timeout != 0)
            uid = _uidFactory.createUIDFromCounter();

        if (fromReplication && timeout != 0) {
            throw new RuntimeException("operation with timeout not= NO_WAIT came from replication");
        }

        final long startTime = SystemTime.timeMillis();

        // build Template Holder and mark it stable
        IResponseContext respContext = ResponseContext.getResponseContext();

        ITemplateHolder tHolder = TemplateHolderFactory.createTemplateHolder(typeDesc, template,
                uid, LeaseManager.toAbsoluteTime(timeout, startTime) /* expiration time*/,
                txnEntry, startTime, SpaceOperations.UPDATE, respContext, returnOnlyUid,
                operationModifiers, isFifoOperation, fromReplication);

        tHolder.setAnswerHolder(new ExtendedAnswerHolder());
        tHolder.setID(template.getID());
        tHolder.setMutators(mutators);
        long expiration_time = lease != UPDATE_NO_LEASE ? LeaseManager.toAbsoluteTime(lease, startTime) : UPDATE_NO_LEASE;
        tHolder.setChangeExpiration(expiration_time);

        if (_filterManager._isFilter[FilterOperationCodes.BEFORE_CHANGE])
            _filterManager.invokeFilters(FilterOperationCodes.BEFORE_CHANGE, sc, tHolder);

        //set fields for after filter (if one will be enlisted)
        tHolder.setForAfterOperationFilter(FilterOperationCodes.AFTER_CHANGE, sc, _filterManager, null);

        boolean answerSetByThisThread = false;
        int numOfEntriesMatched;
        Context context = (fromReplication && getReplicationNode().getBlobStoreReplicationBulkConsumeHelper() != null)
                ? getReplicationNode().getBlobStoreReplicationBulkConsumeHelper().getContext() : null;
        boolean suppliedContext = context != null;
        try {
            if (!suppliedContext) {
                context = _cacheManager.getCacheContext();
            }
            context.setMainThread(true);
            context.setOperationID(template.getOperationID());
            tHolder.setReRegisterLeaseOnUpdate(lease != UPDATE_NO_LEASE);
            context.applyOperationContext(sc);
            _coreProcessor.handleDirectChangeSA(context, tHolder, fromReplication, origin);

            answerSetByThisThread = context.isOpResultByThread();
            numOfEntriesMatched = context.getNumberOfEntriesMatched();
        } finally {
            if (!suppliedContext) {
                _cacheManager.freeCacheContext(context);
            }
        }

        boolean callBackMode = ResponseContext.isCallBackMode();

        // wait on Answer
        if (!callBackMode && !answerSetByThisThread && !tHolder.hasAnswer())
            waitForBlockingAnswer(timeout, tHolder.getAnswerHolder(), startTime, tHolder);

        if (answerSetByThisThread) {
            tHolder.getAnswerHolder().setNumOfEntriesMatched(numOfEntriesMatched);
            return (ExtendedAnswerHolder) tHolder.getAnswerHolder();
        }

        if (callBackMode) {
            prepareCallBackModeAnswer(tHolder, false);
            return (ExtendedAnswerHolder) tHolder.getAnswerHolder();
        }
        prepareBlockingModeAnswer(tHolder, false);
        return (ExtendedAnswerHolder) tHolder.getAnswerHolder();
    }

    private ExtendedAnswerHolder changeMultiple(ITemplatePacket template, Transaction txn, long lease, long timeout,
                                                SpaceContext sc, boolean fromReplication, boolean origin,
                                                Collection<SpaceEntryMutator> mutators, int operationModifiers, boolean returnOnlyUid, IServerTypeDesc typeDesc)
            throws UnusableEntryException, UnknownTypeException, TransactionException, RemoteException, InterruptedException {
        if (Modifiers.contains(operationModifiers, Modifiers.EXPLAIN_PLAN)) {
            throw new UnsupportedOperationException("Sql explain plan is not supported for clear operation");
        }
        // Check if FIFO:
        boolean isFifoOperation = template.isFifo() || ReadModifiers.isFifo(operationModifiers);
        // Validate FIFO:
        if (isFifoOperation && !typeDesc.isFifoSupported())
            throw new InvalidFifoTemplateException(template.getTypeName());
        // Disable FIFO if not required:
        if (isFifoOperation && template.getID() != null)
            isFifoOperation = false;

        final XtnEntry txnEntry = initTransactionEntry(txn, sc, fromReplication);

        // create template UID
        String uid = null;
        if (timeout != 0)
            uid = _uidFactory.createUIDFromCounter();

        if (fromReplication && timeout != 0) {
            throw new RuntimeException("operation with timeout not= NO_WAIT came from replication");
        }

        final long startTime = SystemTime.timeMillis();

        // build Template Holder and mark it stable
        IResponseContext respContext = ResponseContext.getResponseContext();

        ITemplateHolder tHolder = TemplateHolderFactory.createTemplateHolder(typeDesc, template,
                uid, LeaseManager.toAbsoluteTime(timeout, startTime) /* expiration time*/,
                txnEntry, startTime, SpaceOperations.UPDATE, respContext, returnOnlyUid,
                operationModifiers, isFifoOperation, fromReplication);

        tHolder.setAnswerHolder(new ExtendedAnswerHolder());
        tHolder.setID(template.getID());
        tHolder.setMutators(mutators);
        long expiration_time = lease != UPDATE_NO_LEASE ? LeaseManager.toAbsoluteTime(lease, startTime) : UPDATE_NO_LEASE;
        tHolder.setChangeExpiration(expiration_time);

        ChangeMultipleContext opContext = new ChangeMultipleContext(template, Integer.MAX_VALUE, Integer.MAX_VALUE);
        tHolder.setBatchOperationContext(opContext);

        if (_filterManager._isFilter[FilterOperationCodes.BEFORE_CHANGE])
            _filterManager.invokeFilters(FilterOperationCodes.BEFORE_CHANGE, sc, tHolder);

        //set fields for after filter (if one will be enlisted)
        tHolder.setForAfterOperationFilter(FilterOperationCodes.AFTER_CHANGE, sc, _filterManager, null);

        Context context = null;
        boolean answerSetByThisThread = false;
        int numOfEntriesMatched;

        try {
            context = _cacheManager.getCacheContext();
            context.setMainThread(true);
            context.setOperationID(template.getOperationID());
            tHolder.setReRegisterLeaseOnUpdate(lease != UPDATE_NO_LEASE);
            context.applyOperationContext(sc);
            if (txn == null && _cacheManager.isBlobStoreCachePolicy() && _cacheManager.useBlobStoreBulks()) {//can we exploit blob-store bulking ?
                context.setBlobStoreBulkInfo(new BlobStoreBulkInfo(_cacheManager, false /*takeMultipleBulk*/));
            }

            _coreProcessor.handleDirectMultipleChangeSA(context, tHolder);


            answerSetByThisThread = context.isOpResultByThread();
            numOfEntriesMatched = context.getNumberOfEntriesMatched();
        } finally {
            context = _cacheManager.freeCacheContext(context);
        }

        boolean callBackMode = ResponseContext.isCallBackMode();

        // wait on Answer
        if (!callBackMode && !answerSetByThisThread && !tHolder.hasAnswer())
            waitForBlockingAnswer(timeout, tHolder.getAnswerHolder(), startTime, tHolder);

        if (answerSetByThisThread) {
            tHolder.getAnswerHolder().setNumOfEntriesMatched(numOfEntriesMatched);
            return (ExtendedAnswerHolder) tHolder.getAnswerHolder();
        }

        if (callBackMode) {
            prepareCallBackModeAnswer(tHolder, false);
            return (ExtendedAnswerHolder) tHolder.getAnswerHolder();
        }
        prepareBlockingModeAnswer(tHolder, false);
        return (ExtendedAnswerHolder) tHolder.getAnswerHolder();
    }


    private void throwNoRoutingProvidedWhenNeeded(IServerTypeDesc typeDesc, String operation) throws ProtectiveModeException {
        throw new ProtectiveModeException("Operation is rejected - no routing value provided when " + operation + " an entry of type '" + typeDesc.getTypeName() + "' in a partitioned space. A routing value should be specified within the entry's property named '"
                + typeDesc.getTypeDesc().getRoutingPropertyName() + "'. Missing a routing value would result in a remote client not being able to locate this entry as the routing value will not match the partition the entry is located. (you can disable this protection, though it is not recommended, by setting the following system property: " + ProtectiveMode.WRONG_ENTRY_ROUTING_USAGE + "=false)");
    }

    private void checkEntryRoutingValueMatchesCurrentPartition(IEntryPacket updated_entry, IServerTypeDesc typeDesc, String operation) throws ProtectiveModeException {
        ProtectiveModeException exception = createExceptionIfRoutingValueDoesntMatchesCurrentPartition(updated_entry.getRoutingFieldValue(), typeDesc, operation);
        if (exception != null)
            throw exception;
    }

    private ProtectiveModeException createExceptionIfRoutingValueDoesntMatchesCurrentPartition(Object routingValue, IServerTypeDesc typeDesc, String operation) {
        int partitionIdZeroBased = PartitionedClusterUtils.getPartitionId(routingValue, this.getSpaceImpl().getClusterInfo());
        if (partitionIdZeroBased != getPartitionIdZeroBased()) {
            if (ProtectiveMode.shouldIgnoreWrongRoutingProtectiveMode(typeDesc.getTypeName()))
                return null;
            return new ProtectiveModeException("Operation is rejected - the routing value in the " + operation + " entry of type '" + typeDesc.getTypeName() + "' does not match this space partition id. " +
                    "The value within the entry's routing property named '" + typeDesc.getTypeDesc().getRoutingPropertyName() + "' is " + routingValue +
                    " which matches partition id " + (partitionIdZeroBased + 1) + " while current partition id is " + getPartitionIdOneBased() + ". Having a mismatching routing value would result in a remote client not being able to locate this entry as the routing value will not match the partition the entry is located. (you can disable this protection, though it is not recommended, by setting the following system property: " + ProtectiveMode.WRONG_ENTRY_ROUTING_USAGE + "=false)");
        }
        return null;
    }


    /**
     * perform update of multiple entries. the entries array contains the entries to update, each
     * entry must contain a UID and (optionally) a versionID NOTE- returnonlyUID is ignored
     */
    public Object[] updateMultiple(IEntryPacket[] entries, Transaction txn,
                                   long[] leases, SpaceContext sc, int operationModifiers, boolean newRouter)
            throws UnusableEntryException, UnknownTypeException,
            TransactionException, RemoteException {
        monitorMemoryUsage(true);
        monitorReplicationStateForModifyingOperation(txn, OperationWeightInfoFactory.create(entries.length, WeightInfoOperationType.UPDATE));

        //verify that each entry has a UID and its type is known
        for (int i = 0; i < entries.length; i++) {
            // must be done before getUID() to ensure unmarsh of MarshalledExternalEntry in embedded space.
            _typeManager.loadServerTypeDesc(entries[i]);
        }

        // attach to Xtn
        XtnEntry txnEntry = null;
        if (txn != null) {
            txnEntry = attachToXtn((ServerTransaction) txn, false);
            attachFromGatewayStateToTransactionIfNeeded(sc, txnEntry);
        }
        Context context = null;
        try {
            context = _cacheManager.getCacheContext();
            if (_isSyncReplication)
                context.setSyncReplFromMultipleOperation(true);

            context.setMultipleOperation();

            context.applyOperationContext(sc);

            if (txnEntry == null)
                return updateMultipleLoop(context, entries, leases, null, sc, operationModifiers, newRouter);

            txnEntry.lock();

            try {//lock xtn table
                if (!txnEntry.m_Active)
                    throw new TransactionException("The transaction is not active: " + txnEntry.m_Transaction);

                context.setTransactionalMultipleOperation(true);
                return updateMultipleLoop(context, entries, leases, txnEntry, sc, operationModifiers, newRouter);
            } finally {
                txnEntry.unlock();
            }
        } finally {
            if (txnEntry != null)
                txnEntry.decrementUsed(true /* disable server-side cleanup as unused */);
            replicateAndfreeCache(context);
        }// Finally
    }

    private Object[] updateMultipleLoop(Context context, IEntryPacket[] entries, long[] leases, XtnEntry xtnEntry,
                                        SpaceContext sc, int operationModifiers, boolean newRouter) {
        ServerTransaction st = xtnEntry != null ? xtnEntry.m_Transaction : null;
        Object[] answer = new Object[entries.length];
        for (int i = 0; i < entries.length; i++) {
            try {
                if (xtnEntry != null
                        && context.isFromGateway()
                        && Modifiers.contains(operationModifiers,
                        Modifiers.OVERRIDE_VERSION)) {
                    xtnEntry.setGatewayOverrideVersion(entries[i].getUID());
                }
                AnswerPacket ap = update(context, entries[i], st, leases[i], 0, sc, false /*fromReplication*/,
                        newRouter, operationModifiers, null, null).m_AnswerPacket;
                answer[i] = newRouter ? ap.getWriteEntryResult() : ap.m_EntryPacket;

                performReplIfChunkReached(context);
            } catch (Exception ex) {
                answer[i] = ex;
                continue;
            }

        }//for

        return answer;
    }

    /**
     * perform update of multiple entries. the entries array contains the entries to update, each
     * entry must contain a UID and (optionally) a versionID NOTE- returnonlyUID is ignored
     */
    public AnswerHolder newUpdateMultiple(IEntryPacket[] entries, Transaction txn,
                                          long[] leases, SpaceContext sc, long timeout, int operationModifiers)
            throws UnusableEntryException, UnknownTypeException,
            TransactionException, RemoteException, InterruptedException {
        int replicationLevel = 0;
        try {
            //no need to check redolog- done is writeMultiple calling this routine

            //verify that each entry has a UID and its type is known
            for (int i = 0; i < entries.length; i++) {
                // must be done before getUID() to ensure unmarsh of MarshalledExternalEntry in embedded space.
                _typeManager.loadServerTypeDesc(entries[i]);
            }

            // attach to Xtn
            XtnEntry txnEntry = null;
            if (txn != null) {
                txnEntry = attachToXtn((ServerTransaction) txn, false);
                attachFromGatewayStateToTransactionIfNeeded(sc, txnEntry);
            }

            //create a concentrating template to handle the by ids ops
            // create template UID
            String uid = null;
            if (timeout != 0)
                uid = _uidFactory.createUIDFromCounter();

            IResponseContext respContext = ResponseContext.getResponseContext();
            final long startTime = SystemTime.timeMillis();

            ITemplateHolder tHolder = TemplateHolderFactory.createTemplateHolder(getTypeManager().getServerTypeDesc(IServerTypeDesc.ROOT_TYPE_NAME) /*typeDesc*/, new TemplatePacket(),
                    null /*uid*/, Long.MAX_VALUE /* dummy expiration time*/,
                    txnEntry, startTime, SpaceOperations.UPDATE, respContext, false /*returnOnlyUid*/,
                    operationModifiers, false /*isFifoOperation*/);


            tHolder.setAnswerHolder(new AnswerHolder());
            UpdateMultipleContext multipleIdsContext = new UpdateMultipleContext(tHolder, entries, leases, operationModifiers,
                    timeout, this, txnEntry);
            tHolder.setMultipleIdsContext(multipleIdsContext);


            Context context = null;
            try {
                context = _cacheManager.getCacheContext();
                if (_isSyncReplication)
                    context.setSyncReplFromMultipleOperation(true);

                context.setMultipleOperation();
                context.setMainThread(true);
                context.applyOperationContext(sc);

                if (txnEntry == null) {
                    if (_cacheManager.isBlobStoreCachePolicy() && _cacheManager.useBlobStoreBulks() && timeout == JavaSpace.NO_WAIT) {//can we exploit blob-store bulking ?
                        context.setBlobStoreBulkInfo(new BlobStoreBulkInfo(_cacheManager, false /*takeMultipleBulk*/));
                    }
                    newUpdateMultipleLoop(context, tHolder, entries, leases, null, sc, operationModifiers, timeout);
                } else {

                    txnEntry.lock();
                    try {//lock xtn table
                        if (!txnEntry.m_Active)
                            throw new TransactionException("The transaction is not active: " + txnEntry.m_Transaction);

                        context.setTransactionalMultipleOperation(true);
                        newUpdateMultipleLoop(context, tHolder, entries, leases, txnEntry, sc, operationModifiers, timeout);
                    } finally {
                        txnEntry.unlock();
                    }
                }
            } finally {
                replicationLevel = replicateAndfreeCache(context);  //replicate the updates accumulated so far
            }// Finally

            boolean callBackMode = ResponseContext.isCallBackMode();

            //wait on Answer- from all the update templates into the concentrating template
            if (!callBackMode && !tHolder.hasAnswer())
                waitForBlockingAnswer(timeout, tHolder.getAnswerHolder(), startTime, tHolder);

            if (tHolder.hasAnswer())
                return tHolder.getAnswerHolder();

            if (callBackMode) {
                prepareCallBackModeAnswer(tHolder, false);
                return tHolder.getAnswerHolder();
            }
            prepareBlockingModeAnswer(tHolder, false);
            return tHolder.getAnswerHolder();
        } finally {
            checkIfConsistencyLevelIsCompromised(false, replicationLevel);
        }
    }


    private void newUpdateMultipleLoop(Context context, ITemplateHolder concentratingTemplate, IEntryPacket[] entries, long[] leases, XtnEntry xtnEntry,
                                       SpaceContext sc, int operationModifiers, long timeout) {
        ServerTransaction st = xtnEntry != null ? xtnEntry.m_Transaction : null;

        for (int i = 0; i < entries.length; i++) {
            try {
                if (xtnEntry != null
                        && context.isFromGateway()
                        && Modifiers.contains(operationModifiers,
                        Modifiers.OVERRIDE_VERSION)) {
                    xtnEntry.setGatewayOverrideVersion(entries[i].getUID());
                }
                context.setOrdinalForMultipleIdsOperation(i);
                update(context, entries[i], st, leases[i], timeout, sc, false /*fromReplication*/,
                        true /*newRouter*/, operationModifiers, null, concentratingTemplate.getMultipleIdsContext());

                performReplIfChunkReached(context);
            } catch (InterruptedException e) {
                // restore interrupt state just in case it does get here
                Thread.currentThread().interrupt();
                concentratingTemplate.getMultipleIdsContext().setAnswer(e, i);
                continue;
            } catch (Exception ex) {
                concentratingTemplate.getMultipleIdsContext().setAnswer(ex, i);
                continue;
            }

        }//for
    }



    /*----------------- Extended API : end -----------------*/

    /*----------------- Admin API : begin -----------------*/

    /**
     * Returns the runtime information for all objects in space
     */
    public SpaceRuntimeInfo getRuntimeInfo() {
        // Memory manager is called from getRuntimeInfo(String).
        return getRuntimeInfo(IServerTypeDesc.ROOT_TYPE_NAME);
    }

    /**
     * Returns the runtime information for a given class and its subclasses
     *
     * @param typeName the name of the class
     */
    public SpaceRuntimeInfo getRuntimeInfo(String typeName) {
        monitorMemoryUsage(false);
        return _cacheManager.getRuntimeInfo(typeName);
    }

    public String createUIDFromCounter() {
        return _uidFactory.createUIDFromCounter();
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
    public synchronized void dropClass(String className)
            throws DropClassException {
        _typeManager.dropClass(className);
    }

    /*----------------- Admin API : end -----------------*/

    /*--------------- Transaction Participant : beginning ---------------*/

    public int prepare(TransactionManager mgr, ServerTransaction st, boolean singleParticipant, boolean supportsTwoPhaseReplication, OperationID operationID)
            throws UnknownTransactionException, RemoteException {
        Context context = null;
        XtnEntry xtnEntry = null;
        XtnEntry xtnEntryLocked = null;
        XtnStatus status;
        boolean lockedXtnTable = false;
        boolean isTxnLocked = false;
        try {
            monitorMemoryUsage(false);
            context = _cacheManager.getCacheContext();
            context.setOperationID(operationID);
            while (true) {
                boolean relock = false;
                xtnEntry = getTransaction(st);
                xtnEntryLocked = xtnEntry;
                if (xtnEntry == null) {
                    if (mgr instanceof LocalTransactionManager)
                        return TransactionConstants.COMMITTED;
                    throw new UnknownTransactionException();
                }
                try {
                    lockedXtnTable = getTransactionHandler().lockXtnOnXtnEnd(xtnEntryLocked);
                    xtnEntry = getTransaction(st);
                    if (xtnEntry != xtnEntryLocked)
                        relock = true;

                } finally {
                    if (!relock)
                        break;
                    //very rare race condition
                    getTransactionHandler().unlockXtnOnXtnEnd(xtnEntryLocked, lockedXtnTable);
                }
            }
            isTxnLocked = true;
            monitorReplicationState(createOperationWeightInfo(xtnEntry));


            try {
                if (xtnEntry != null && !xtnEntry.addUsedIfPossible())
                    xtnEntry = null; //unsed status, will be removed by lease manager
                if (xtnEntry == null) {
                    // if transaction wasn't found treat it as success
                    // since there is no way of knowing whether it is an empty transaction
                    // or an expired transaction
                    if (mgr instanceof LocalTransactionManager)
                        return TransactionConstants.COMMITTED;
                    throw new UnknownTransactionException();
                }
                if (!xtnEntry.m_Active)
                    throw new UnknownTransactionException();

                if (st.isXid()) { //used in XA recovering of distributed xtns
                    xtnEntry.m_Transaction.setMetaData(st.getMetaData());

                }

                // check how many changes were made under transaction - if none were made - treat it as single participant
                // and return and indication to mahalo - so it doesn't call commit for this participant
                if (!singleParticipant && xtnEntry.getNumOfChangesUnderTransaction(_cacheManager) == 0 && !st.isXid()) {
                    prepareAndCommit(mgr, st, null /*Operation ID*/);
                    return TransactionConstants.NOTCHANGED;

                }
                if (xtnEntry.m_SingleParticipant)
                    context.setCommittingXtn(xtnEntry.m_Transaction);

                status = xtnEntry.checkAndGetState_Granular();

                if (status == XtnStatus.PREPARED || status == XtnStatus.PREPARING ||
                        status == XtnStatus.COMMITING || status == XtnStatus.COMMITED)
                    return TransactionConstants.PREPARED;
                if (status == XtnStatus.ROLLING ||
                        status == XtnStatus.ROLLED)
                    return TransactionConstants.ABORTED;


                boolean fifoEntries = singleParticipant && xtnEntry.anyFifoEntriesUnderXtn();

                xtnEntry.setStatus(XtnStatus.PREPARING);
                xtnEntry.m_Active = false;
                //call cache-manager pre-prepare method in order to create
                // a snapshot of new/updated entries for notification
                _cacheManager.prePrepare(xtnEntry);
                xtnEntry.m_SingleParticipant = singleParticipant;


                // write to SA + update xtn record in xtn log
                _cacheManager.prepare(context, xtnEntry, supportsTwoPhaseReplication, true /*handleReplication*/, singleParticipant/* handleSA*/);
                if (singleParticipant && xtnEntry.m_AnyUpdates)
                    handleUnderXtnUpdates(context, xtnEntry, true/*isCommitting*/);

                long currentFifoXtn = TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN;
                if (xtnEntry.m_SingleParticipant)
                    xtnEntry.m_CommitRollbackTimeStamp = SystemTime.timeMillis();

                if (fifoEntries) {
                    currentFifoXtn = _cacheManager.getLatestTTransactionTerminationNum();
                    //update the new number in the transaction object
                    currentFifoXtn++;
                    if (currentFifoXtn < 0)
                        currentFifoXtn = 1;
                    _cacheManager.setFifoXtnNumber(xtnEntry, currentFifoXtn);
                    //mark the locked entries with the new fifo xtn number
                    _coreProcessor.handleLockedFifoEntriesBeforeXtnEnd(context, xtnEntry, false  /*fromRollback*/);
                }


                boolean considerNotifyFifoForNonFifoEvents = false;

                if (xtnEntry.m_SingleParticipant)
                    considerNotifyFifoForNonFifoEvents = _coreProcessor.handleNotifyFifoInCommit(context, xtnEntry, true /*fifoNotifyForNonFifoEvents*/);

                if (xtnEntry.m_SingleParticipant)
                    _fifoGroupsHandler.prepareForFifoGroupsAfterXtnScans(context, xtnEntry);


                xtnEntry.setStatus(XtnStatus.PREPARED);
                xtnEntry.m_AlreadyPrepared = true;

                //add info of  prepared2PCXtns info in order to handle stuck tm or participant
                if (!xtnEntry.m_SingleParticipant)
                    getTransactionHandler().addToPrepared2PCXtns(xtnEntry);

                //fifo group op performed under this xtn
                if (xtnEntry.m_SingleParticipant && xtnEntry.getXtnData().anyFifoGroupOperations())
                    _cacheManager.handleFifoGroupsCacheOnXtnEnd(context, xtnEntry);
                //for 1PC- handle taken entries under xtn
                if (xtnEntry.m_SingleParticipant) {
                    _coreProcessor.handleCommittedTakenEntries(context, xtnEntry);
                }


                //set latest fifo xtn number if relevant
                if (fifoEntries && currentFifoXtn != TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN)
                    _cacheManager.setLatestTransactionTerminationNum(currentFifoXtn);


                //handle fifo entries locked under the xtn. This is done not in
                //background in order to enable fifo serialization per client thread
                if (fifoEntries)
                    _coreProcessor.handleLockedFifoEntriesOnXtnEnd(context, xtnEntry, false /*fromRollback*/);

                if (fifoEntries)
                    _coreProcessor.handleNotifyFifoInCommit(context, xtnEntry, false /*fifoNotifyForNonFifoEvents*/);

                if (considerNotifyFifoForNonFifoEvents)
                    xtnEntry.getAllowFifoNotificationsForNonFifoEntries().allow();
            } finally {
                getTransactionHandler().unlockXtnOnXtnEnd(xtnEntryLocked, lockedXtnTable);
                isTxnLocked = false;
            }
            return TransactionConstants.PREPARED;
        } catch (RemoteException ex) {
            throw ex;
        } catch (UnknownTransactionException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error(ex.toString(), ex);
            }
            handleExceptionOnPrepare(mgr,
                    st,
                    supportsTwoPhaseReplication,
                    xtnEntry);
            throw ex;
        } catch (SAException ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error(ex.toString(), ex);
            }

            handleExceptionOnPrepare(mgr,
                    st,
                    supportsTwoPhaseReplication,
                    xtnEntry);

            if (ex instanceof SyncHybridSAException || SyncHybridSAException.getSyncHybridException(ex) != null) {
                throw new SyncHybridTransactionException("Failed to prepare transaction in sync hybrid mode", ex);
            }

            return TransactionConstants.ABORTED;
        } finally {
            if (isTxnLocked && xtnEntryLocked != null) {
                getTransactionHandler().unlockXtnOnXtnEnd(xtnEntryLocked, lockedXtnTable);
            }
            replicateAndfreeCacheContextTxn(context, st);
        }// finally
    }

    private OperationWeightInfo createOperationWeightInfo(XtnEntry xtnEntry) {

        XtnData xtnData = xtnEntry.getXtnData();
        IStoredList<IEntryCacheInfo> lockedEntries = xtnData.getLockedEntries();
        int numOfLocked = 0;
        if (lockedEntries != null) {
            numOfLocked += xtnData.getNewEntries() != null ? xtnData.getNewEntries().size() : 0;
            numOfLocked += xtnData.getUpdatedEntries() != null ? xtnData.getUpdatedEntries().size() : 0;
            numOfLocked += xtnData.getTakenEntries() != null ? xtnData.getTakenEntries().size() : 0;
        }
        return OperationWeightInfoFactory.create(numOfLocked, WeightInfoOperationType.PREPARE);
    }

    protected void handleExceptionOnPrepare(TransactionManager mgr,
                                            ServerTransaction st, boolean supportsTwoPhaseReplication,
                                            XtnEntry xtnEntry) throws RemoteException {
        if (_spaceImpl.isAborted()) {
            throw new RemoteException("Can not prepare transaction - space is aborting");
        }

        // If xtn entry is null - getCacheContext failed - system is aborting
        // TODO replace with specific handling of SAException during shutdown
        if (xtnEntry != null) {
            xtnEntry.setStatus(XtnStatus.ERROR);
        }
        try {
            abort(mgr, st, supportsTwoPhaseReplication, null);
        } catch (Exception ei) {
            if (_logger.isWarnEnabled())
                _logger.warn("Exception occurred while aborting a transaction [" + st + "] which failed prepare process", ei);
        }
    }

    /**
     * Prepare and commit the transaction - called for single participant transaction or transaction
     * without changes
     */
    public int prepareAndCommit(TransactionManager mgr, ServerTransaction st, OperationID operationID) throws UnknownTransactionException, RemoteException {
        if (_operationLogger.isTraceEnabled())
            _operationLogger.trace("preparing and committing transaction [" + createTransactionDetailsString(st, operationID) + "]");
        int result;
        if (isExecutedAlready(operationID)) {
            handleDuplicateCommitOperation(st, operationID);
            result = TransactionConstants.COMMITTED;
        } else {
            result = prepare(mgr, st, true /*singleParticipant*/, false, operationID);
            if (result == TransactionConstants.PREPARED) {
                commitSA(mgr, st, false, null/* operationID */, true);
                result = TransactionConstants.COMMITTED;
            }
        }
        if (_operationLogger.isTraceEnabled())
            _operationLogger.trace("prepared and committed transaction [" + createTransactionDetailsString(st, operationID) + "] result=" + result);
        return result;
    }

    public void commit(TransactionManager mgr, ServerTransaction st, boolean supportsTwoPhaseReplication, OperationID operationID, boolean mayBeFromReplication)
            throws UnknownTransactionException, RemoteException {
        if (_operationLogger.isTraceEnabled())
            _operationLogger.trace("committing transaction [" + createTransactionDetailsString(st, operationID) + "]");

        if (isExecutedAlready(operationID)) {
            handleDuplicateCommitOperation(st, operationID);
            return;
        }
        monitorMemoryUsage(false);
        commitSA(mgr, st, supportsTwoPhaseReplication, operationID, mayBeFromReplication);

        if (_operationLogger.isTraceEnabled())
            _operationLogger.trace("committed transaction [" + createTransactionDetailsString(st, operationID) + "]");
    }

    public String createTransactionDetailsString(ServerTransaction st,
                                                 OperationID operationID) {
        String details = st + ", OperationID=" + operationID;
        return details;
    }

    private void handleDuplicateCommitOperation(ServerTransaction st, OperationID operationID) {
        if (_operationLogger.isDebugEnabled())
            _operationLogger.debug("encountered duplicate transaction commit which may occur after failover, ignoring the operation is it is already applied to space [" + createTransactionDetailsString(st, operationID) + "]");
        //Do nothing, commit already executed
    }

    public void abort(TransactionManager mgr, ServerTransaction st, boolean supportsTwoPhaseReplication, OperationID operationID)
            throws UnknownTransactionException {
        if (_operationLogger.isTraceEnabled())
            _operationLogger.trace("aborting transaction [" + createTransactionDetailsString(st, operationID) + "]");
        if (isExecutedAlready(operationID)) {
            handleDuplicateAbortOperation(st, operationID);
            return;
        }
        monitorMemoryUsage(false);
        abortSA(mgr, st, false /*fromCancelLease*/, false/*verifyExpiredXtn*/, supportsTwoPhaseReplication, operationID);
        if (_operationLogger.isTraceEnabled())
            _operationLogger.trace("aborted transaction [" + createTransactionDetailsString(st, operationID) + "]");
    }

    private void handleDuplicateAbortOperation(ServerTransaction st, OperationID operationID) {
        if (_operationLogger.isDebugEnabled())
            _operationLogger.debug("encountered duplicate transaction abort which may occur after failover, ignoring the operation is it is already applied to space [" + createTransactionDetailsString(st, operationID) + "]");
        //Do nothing, abort already executed
    }

    public List<TemplateInfo> getTemplatesInfo(String className) {
        monitorMemoryUsage(false);
        return _cacheManager.getTemplatesInfo(className);
    }


    /**
     * Returns all the transactions that have the type and status.
     *
     * @param type   TransactionInfo.LOCAL, TransactionInfo.JINI or TransactionInfo.XA
     * @param status TransactionConstants
     * @return array of TransactionInfo
     */
    public TransactionInfo[] getTransactionsInfo(int type, int status)
            throws RemoteException {
        ArrayList<TransactionInfo> result = new ArrayList<>();
        getTransactionsInfo(type, status, result);
        return result.toArray(new TransactionInfo[result.size()]);
    }

    public int countTransactions(int type, int status)
            throws RemoteException {
        return getTransactionsInfo(type, status, null);
    }

    private int getTransactionsInfo(int type, int status, List<TransactionInfo> result)
            throws RemoteException {
        int count = 0;
        for (XtnEntry xtnInfo : getTransactionHandler().getXtnTable().values()) {
            xtnInfo.lock();
            try {
                final int txnType = getXtnInfoType(xtnInfo);
                if (txnType != type && type != TransactionInfo.Types.ALL)
                    continue;

                if (status != TransactionConstants.NOTCHANGED && status != getXtnInfoStatus(xtnInfo))
                    continue;// NOTCHANGED used as a special wildcard!

                count++;
                if (result == null)
                    continue;

                long lease = -1; /* when not a local transaction */
                if (txnType == TransactionInfo.Types.LOCAL || txnType == TransactionInfo.Types.XA) {
                    Long savedLease = getTransactionHandler().getTimedXtns().get(xtnInfo.m_Transaction);
                    if (savedLease != null)
                        lease = savedLease - SystemTime.timeMillis() - TransactionHandler.XTN_ADDITIONAL_TIMEOUT;
                }

                result.add(new TransactionInfo(txnType, status, xtnInfo.m_Transaction, lease, xtnInfo.m_startTime,
                        _cacheManager.getNumberOfLockedObjectsUnderTxn((XtnEntry) xtnInfo)));
            } finally {
                xtnInfo.unlock();
            }
        }

        return count;
    }

    private static int getXtnInfoType(XtnInfo xtnInfo) {
        int result;
        if (xtnInfo.m_Transaction instanceof GSServerTransaction) {
            result = ((GSServerTransaction) xtnInfo.m_Transaction).getId() instanceof Xid ? TransactionInfo.Types.XA
                    : TransactionInfo.Types.LOCAL;
        } else {
            result = xtnInfo.m_Transaction.mgr instanceof LocalTransactionManager ? TransactionInfo.Types.LOCAL
                    : TransactionInfo.Types.JINI;
        }
        return result;
    }

    private static int getXtnInfoStatus(XtnInfo xtnInfo) {
        switch (xtnInfo.getStatus()) {
            case BEGUN:
            case PREPARING:
                return TransactionConstants.ACTIVE;
            case PREPARED:
            case COMMITING:
                return TransactionConstants.PREPARED;
            case COMMITED:
                return TransactionConstants.COMMITTED;
            case ROLLING:
            case ROLLED:
                return TransactionConstants.ABORTED;
            default:
                return -1;
        }
    }

    /**
     * getLockedObjects returns Info about all the locked Object by the given Transaction.
     *
     * @param txn - the Transaction that locks the Objects.
     * @return List of UnderTxnLockedObject
     */
    public List<UnderTxnLockedObject> getLockedObjects(Transaction txn) throws RemoteException {
        List<UnderTxnLockedObject> result = new ArrayList<>();
        Context context = null;
        ISAdapterIterator iter = null;
        try {
            context = _cacheManager.getCacheContext();

            XtnEntry xtnEntry = getTransaction((ServerTransaction) txn);
            if (xtnEntry == null)
                return result;
            xtnEntry.lock();
            try {
                //lock transaction table
                iter = _cacheManager.makeUnderXtnEntriesIter(
                        context, xtnEntry, SelectType.ALL_ENTRIES);
                if (iter != null)
                    while (true) {
                        IEntryHolder entry = (IEntryHolder) iter.next();
                        if (entry == null)
                            break;
                        int lockType, operType;
                        ServerTransaction writeLockOwner = entry.getWriteLockTransaction();
                        if ((writeLockOwner == null) || (!writeLockOwner.equals(txn))) {
                            lockType = UnderTxnLockedObject.LOCK_TYPE_READ_LOCK;
                            operType = SpaceOperations.READ;
                        } else {
                            lockType = UnderTxnLockedObject.LOCK_TYPE_WRITE_LOCK;
                            operType = entry.getWriteLockOperation();
                        }
                        result.add(new UnderTxnLockedObject(entry.getUID(), lockType, operType, entry.getClassName()));
                    }
            } finally {
                xtnEntry.unlock();
            }
        } catch (SAException ex) {
            throw new InternalSpaceException(ex.getMessage(), ex);
        } finally {
            try {
                if (iter != null)
                    iter.close();
            } catch (SAException ex) {
                _cacheManager.freeCacheContext(context);
                context = null;
                throw new InternalSpaceException(ex.getMessage(), ex);
            }
            if (context != null)
                _cacheManager.freeCacheContext(context);
        }
        return result;
    }
    /*--------------- Transaction Participant : end ---------------*/

    /**
     * delegate redo log monitoring
     */
    private void monitorReplicationStateForModifyingOperation(Transaction transaction, OperationWeightInfo info) {
        if (transaction != null)
            return;
        monitorReplicationState(info);
    }

    private void monitorReplicationState(OperationWeightInfo info) {
        if (!_spaceImpl.isPrimary())
            return;
        if (isReplicated())
            getReplicationNode().getAdmin().monitorState(info);
    }

    public IServerTypeDesc getTypeTableEntry(String className) {
        return _typeManager.getServerTypeDesc(className);
    }

    /**
     * Wait until the engine is a consistent state, which means it has no pending operations that
     * will be lost when closing the engine
     */
    public void waitForConsistentState() {
        if (isReplicated()) {
            try {
                if (shouldFlushPendingReplication())
                    getReplicationNode().getAdmin().flushPendingReplication(_clusterPolicy.getReplicationPolicy().getAsyncChannelShutdownTimeout(), TimeUnit.MILLISECONDS);
            } catch (RuntimeException e) {
                //postpone runtime exception .. we want to export to disk before process goes down and data is lost.
                if (_logger.isErrorEnabled())
                    _logger.error("Flush pending replication raised an exception:", e);
                throw e;
            }

        }
    }

    private boolean shouldFlushPendingReplication() {
        SpaceHealthStatus spaceHealthStatus = _spaceImpl.getLocalSpaceHealthStatus();
        //If space is unhealthy do not wait for pending replication flush
        if (!spaceHealthStatus.isHealthy()) {
            if (_logger.isDebugEnabled())
                _logger.debug(getFullSpaceName() + " is unhealthy [" + spaceHealthStatus.getUnhealthyReason() + "], skipping flush of pending replication packets");
            return false;
        }
        return true;
    }

    public Map<String, Object> getMetricsSnapshotByPrefix(Collection<String> prefixes) {
        return _metricManager.getSnapshotsByPrefix(prefixes);
    }

    /**
     * Closes engine, including all working groups and contexts.
     */
    public void close() {
        _spaceImpl.removeInternalSpaceModeListener(this);
        _cacheManager.closeQueryExtensionManagers();

        if (_replicationManager != null)
            _replicationManager.close();

        // close all working groups plus LeaseManager
        if (_processorWG != null)
            _processorWG.shutdown();

        if (_leaseManager != null)
            _leaseManager.close();

        if (_filterManager != null)
            _filterManager.close();

        if (_memoryManager != null)
            _memoryManager.close();

        if (_cacheManager != null)
            _cacheManager.shutDown();

        if (_dataEventManager != null)
            _dataEventManager.close();

        if (_spaceImpl.getClusterFailureDetector() != null) {
            _spaceImpl.getClusterFailureDetector().terminate();
        }

        if (_metricRegistrator != null)
            _metricRegistrator.clear();

        if (_metricManager != null)
            _metricManager.close();

        if( !_dataTypesMetricRegistrators.isEmpty() ){
            for( MetricRegistrator metricRegistrator : _dataTypesMetricRegistrators.values() ){
                metricRegistrator.clear();
            }
        }

        this._spaceImpl.removeClusterInfoChangedListener(this);

        _dataTypesMetricRegistrators.clear();
    }


    /**
     * Searches the engine for a matching entry using the BFS tree of the specified template. If a
     * matched entry is found, perform template on entry and return reference to entry. Otherwise,
     * return <code>null</code>.
     *
     * Handles also null templates.
     */


    public IEntryHolder getMatchedEntryAndOperateSA(Context context,
                                                    ITemplateHolder template, boolean makeWaitForInfo,
                                                    boolean useSCN)
            throws TransactionException, TemplateDeletedException,
            SAException {
        // is it an operation by id ?? if so search only for specific entry
        if (template.getUidToOperateBy() != null)
            return getEntryByIdAndOperateSA(context, template,
                    makeWaitForInfo, useSCN);

        // Get template BFS class names list from Type Table (if template is null,
        // use BFS of java.lang.Object).
        IEntryHolder res = null;
        final IServerTypeDesc serverTypeDesc = _typeManager.getServerTypeDesc(template.getClassName());

        long scnFilter = useSCN ? template.getSCN() : 0;
        long leaseFilter = SystemTime.timeMillis();

        //optimize performance for read-by-id
        if (template.getID() != null && template.getExtendedMatchCodes() == null) {
            IScanListIterator<IEntryCacheInfo> toScan = _cacheManager.getEntryByUniqueId(serverTypeDesc, template.getID(), template);
            if (toScan != null && !toScan.isIterator()) {
                res = getMatchedEntryAndOperateSA_Entry(context,
                        template, makeWaitForInfo,
                        true /*needmatch*/, -1 /*indexPos*/, null, leaseFilter,
                        useSCN, toScan.next());

                if (res != null)
                    return res;
            }
        }
        final IServerTypeDesc[] subTypes = serverTypeDesc.getAssignableTypes();

        if (context.getReadByIdsInfo() != null) {//read by ids, try using the entries prefetched earlier
            if (context.getReadByIdsInfo().getPos() == -1)
                //prefetch all
                context.getReadByIdsInfo().setEntries(_cacheManager.getEntriesByUniqueIds(serverTypeDesc, context.getReadByIdsInfo().getIds(), template));

            if (context.getReadByIdsInfo().getEntries()[context.getReadByIdsInfo().incrementPos()] != null) {
                res =
                        getMatchedEntryAndOperateSA_Entry(context,
                                template, makeWaitForInfo,
                                true, -1, null, leaseFilter,
                                useSCN, context.getReadByIdsInfo().getEntries()[context.getReadByIdsInfo().getPos()]);
                if (res != null)
                    return res;
            }
        }

        if (getCacheManager().isEvictableCachePolicy() && !_cacheManager.isMemorySpace()) {
            IScanListIterator<IEntryCacheInfo> toScan =
                    _cacheManager.makeScanableEntriesIter(context, template, serverTypeDesc,
                            scnFilter, leaseFilter, isMemoryOnlyOperation(template) /*memoryonly*/);
            return
                    getMatchedEntryAndOperateSA_Scan(context,
                            template, makeWaitForInfo,
                            useSCN, toScan);

        }//if (m_CacheManager.m_CachePolicy != CacheManager.CACHE_POLICY_ALL_IN_CACHE
        else // all in cache- direct work with cm SLs- performance!!!
        { //m_CacheManager.m_CachePolicy == CacheManager.CACHE_POLICY_ALL_IN_CACHE || m_CacheManager.m_IsMemorySA

            /**
             * perform anti-starvation random scan
             */
            int actual = 0;
            if (subTypes.length > 1) {
                int size = subTypes.length - 1;
                // random start
                if (!template.isFifoSearch())
                    actual = _random.nextInt(size);// (int) (Math.random() * size);
            }

            for (int k = 0; k < subTypes.length; k++, actual++) {
                if (actual >= subTypes.length)
                    actual = 0;


                if ((res = getMatchedEntryAndOperateSA_type(context,
                        template, makeWaitForInfo,
                        useSCN, leaseFilter, subTypes[actual])) != null)
                    return res;
            }//for (int k=0;...
        }//else
        if (template.isFifoGroupPoll() && !context.isAnyFifoGroupIndex())
            throw new IllegalArgumentException("fifo grouping specified but no fifo grouping property defined type=" + template.getServerTypeDesc().getTypeName());

        return null;
    }

    private IEntryHolder getMatchedEntryAndOperateSA_type(Context context,
                                                          ITemplateHolder template, boolean makeWaitForInfo,
                                                          boolean useSCN, long leaseFilter, IServerTypeDesc typeDesc)
            throws TransactionException, TemplateDeletedException,
            SAException {
        final IServerTypeDesc serverTypeDesc = _typeManager.getServerTypeDesc(template.getClassName());
        if (template.isChangeById() && !serverTypeDesc.getTypeName().equals(template.getServerTypeDesc().getTypeName()))
            return null;  //in-place-update by id no inheritance
        if(serverTypeDesc.getTypeDesc().isBroadcast() && skipBroadcastTable(context, template))
            return null;
        IScanListIterator<IEntryCacheInfo> toScan = template.isServerIterator() ? getOrCreateScanListIteratorFromServerIterator(context, typeDesc, template, serverTypeDesc) : _cacheManager.getMatchingMemoryEntriesForScanning(context, typeDesc, template, serverTypeDesc);

        if (toScan == null)
            return null;

        if (!toScan.isIterator())
            return
                    getMatchedEntryAndOperateSA_Entry(context,
                            template, makeWaitForInfo,
                            true /*needmatch*/, -1 /*indexPos*/, null, leaseFilter,
                            useSCN, toScan.next());

        return
                getMatchedEntryAndOperateSA_Scan(context,
                        template, makeWaitForInfo,
                        useSCN, toScan);
    }


    private IEntryHolder getMatchedEntryAndOperateSA_Scan(Context context,
                                                          ITemplateHolder template, boolean makeWaitForInfo,
                                                          boolean useSCN, final IScanListIterator<IEntryCacheInfo> toScan)
            throws TransactionException, TemplateDeletedException,
            SAException {
        if (template.isFifoGroupPoll() && toScan.isIterator())
            return _fifoGroupsHandler.getMatchedEntryAndOperateSA_Scan(context,
                    template, makeWaitForInfo,
                    useSCN, toScan);

        long leaseFilter = SystemTime.timeMillis();
        boolean needMatch = !toScan.isAlreadyMatched();
        int alreadyMatchedFixedPropertyIndexPos = toScan.getAlreadyMatchedFixedPropertyIndexPos();
        String alreadyMatchedIndexPath = toScan.getAlreadyMatchedIndexPath();

        try {
            while (toScan.hasNext()) {
                IEntryCacheInfo pEntry = toScan.next();
                if (pEntry == null)
                    continue;
                IEntryHolder res = getMatchedEntryAndOperateSA_Entry(context,
                        template, makeWaitForInfo,
                        needMatch, alreadyMatchedFixedPropertyIndexPos, alreadyMatchedIndexPath, leaseFilter,
                        useSCN, pEntry);
                if (res != null)
                    return res;
            }
            return null;
        }//try
        finally {
            // scan ended, release resource
            toScan.releaseScan();
            if (context.isPendingExpiredEntriesExist() && _cacheManager.getTemplatesManager().anyNotifyLeaseTemplates()) {
                try {
                    _leaseManager.forceLeaseReaperCycle(false);
                } catch (InterruptedException e) {
                }
                context.setPendingExpiredEntriesExist(false);
            }
        }

    }

    IEntryHolder getMatchedEntryAndOperateSA_Entry(Context context,
                                                   ITemplateHolder template, boolean makeWaitForInfo,
                                                   boolean needMatch,
                                                   int skipAlreadyMatchedFixedPropertyIndex, String skipAlreadyMatchedIndexPath,
                                                   long leaseFilter,
                                                   boolean useSCN, IEntryCacheInfo pEntry)
            throws TransactionException, TemplateDeletedException,
            SAException {
        if (pEntry.isBlobStoreEntry() && !pEntry.preMatch(context, template))
            return null; //try to save getting the entry to memory

        long scnFilter = useSCN ? template.getSCN() : 0;

        IEntryHolder entry;
        if (pEntry.isBlobStoreEntry()) {
            boolean onlyIndexesPart = BlobStoreOperationOptimizations.isConsiderOptimizedForBlobstore(this, context, template, pEntry);
            entry = ((BlobStoreRefEntryCacheInfo) pEntry).getLatestEntryVersion(_cacheManager, false/*attach*/,
                    null /*lastKnownEntry*/, context, onlyIndexesPart/* onlyIndexesPart*/);
        } else {
            entry = pEntry.getEntryHolder(_cacheManager, context);
        }
        if(entry.getServerTypeDesc().getTypeDesc().isBroadcast() && skipBroadcastTable(context, template))
            return null;
        if (scnFilter != 0 && entry.getSCN() < scnFilter)
            return null;
        if (entry.isExpired(leaseFilter) && disqualifyExpiredEntry(entry) && template.isReadOperation()) {
            context.setPendingExpiredEntriesExist(true);
            return null;
        }

        if (needMatch && !_templateScanner.match(context, entry, template, skipAlreadyMatchedFixedPropertyIndex, skipAlreadyMatchedIndexPath, false))
            return null;
        if (!template.isNonBlockingRead() && entry.isDeleted())
            return null;

        try {
            performTemplateOnEntrySA(context, template, entry,
                    makeWaitForInfo);

            return entry;
        } catch (EntryDeletedException ex) {
            return null;
        } catch (NoMatchException ex) {
            return null;
        } catch (FifoException ex) {
            return null;
        } catch (TransactionConflictException ex) {
            if (template.isFifoGroupPoll())
                context.setFifoGroupScanEncounteredXtnConflict(true);
            return null;
        } catch (TransactionNotActiveException ex) {
            if (ex.m_Xtn.equals(template.getXidOriginatedTransaction()))
                throw new TransactionException("Transaction not active : " + template.getXidOriginatedTransaction());
            return null;
        }

    }

    /**
     * using a uid of the designated entry locate and operate according to template type
     */
    private IEntryHolder getEntryByIdAndOperateSA(Context context, ITemplateHolder template,
                                                  boolean makeWaitForInfo, boolean useSCN)
            throws TransactionException, TemplateDeletedException, SAException {
        String uid = template.getUidToOperateBy();
        if (uid == null)
            throw new RuntimeException("internal error: getEntryByIdAndOperateSA called with null uid");

        boolean replicatedFromCentralDB = isReplicatedFromCentralDB(context);

        //if update/take/takeIE arrives from central-db replication only retrieve from pure cache
        boolean cacheOnly = (replicatedFromCentralDB && (template.getTemplateOperation() == SpaceOperations.UPDATE
                || template.getTemplateOperation() == SpaceOperations.TAKE_IE
                || template.getTemplateOperation() == SpaceOperations.TAKE))
                || template.isInitiatedEvictionOperation() || template.isMemoryOnlySearch();
        IEntryHolder entry = _cacheManager.getEntry(context, uid, template.getClassName(),
                template, true /*tryInsertToCache*/, false /*lockedEntry*/, cacheOnly);

        boolean exceptionIfNoEntry = false;
        if (template.isMatchByID()) {
            if (entry != null) {
                ITransactionalEntryData ed = entry.getTxnEntryData();
                context.setRawmatchResult(ed, ed.getOtherUpdateUnderXtnEntry() != null ? MatchResult.MASTER_AND_SHADOW : MatchResult.MASTER, entry, template);

            }
            exceptionIfNoEntry = !replicatedFromCentralDB && (template.getTemplateOperation() == SpaceOperations.UPDATE ||
                    ((template.getTemplateOperation() == SpaceOperations.READ_IE || template.getTemplateOperation() == SpaceOperations.TAKE_IE) &&
                            template.getUidToOperateBy() != null)) && !template.isInitiatedEvictionOperation() && !template.isChangeById();
        } else {
            exceptionIfNoEntry = !replicatedFromCentralDB &&
                    template.getTemplateOperation() == SpaceOperations.UPDATE;
        }


        if (entry == null || (useSCN && (template.getSCN() > entry.getSCN()))) {
            if (exceptionIfNoEntry)//update
                returnEntryNotInSpaceError(context, template, null, makeWaitForInfo);
            return null;
        }

        // match should be done against all fields even when uid is provided gs-4091
        if ( ( !template.isMatchByID() || template.isChangeQuery() /*fix for GS-13604*/ )
                && !_templateScanner.match(context, entry, template))
            return null;

        try {
            performTemplateOnEntrySA(context, template, entry,
                    makeWaitForInfo);
            return entry;
        } catch (EntryDeletedException edx) {
            if (exceptionIfNoEntry)
                //update/readie/takeie + uid and entry not in space
                returnEntryNotInSpaceError(context, template, edx, makeWaitForInfo);
            return null;
        } catch (TransactionConflictException tcx) {
            return null;
        } catch (NoMatchException ex) { //cannot happen
            return null;
        } catch (FifoException ex) {//cannot happen
            return null;
        } catch (TransactionNotActiveException tnae) {
            if (template.getXidOriginatedTransaction() != null && tnae.m_Xtn.equals(template.getXidOriginatedTransaction()))
                throw new TransactionException("Transaction not active " + tnae.m_Xtn, tnae);

            return null;
        }
    }


    /**
     * notify the use (of a by-UID operation) that the requested entry is not in space.
     */
    private void returnEntryNotInSpaceError(Context context, ITemplateHolder template,
                                            EntryDeletedException edx, boolean makeWaitForInfo) {

        EntryNotInSpaceException exv =
                new EntryNotInSpaceException(template.getUidToOperateBy(), getFullSpaceName(),
                        (edx != null && edx.deletedByOwnTxn()) /* DeletedByOwnTxn */);

        ILockObject templateLock = null;
        try {
            if (template.isInCache() || makeWaitForInfo) {
                templateLock = getTemplateLockObject(template);
                synchronized (templateLock) {
                    if (!template.isDeleted()) {
                        context.setOperationAnswer(template, null, exv);

                        if (template.isInCache())
                            _cacheManager.removeTemplate(context, template,
                                    false /*fromReplication*/, true /*origin*/, false);
                    }
                }
            } else {
                context.setOperationAnswer(template, null, exv);
                template.setDeleted(true); //just to signal processor
            }
        } finally {
            if (templateLock != null)
                freeTemplateLockObject(templateLock);
        }
    }

    /**
     * Find and add to matchedEntries the entry that match the given uid
     */
    private void handleEntryByIdAndOperateSA(Context context, ITemplateHolder template)
            throws TransactionException, TemplateDeletedException, SAException {
        IEntryHolder eh = getEntryByIdAndOperateSA(context, template,
                false, false);
        performReplIfChunkReached(context);

    }

    /**
     * Searches the engine for a matching entry using the BFS tree of the specified template. If a
     * matched entry is found, perform template on entry and return reference to entry. Otherwise,
     * return empty Collection.
     *
     * Handles also null templates.
     */

    public void executeOnMatchingEntries(Context context, ITemplateHolder template, boolean makeWaitForInfo)
            throws TransactionException, TemplateDeletedException, SAException {
        // If template is a multiple uids template:
        final String[] multipleUids = template.getMultipleUids();

        if (multipleUids != null) {
            for (int i = 0; i < multipleUids.length && template.getBatchOperationContext().getNumResults() < template.getBatchOperationContext().getMaxEntries(); i++) {
                if (multipleUids[i] != null) {
                    template.setUidToOperateBy(multipleUids[i]);
                    handleEntryByIdAndOperateSA(context, template);
                }
            }
            return;
        }

        if (template.getUidToOperateBy() != null) {
            //add matching entry to matchedEntries
            handleEntryByIdAndOperateSA(context, template);
            return;
        }


        // get template BFS class names list from Type Table (if template is null, use BFS of java.lang.Object).
        final IServerTypeDesc serverTypeDesc = _typeManager.getServerTypeDesc(template.getClassName());
        final IServerTypeDesc[] subTypes = serverTypeDesc.getAssignableTypes();

        long leaseFilter = SystemTime.timeMillis();

        if (getCacheManager().isEvictableCachePolicy() && !_cacheManager.isMemorySpace()) {
            IScanListIterator<IEntryCacheInfo> toScan =
                    _cacheManager.makeScanableEntriesIter(context, template, serverTypeDesc,
                            0 /*scnFilter*/, leaseFilter /*leaseFilter*/,
                            isMemoryOnlyOperation(template)/*memoryonly*/);

            getMatchedEntriesAndOperateSA_Scan(context,
                    template,
                    toScan, makeWaitForInfo, null);
        }//if (m_CacheManager.m_CachePolicy != CacheManager.CACHE_POLICY_ALL_IN_CACHE
        else // all in cache- direct work with cm SLs- performance!!!
        { //m_CacheManager.m_CachePolicy == CacheManager.CACHE_POLICY_ALL_IN_CACHE || m_CacheManager.m_IsMemorySA
            // perform anti-starvation random scan
            CircularNumerator<IServerTypeDesc> subTypesCircularNumerator = template.isServerIterator() ? template.getServerIteratorInfo().getSubTypesCircularNumerator() : null;
            if(subTypesCircularNumerator == null) {
                int start =  template.isFifoSearch() || subTypes.length == 1 ? 0 : new Random().nextInt(subTypes.length);
                subTypesCircularNumerator = new CircularNumerator<>(subTypes, start);
                if(template.isServerIterator())
                    template.getServerIteratorInfo().setSubTypesCircularNumerator(subTypesCircularNumerator);
            }
            while (subTypesCircularNumerator.getCurrent() != null){
                getMatchedEntriesAndOperateSA_Type(context,
                        template,
                        subTypesCircularNumerator.getCurrent(), makeWaitForInfo);
                if (template.getBatchOperationContext().reachedMaxEntries())
                    return;
                subTypesCircularNumerator.next();
            }
        }
        return;
    }


    private void getMatchedEntriesAndOperateSA_Scan(Context context,
                                                    ITemplateHolder template,
                                                    IScanListIterator<IEntryCacheInfo> toScan,
                                                    boolean makeWaitForInfo,
                                                    IServerTypeDesc entryTypeDesc)
            throws TransactionException, TemplateDeletedException,
            SAException {

        if (template.isFifoGroupPoll() && toScan.isIterator()) {
            _fifoGroupsHandler.getMatchedEntriesAndOperateSA_Scan(context,
                    template,
                    toScan,
                    makeWaitForInfo, entryTypeDesc);

            return;
        }

        long leaseFilter = SystemTime.timeMillis();
        boolean needMatch = !toScan.isAlreadyMatched();
        int alreadyMatchedFixedPropertyIndexPos = toScan.getAlreadyMatchedFixedPropertyIndexPos();
        String alreadyMatchedIndexPath = toScan.getAlreadyMatchedIndexPath();
        boolean checkResultSize = 0 < _resultsSizeLimit && !template.isReturnOnlyUid() && template.isReadMultiple();
        int resultSizeOverflow = 0;
        boolean hasNext = false;
        try {
            //can we use blob-store prefetch ?
            toScan = BlobStorePreFetchIteratorBasedHandler.createPreFetchIterIfRelevant(context, _cacheManager, toScan, template, _logger);
            while (hasNext = toScan.hasNext()) {
                IEntryCacheInfo pEntry = toScan.next();
                if (pEntry == null) {
                    continue;
                }
                getMatchedEntriesAndOperateSA_Entry(context,
                        template,
                        needMatch, alreadyMatchedFixedPropertyIndexPos, alreadyMatchedIndexPath, leaseFilter,
                        pEntry, makeWaitForInfo, entryTypeDesc);
                if (template.getBatchOperationContext().reachedMaxEntries()) {
                    return;
                }

                if (checkResultSize && _resultsSizeLimit < template.getBatchOperationContext().getNumResults()) {
                    if (0 < _resultsSizeLimitMemoryCheckBatchSize && _memoryManager.isEnabled()) {
                        if ((resultSizeOverflow % _resultsSizeLimitMemoryCheckBatchSize == 0)) {
                            _memoryManager.monitorMemoryUsage(false);
                        }
                    } else {
                        throw new LimitExceededException("Query max result", _resultsSizeLimit);
                    }
                    resultSizeOverflow += 1;
                }
            }
        } finally {
            if( template.isServerIterator()){
                if(!hasNext){
                    toScan.releaseScan();
                    template.getServerIteratorInfo().setScanListIterator(null);
                }
            }
            else{
                // scan ended, release resource
                toScan.releaseScan();
            }

            if (context.isPendingExpiredEntriesExist() && _cacheManager.getTemplatesManager().anyNotifyLeaseTemplates()) {
                try {
                    _leaseManager.forceLeaseReaperCycle(false);
                } catch (InterruptedException ignored) {

                }
                context.setPendingExpiredEntriesExist(false);
            }
        }
    }


    void getMatchedEntriesAndOperateSA_Entry(Context context,
                                             ITemplateHolder template,
                                             boolean needMatch,
                                             int skipAlreadyMatchedFixedPropertyIndex, String skipAlreadyMatchedIndexPath,
                                             long leaseFilter,
                                             IEntryCacheInfo pEntry, boolean makeWaitForInfo,
                                             IServerTypeDesc entryTypeDesc /*can be null in LRU (non blobstore cache policy)*/)
            throws TransactionException, TemplateDeletedException,
            SAException {
        if (pEntry.isBlobStoreEntry() && !pEntry.preMatch(context, template))
            return; //try to save getting the entry to memory

        IEntryHolder entry;
        if(BlobStoreOperationOptimizations.isConsiderOptimizedForBlobstore(this, context, template, pEntry)){
            entry = ((BlobStoreRefEntryCacheInfo) pEntry).getLatestEntryVersion(_cacheManager, false/*attach*/, null /*lastKnownEntry*/, context, true/* onlyIndexesPart*/);
        }else{
            entry = pEntry.getEntryHolder(_cacheManager, context);
        }

        if ((template.getExpirationTime() == 0 || !template.isInCache()) && template.getBatchOperationContext().isInProcessedUids(entry.getUID()))
            return; //when template is inserted we check under lock


        if (entry.isExpired(leaseFilter) && disqualifyExpiredEntry(entry) && template.isReadOperation()) {
            context.setPendingExpiredEntriesExist(true);
            return;
        }

        if (needMatch && !_templateScanner.match(context, entry, template, skipAlreadyMatchedFixedPropertyIndex, skipAlreadyMatchedIndexPath, false))
            return;
        if (!template.isNonBlockingRead() && entry.isDeleted())
            return;
        try {
            performTemplate(context, template, entry, makeWaitForInfo);

        } catch (EntryDeletedException ex) {
            return;
        } catch (NoMatchException ex) {
            return;
        } catch (FifoException ex) {
            return;
        } catch (TransactionConflictException ex) {
            if (template.isFifoGroupPoll())
                context.setFifoGroupScanEncounteredXtnConflict(true);
            return;
        } catch (TransactionNotActiveException ex) {
            if (ex.m_Xtn.equals(template.getXidOriginatedTransaction()))
                throw new TransactionException("Transaction not active : " + template.getXidOriginatedTransaction(), ex);

            return;
        }
    }

    private void getMatchedEntriesAndOperateSA_Type(Context context,
                                                    ITemplateHolder template,
                                                    IServerTypeDesc entryTypeDesc, boolean makeWaitForInfo)
            throws TransactionException, TemplateDeletedException,
            SAException {
        final IServerTypeDesc serverTypeDesc = _typeManager.getServerTypeDesc(template.getClassName());
        if (template.isChangeById() && !serverTypeDesc.getTypeName().equals(template.getServerTypeDesc().getTypeName()))
            return;  //in-place-update by id no inheritance
        if(entryTypeDesc.getTypeDesc().isBroadcast() && skipBroadcastTable(context, template))
            return;
        IScanListIterator<IEntryCacheInfo> toScan = template.isServerIterator() ? getOrCreateScanListIteratorFromServerIterator(context, entryTypeDesc, template, serverTypeDesc) : _cacheManager.getMatchingMemoryEntriesForScanning(context, entryTypeDesc, template, serverTypeDesc);
        if (toScan == null)
            return;

        if (!toScan.isIterator()) {
            getMatchedEntriesAndOperateSA_Entry(context,
                    template,
                    true /*needMatch*/, -1 /*indexPos*/, null, SystemTime.timeMillis(),
                    toScan.next(), makeWaitForInfo, entryTypeDesc);
            if(template.isServerIterator()){
                template.getServerIteratorInfo().setScanListIterator(null);
            }
        }
        else
            getMatchedEntriesAndOperateSA_Scan(context,
                    template,
                    toScan,
                    makeWaitForInfo, entryTypeDesc);
    }
    /*
    Determine if engine should skip any operation in context of broadcast tables
    returns false if one the following conditions is met:
    1. Operation is clear
    2. Operation is from collocated proxy and not server iterator
    3. Cluster is not partitioned
    4. Operation is in leader partition
    Returns true if:
    1. Operation is take (take operation deletes only partitioned entries)
    2. None of the false conditions is met
     */
    public boolean skipBroadcastTable(Context context,
                                      ITemplateHolder template){
        if(template != null){
            if(template.isClear()) {
                return false;
            }
            if(template.isTakeOperation()) {
                return true;
            }
            if(!context.isFromClustered() && !template.isServerIterator()) {
                return false;
            }
        }
        if(!getClusterInfo().isPartitioned())
            return false;
        if(getPartitionIdZeroBased() == 0)
            return false;
        return true;
    }

    private IScanListIterator<IEntryCacheInfo> getOrCreateScanListIteratorFromServerIterator(Context context, IServerTypeDesc entryTypeDesc, ITemplateHolder template, IServerTypeDesc serverTypeDesc) {
        ServerIteratorInfo serverIteratorInfo = template.getServerIteratorInfo();
        if(!serverIteratorInfo.isActive()){
            if(_logger.isDebugEnabled())
                _logger.debug("Server Iterator " + serverIteratorInfo.getUuid() + " is not active.");
            return null;
        }
        if(serverIteratorInfo.getScanListIterator() == null){
            IScanListIterator scanListIterator = _cacheManager.getMatchingMemoryEntriesForScanning(context, entryTypeDesc, template, serverTypeDesc);
            if(scanListIterator != null) {
                serverIteratorInfo.setScanListIterator(scanListIterator);
                return scanListIterator;
            }
            return null;
        }
        return serverIteratorInfo.getScanListIterator();
    }


    private void performTemplate(Context context, ITemplateHolder template,
                                 IEntryHolder entry, boolean makeWaitForInfo)
            throws TransactionConflictException, EntryDeletedException,
            TemplateDeletedException, TransactionNotActiveException,
            SAException, NoMatchException, FifoException {

        performTemplateOnEntrySA(context, template, entry, makeWaitForInfo);

        performReplIfChunkReached(context);
    }

    /**
     * @param template
     * @return
     */
    private boolean isMemoryOnlyOperation(ITemplateHolder template) {
        return (_cacheManager.getStorageAdapter().isReadOnly() && !hasMirror() && !template.isReadOperation()) || template.isInitiatedEvictionOperation();
    }

    /**
     * @return
     */
    public boolean hasMirror() {
        return getClusterPolicy() != null
                && getClusterPolicy().getReplicationPolicy() != null
                && getClusterPolicy().getReplicationPolicy()
                .isMirrorServiceEnabled();
    }

    /**
     * try to process an entry , if the entry has a shadow try it too if the entry don't  suite
     */
    public void performTemplateOnEntrySA(Context context, ITemplateHolder template, IEntryHolder entry,
                                         boolean makeWaitForInfo)
            throws TransactionConflictException, EntryDeletedException,
            TemplateDeletedException, TransactionNotActiveException,
            SAException, NoMatchException, FifoException {
        //if op is update/change and its blob store verify memory shortage
        if (entry.isBlobStoreEntry() && template.getTemplateOperation() == SpaceOperations.UPDATE)
            _cacheManager.getBlobStoreMemoryMonitor().onMemoryAllocation(((IBlobStoreEntryHolder) entry).getBlobStoreResidentPart().getStorageKey());

        /** disable sync-replication within this code */
        context.setDisableSyncReplication(true);

        // make sure that when creating an EntryPacket(for replication/result) use the operation id
        // of the read/take action and not the operation id of the trigger operation (write/update)
        context.setOperationID(template.getOperationID());

        try {
            //validity check
            if (template.getTemplateOperation() == SpaceOperations.UPDATE && !template.isChange()) {
                if (!template.getUpdatedEntry().getClassName().equals(entry.getClassName())) {
                    ILockObject templateLock = getTemplateLockObject(template);
                    try {
                        synchronized (templateLock) {
                            if (!template.hasAnswer()) {
                                RuntimeException rtex = new RuntimeException(" Update: original entry and updated entry not from same class updated=" + template.getUpdatedEntry().getClassName() + " original=" + entry.getClassName());
                                context.setOperationAnswer(template, null, rtex);

                                if (!template.isDeleted() && template.isInCache())
                                    _cacheManager.removeTemplate(context, template,
                                            false /*fromReplication*/, true /*origin*/, false);
                            }

                            throw new TemplateDeletedException(template);
                        }
                    } finally {
                        if (templateLock != null)
                            freeTemplateLockObject(templateLock);
                    }
                }
            }

            ILockObject entryLock = null;
            ILockObject templateLock = null;
            boolean need_xtn_lock = false;
            boolean upgrade_lock = false;
            while (true) {
                context.setNonBlockingReadOp(template.isNonBlockingRead() && !context.isUnstableEntry() && !context.isTransactionalMultipleOperation());
                if (!template.isFifoSearch())
                    need_xtn_lock = need_xtn_lock || (!context.isNonBlockingReadOp() && template.getXidOriginated() != null);
                else
                    need_xtn_lock = need_xtn_lock || (!context.isNonBlockingReadOp() && (template.getXidOriginated() != null || entry.isMaybeUnderXtn()));

                try {
                    if (!context.isNonBlockingReadOp()) {
                        // if the operation is multiple - the lock was already acquired.
                        if (need_xtn_lock) {
                            if (!context.isTransactionalMultipleOperation())
                                getTransactionHandler().xtnLockEntryOnTemplateOperation(context, entry, template, (context.isTransactionalMultipleOperation() ? template.getXidOriginated() : null));
                            if (template.isFifoSearch())
                                getTransactionHandler().getTxReadLock().lock();
                        }
                        try {
                            entryLock = _cacheManager.getLockManager().getLockObject(entry);
                            synchronized (entryLock) {
                                try {
                                    if (makeWaitForInfo || template.isInCache()) {
                                        templateLock = getTemplateLockObject(template);
                                        synchronized (templateLock) {
                                            upgrade_lock = performTemplateOnEntryXtnAwareSA(context, template, entry,
                                                    need_xtn_lock, makeWaitForInfo);
                                        } /* synchronized(template) */
                                    } // if (realTemplateInEngine
                                    else {
                                        upgrade_lock = performTemplateOnEntryXtnAwareSA(context, template, entry,
                                                need_xtn_lock, makeWaitForInfo);
                                    }
                                } finally {
                                    //while entry still locked
                                    if (getCacheManager().mayNeedEntriesUnpinning())
                                        _cacheManager.unpinIfNeeded(context, entry, template, null /* pEntry*/);
                                }
                            } /* synchronized(entry) */
                        } finally {
                            if (need_xtn_lock) {
                                if (!context.isTransactionalMultipleOperation())
                                    getTransactionHandler().xtnUnlockEntryOnTemplateOperation(template, (context.isTransactionalMultipleOperation() ? template.getXidOriginated() : null));
                                if (template.isFifoSearch())
                                    getTransactionHandler().getTxReadLock().unlock();
                            }
                        }
                        if (upgrade_lock && !need_xtn_lock)
                            need_xtn_lock = true;
                        if (!upgrade_lock)
                            break;
                    }//if
                    else {//NON_BLOCKING READ
                        if (makeWaitForInfo || template.isInCache()) {
                            templateLock = getTemplateLockObject(template);
                            synchronized (templateLock) {
                                upgrade_lock = performTemplateOnEntryXtnAwareSA(context,
                                        template, entry,
                                        false/*needXtnTableLocked*/, makeWaitForInfo);
                            } /* synchronized (template) */
                        }  //if (realTemplateInEngine
                        else {
                            upgrade_lock = performTemplateOnEntryXtnAwareSA(context,
                                    template, entry,
                                    false/*needXtnTableLocked*/, makeWaitForInfo);
                        }
                        if (upgrade_lock)
                            continue; //need to lock entry- its pending
                        break;
                    }
                } finally {
                    if (templateLock != null) {
                        freeTemplateLockObject(templateLock);
                        templateLock = null;
                    }
                    if (entryLock != null) {
                        _cacheManager.getLockManager().freeLockObject(entryLock);
                        entryLock = null;
                    }
                }

            }// while (true)
        } finally {
            /** enable sync-replication within this code */
            context.setDisableSyncReplication(false);
            context.setLastRawMatchSnapshot(null);

            if (template.getXidOriginatedTransaction() == null) {
                if (context.isSyncReplFromMultipleOperation())
                    performReplIfChunkReached(context);
                else
                    performReplication(context);
            }
        }
    }

    private boolean performTemplateOnEntryXtnAwareSA(Context context,
                                                     ITemplateHolder tmpl, IEntryHolder ent,
                                                     boolean needXtnLocked, boolean makeWaitForInfo)
            throws TransactionConflictException, EntryDeletedException,
            TemplateDeletedException, TransactionNotActiveException,
            SAException, NoMatchException, FifoException {
        if(ent.isBlobStoreEntry() ){
            BlobStoreRefEntryCacheInfo blobStoreRefEntryCacheInfo = ((BlobStoreEntryHolder) ent).getBlobStoreResidentPart();
            BlobStoreOperationOptimizations.isConsiderOptimizedForBlobstore(this, context, tmpl, blobStoreRefEntryCacheInfo);
        }
        boolean needRematch = false;
        if (needXtnLocked) {
            if (ent.isDeleted()) {
                throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/;
            }
            IEntryHolder entry = ent;
            if (getCacheManager().needReReadAfterEntryLock()) {
                boolean replicatedFromCentralDB = isReplicatedFromCentralDB(context);

                //if update/take/takeIE arrives from replication only retrieve from pure cache
                if (replicatedFromCentralDB
                        && (tmpl.getTemplateOperation() == SpaceOperations.UPDATE
                        || tmpl.getTemplateOperation() == SpaceOperations.TAKE_IE
                        || tmpl.getTemplateOperation() == SpaceOperations.TAKE))
                    entry = _cacheManager.getEntry(context, ent, true /*tryInsertToCache*/, true /*lockeEntry*/, true /*useOnlyCache*/);
                else
                    entry = _cacheManager.getEntry(context, ent, true /*tryInsertToCache*/, true /*lockeEntry*/, tmpl.isMemoryOnlySearch() /*useOnlyCache*/);

                if (entry == null)
                    throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/;
                if (entry.isDeleted())
                    throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/;
                if (entry.isBlobStoreEntry() && !entry.isSameEntryInstance(ent))
                    throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/; //not the same entry  lock is not relevant
                if (ent != entry)
                    //renewed lru memory entry- mark to rematch
                    needRematch = true;
            }
            // if entry was written under Xtn and the Xtn is aborted, the entry
            // is considered as not exists.
            if (entry.getWriteLockOwner() != null && entry.getWriteLockOperation() == SpaceOperations.WRITE) {
                XtnEntry xtnEntry = entry.getWriteLockOwner();
                if (xtnEntry != null && (xtnEntry.getStatus() == XtnStatus.ROLLED ||
                        xtnEntry.getStatus() == XtnStatus.ROLLING))
                    throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/;
            }
            // if entry was taken under a Xtn that was committed, the entry
            // is considered not exists.
            if (entry.getWriteLockTransaction() != null &&
                    (entry.getWriteLockOperation() == SpaceOperations.TAKE ||
                            entry.getWriteLockOperation() == SpaceOperations.TAKE_IE)) {
                XtnEntry xtnEntry = entry.getWriteLockOwner();
                if (xtnEntry != null &&
                        (xtnEntry.getStatus() == XtnStatus.COMMITED ||
                                xtnEntry.getStatus() == XtnStatus.COMMITING))
                    throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/;
            }

            if (entry.isExpired() && !isExpiredEntryStayInSpace(entry)) {
                boolean continueOp = tmpl.getXidOriginated() != null && tmpl.getXidOriginated() == entry.getWriteLockOwner();
                if (!continueOp && entry.isExpired(_leaseManager.getEffectiveEntryLeaseTime(SystemTime.timeMillis()))) {
                    if (!_leaseManager.replicateLeaseExpirationEventsForEntries() && (!_leaseManager.isNoReapUnderXtnLeases() || !entry.isEntryUnderWriteLockXtn())) //dont reap entries under writelock xtn
                    {
                        IServerTypeDesc typeDesc = _typeManager.getServerTypeDesc(entry.getClassName());
                        removeEntrySA(context, entry, typeDesc,
                                false /*fromReplication*/, true/*origin*/, EntryRemoveReasonCodes.LEASE_EXPIRED);
                    }
                    throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(entry.m_UID)*/;

                }
            }
            if (!needRematch && !tmpl.isMatchByID()) {
                if (context.getLastRawMatchSnapshot() == null || context.getLastRawmatchTemplate() != tmpl || context.getLastRawmatchEntry() != entry
                        || context.getLastRawMatchSnapshot() != entry.getTxnEntryData() ||
                        context.getLastRawMatchSnapshot().getVersion() != entry.getTxnEntryData().getVersion()
                        || context.getLastRawMatchSnapshot().getOtherUpdateUnderXtnEntry() != null)
                    needRematch = true;
            }

            if (tmpl.isInCache()) {

                ITemplateHolder template = tmpl;
                if (template.isDeleted())
                    throw new TemplateDeletedException(template);
                if (template.isExpired()) {
                    boolean cancelPendingOp = !template.isInitialIfExistSearchActive() || (template.getEntriesWaitingForTemplate() != null && !template.getEntriesWaitingForTemplate().isEmpty());
                    if (cancelPendingOp) {
                        if (!template.hasAnswer())
                            context.setOperationAnswer(template, null, null);
                        _cacheManager.removeTemplate(context, template,
                                false /*fromReplication*/, true /*origin*/, false);
                        throw new TemplateDeletedException(template);
                    }
                }
                performTemplateOnEntryCoreSA(context, template, entry, makeWaitForInfo,
                        needRematch);
            } else { /* template not in engine */
                performTemplateOnEntryCoreSA(context, tmpl, entry,
                        makeWaitForInfo, needRematch);
            }
        } else { /* needXtnLocked == false */

            if (ent.isDeleted())
                throw ENTRY_DELETED_EXCEPTION;

            if (context.isNonBlockingReadOp() && _cacheManager.isDummyEntry(ent)) { //spare touching volatiles
                throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/;
            }
            IEntryHolder entry = ent;
            if (getCacheManager().needReReadAfterEntryLock() &&
                    (!context.isNonBlockingReadOp() || !context.isMainThread())) {
                entry = _cacheManager.getEntry(context, ent, true /*tryInsertToCache*/, !context.isNonBlockingReadOp() /*lockeEntry*/, tmpl.isInitiatedEvictionOperation() || tmpl.isMemoryOnlySearch() /*useOnlyCache*/);
                if (entry == null && tmpl.isInitiatedEvictionOperation())
                    throw NO_MATCH_EXCEPTION;
                if (entry == null)
                    throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/;
                if (!context.isNonBlockingReadOp() && entry.isDeleted())
                    throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/;
                if (entry.isBlobStoreEntry() && !context.isNonBlockingReadOp() && !entry.isSameEntryInstance(ent))
                    throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/; //not the same entry  lock is not relevant
                if (ent != entry)
                    needRematch = true;
            }

            if ((tmpl.isFifoSearch()) && !context.isNonBlockingReadOp() && entry.isMaybeUnderXtn())
                return true;

            if (!needRematch && !tmpl.isMatchByID()) {
                if (context.getLastRawMatchSnapshot() == null || context.getLastRawmatchTemplate() != tmpl || context.getLastRawmatchEntry() != entry)
                    needRematch = true;
                if (!needRematch && (context.getLastRawMatchSnapshot() != entry.getTxnEntryData() ||
                        context.getLastRawMatchSnapshot().getVersion() != entry.getTxnEntryData().getVersion()
                        || context.getLastRawMatchSnapshot().getOtherUpdateUnderXtnEntry() != null))
                    needRematch = true;
            }

            if (context.isNonBlockingReadOp() && needRematch) {
                if (!_templateScanner.match(context, entry, tmpl))
                    throw NO_MATCH_EXCEPTION;
                if (entry.isDeleted())//recheck since entry-data may have been cleaned in removeEntrySA
                    throw ENTRY_DELETED_EXCEPTION;

                needRematch = false;
                if (context.isUnstableEntry())
                    //entry was rematched && its snapshot may be unstable- retry with locked entry
                    return true;
            }


            boolean expired = context.isNonBlockingReadOp() ? context.getLastRawMatchSnapshot().isExpired() : entry.isExpired();
            expired = expired && !isExpiredEntryStayInSpace(entry);
            if (expired && !context.isNonBlockingReadOp() && !entry.isExpired(_leaseManager.getEffectiveEntryLeaseTime(SystemTime.timeMillis())))
                expired = false;
            if (expired) {
                if (!context.isNonBlockingReadOp()) {
                    if (!_leaseManager.isNoReapUnderXtnLeases() || !entry.isEntryUnderWriteLockXtn()) //dont reap entries under writelock xtn
                    {
                        if (!tmpl.isInitiatedEvictionOperation() && !_leaseManager.replicateLeaseExpirationEventsForEntries()) {
                            IServerTypeDesc typeDesc = _typeManager.getServerTypeDesc(entry.getClassName());
                            removeEntrySA(context, entry, typeDesc,
                                    false /*fromReplication*/, true/*origin*/, EntryRemoveReasonCodes.LEASE_EXPIRED);
                        }
                    }
                } else {//NBR- entry may be expired - try it under lock in order to prevent
                    //situation of long stale entries
                    context.setUnstableEntry(true);
                    return true;
                }
                throw ENTRY_DELETED_EXCEPTION/*new EntryDeletedException(ent.m_UID)*/;
            }


            if (tmpl.isInCache()) {
                ITemplateHolder template = tmpl;
                if (template.isDeleted())
                    throw new TemplateDeletedException(template);
                if (template.isExpired()) {
                    boolean cancelPendingOp = !template.isInitialIfExistSearchActive() || (template.getEntriesWaitingForTemplate() != null && !template.getEntriesWaitingForTemplate().isEmpty());
                    if (cancelPendingOp) {
                        if (!template.hasAnswer())
                            context.setOperationAnswer(template, null, null);
                        _cacheManager.removeTemplate(context, template,
                                false /*fromReplication*/, true /*origin*/, false);
                        throw new TemplateDeletedException(template);
                    }
                }
                performTemplateOnEntryCoreSA(context, template, entry, makeWaitForInfo,
                        needRematch);
            } else { /* template not in engine */
                performTemplateOnEntryCoreSA(context,
                        tmpl, entry, makeWaitForInfo, needRematch);
            } /* else */
        } /* else */
        return false;

    }

    /**
     * Performs the specified template operation on the specified entry.
     *
     * updateShadowUid is not null if the original match that caused this invocation was caused by a
     * update-shadow entry
     *
     * This method assumes objects that need to be locked are locked.
     */
    private void performTemplateOnEntryCoreSA(Context context, ITemplateHolder template, IEntryHolder entry,
                                              boolean makeWaitForInfo,
                                              boolean needRematch)
            throws TransactionConflictException, EntryDeletedException,
            TemplateDeletedException, TransactionNotActiveException,
            SAException, NoMatchException, FifoException {
        //if entry was recently update- is there still a match between
        // entry & template ?
        if (!context.isNonBlockingReadOp() && !needRematch)
            needRematch = !template.isMatchByID() && entry.hasShadow(true /*safeEntry*/);

        boolean tryMaster = true;
        boolean tryShadow = false;

        if (!context.isNonBlockingReadOp()) {
            if (template.isMatchByID()) {
                tryShadow = entry.hasShadow(true /*safeEntry*/);
            } else {
                if (needRematch) {

                    MatchResult mr = template.match(_cacheManager, entry, -1 /*skipIndex*/, null, true /*safeEntry*/, context, _templateScanner.getRegexCache());
                    tryMaster = (mr == MatchResult.MASTER || mr == MatchResult.MASTER_AND_SHADOW);
                    tryShadow = (mr == MatchResult.SHADOW || mr == MatchResult.MASTER_AND_SHADOW);
                }
            }
        } else {//non blocking read
            tryMaster = (context.getLastMatchResult() == MatchResult.MASTER || context.getLastMatchResult() == MatchResult.MASTER_AND_SHADOW);
            tryShadow = (context.getLastMatchResult() == MatchResult.SHADOW || context.getLastMatchResult() == MatchResult.MASTER_AND_SHADOW);
        }

        if (!tryMaster && !tryShadow)
            throw NO_MATCH_EXCEPTION;

        if (template.isInitiatedEvictionOperation()) {
            if (!tryMaster)
                throw NO_MATCH_EXCEPTION;
            tryShadow = false;
        }

        if (tryMaster) {
            try {

                performTemplateOnEntryCoreSA_impl(context, template, entry,
                        makeWaitForInfo,
                        false /*isShadow*/, tryShadow /*shadowMatches*/);
                return; //finished ok
            } catch (TransactionConflictException ex) {
                if (!template.isReadOperation())
                    tryShadow = false; //dont bother any more
                if (!tryShadow)
                    throw ex;

                //perform the op on the original entry + shadow if need to
                //all under entries lock
                try {
                    //shadow
                    performTemplateOnEntryCoreSA_impl(context, template, entry,
                            makeWaitForInfo,
                            true /*isShadow*/, true /*shadowMatches*/);
                    return; //finished ok with shadow

                } catch (Exception ex_) {
                }
                throw ex;
            }
        }
        //shadow
        performTemplateOnEntryCoreSA_impl(context, template, entry,
                makeWaitForInfo,
                true /*isShadow*/, true /*shadowMatches*/);
        return; //finished ok with shadow

    }


    /**
     * Performs the specified template operation on the specified entry.
     *
     * isShadow = true if processing of the shadow needed shadowMatches - if the shadow matches when
     * the master is called with
     *
     * This method assumes objects that need to be locked are locked.
     */
    private void performTemplateOnEntryCoreSA_impl(Context context, ITemplateHolder template, IEntryHolder entry,
                                                   boolean makeWaitForInfo,
                                                   boolean isShadow, boolean shadowMatches)
            throws TransactionConflictException, EntryDeletedException,
            TemplateDeletedException, TransactionNotActiveException,
            SAException, FifoException, NoMatchException {
        // check for TransactionNotActiveException
        XtnEntry templateTransactionEntry = null;
        // check for TransactionNotActiveException in template
        if (template.getXidOriginatedTransaction() != null) {
            templateTransactionEntry = getTransaction(template.getXidOriginatedTransaction());
            if (templateTransactionEntry == null || !templateTransactionEntry.m_Active)
                throw new TransactionNotActiveException(template.getXidOriginatedTransaction());
        }

        if (isShadow) //invocation was caused by a shadow entry, is it relevant still ?
        {
            if (template.getXidOriginatedTransaction() != null && template.getXidOriginatedTransaction().equals(entry.getWriteLockTransaction()))
                //an operation under same transaction which is updating the entry- the shadow is irrelevant only the new content
                throw ENTRY_DELETED_EXCEPTION;
        }

        //++++++++++++++++++++   FIFO RACE+++++++++++++++++++++++++++++++++++
        /**
         * check if the fifo operation is valid, if not throw FifoExecption
         */
        if (template.isFifoSearch()) {
            checkValidityOfFifoTemplateOnEntry(context, entry, template);
            if (context.isNonBlockingReadOp() && entry.isDeleted())
                throw ENTRY_DELETED_EXCEPTION;
        }
        //---------------------------------------------------------------

        final boolean isReadCommitted = template.isReadOperation()
                && indicateReadCommitted(context.isNonBlockingReadOp() ? context.getLastRawMatchSnapshot() : entry.getTxnEntryData(), template);

        // check for TransactionConflictException
        XtnConfilctCheckIndicators cres = checkTransactionConflict(context, entry, template, isShadow);

        if (cres != XtnConfilctCheckIndicators.NO_CONFLICT) {
            if (cres == XtnConfilctCheckIndicators.ENTRY_DELETED || cres == XtnConfilctCheckIndicators.DELETED_BY_OWN_XTN) {
                throw ENTRY_DELETED_EXCEPTION;
            }

            // now we know ex is instanceOf TransactionConflictException
            boolean skipWFenlist = isReadCommitted && !isShadow && shadowMatches;
            if (makeWaitForInfo && !skipWFenlist && !template.isDeleted()) {
                if (!template.isInCache()) {
                    if (template.isMultipleIdsOperation())
                        template.getMultipleIdsContext().setNonMainThreadUsed();
                    _cacheManager.insertTemplate(context, template, false);
                }
                _cacheManager.makeWaitForInfo(context, entry, template);
            }
            if (!skipWFenlist && template.isChange() && !template.isInCache()) {//blocking xtn and template is no-wait--> record the error info. in template with time-out we
                //get the blocking xtns info from the waiting-for list
                if (template.isChangeMultiple())
                    ((ExtendedAnswerHolder) (template.getAnswerHolder())).addRejectedEntriesInfo(Context._operationTimeoutExecption, entry.getEntryData(), entry.getUID());
                else
                    template.setRejectedOpOriginalExceptionAndEntry(new OperationTimeoutException(), entry.getEntryData());
            }
            if (!makeWaitForInfo && template.isIfExist())
                context.setPossibleIEBlockingMatch(true);

            throw TX_CONFLICT_EXCEPTION;
        }
        //++++++++++++++++++++   FIFO RACE+++++++++++++++++++++++++++++++++++
        if (template.isFifoTemplate() && !_isLocalCache && context.isNonBlockingReadOp()) {
            //since in NBR the object & xtn table are not locked, try again as blocking read if
            //another transaction finished since op start and xtn conflict check
            checkValidityOfFifoTemplateOnEntry(context, entry, template);
        }
        //---------------------------------------------------------------


        final IServerTypeDesc tte = _typeManager.getServerTypeDesc(entry.getClassName());

        //fifo groups- verify the entry can be used as a fifo group representetive
        if (template.isFifoGroupPoll())
            _fifoGroupsHandler.fifoGroupsOnOperationAction(context, entry, template, tte);

        if (template.isBatchOperation() && template.getBatchOperationContext().isInProcessedUids(entry.getUID()))
            throw NO_MATCH_EXCEPTION; //we already have this uid in our set

        switch (template.getTemplateOperation()) {
            case SpaceOperations.READ:
            case SpaceOperations.READ_IE:

                performReadTemplateOnEntryCoreSA(context, template, entry,
                        isShadow, isReadCommitted);
                break;
            case SpaceOperations.TAKE:
            case SpaceOperations.TAKE_IE:

                if (template.isInitiatedEvictionOperation())
                    performInitiatedEvictionTemplateOnEntryCoreSA(context, template, entry, tte);
                else
                    performTakeTemplateOnEntryCoreSA(context, template, entry,
                            tte);

                break;
            case SpaceOperations.UPDATE:

                try {
                    performUpdateTemplateOnEntryCoreSA(context, template, entry,
                            templateTransactionEntry, tte);
                } catch (ChangeInternalException ex) {
                    if (template.isBatchOperation())
                        template.getBatchOperationContext().addToProcessedUidsIfNeeded(entry.getUID());
                    //in case of change-exception resulted from user mutator - bypass this entry and keep on
                    throw NO_MATCH_EXCEPTION;
                }
                break;
            default:
                if (_logger.isErrorEnabled())
                    _logger.error("Unknown operation in template.");
        }
    }

    private void performReadTemplateOnEntryCoreSA(Context context, ITemplateHolder template,
                                                  IEntryHolder entry,
                                                  boolean isShadow, boolean isReadCommitted)
            throws SAException {
        IEntryHolder shadowEntryToUse = null;


        boolean useReadCommittedOfUpdatedEntry = isShadow && isReadCommitted;
        if (useReadCommittedOfUpdatedEntry)
            //match shadow entry
            shadowEntryToUse = context.isNonBlockingReadOp() ?
                    context.getLastRawMatchSnapshot().getOtherUpdateUnderXtnEntry() : entry.getShadow();

        boolean isDirtyRead = indicateDirtyRead(template);
        if (template.getXidOriginated() != null && !isReadCommitted && !isDirtyRead)
            //associate if read under xtn whitch is not read-committenotify_ehd
            _cacheManager.associateEntryWithXtn(context, entry, template, template.getXidOriginated(), null);

        if (template.isIfExist() && template.isInCache()) {
            _cacheManager.removeWaitingForInfo(context, entry, template, false /*unpinIfPossible*/);

        }

        IEntryPacket entryPacket = null;
        EntryHolderAggregatorContext aggregatorContext = template.getAggregatorContext();
        if (shadowEntryToUse != null) {
            if (aggregatorContext != null)
                aggregatorContext.scan(context, shadowEntryToUse.getEntryData(), entry.getUID(), entry.isTransient());
            else
                entryPacket = EntryPacketFactory.createFullPacket(context, shadowEntryToUse, template, entry.getUID());
        }else {
            IEntryData entryData = context.isNonBlockingReadOp() ? context.getLastRawMatchSnapshot() : entry.getEntryData();
            if (aggregatorContext != null)
                aggregatorContext.scan(context, entryData, entry.getUID(), entry.isTransient());
            else
                entryPacket = EntryPacketFactory.createFullPacket(context, entry, template, entryData);
        }

        if (!template.isBatchOperation()) {
            context.setOperationAnswer(template, entryPacket, null);
            if (template.isInCache())
                _cacheManager.removeTemplate(context, template, false /*fromReplication*/, true /*origin*/, false);
        } else {
            template.getBatchOperationContext().addResult(entryPacket);
            template.getBatchOperationContext().addToProcessedUidsIfNeeded(entry.getUID());
            if (template.canFinishBatchOperation()) {
                context.setOperationAnswer(template, null, null);
                if (template.isInCache())
                    _cacheManager.removeTemplate(context, template, false /*fromReplication*/, true /*origin*/, false);
            }
        }


        if (getCacheManager().isEvictableCachePolicy() || entry.isBlobStoreEntry())
            _cacheManager.touchByEntry(context, entry, false /*modifyOp*/, template, CacheOperationReason.ON_READ);

    }

    private void performTakeTemplateOnEntryCoreSA(Context context, ITemplateHolder template,
                                                  IEntryHolder entry,
                                                  IServerTypeDesc typeDesc)
            throws SAException, TemplateDeletedException {
        boolean called_remove = false;
        // any versionId conflict ????
        if (!_isLocalCache) // in case not local-cache-mode
        {
            int templateVersion = template.getEntryData().getVersion();
            if (templateVersion > 0 && (template.getUidToOperateBy() != null || template.getID() != null)) {
                // this code is not relevant for now,since version is not passed in replication
                int entryVersion = entry.getEntryData().getVersion();
                if (context.isFromReplication()) {
                    if (templateVersion < entryVersion) {
                        EntryVersionConflictException exv = new EntryVersionConflictException(template.getUidToOperateBy(), entryVersion, templateVersion, "Take");
                        context.setOperationAnswer(template, null, exv);
                        return;
                    }
                } else {
                    //version conflict
                    if (templateVersion != entryVersion) {
                        EntryVersionConflictException exv = new EntryVersionConflictException(template.getUidToOperateBy(), entryVersion, templateVersion, "Take/TakeIE");
                        context.setOperationAnswer(template, null, exv);

                        if (template.isInCache())
                            _cacheManager.removeTemplate(context, template, false /*fromReplication*/, true /*origin*/, false);
                        throw TEMPLATE_DELETED_EXCEPTION;
                    }
                }
            }

        }

        // TODO: what sould that mean ???
        //				if (template.getXidOriginatedTransaction() == null)
        //					timeToLive = 0;

        //creation of entry packet of result is done prior to calling associateEntryWithXtn since
        //shadow entry is surfacing again replacing the field values if pending update under same xtn
        IEntryPacket ep = EntryPacketFactory.createFullPacket(context, entry, template, entry.getUID());

        if (template.getXidOriginatedTransaction() != null) {
            // the following associates the entry with the template Xtn,
            // but only if the entry should not be deleted (this
            // occurs when the entry was written under the same Xtn as
            // the Take/TakeIE
            if (entry.getXidOriginatedTransaction() == null ||
                    !template.getXidOriginatedTransaction().equals(entry.getXidOriginatedTransaction()))
                _cacheManager.associateEntryWithXtn(context, entry, template, template.getXidOriginated(), null);
        }

        if (template.isIfExist() && template.isInCache()) {
            _cacheManager.removeWaitingForInfo(context, entry, template, false /*unpinIfPossible*/);
        }


        final boolean fromReplication = context.isFromReplication();
        final boolean origin = context.isOrigin();

        context.set_take_template(template);
        context.set_take_typeDesc(typeDesc);
        if ((template.getXidOriginatedTransaction() == null) ||
                (entry.getXidOriginatedTransaction() != null &&
                        template.getXidOriginatedTransaction().equals(entry.getXidOriginatedTransaction()))) {
            removeEntrySA(context, entry, typeDesc, fromReplication /*fromReplication*/,
                    origin, EntryRemoveReasonCodes.TAKE);
            called_remove = true;
        }


        if (!template.isBatchOperation()) {
            context.setOperationAnswer(template, ep, null);
            if (template.isInCache())
                _cacheManager.removeTemplate(context, template, false /*fromReplication*/, true /*origin*/, false);
        } else {
            template.getBatchOperationContext().addResult(ep);
            template.getBatchOperationContext().addToProcessedUidsIfNeeded(entry.getUID());
            if (template.canFinishBatchOperation()) {
                context.setOperationAnswer(template, null, null);
                if (template.isInCache())
                    _cacheManager.removeTemplate(context, template, false /*fromReplication*/, true /*origin*/, false);
            }
        }
        boolean anyNotityTakeTemplates = _cacheManager.getTemplatesManager().anyNotifyTakeTemplates();
        if (anyNotityTakeTemplates && !context.isBackupOnly()) {
            IEntryHolder notifytake_eh = template.getXidOriginatedTransaction() != null ?
                    entry.createCopy() : entry;
            boolean fifoNotify = !called_remove &&
                    ((context.get_take_typeDesc().isFifoSupported() && _cacheManager.getTemplatesManager().anyNotifyFifoTakeTemplates())
                            || (!context.get_take_typeDesc().isFifoSupported() && _cacheManager.getTemplatesManager().anyNotifyFifoForNonFifoTypePerOperation(context.get_take_typeDesc(), SpaceOperations.TAKE)));
            if (fifoNotify) {
                //NOTE- for notify fifo not under xtn we enque the requestr in removeEntrySA in order to enable order between take & write with same uid while object vto delete is still inside
                FifoBackgroundRequest red =
                        new FifoBackgroundRequest(
                                context.getOperationID(),
                                true /*isNotifyRequest*/,
                                false /* isNonNotifyRequest*/,
                                entry,
                                null,
                                context.isFromReplication(),
                                SpaceOperations.TAKE,
                                context.get_take_template().getXidOriginatedTransaction(),
                                notifytake_eh);

                _cacheManager.getFifoBackgroundDispatcher().positionAndActivateRequest(red);
            }

            if (template.getXidOriginatedTransaction() == null) {
                EntryTakenPacket etp = new EntryTakenPacket(context.getOperationID(), entry,
                        null /* txn */,
                        notifytake_eh, context.isFromReplication());
                _processorWG.enqueueBlocked(etp);
            }
        }
        if ((getCacheManager().isEvictableCachePolicy() || entry.isBlobStoreEntry()) && template.getXidOriginatedTransaction() != null)
            _cacheManager.touchByEntry(context,entry, true /*modifyOp*/, template, CacheOperationReason.ON_TAKE);
    }

    private void performUpdateTemplateOnEntryCoreSA(Context context, ITemplateHolder template,
                                                    IEntryHolder entry, XtnEntry templateTransactionEntry, IServerTypeDesc typeDesc)
            throws SAException, TemplateDeletedException {
        // any versionId conflict ????
        int templateVersion = template.getEntryData().getVersion();
        int previousTemplateVersion = template.getPreviousVersion();
        boolean overrideVersion = Modifiers.contains(template.getOperationModifiers(),
                Modifiers.OVERRIDE_VERSION);
        context.setReRegisterLeaseOnUpdate(template.isReRegisterLeaseOnUpdate());
        if (templateVersion > 0) {
            if (template.isChangeMultiple())
                throw new RuntimeException(" change multiple: version-id not allowed!");
            boolean versionIdConfilct = false;
            int entryVersion = entry.getEntryData().getVersion();

            if (context.isFromReplication()) {
                // updates with older version are ignored in replication
                if (templateVersion <= entryVersion && !overrideVersion) {
                    EntryVersionConflictException exv = new EntryVersionConflictException(template.getUidToOperateBy(), entryVersion, templateVersion, "Update");
                    context.setOperationAnswer(template, null, exv);
                    return;
                }
            } else if (context.isFromGateway()) {
                // Current entry version should be as previousTemplateVersion
                if (entryVersion != previousTemplateVersion && !overrideVersion)
                    versionIdConfilct = true;
            } else if (_isLocalCache) { //local cache mode, different versionID verification
                if (templateVersion < entryVersion || (templateVersion != entryVersion && UpdateModifiers.isPartialUpdate(template.getOperationModifiers())))
                    versionIdConfilct = true;
            } else {
                if (templateVersion != entryVersion && !overrideVersion)
                    versionIdConfilct = true;

            }
            if (versionIdConfilct) {//version conflict
                EntryVersionConflictException exv = new EntryVersionConflictException(template.getUidToOperateBy(), entryVersion, templateVersion, "Update");
                if (template.isSetSingleOperationExtendedErrorInfo()) {
                    template.setRejectedOpOriginalExceptionAndEntry(exv, entry.getEntryData());
                    context.setOperationAnswer(template, null, null);
                } else {
                    if (template.isChangeMultiple())
                        ((ExtendedAnswerHolder) template.getAnswerHolder()).addRejectedEntriesInfo(exv, entry.getEntryData(), entry.getUID());
                    context.setOperationAnswer(template, null, exv);
                }

                if (template.isInCache())
                    _cacheManager.removeTemplate(context, template, context.isFromReplication(), true /*origin*/, false);

                throw TEMPLATE_DELETED_EXCEPTION;
            }
        }

        IEntryData udata;
        IEntryData edata;
        IEntryHolder newEntry;
        boolean reWrittenUnderXtn = false;
        boolean needVerifyWF = entry.isHasWaitingFor();
        boolean fromReplication = false;
        int newVersionID;
        try {
            //in case of in-place update create an entry-holder with the new values
            if (template.isChange()) {
                IEntryHolder updateEntry = _cacheManager.makeChangeOpEntry(context, template, entry);
                if (!context.isFromReplication() && isPartitionedSpace() && updateEntry.getRoutingValue() != entry.getRoutingValue() && ProtectiveMode.isWrongRoutingUsageProtectionEnabled()) {
                    ProtectiveModeException exception = createExceptionIfRoutingValueDoesntMatchesCurrentPartition(updateEntry.getRoutingValue(), typeDesc, "change");
                    if (exception != null)
                        throw exception;
                }
                template.setUpdatedEntry(updateEntry);
            }


            boolean renewLease = false;
            udata = template.getUpdatedEntry().getEntryData();
            edata = entry.getEntryData();

            if (udata.getExpirationTime() != UPDATE_NO_LEASE)  // dont take from original
                renewLease = true;

            if (_isLocalCache || context.isFromReplication() || context.isFromGateway() || overrideVersion)
                // local cache mode, template must contain version-id
                newVersionID = template.getEntryData().getVersion();
            else
                newVersionID = edata.getVersion() + 1; //increment version ID of updated entry

            if (newVersionID != udata.getVersion() || udata.getExpirationTime() == UPDATE_NO_LEASE) {
                template.getUpdatedEntry().updateVersionAndExpiration(newVersionID, renewLease ? udata.getExpirationTime() : edata.getExpirationTime());
                udata = template.getUpdatedEntry().getEntryData();
            }


            if (template.getXidOriginatedTransaction() != null) {

                if (entry.getWriteLockTransaction() != null && (entry.getWriteLockOperation() == SpaceOperations.TAKE ||
                        entry.getWriteLockOperation() == SpaceOperations.TAKE_IE) &&
                        template.getXidOriginatedTransaction().equals(entry.getWriteLockTransaction())) {
                    reWrittenUnderXtn = true;  //take + write under xtn
                }

                templateTransactionEntry.m_AnyUpdates = true;
                // the following associates the entry with the template Xtn,
                newEntry = _cacheManager.associateEntryWithXtn(context, entry, template, template.getXidOriginated(),
                        template.getUpdatedEntry());
            } else { //non-transactional update in SA
                fromReplication = context.isFromReplication();
                final boolean origin = context.isOrigin();
                // update in a serial mode
                newEntry = updateEntrySA(context, entry, template, fromReplication, origin, typeDesc);
            }

        } catch (RuntimeException e) {
            //any reason to keep on ?
            if (template.isInCache())
                _cacheManager.removeWaitingForInfo(context, entry, template, false/*unpinIfPossible*/);
            if (template.isSetSingleOperationExtendedErrorInfo() || template.isChangeMultiple()) {
                Exception exception = e instanceof ChangeInternalException ? ((ChangeInternalException) e).getInternalException() : e;
                if (template.isSetSingleOperationExtendedErrorInfo())
                    template.setRejectedOpOriginalExceptionAndEntry(exception, entry.getEntryData());
                else
                    ((ExtendedAnswerHolder) (template.getAnswerHolder())).addRejectedEntriesInfo(exception, entry.getEntryData(), entry.getUID());


                boolean removeTemplateIfNeeded = false;
                if (template.isChangeById()) {
                    if (!template.isInitialIfExistSearchActive()) {
                        context.setOperationAnswer(template, null, exception);
                        removeTemplateIfNeeded = true;
                    }
                } else if (template.isChangeMultiple()) {
                    if (template.canFinishBatchOperation()) {
                        context.setOperationAnswer(template, null, null);
                        removeTemplateIfNeeded = true;
                    }
                }
                if (removeTemplateIfNeeded && template.isInCache())
                    _cacheManager.removeTemplate(context, template, false /*fromReplication*/, true /*origin*/, false);
            } else {
                context.setOperationAnswer(template, null, e);
                if (template.isInCache())
                    _cacheManager.removeTemplate(context, template, context.isFromReplication(), true /*origin*/, false);
            }
            throw e;
        }
        if (template.isChangeMultiple()) {
            ((ExtendedAnswerHolder) (template.getAnswerHolder())).addModifiedEntriesData(newEntry.getEntryData(), entry.getUID(), context.getChangeResultsForCurrentEntry());
        } else {
            ((ExtendedAnswerHolder) (template.getAnswerHolder())).setModifiedEntryData(newEntry.getEntryData());
            ((ExtendedAnswerHolder) (template.getAnswerHolder())).setPreviousEntryData(edata);
            if (template.isChange())
                ((ExtendedAnswerHolder) (template.getAnswerHolder())).setSingleChangeResults(context.getChangeResultsForCurrentEntry());
        }
        IEntryPacket ep = EntryPacketFactory.createFullPacket(context, entry, template, edata);
        if (template.isUpdateOperation()) {
            if (context.getWriteResult() == null || !entry.getUID().equals(context.getWriteResult().getUid()))
                context.setWriteResult(new WriteEntryResult(entry.getUID(), newVersionID, newEntry.getEntryData().getExpirationTime()));
        }
        if (template.isIfExist() && template.isInCache()) {
            _cacheManager.removeWaitingForInfo(context, entry, template, false/*unpinIfPossible*/);
        }

        if (!template.isBatchOperation()) {
            context.setOperationAnswer(template, ep, null);
            if (template.isInCache())
                _cacheManager.removeTemplate(context, template, false /*fromReplication*/, true /*origin*/, false);
        } else {
            template.getBatchOperationContext().addResult(ep);
            template.getBatchOperationContext().addToProcessedUidsIfNeeded(entry.getUID());
            if (template.canFinishBatchOperation()) {
                context.setOperationAnswer(template, null, null);
                if (template.isInCache())
                    _cacheManager.removeTemplate(context, template, false /*fromReplication*/, true /*origin*/, false);
            }
        }

        IEntryHolder notify_eh = newEntry.createCopy();

        //FIFO+++++++++++++++
        //if its a fifo type and there are fifo templates.....
        boolean fifoNotify = false;
        if (typeDesc.isFifoSupported())
            fifoNotify =
                    (!reWrittenUnderXtn && _cacheManager.getTemplatesManager().anyNotifyFifoUpdateTemplates()) ||
                            (reWrittenUnderXtn && _cacheManager.getTemplatesManager().anyNotifyFifoWriteTemplates());
        else
            fifoNotify =
                    (!reWrittenUnderXtn && _cacheManager.getTemplatesManager().anyNotifyFifoForNonFifoTypePerOperation(typeDesc, SpaceOperations.UPDATE)) ||
                            (reWrittenUnderXtn && _cacheManager.getTemplatesManager().anyNotifyFifoForNonFifoTypePerOperation(typeDesc, SpaceOperations.WRITE));


        FifoBackgroundRequest red = null;
        int operation = !reWrittenUnderXtn ? SpaceOperations.UPDATE : SpaceOperations.WRITE;
        if (fifoNotify) {

            IEntryHolder originalEntryHolder = EntryHolderFactory.createEntryHolder(typeDesc, ep, _entryDataType);
            red = new FifoBackgroundRequest(context.getOperationID(),
                    true /*isNotifyRequest*/,
                    (typeDesc.isFifoSupported() && _cacheManager.getTemplatesManager().anyNonNotifyFifoTemplates())/* isNonNotifyRequest*/,
                    newEntry,
                    originalEntryHolder,
                    context.isFromReplication(),
                    operation,
                    template.getXidOriginatedTransaction(),
                    notify_eh);
        } else {
            if (typeDesc.isFifoSupported() && _cacheManager.getTemplatesManager().anyNonNotifyFifoTemplates())
                red = new FifoBackgroundRequest(context.getOperationID(),
                        false/*isNotifyRequest*/,
                        true/*isNonNotifyRequest*/,
                        newEntry,
                        null,
                        context.isFromReplication(),
                        operation,
                        template.getXidOriginatedTransaction(),
                        null /*cloneEH*/);
        }
        long curTime = SystemTime.timeMillis();
        if (red != null) {
            red.setTime(curTime);
            _cacheManager.getFifoBackgroundDispatcher().positionAndActivateRequest(red);
        }
        //FIFO-------------------

        // sync volatile variable
        setLastEntryTimestamp(curTime);

        boolean isNotifyMatched = _cacheManager.getTemplatesManager().anyNotifyMatchedTemplates();
        boolean isNotifyRematched = _cacheManager.getTemplatesManager().anyNotifyRematchedTemplates();
        if (_cacheManager.getTemplatesManager().anyNonNotifyTemplates() || _cacheManager.getTemplatesManager().anyNotifyUpdateTemplates() || isNotifyMatched || isNotifyRematched) {

            if (reWrittenUnderXtn) // it is not a true update, just take+write under xtn
            {
                EntryArrivedPacket ea = _entryArrivedFactory.getPacket(context.getOperationID(), newEntry, template.getXidOriginatedTransaction(),
                        true, notify_eh, fromReplication);
                _processorWG.enqueueBlocked(ea);
            } else {
                IEntryHolder originalEntryHolder = EntryHolderFactory.createEntryHolder(typeDesc, ep, _entryDataType);
                EntryUpdatedPacket entryUpdatedPacket = new EntryUpdatedPacket(context.getOperationID(), originalEntryHolder,
                        newEntry, notify_eh, template.getXidOriginatedTransaction(), fromReplication, isNotifyMatched, isNotifyRematched);
                _processorWG.enqueueBlocked(entryUpdatedPacket);
            }
        }
        /**
         * Only if at least one template exists in cache -
         * Inform to all waited templates about new updated packet.
         **/
        if (_cacheManager.getTemplatesManager().anyNotifyUnmatchedTemplates() && template.getXidOriginatedTransaction() == null) {
            IEntryHolder originalEntryHolder = EntryHolderFactory.createEntryHolder(typeDesc, ep, _entryDataType);
            EntryUnmatchedPacket ea = new EntryUnmatchedPacket(context.getOperationID(), originalEntryHolder,
                    notify_eh, null /* txn */, fromReplication);
            _processorWG.enqueueBlocked(ea);
        }


        if (needVerifyWF)
            checkWFValidityAfterUpdate(context, entry);

        if (getCacheManager().isEvictableCachePolicy() || entry.isBlobStoreEntry())
            _cacheManager.touchByEntry(context,entry, true /*modifyOp*/,template,CacheOperationReason.ON_UPDATE);
    }


    private void performInitiatedEvictionTemplateOnEntryCoreSA(Context context, ITemplateHolder template,
                                                               IEntryHolder entry, IServerTypeDesc typeDesc)
            throws SAException, NoMatchException {
        IEntryPacket ep = EntryPacketFactory.createFullPacket(context, entry, template);

        boolean fromReplication = context.isFromReplication();
        boolean shouldReplicate = false;
        if (isReplicated())
            shouldReplicate = shouldReplicate(ReplicationOperationType.EVICT, typeDesc, false /*disableReplication*/, fromReplication);

        if (!_cacheManager.initiatedEviction(context, entry, shouldReplicate))
            throw NO_MATCH_EXCEPTION;  //entry no relevant
        if (!template.isBatchOperation()) {
            context.setOperationAnswer(template, ep, null);
            if (template.isInCache())
                _cacheManager.removeTemplate(context, template, false /*fromReplication*/, true /*origin*/, false);
        } else {
            template.getBatchOperationContext().addResult(ep);
            template.getBatchOperationContext().addToProcessedUidsIfNeeded(entry.getUID());
            if (template.canFinishBatchOperation()) {
                context.setOperationAnswer(template, null, null);
                if (template.isInCache())
                    _cacheManager.removeTemplate(context, template, false /*fromReplication*/, true /*origin*/, false);
            }
        }

    }

    /**
     * given an entry and a template, return true if read-committed should be performed should be
     * called when xtn & entry are locked (but called from count w/o locks in order not to harm
     * performance
     */
    private boolean indicateReadCommitted(ITransactionalEntryData entry, ITemplateHolder template) {
        if (indicateDirtyRead(template) || !template.isReadCommittedRequested())
            return false;

        ServerTransaction est = entry.getWriteLockTransaction();

        if (template.getXidOriginatedTransaction() != null && est != null && template.getXidOriginatedTransaction().equals(est))
            //if both entry & template under same xtn, its not read committed
            return false;

        return true;
    }

    /**
     * given an entry and a template, return true if dirty-read should be performed
     */
    public boolean indicateDirtyRead(ITemplateHolder template) {
        return ((template.isReadOperation() && _useDirtyRead) || template.isDirtyReadRequested());

    }


    //++++++++++++++++++++   FIFO RACE+++++++++++++++++++++++++++++++++++

    /**
     * check if the fifo operation is valid, if not throw FifoExecption
     */

    private void checkValidityOfFifoTemplateOnEntry(Context context, IEntryHolder entry, ITemplateHolder template)
            throws FifoException {
        TerminatingFifoXtnsInfo.FifoXtnEntryInfo eti = null;
        //if this is the initial search thread-filter entries according to xtn numbers
        boolean isDirtyRead = false;
        boolean isReadCommitted = false;


        if (context.isTemplateInitialSearch() && template.isInitialFifoSearchActive()) {
            eti = _cacheManager.getFifoEntryXtnInfo(entry);
            if (eti != null) {
                //entry came in after template insert + xtn, reject
                if (TerminatingFifoXtnsInfo.isSeqTransactionGT(eti.getCreatingXtn(), template.getFifoXtnNumberOnSearchStart()))
                    throw _fifoException;

                long xtnEntry;
                if (template.isReadOperation()) {
                    isDirtyRead = indicateDirtyRead(template);
                    if (isDirtyRead)
                        return; //ok
                    isReadCommitted = indicateReadCommitted(context.isNonBlockingReadOp() ? context.getLastRawMatchSnapshot() : entry.getTxnEntryData(), template);
                    if (isReadCommitted) {
                        xtnEntry = eti.getEntryWriteXtnNumber();//the writing xtn of entry when committed
                        if (TerminatingFifoXtnsInfo.isSeqTransactionGT(xtnEntry, template.getFifoXtnNumberOnSearchStart()))
                            throw _fifoException;
                        return; //ok
                    }
                }
                xtnEntry = eti.getTerminatingXtnWriteLock();

                if (TerminatingFifoXtnsInfo.isSeqTransactionGT(xtnEntry, template.getFifoXtnNumberOnSearchStart()))
                    throw _fifoException;
                if (template.isWriteLockOperation()) {
                    xtnEntry = eti.getTerminatingXtnReadLock();
                    if (TerminatingFifoXtnsInfo.isSeqTransactionGT(xtnEntry, template.getFifoXtnNumberOnSearchStart()))
                        throw _fifoException;
                }

            }
        }
        //if fifo template & template is performing initial search & this thread
        //is not the initial search performer- block the operation & enlist the entry
        //in template for later op'
        if (!context.isTemplateInitialSearch() && template.isInitialFifoSearchActive() && template.getPendingFifoSearchObject() != null) {
            template.getPendingFifoSearchObject().addRejectedEntry(entry, context.getRecentFifoObject());
            throw _fifoException;
        }

        //if this is the fifo thread-filter entries according to xtn numbers
        if (context.isFifoThread()) {
            //entry came in before template insert + xtn, reject
            long fifoThreadXtn = context.getRecentFifoObject().getFifoXtnNumber();
            if (TerminatingFifoXtnsInfo.isSeqTransactionGT(template.getFifoXtnNumberOnSearchStart(), fifoThreadXtn))
                throw _fifoException;

            //was entry involved in terminated xtn ?
            eti = _cacheManager.getFifoEntryXtnInfo(entry);
            if (eti != null) {

                long xtnEntry;
                if (template.isReadOperation()) {
                    isDirtyRead = indicateDirtyRead(template);
                    if (isDirtyRead)
                        return; //is ok
                    isReadCommitted = indicateReadCommitted(context.isNonBlockingReadOp() ? context.getLastRawMatchSnapshot() : entry.getTxnEntryData(), template);
                    if (isReadCommitted) {
                        xtnEntry = eti.getEntryWriteXtnNumber();//the writing xtn of entry when committed
                        if (TerminatingFifoXtnsInfo.isSeqTransactionGT(xtnEntry, fifoThreadXtn))
                            throw _fifoException;
                        return; //ok
                    }
                }
                xtnEntry = eti.getTerminatingXtnWriteLock();
                if (TerminatingFifoXtnsInfo.isSeqTransactionGT(xtnEntry, fifoThreadXtn))
                    throw _fifoException;
                if (template.isWriteLockOperation()) {
                    xtnEntry = eti.getTerminatingXtnReadLock();
                    if (TerminatingFifoXtnsInfo.isSeqTransactionGT(xtnEntry, fifoThreadXtn))
                        throw _fifoException;
                }
            }
        }
    }


    //--------------------FIFO RACE -------------------------------------------

    /**
     * update performed for an entry under xtn, if there are templates wf this entry, is there any
     * use to keep on waiting (i.e. is there still a match with master or shadow ?) NOTE: entry is
     * locked while calling this routine
     */
    public void checkWFValidityAfterUpdate(Context context, IEntryHolder entry)
            throws SAException {
        if (!entry.isHasWaitingFor() || entry.isDeleted())
            return;

        Collection<ITemplateHolder> wf = entry.getTemplatesWaitingForEntry();
        for (ITemplateHolder template : wf) {
            if (!template.isDeleted()) {
                boolean match = _templateScanner.match(context, entry, template, -1, null, true);
                if (!match) {
                    // entry should be removed from template wf and visa-versa


                    // create and dispatch RemoveWaitingForInfoSABusPacket
                    // so that the removal of waiting for info will be done
                    // asynchronously
                    RemoveWaitingForInfoSABusPacket wfpacket =
                            new RemoveWaitingForInfoSABusPacket(context.getOperationID(), entry,
                                    template);
                    _processorWG.enqueueBlocked(wfpacket);
                    continue; /* to next template */
                }//if (!Match
            }//if (!template.m_IsNull)
        }//for
    }


    /**
     * Join the specified transaction, if not already joined.
     *
     * @param txn the transaction to join
     */
    public XtnEntry attachToXtn(ServerTransaction txn, boolean fromReplication)
            throws TransactionException, RemoteException {
        return _transactionHandler.attachToXtnGranular(txn, fromReplication);
    }

    /**
     * Delete an entry (direct delete). Assumes the specified entry is locked.
     */
    public void removeEntrySA(Context context, IEntryHolder entry, IServerTypeDesc typeDesc,
                              boolean fromReplication, boolean origin, EntryRemoveReasonCodes removeReason)
            throws SAException {
        removeEntrySA(context, entry, typeDesc, fromReplication, origin,
                removeReason, false /*disableReplication-mostly if lease expired*/,
                false /*disableProcessorCall*/, false /*disableSADelete*/);
    }

    /**
     * Delete an entry (direct delete). Assumes the specified entry is locked.
     */
    public void removeEntrySA(Context context, IEntryHolder entry, IServerTypeDesc typeDesc,
                              boolean fromReplication, boolean origin, EntryRemoveReasonCodes removeReason,
                              boolean disableReplication, boolean disableProcessorCall, boolean disableSADelete)
            throws SAException {
        removeEntrySA(context, entry, fromReplication, origin,
                typeDesc.getTypeDesc().isReplicable(), removeReason,
                disableReplication, disableProcessorCall, disableSADelete);
    }

    public void removeEntrySA(Context context, IEntryHolder entry,
                              boolean fromReplication, boolean origin,
                              boolean ofReplicatableClass, EntryRemoveReasonCodes removeReason,
                              boolean disableReplication, boolean disableProcessorCall, boolean disableSADelete)
            throws SAException {
        boolean fromLeaseExpiration = removeReason == EntryRemoveReasonCodes.LEASE_CANCEL || removeReason == EntryRemoveReasonCodes.LEASE_EXPIRED;
        // check for before-remove filter
        if ((fromLeaseExpiration || _general_purpose_remove_filters) && _filterManager._isFilter[FilterOperationCodes.BEFORE_REMOVE])
            _filterManager.invokeFilters(FilterOperationCodes.BEFORE_REMOVE, null, entry);

        boolean noRealRemoveFromSpace = fromLeaseExpiration && isExpiredEntryStayInSpace(entry);

        disableReplication = disableReplication || (removeReason == EntryRemoveReasonCodes.LEASE_EXPIRED && !_leaseManager.replicateLeaseExpirationEventsForEntries());
        // mark entry as deleted
        if (!noRealRemoveFromSpace) {
            entry.setDeleted(true);
        }
        //if entry has a shadow delete it too
        if (entry.hasShadow(true /*safeEntry*/)) {
            IEntryHolder shadoweh = entry.getShadow();
            if (shadoweh != null)
                shadoweh.setDeleted(true);
        }

        // handle waiting for templates, if exist
        boolean originalWF = false;
        if (entry.isHasWaitingFor()) {
            originalWF = true;
            // create and dispatch RemoveWaitingForInfoSABusPacket
            // so that the removal of waiting for info will be done
            // asynchronously
            RemoveWaitingForInfoSABusPacket packet =
                    new RemoveWaitingForInfoSABusPacket(context.getOperationID(), entry, null);
            _processorWG.enqueueBlocked(packet);

        } /* if (entry.isHasWaitingFor()) */

        // remove entry from Storage Adapter
        boolean shouldReplicate = false;
        if (isReplicated()) {
            ReplicationOperationType operType = removeReason == EntryRemoveReasonCodes.TAKE ? ReplicationOperationType.TAKE : ReplicationOperationType.LEASE_EXPIRATION;
            shouldReplicate = shouldReplicate(operType, ofReplicatableClass, disableReplication, fromReplication);
        }
        FifoBackgroundRequest red = null;
        if (context.get_take_template() != null || fromLeaseExpiration) {

            boolean fifoNotifyTake = context.get_take_template() != null && _cacheManager.getTemplatesManager().anyNotifyTakeTemplates()
                    && ((context.get_take_typeDesc().isFifoSupported() && _cacheManager.getTemplatesManager().anyNotifyFifoTakeTemplates()) || (!context.get_take_typeDesc().isFifoSupported() && _cacheManager.getTemplatesManager().anyNotifyFifoForNonFifoTypePerOperation(context.get_take_typeDesc(), SpaceOperations.TAKE)));

            boolean fifoNotifyLeaseExpiration = !fifoNotifyTake && fromLeaseExpiration && _cacheManager.getTemplatesManager().anyNotifyLeaseTemplates();
            if (fifoNotifyLeaseExpiration) {
                final IServerTypeDesc tte = _typeManager.getServerTypeDesc(entry.getClassName());
                fifoNotifyLeaseExpiration = (tte.isFifoSupported() && _cacheManager.getTemplatesManager().anyNotifyFifoLeaseExpirationTemplates()) || (!tte.isFifoSupported() && _cacheManager.getTemplatesManager().anyNotifyFifoForNonFifoTypePerOperation(tte, SpaceOperations.LEASE_EXPIRATION));
            }
            if (fifoNotifyTake || fifoNotifyLeaseExpiration) {
                int spaceOperation = fifoNotifyTake ? SpaceOperations.TAKE : SpaceOperations.LEASE_EXPIRATION;
                red = fifoNotifyTake ? new FifoBackgroundRequest(
                        context.getOperationID(),
                        true /*isNotifyRequest*/,
                        false /* isNonNotifyRequest*/,
                        entry,
                        null,
                        context.isFromReplication(),
                        spaceOperation,
                        context.get_take_template().getXidOriginatedTransaction(),
                        entry)
                        : new FifoBackgroundRequest(
                        context.getOperationID(),
                        true /*isNotifyRequest*/,
                        false /* isNonNotifyRequest*/,
                        entry,
                        null,
                        context.isFromReplication(),
                        spaceOperation,
                        null,
                        entry);

                red.setAllowFifoNotificationsForNonFifoEvents(new FifoBackgroundRequest.AllowFifoNotificationsForNonFifoType());
                _cacheManager.getFifoBackgroundDispatcher().positionAndActivateRequest(red);
            }
        }
        try {
            //delete from cache + SA
            IEntryCacheInfo pEntry = null;
            if (noRealRemoveFromSpace) {
                pEntry = _cacheManager.getEntryCacheInfo(entry);
                pEntry.setRemoving(true /*pinned*/);
            }
            _cacheManager.removeEntry(context, entry, pEntry, shouldReplicate, origin, removeReason, disableSADelete);
        } catch (SAException e) {
            if (_cacheManager.getEntryByUidFromPureCache(entry.getUID()) == entry) {
                if (red != null) {
                    red.setCancelled();
                    red.getAllowFifoNotificationsForNonFifoEvents().allow();
                }
                entry.setDeleted(false);
            }
            throw e;
        } catch (RuntimeException re) {
            if (red != null) {
                red.setCancelled();
                red.getAllowFifoNotificationsForNonFifoEvents().allow();
            }
            throw re;
        }
        if (red != null) {
            red.getAllowFifoNotificationsForNonFifoEvents().allow();
        }
        //remove from recent updates- if its inside
        _cacheManager.removeFromRecentUpdatesIfNeeded(entry);


        //remove xtn related info from entry- faster gc
        if (((ITransactionalEntryData) entry.getEntryData()).getEntryXtnInfo() != null) {
            if (originalWF) {
                if (entry.getXidOriginated() != null)
                    entry.resetXidOriginated();
                if (entry.getWriteLockOwner() != null)
                    entry.resetWriteLockOwner();
                if (entry.getReadLockOwners() != null && entry.getReadLockOwners().size() > 0)
                    entry.getReadLockOwners().clear();
            } else
                entry.resetEntryXtnInfo();
        }
        //NOTE- fifo related info not removed - for NBR of fifo we need to check the deleted bit
        //at the end of fifocheck
        if (_cacheManager.isFromFifoClass(entry.getServerTypeDesc()))
            _cacheManager.removeFifoXtnInfoForEntry(entry);

        // check for after-remove filter
        try {
            if ((fromLeaseExpiration || _general_purpose_remove_filters) && _filterManager._isFilter[FilterOperationCodes.AFTER_REMOVE])
                _filterManager.invokeFilters(FilterOperationCodes.AFTER_REMOVE, null, entry);
        } catch (RuntimeException ex) {
            // keep on with thread operation
            if (_logger.isErrorEnabled()) {
                _logger.error("Failed AFTER_REMOVE filter.", ex);
            }
        }

        if (fromLeaseExpiration && !disableProcessorCall) {
            if (_cacheManager.getTemplatesManager().anyNotifyLeaseTemplates() && entry.getWriteLockTransaction() == null) {
                _processorWG.enqueueBlocked(new EntryExpiredBusPacket(entry, null /* txn */, fromReplication));
            }
        }

    }


    /**
     * update the entry- drop waiting-for from original, call cache-manager to update
     */
    private IEntryHolder updateEntrySA(Context context, IEntryHolder entry, ITemplateHolder template,
                                       boolean fromReplication, boolean origin, IServerTypeDesc typeDesc)
            throws SAException {
        //insert UID of entry to hash of updated entries
        _cacheManager.insertToRecentUpdatesIfNeeded(entry, _cacheManager.requiresEvictionReplicationProtection() ? Long.MAX_VALUE : 0, null);

        // update entry in Storage Adapter
        boolean shouldReplicate = false;
        if (isReplicated())
            shouldReplicate = shouldReplicate(ReplicationOperationType.UPDATE, typeDesc, false, fromReplication);

        return _cacheManager.updateEntry(context, entry, template, shouldReplicate, origin);
    }


    private void commitSA(TransactionManager mgr, ServerTransaction st, boolean supportsTwoPhaseReplication, OperationID operationID, boolean mayBeFromReplication)
            throws UnknownTransactionException, RemoteException {
        Context context = null;
        XtnEntry xtnEntry = null;
        XtnEntry xtnEntryLocked;

        try {
            context = _cacheManager.getCacheContext();
            getTransactionHandler().getTimedXtns().remove(st);

            boolean lockedXtnTable = false;
            while (true) {
                boolean relock = false;
                xtnEntry = getTransaction(st);
                xtnEntryLocked = xtnEntry;
                if (xtnEntry == null) {
                    if (mgr instanceof LocalTransactionManager)
                        return;
                    throw new UnknownTransactionException("Unknown Transaction [memberName=" + _spaceImpl.getServiceName() + "] transaction: " + st);
                }
                try {
                    lockedXtnTable = getTransactionHandler().lockXtnOnXtnEnd(xtnEntryLocked);
                    xtnEntry = getTransaction(st);


                    if (xtnEntry != xtnEntryLocked)
                        relock = true;

                } finally {
                    if (!relock)
                        break;
                    //very rare race condition
                    getTransactionHandler().unlockXtnOnXtnEnd(xtnEntryLocked, lockedXtnTable);
                }
            }

            try {
                if (xtnEntry != null && !xtnEntry.addUsedIfPossible())
                    xtnEntry = null; //unsed status, will be removed by lease manager

                if (xtnEntry == null) {
                    if (mgr instanceof LocalTransactionManager)
                        return;
                    throw new UnknownTransactionException("Unknown Transaction " + st);
                }

                if (xtnEntry.getStatus() == XtnStatus.ROLLED ||
                        xtnEntry.getStatus() == XtnStatus.ROLLING)
                    throw new UnknownTransactionException("Commit failed, transaction : " + st + " already rolled.");

                if (xtnEntry.getStatus() == XtnStatus.COMMITING ||
                        xtnEntry.getStatus() == XtnStatus.COMMITED)
                    return;

                boolean fifoEntries = !xtnEntry.m_SingleParticipant && xtnEntry.anyFifoEntriesUnderXtn();

                if (!xtnEntry.m_SingleParticipant) {
                    context.setCommittingXtn(xtnEntry.m_Transaction);
                    getTransactionHandler().removeFromPrepared2PCXtns(xtnEntry.getServerTransaction());
                }

                //Indicate this commit operation is not from replication, and we may need to replicate this commit operation
                if (!mayBeFromReplication)
                    xtnEntry.setFromReplication(false);

                // Set from-gateway state on context
                context.setFromGateway(xtnEntry.isFromGateway());

                /* in case of replication and full 2PC (non-singleparticipant)
                 call pashe 2 of prepare in order to write to redo && update
				 the xtn record in the same LUW*/

                if (!xtnEntry.m_SingleParticipant)
                    xtnEntry.m_CommitRollbackTimeStamp = SystemTime.timeMillis();

                //write to SA in case of 2PC
                _cacheManager.prepare(context, xtnEntry, supportsTwoPhaseReplication, false /*handleReplication*/, !xtnEntry.m_SingleParticipant/* handleSA*/);

                _cacheManager.preCommit(context, xtnEntry, supportsTwoPhaseReplication);

                /* handle possible updates in full 2pc */
                if (xtnEntry.m_AnyUpdates && !xtnEntry.m_SingleParticipant) {
                    handleUnderXtnUpdates(context, xtnEntry, true/*isCommitting*/);
                }
                long currentFifoXtn = TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN;
                if (fifoEntries) {
                    currentFifoXtn = _cacheManager.getLatestTTransactionTerminationNum();
                    //update the new number in the transaction object
                    currentFifoXtn++;
                    if (currentFifoXtn < 0)
                        currentFifoXtn = 1;
                    _cacheManager.setFifoXtnNumber(xtnEntry, currentFifoXtn);
                    //mark the locked entries with the new fifo xtn number
                    _coreProcessor.handleLockedFifoEntriesBeforeXtnEnd(context, xtnEntry, false /*fromRollback*/);
                }
                boolean considerNotifyFifoForNonFifoEvents = false;

                if (!xtnEntry.m_SingleParticipant)
                    considerNotifyFifoForNonFifoEvents = _coreProcessor.handleNotifyFifoInCommit(context, xtnEntry, true /*fifoNotifyForNonFifoEvents*/);

                if (!xtnEntry.m_SingleParticipant)
                    _fifoGroupsHandler.prepareForFifoGroupsAfterXtnScans(context, xtnEntry);

                context.setOperationID(operationID);
                _cacheManager.commit(context, xtnEntry, xtnEntry.m_SingleParticipant, xtnEntry.m_AnyUpdates, supportsTwoPhaseReplication);

                xtnEntry.setStatus(XtnStatus.COMMITING);
                //fifo group op performed under this xtn
                if (!xtnEntry.m_SingleParticipant && xtnEntry.getXtnData().anyFifoGroupOperations())
                    _cacheManager.handleFifoGroupsCacheOnXtnEnd(context, xtnEntry);
                //for 2PC- handle taken entries under xtn
                if (!xtnEntry.m_SingleParticipant) {
                    _coreProcessor.handleCommittedTakenEntries(context, xtnEntry);
                }

                if (considerNotifyFifoForNonFifoEvents)
                    xtnEntry.getAllowFifoNotificationsForNonFifoEntries().allow();


                xtnEntry.setStatus(XtnStatus.COMMITED);

                //set latest fifo xtn number if relevant
                if (fifoEntries && currentFifoXtn != TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN)
                    _cacheManager.setLatestTransactionTerminationNum(currentFifoXtn);

                //handle fifo entries locked under the xtn. This is done not in
                //background in order to enable fifo serialization per client thread
                if (fifoEntries)
                    _coreProcessor.handleLockedFifoEntriesOnXtnEnd(context, xtnEntry, false /*fromRollback*/);
                if (fifoEntries)
                    _coreProcessor.handleNotifyFifoInCommit(context, xtnEntry, false);

            } finally {
                getTransactionHandler().unlockXtnOnXtnEnd(xtnEntryLocked, lockedXtnTable);
            }

            if (getTransactionHandler().isLightTransaction(xtnEntry)) {
                try {
                    _coreProcessor.handleCommitSA(new CommitBusPacket(xtnEntry));
                } catch (Exception ex) {
                }//inner routines log errors
            } else {
                _processorWG.enqueueBlocked(new CommitBusPacket(xtnEntry));
            }
        } catch (SAException ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error(ex.toString(), ex);
            }

            if (_spaceImpl.isAborted()) {
                throw new RemoteException("Can not commit transaction - space is aborting", ex);
            }

            // If xtn entry is null - getCacheContext failed - system is aborting
            // TODO replace with specific handling of SAException during shutdown
            if (xtnEntry != null && xtnEntry.getStatus() == XtnStatus.PREPARED) {
                xtnEntry.setStatus(XtnStatus.ERROR);
                abort(mgr, st, supportsTwoPhaseReplication, null);
            }
            JSpaceUtilities.throwEngineInternalSpaceException(ex.getMessage() + " aborting transaction", ex);
        } finally {
            replicateAndfreeCacheContextTxn(context, st);
        }// finnaly
    }

    public boolean isReplicated() {
        return _isReplicated;
    }

    public boolean isReplicatedPersistentBlobstore() {
        return _isReplicatedPersistentBlobstore;
    }

    public void abortSA(TransactionManager mgr, ServerTransaction st, boolean fromLeaseExpiration, boolean verifyExpiredXtn, boolean supportsTwoPhaseReplication, OperationID operationID)
            throws UnknownTransactionException {
        Context context = null;
        XtnEntry xtnEntry = null;
        XtnEntry xtnEntryLocked;
        XtnStatus status;
        try {
            context = _cacheManager.getCacheContext();
            boolean lockedXtnTable = false;
            while (true) {
                boolean relock = false;
                xtnEntry = getTransaction(st);
                xtnEntryLocked = xtnEntry;
                if (xtnEntry == null) {
                    if (mgr instanceof LocalTransactionManager && !fromLeaseExpiration)
                        return;
                    //may be a phantom global xtn
                    getTransactionHandler().addToPhantomGlobalXtns(st);
                    throw new UnknownTransactionException();
                }
                try {
                    lockedXtnTable = getTransactionHandler().lockXtnOnXtnEnd(xtnEntryLocked);
                    xtnEntry = getTransaction(st);
                    if (xtnEntry != xtnEntryLocked)
                        relock = true;
                } finally {
                    if (!relock)
                        break;
                    //very rare race condition
                    getTransactionHandler().unlockXtnOnXtnEnd(xtnEntryLocked, lockedXtnTable);
                }
            }

            try {
                if (xtnEntry != null && !xtnEntry.addUsedIfPossible())
                    xtnEntry = null; //unsed status, will be removed by lease manager
                if (xtnEntry == null) {
                    if (mgr instanceof LocalTransactionManager && !fromLeaseExpiration)
                        return;
                    throw new UnknownTransactionException();
                }

                if (verifyExpiredXtn) {//verify under lock that the xtn is a local expired one.
                    //this param is called from leaseManager

                    Long Llimit = getTransactionHandler().getTimedXtns().get(st);
                    if (Llimit == null)
                        return;  // already terminated or lease change

                    long limit = Llimit.longValue();
                    if (SystemTime.timeMillis() <= limit)
                        return;  // renewed lease

                    if (!xtnEntry.m_Active && !xtnEntry.isFromGateway())
                        return;
                }
                boolean fifoEntries = xtnEntry.anyFifoEntriesUnderXtn();

                getTransactionHandler().getTimedXtns().remove(st);

                status = xtnEntry.checkAndGetState_Granular();

                if ((status == XtnStatus.COMMITING ||
                        status == XtnStatus.COMMITED ||
                        status == XtnStatus.ROLLING ||
                        status == XtnStatus.ROLLED ||
                        (xtnEntry.m_SingleParticipant &&
                                (status == XtnStatus.PREPARED ||
                                        status == XtnStatus.PREPARING))))
                    return;

                if (!xtnEntry.m_SingleParticipant && xtnEntry.m_AlreadyPrepared)
                    getTransactionHandler().removeFromPrepared2PCXtns(xtnEntry.getServerTransaction());

                context.setOperationID(operationID);

                // Set from-gateway state on context
                context.setFromGateway(xtnEntry.isFromGateway());

                _cacheManager.rollback(context, xtnEntry, xtnEntry.m_AlreadyPrepared, xtnEntry.m_AnyUpdates, supportsTwoPhaseReplication);

                xtnEntry.m_Active = false;
                //call cache-manager pre-rollback method in order to restore
                // updated entries to original values
                if (xtnEntry.m_AnyUpdates)
                    handleUnderXtnUpdates(context, xtnEntry, false/*isCommitting*/);

                xtnEntry.m_CommitRollbackTimeStamp = SystemTime.timeMillis();


                long currentFifoXtn = TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN;
                if (fifoEntries) {
                    currentFifoXtn = _cacheManager.getLatestTTransactionTerminationNum();
                    //update the new number in the transaction object
                    currentFifoXtn++;
                    if (currentFifoXtn < 0)
                        currentFifoXtn = 1;
                    _cacheManager.setFifoXtnNumber(xtnEntry, currentFifoXtn);
                    //mark the locked entries with the new fifo xtn number
                    _coreProcessor.handleLockedFifoEntriesBeforeXtnEnd(context, xtnEntry, true /*fromRollback*/);
                }

                xtnEntry.setStatus(XtnStatus.ROLLING);

                //fifo group op performed under this xtn
                if (xtnEntry.getXtnData().anyFifoGroupOperations())
                    _cacheManager.handleFifoGroupsCacheOnXtnEnd(context, xtnEntry);
                _fifoGroupsHandler.prepareForFifoGroupsAfterXtnScans(context, xtnEntry);

                //handle new entries under xtn-remove from cache
                boolean new_entries_deleted = false;
                if (!xtnEntry.m_AlreadyPrepared) {
                    _coreProcessor.handleNewRolledbackEntries(context, xtnEntry);
                    new_entries_deleted = true;
                }

                xtnEntry.setStatus(XtnStatus.ROLLED);
                //handle fifo entries locked under the xtn. This is done not in
                //background in order to enable fifo serialization per client thread

                //set latest fifo xtn number if relevant
                if (fifoEntries && currentFifoXtn != TerminatingFifoXtnsInfo.UNKNOWN_FIFO_XTN)
                    _cacheManager.setLatestTransactionTerminationNum(currentFifoXtn);

                if (fifoEntries)
                    _coreProcessor.handleLockedFifoEntriesOnXtnEnd(context, xtnEntry, true /*fromRollback*/);
                //handle new entries under xtn-remove from cache
                if (!new_entries_deleted)
                    _coreProcessor.handleNewRolledbackEntries(context, xtnEntry);

            } finally {
                getTransactionHandler().unlockXtnOnXtnEnd(xtnEntryLocked, lockedXtnTable);
            }


            if (getTransactionHandler().isLightTransaction(xtnEntry)) {
                try {
                    _coreProcessor.handleRollbackSA(new RollbackBusPacket(xtnEntry));
                } catch (Exception ex) {
                }//inner routines log errors
            } else {
                _processorWG.enqueueBlocked(new RollbackBusPacket(xtnEntry));
            }
        } catch (SAException ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error(ex.toString(), ex);
            }
            JSpaceUtilities.throwEngineInternalSpaceException(ex.getMessage(), ex);
        } finally {
            _cacheManager.freeCacheContext(context);
        }
    }

    /**
     * @param st
     * @return
     */
    public XtnEntry getTransaction(ServerTransaction st) {
        XtnEntry xtnEntry = getTransactionHandler().getXtnTable().get(st);

        if (xtnEntry != null) {
            ServerTransaction internalTx = xtnEntry.getXtnData().getXtn();

            if (internalTx != null)
                internalTx.setMetaData(st.getMetaData());
        }
        return xtnEntry;
    }


    //	++++++++++++++++++local xtn manager lease apis +++++++++++++++

    /**
     * cancel local xtn lease.
     */
    void cancelLocalXtn(TransactionManager mgr, ServerTransaction st)
            throws UnknownLeaseException {

        if (!(mgr instanceof LocalTransactionManagerImpl))
            throw new RuntimeException("cancel local xtn lease: invalid transaction manager");

        LocalTransactionManagerImpl ltx = (LocalTransactionManagerImpl) mgr;
        //call abort form this xtn
        try {
            abortSA(mgr, st, true /*from cancel-lease*/, false/*verifyExpiredXtn*/, false, null/* operationID */);
        } catch (UnknownTransactionException ex) {
            throw new UnknownLeaseException("cancel:unknown transaction at space-server transaction-id= " + st + " manager=" + ltx.getManagerID());
        }
    }


    /**
     * renew local xtn lease.
     */
    void renewXtn(ServerTransaction st, long time)
            throws LeaseDeniedException, UnknownLeaseException {
        XtnEntry xtnEntry = getTransaction(st);
        if (xtnEntry == null) {
            //this can be the result of unused xtn just cleaned and a renew
            //comming from mgr
            //in case of embedded mahalo it can also be a renew bypassing the operation
            //so- insert a record to the timed map
            if (time <= 0 || time == Lease.FOREVER || time == Lease.ANY)
                getTransactionHandler().getTimedXtns().remove(st); //no time limit
            else
                //insert a record to time limit table
                getTransactionHandler().renewTransactionLease(st, time);

            return;
        }
        //is this xtn active ?
        xtnEntry.lock();

        boolean decrementUsed = true;
        try {
            if (!xtnEntry.m_Active) {
                decrementUsed = false;
                throw new LeaseDeniedException(
                        "renew:transaction not active any more at space-server transaction= " + st);
            }
            if (!xtnEntry.addUsedIfPossible())
                decrementUsed = false;

            if (time <= 0 || time == Lease.FOREVER || time == Lease.ANY)
                getTransactionHandler().getTimedXtns().remove(st); //no time limit
            else
                //insert a record to time limit table
                getTransactionHandler().renewTransactionLease(st, time);

            //xtn is alive- just put a new time if need to

        } finally {
            if (decrementUsed)
                xtnEntry.decrementUsed();

            xtnEntry.unlock();
        }
    }

    //	--------------local xtn manager lease apis -------------------

    /**
     * handle under xtn updates- restore in case of RB or remove shadow in case of commit.
     */
    private void handleUnderXtnUpdates(Context context, XtnEntry xtnEntry, boolean isCommitting)
            throws SAException {
        ISAdapterIterator iter = null;
        ILockObject entryLock = null;
        final XtnData pXtn = xtnEntry.getXtnData();
        try {
            iter = _cacheManager.makeUnderXtnEntriesIter(context,
                    xtnEntry, SelectType.ALL_ENTRIES /*.LockedEntries*/);
            // iterate over iter
            if (iter == null)
                return;
            while (true) {
                IEntryHolder eh = (IEntryHolder) iter.next();
                if (eh == null)
                    break;

                try {
                    entryLock = _cacheManager.getLockManager().getLockObject(eh);
                    synchronized (entryLock) {
                        if (eh.isDeleted())
                            continue;
                        if (!_leaseManager.isNoReapUnderXtnLeases() && eh.isExpired() && !_leaseManager.isSlaveLeaseManagerForEntries() && !isExpiredEntryStayInSpace(eh))
                            continue;

                        if (eh.getWriteLockOwner() == null || eh.getWriteLockOwner() != xtnEntry)
                            continue;

                        if (eh.getWriteLockOperation() == SpaceOperations.READ ||
                                eh.getWriteLockOperation() == SpaceOperations.READ_IE)
                            continue;//exclusive r-lock, no-op


                        context.setOperationID(pXtn.getOperationID(eh.getUID()));
                        if (!isCommitting) {
                            //rolling back the xtn
                            if (eh.getWriteLockOperation() != SpaceOperations.UPDATE &&
                                    eh.getWriteLockOperation() != SpaceOperations.TAKE &&
                                    eh.getWriteLockOperation() != SpaceOperations.TAKE_IE)
                                continue;
                            if (eh.getXidOriginated() == xtnEntry)
                                continue;

                            if (eh.hasShadow(true /*safeEntry*/))
                                _cacheManager.handleUnderXtnUpdate(context, xtnEntry.m_Transaction, eh, isCommitting, pXtn);
                        }//                  if (!isCommitting)
                        else {// Committing)
                            if (eh.getWriteLockOperation() != SpaceOperations.WRITE && eh.hasShadow(true /*safeEntry*/))
                                _cacheManager.handleUnderXtnUpdate(context, xtnEntry.m_Transaction, eh, isCommitting, pXtn);
                        }
                    }//synchronized
                } finally {
                    if (entryLock != null) {
                        _cacheManager.getLockManager().freeLockObject(entryLock);
                        entryLock = null;
                    }
                }
            }//while

        } /* try */ finally {
            if (iter != null)
                iter.close();
        }
    }

    /**
     * Checks if there is a transaction conflict between the specified entry and the operation
     * implied by the specified template. templateTransaction is not null if template is under xtn
     * isShadow is true if the call is on behalf of a shadow entry
     *
     * @return an indication in case of a conflict, <code>null</code> otherwise.
     */
    private XtnConfilctCheckIndicators checkTransactionConflict(Context context, IEntryHolder entry, ITemplateHolder template, boolean isShadow) {
        XtnEntry xtnEntry;

        if ((template.getTemplateOperation() == SpaceOperations.READ || template.getTemplateOperation() == SpaceOperations.READ_IE)
                && !template.isExclusiveReadLockOperation()) {
            ITransactionalEntryData edata = context.isNonBlockingReadOp() ?
                    context.getLastRawMatchSnapshot() : entry.getTxnEntryData();

            xtnEntry = edata.getWriteLockOwner();
            if (xtnEntry == null)
                return XtnConfilctCheckIndicators.NO_CONFLICT;

            final XtnStatus entryWriteLockStatus = xtnEntry.getStatus();
            final int entryWriteLockOperation = edata.getWriteLockOperation();
            final boolean isDirtyRead = indicateDirtyRead(template);
            final boolean isReadCommitted = indicateReadCommitted(edata, template);

            if (template.getXidOriginatedTransaction() == null ||
                    !edata.getWriteLockTransaction().equals(template.getXidOriginatedTransaction())) // template is under Xtn
            {
                if (isDirtyRead)
                    return checkTransactionConflictDirtyRead(context, xtnEntry,
                            entryWriteLockStatus, entryWriteLockOperation, entry, edata, isShadow);

                if (isReadCommitted)
                    return checkTransactionConflictReadCommitted(context, xtnEntry,
                            entryWriteLockStatus, entryWriteLockOperation, entry, edata, isShadow);

                return checkTransactionConflict(xtnEntry, entryWriteLockStatus, entryWriteLockOperation);
            }
            // entry.m_WriteLockOwner.equals(template.m_XidOriginated)
            if (entryWriteLockOperation == SpaceOperations.TAKE ||
                    entryWriteLockOperation == SpaceOperations.TAKE_IE)
                return XtnConfilctCheckIndicators.DELETED_BY_OWN_XTN;
            if (isReadCommitted && isShadow)
                return XtnConfilctCheckIndicators.XTN_CONFLICT;//  read-committed under same xtn- only master counts

            return XtnConfilctCheckIndicators.NO_CONFLICT;
        }//			if ((template.m_TemplateOperation == SpaceOperations.READ ||template.m_TemplateOperation == SpaceOperations.READ)


        if ((template.getTemplateOperation() == SpaceOperations.TAKE_IE || template.getTemplateOperation() == SpaceOperations.TAKE)) {

            List<XtnEntry> readWriteLock = entry.getReadLockOwners();

            if (entry.getWriteLockTransaction() == null &&
                    (readWriteLock == null || readWriteLock.isEmpty()))
                return XtnConfilctCheckIndicators.NO_CONFLICT;

            // check conflicting read
            if (readWriteLock != null && !readWriteLock.isEmpty()) {
                for (XtnEntry readLockOwner : readWriteLock) {
                    xtnEntry = readLockOwner;

                    if (xtnEntry != null) {
                        XtnStatus entryReadLockStatus = xtnEntry.getStatus();

                        if (entryReadLockStatus == XtnStatus.COMMITED ||
                                entryReadLockStatus == XtnStatus.COMMITING ||
                                (entryReadLockStatus == XtnStatus.PREPARED && xtnEntry.m_SingleParticipant) ||
                                entryReadLockStatus == XtnStatus.ROLLED
                                || (entryReadLockStatus == XtnStatus.ROLLING && !xtnEntry.m_AlreadyPrepared))
                            continue;
                        if (template.getXidOriginatedTransaction() == null || !readLockOwner.m_Transaction.equals(template.getXidOriginatedTransaction()))
                            return XtnConfilctCheckIndicators.XTN_CONFLICT; //new TransactionConflictException(readLockOwner.m_Transaction, template.getXidOriginatedTransaction());
                    }
                }
            }

            xtnEntry = entry.getWriteLockOwner();
            if (xtnEntry == null)
                return XtnConfilctCheckIndicators.NO_CONFLICT;

            XtnStatus entryWriteLockStatus = xtnEntry.getStatus();
            int entryWriteLockOperation = entry.getWriteLockOperation();

            if (template.getXidOriginatedTransaction() == null ||
                    !entry.getWriteLockTransaction().equals(template.getXidOriginatedTransaction())) // template is under Xtn
            {
                return checkTransactionConflict(xtnEntry, entryWriteLockStatus, entryWriteLockOperation);
            }
            // entry.m_WriteLockOwner.equals(template.m_XidOriginated)
            if (entryWriteLockOperation == SpaceOperations.TAKE ||
                    entryWriteLockOperation == SpaceOperations.TAKE_IE)
                return XtnConfilctCheckIndicators.DELETED_BY_OWN_XTN;


            return XtnConfilctCheckIndicators.NO_CONFLICT;
        }//			if ((template.m_TemplateOperation == SpaceOperations.TAKE_IE ||template.m_TemplateOperation == SpaceOperations.TAKE))

        if (template.getTemplateOperation() == SpaceOperations.UPDATE || template.isExclusiveReadLockOperation()) {
            List<XtnEntry> rwLock = entry.getReadLockOwners();

            if (entry.getWriteLockTransaction() == null && (rwLock == null || rwLock.isEmpty()))
                return XtnConfilctCheckIndicators.NO_CONFLICT;

            // check conflicting read
            if (rwLock != null && !rwLock.isEmpty()) {
                for (XtnEntry readLockOwner : rwLock) {
                    xtnEntry = readLockOwner;

                    if (xtnEntry != null) {
                        XtnStatus entryReadLockStatus = xtnEntry.getStatus();

                        if (entryReadLockStatus == XtnStatus.COMMITED ||
                                entryReadLockStatus == XtnStatus.COMMITING ||
                                (entryReadLockStatus == XtnStatus.PREPARED && xtnEntry.m_SingleParticipant) ||
                                entryReadLockStatus == XtnStatus.ROLLED || (entryReadLockStatus == XtnStatus.ROLLING && !xtnEntry.m_AlreadyPrepared))
                            continue;

                        if (template.getXidOriginatedTransaction() == null || !readLockOwner.m_Transaction.equals(template.getXidOriginatedTransaction()))
                            return XtnConfilctCheckIndicators.XTN_CONFLICT;//new TransactionConflictException(readLockOwner.m_Transaction, template.getXidOriginatedTransaction());
                    }
                }
            }

            xtnEntry = entry.getWriteLockOwner();
            if (xtnEntry == null)
                return XtnConfilctCheckIndicators.NO_CONFLICT;

            XtnStatus entryWriteLockStatus = xtnEntry.getStatus();
            int entryWriteLockOperation = entry.getWriteLockOperation();

            if (template.getXidOriginatedTransaction() == null ||
                    !entry.getWriteLockTransaction().equals(template.getXidOriginatedTransaction())) {
                return checkTransactionConflict(xtnEntry, entryWriteLockStatus, entryWriteLockOperation);
            }
            // entry.m_WriteLockOwner.equals(template.m_XidOriginated)
            if (entryWriteLockOperation == SpaceOperations.TAKE ||
                    entryWriteLockOperation == SpaceOperations.TAKE_IE)
                //for update-or-write for a taken entry under same xtn
                return UpdateModifiers.isUpdateOrWrite(template.getOperationModifiers()) ? XtnConfilctCheckIndicators.NO_CONFLICT : XtnConfilctCheckIndicators.DELETED_BY_OWN_XTN;

            //fifo groups- allow traversing the group with same xtn in case of repetitive read-exclusive
            if (template.isFifoGroupPoll() && xtnEntry == template.getXidOriginated() && template.isExclusiveReadLockOperation())
                return XtnConfilctCheckIndicators.DELETED_BY_OWN_XTN;


            return XtnConfilctCheckIndicators.NO_CONFLICT;
        } //			if (template.m_TemplateOperation == SpaceOperations.UPDATE)

        return XtnConfilctCheckIndicators.NO_CONFLICT; /* not reached - keep compiler happy. */
    }

    private XtnConfilctCheckIndicators checkTransactionConflict(XtnEntry xtnEntry,
                                                                XtnStatus entryWriteLockStatus, int entryWriteLockOperation) {
        if ((entryWriteLockOperation == SpaceOperations.TAKE ||
                entryWriteLockOperation == SpaceOperations.TAKE_IE) &&
                (entryWriteLockStatus == XtnStatus.ROLLED ||
                        (entryWriteLockStatus == XtnStatus.ROLLING && !xtnEntry.m_AlreadyPrepared)))
            return XtnConfilctCheckIndicators.NO_CONFLICT;


        if ((entryWriteLockOperation == SpaceOperations.TAKE ||
                entryWriteLockOperation == SpaceOperations.TAKE_IE) &&
                (entryWriteLockStatus == XtnStatus.COMMITED ||
                        entryWriteLockStatus == XtnStatus.COMMITING ||
                        ((entryWriteLockStatus == XtnStatus.PREPARED) && xtnEntry.m_SingleParticipant)))
            return XtnConfilctCheckIndicators.ENTRY_DELETED;

        if (entryWriteLockOperation == SpaceOperations.WRITE &&
                (entryWriteLockStatus == XtnStatus.COMMITED ||
                        entryWriteLockStatus == XtnStatus.COMMITING ||
                        ((entryWriteLockStatus == XtnStatus.PREPARED) && xtnEntry.m_SingleParticipant)))
            return XtnConfilctCheckIndicators.NO_CONFLICT;
        if ((entryWriteLockOperation == SpaceOperations.UPDATE ||
                entryWriteLockOperation == SpaceOperations.READ || entryWriteLockOperation == SpaceOperations.READ_IE) &&
                (entryWriteLockStatus == XtnStatus.COMMITED ||
                        entryWriteLockStatus == XtnStatus.COMMITING ||
                        ((entryWriteLockStatus == XtnStatus.PREPARED) && xtnEntry.m_SingleParticipant)))
            return XtnConfilctCheckIndicators.NO_CONFLICT;
        if ((entryWriteLockOperation == SpaceOperations.UPDATE ||
                entryWriteLockOperation == SpaceOperations.READ || entryWriteLockOperation == SpaceOperations.READ_IE) &&
                (entryWriteLockStatus == XtnStatus.ROLLED ||
                        (entryWriteLockStatus == XtnStatus.ROLLING && !xtnEntry.m_AlreadyPrepared)))
            return XtnConfilctCheckIndicators.NO_CONFLICT;

        return XtnConfilctCheckIndicators.XTN_CONFLICT;
    }

    //conflict check for read-committed  when the template xtn != entry writelock xtn
    private XtnConfilctCheckIndicators checkTransactionConflictReadCommitted(Context context, XtnEntry xtnEntry,
                                                                             XtnStatus entryWriteLockStatus, int entryWriteLockOperation, IEntryHolder entry, ITransactionalEntryData edata, boolean isShadow) {
        if (isShadow)
            return XtnConfilctCheckIndicators.NO_CONFLICT;  //read committed

        if (!context.isNonBlockingReadOp()) {
            if (entry.hasShadow(true /*safeEntry*/))
                return XtnConfilctCheckIndicators.XTN_CONFLICT;//  read-committed only for shadow
        } else {//nonblocking read and we have shadow
            if (edata.getOtherUpdateUnderXtnEntry() != null)
                return XtnConfilctCheckIndicators.XTN_CONFLICT;//  read-committed only for shadow
        }
        if ((entryWriteLockOperation == SpaceOperations.TAKE ||
                entryWriteLockOperation == SpaceOperations.TAKE_IE) &&
                (entryWriteLockStatus == XtnStatus.COMMITED ||
                        entryWriteLockStatus == XtnStatus.COMMITING ||
                        ((entryWriteLockStatus == XtnStatus.PREPARED) && xtnEntry.m_SingleParticipant))) {
            if (context.isNonBlockingReadOp()) {//if NBR verify that its not a take + write under same xtn
                ITransactionalEntryData curedata = entry.getTxnEntryData();
                if (curedata.getWriteLockOwner() == null && !entry.isDeleted())
                    return XtnConfilctCheckIndicators.NO_CONFLICT;  //read committed
                if (curedata.getWriteLockOwner() != null && ((curedata.getWriteLockOwner() != edata.getWriteLockOwner()) ||
                        (curedata.getWriteLockOperation() != entryWriteLockOperation)))
                    return XtnConfilctCheckIndicators.NO_CONFLICT;  //read committed

            }
            return XtnConfilctCheckIndicators.ENTRY_DELETED;
        }
        if (entryWriteLockOperation == SpaceOperations.WRITE &&
                (xtnEntry.m_Active ||
                        xtnEntry.getStatus() == XtnStatus.PREPARING ||
                        (xtnEntry.getStatus() == XtnStatus.PREPARED && !xtnEntry.m_SingleParticipant)))
            return XtnConfilctCheckIndicators.XTN_CONFLICT;
        if (entryWriteLockOperation == SpaceOperations.WRITE &&
                (entryWriteLockStatus == XtnStatus.ROLLED ||
                        (entryWriteLockStatus == XtnStatus.ROLLING && !xtnEntry.m_AlreadyPrepared)))
            return XtnConfilctCheckIndicators.ENTRY_DELETED;


        return XtnConfilctCheckIndicators.NO_CONFLICT;  //read committed
    }


    //conflict check for dirty-read when the template xtn != entry writelock xtn
    private XtnConfilctCheckIndicators checkTransactionConflictDirtyRead(Context context, XtnEntry xtnEntry,
                                                                         XtnStatus entryWriteLockStatus, int entryWriteLockOperation, IEntryHolder entry, ITransactionalEntryData edata, boolean isShadow) {
        if (isShadow)
            return XtnConfilctCheckIndicators.XTN_CONFLICT;// dirty read unrelevant here

        if ((entryWriteLockOperation == SpaceOperations.TAKE ||
                entryWriteLockOperation == SpaceOperations.TAKE_IE) &&
                (entryWriteLockStatus == XtnStatus.ROLLED ||
                        (entryWriteLockStatus == XtnStatus.ROLLING && !xtnEntry.m_AlreadyPrepared)))
            return XtnConfilctCheckIndicators.NO_CONFLICT;

        if (entryWriteLockOperation == SpaceOperations.TAKE ||
                entryWriteLockOperation == SpaceOperations.TAKE_IE)
            return XtnConfilctCheckIndicators.XTN_CONFLICT;// take ? dirty read is blocked

        return XtnConfilctCheckIndicators.NO_CONFLICT;  // dirty read, allow xtn conflicts
    }

    public void aggregate(ITemplatePacket queryPacket, List<SpaceEntriesAggregator> aggregators, int readModifiers,
                          SpaceContext sc)
            throws Exception {
        if (Modifiers.contains(readModifiers, Modifiers.EXPLAIN_PLAN)) {
            throw new UnsupportedOperationException("Sql explain plan is not supported for aggregation");
        }
        BatchQueryOperationContext batchContext = new AggregateOperationContext(queryPacket, Integer.MAX_VALUE, 1);
        AnswerHolder ah = readMultiple(queryPacket,
                null /*txn*/,
                0 /*timeout*/,
                false, /*ifExists*/
                false, /*take*/
                sc,
                false, /*returnOnlyUid*/
                readModifiers,
                batchContext,
                aggregators,
                null);

        if (ah != null && ah.getException() != null) {
            throw ah.getException();
        }

    }

    public int countIncomingConnections() throws RemoteException {
        if (isLocalCache())
            return 0;
        IRemoteSpace dynamicProxy = ((LRMISpaceImpl) _spaceImpl.getSpaceStub()).getDynamicProxy();
        long remoteObjID = TransportProtocolHelper.getRemoteObjID(dynamicProxy);
        return remoteObjID != 0 ? LRMIRuntime.getRuntime().countRemoteObjectConnections(remoteObjID) : 0;
    }

    public ServerIteratorsManager getIteratorsManager() {
        return _serverIteratorsManager;
    }

    public MetricManager getMetricManager() {
        return _metricManager;
    }

    @Override
    public void afterClusterInfoChange(SpaceClusterInfo clusterInfo) {
        this._clusterInfo = clusterInfo;
    }

    /**
     * XTN CONFLICT CHECK INDICATOR.
     */
    private static enum XtnConfilctCheckIndicators {
        NO_CONFLICT, XTN_CONFLICT, ENTRY_DELETED, DELETED_BY_OWN_XTN
    }

    /**
     * check authority to get admin.
     */
    public void getAdmin(SpaceContext sc) throws RemoteException {
        try {
            _filterManager.invokeFilters(FilterOperationCodes.BEFORE_GETADMIN, sc, (IEntryHolder) null);
        } catch (RuntimeException e1) {
            throw e1;
        } catch (Exception e2) {
            throw new RemoteException(e2.getMessage(), e2);
        }

    }

    public ClusterPolicy getClusterPolicy() {
        return _clusterPolicy;
    }

    /**
     * return the replication status of all my replication group returns an array of remote member
     * names and an array of replication status to them.
     */
    Object[] getReplicationStatus() {
        monitorMemoryUsage(false);
        if (_clusterPolicy == null)
            throw new RuntimeException("changeReplicationState- space is not clustered.");
        if (!isReplicated())
            throw new RuntimeException("changeReplicationState- space is not a member of any replication group.");

        return getReplicationNode().getAdmin().getStatus();
    }

    /**
     * Invokes the "before-authentication" filter.
     */
    public void beforeAuthentication(com.gigaspaces.security.service.SecurityContext securityContext)
            throws RemoteException {
        try {
            if (_filterManager._isFilter[FilterOperationCodes.BEFORE_AUTHENTICATION]) {
                SpaceContext sc = new SpaceContext(securityContext);
                _filterManager.invokeFilters(FilterOperationCodes.BEFORE_AUTHENTICATION, sc, null);
            }
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RemoteException("Exception occurred invoking before-authentication filter", e);
        }
    }


    @SuppressWarnings("deprecation")
    public ISpaceCopyReplicaState spaceCopyReplica(SpaceURL sourceRemoteUrl, String sourceMemberName,
                                                   IRemoteSpace remoteJSpace, ITemplatePacket templPacket, boolean includeNotifyTemplates,
                                                   int chunkSize, SpaceContext sc, SpaceContext remoteSpaceContext) {
        if (remoteJSpace == null)
            throw new EngineInternalSpaceException("Remote space for : " + sourceRemoteUrl + " can not be null!");

        if ((_filterManager._isFilter[FilterOperationCodes.BEFORE_WRITE])) {
            _filterManager.invokeFilters(FilterOperationCodes.BEFORE_WRITE, sc, templPacket);
        }

        int concurrentConsumers = 1;
        if (getClusterPolicy() != null && getClusterPolicy().getReplicationPolicy() != null)
            concurrentConsumers = getClusterPolicy().getReplicationPolicy().getRecoveryThreadPoolSize();

        long recoveryStartTime = SystemTime.timeMillis();
        ISpaceCopyReplicaRequestContext context = createReplicateSpaceContext(sourceRemoteUrl,
                chunkSize,
                concurrentConsumers,
                templPacket,
                includeNotifyTemplates,
                false,
                remoteSpaceContext,
                ReplicaType.COPY);
        ISpaceCopyReplicaState spaceCopyReplica = getReplicationNode().spaceCopyReplicaRequest(context);

        try {
            waitForCopyResultAndLogStatus(sourceRemoteUrl,
                    recoveryStartTime,
                    spaceCopyReplica,
                    false);
        } catch (InterruptedException ex) {
            if (_logger.isWarnEnabled()) {
                _logger.warn("", ex);
            }
            return FailedSyncSpaceReplicateState.createFailedSyncState(ex);
        }
        return spaceCopyReplica;
    }

    private void waitForCopyResultAndLogStatus(SpaceURL sourceRemoteUrl, long recoveryStartTime, ISpaceCopyReplicaState spaceCopyReplica, boolean spaceSyncOperation)
            throws InterruptedException {
        ISpaceCopyResult result = spaceCopyReplica.waitForCopyResult();
        if(!_spaceImpl.isPrimary()){
            ((ReplicationNode) _replicationManager.getReplicationNode()).getReplicaHandler().clearFifoBatchesHandler();
        }
        LogLevel level = result.isSuccessful() ? LogLevel.INFO : LogLevel.WARNING;
        Throwable error = result.isSuccessful() ? null : result.getFailureReason();
        if (level.isEnabled(_logger))
            level.log(_logger, result.getStringDescription(sourceRemoteUrl.getMemberName(), sourceRemoteUrl.toString(), _fullSpaceName, spaceSyncOperation, SystemTime.timeMillis() - recoveryStartTime), error);
    }

    public String generateGroupName() {
        if (!isReplicated())
            return null;

        int staticPartitionNumber = getPartitionIdOneBased();
        if (staticPartitionNumber == 0)
            staticPartitionNumber = 1;
        return ReplicationNodeConfigBuilder.getInstance().getSynchronizeGroupName(_clusterInfo, staticPartitionNumber, _fullSpaceName);
    }

    @SuppressWarnings("deprecation")
    public ISpaceSynchronizeReplicaState spaceSynchronizeReplica(SpaceURL sourceRemoteUrl,
                                                                 int chunkSize, boolean transientOnly, boolean memoryOnly) {
        long recoveryStartTime = SystemTime.timeMillis();
        String groupName = generateGroupName();

        /** get remote space object */
        long findTimeout = 0;
        int concurrentConsumers = 1;

        /** get find timeout from replication or fail-over policy */
        if (getClusterPolicy().getReplicationPolicy() != null) {
            findTimeout = getClusterPolicy().getReplicationPolicy().m_SpaceFinderTimeout;
            concurrentConsumers = getClusterPolicy().getReplicationPolicy().getRecoveryThreadPoolSize();
        } else if (getClusterPolicy().m_FailOverPolicy != null)
            findTimeout = getClusterPolicy().m_FailOverPolicy.spaceFinderTimeout;

        sourceRemoteUrl.setProperty(SpaceURL.TIMEOUT,
                String.valueOf(findTimeout));

        //if relevant mark the backup as inconsistent utill all data is recovered
        if (getCacheManager().isEmptyAfterInitialLoadStage() && getSpaceImpl().isBackup() && getSpaceImpl().getDirectPersistencyRecoveryHelper() != null) {
            if (_logger.isInfoEnabled()) {
                _logger.info("[" + getFullSpaceName() + "] setting storage state of backup to Inconsistent before recovery from primary");
            }
            getSpaceImpl().getDirectPersistencyRecoveryHelper().setStorageState(StorageConsistencyModes.Inconsistent);
        }

        boolean directPersistencySyncListRecovery = getReplicationNode().getDirectPesistencySyncHandler() != null
                && !getCacheManager().isEmptyAfterInitialLoadStage();

        DirectPersistencySyncListFetcher directPersistencySyncListFetcher = null;
        if (directPersistencySyncListRecovery) {
            try {
                directPersistencySyncListFetcher = new DirectPersistencySyncListFetcher(_spaceImpl.getSpaceStub());
                Iterator<String> entriesForRecovery = getReplicationNode().getDirectPesistencySyncHandler().getEntriesForRecovery();
                DirectPersistencyBackupSyncIteratorHandler directPersistencyBackupSyncIteratorHandler = new DirectPersistencyBackupSyncIteratorHandler(entriesForRecovery);
                getReplicationNode().setDirectPersistencyBackupSyncIteratorHandler(
                        new DirectPersistencyBackupSyncIteratorHandler(entriesForRecovery));
                if (_logger.isInfoEnabled()) {
                    _logger.info(getSpaceName() + " performing direct persistency sync list recovery, batch size is: "
                            + directPersistencyBackupSyncIteratorHandler.getBatchSize());
                }
            } catch (RemoteException e) {
                return FailedSyncSpaceReplicateState.createFailedSyncState(e);
            }
        }

        ISpaceSynchronizeReplicaRequestContext context = createReplicateSpaceContext(sourceRemoteUrl,
                null,
                true,
                transientOnly,
                memoryOnly,
                null,
                ReplicaType.SYNCRONIZE,
                groupName,
                chunkSize,
                concurrentConsumers,
                getCacheManager().isBlobStoreCachePolicy(),
                directPersistencySyncListRecovery ? directPersistencySyncListFetcher : null);

        ISpaceSynchronizeReplicaState spaceSynchronizeReplicaRequest = getReplicationNode().spaceSynchronizeReplicaRequest(context);

        try {
            waitForCopyResultAndLogStatus(sourceRemoteUrl,
                    recoveryStartTime,
                    spaceSynchronizeReplicaRequest,
                    true);
            return spaceSynchronizeReplicaRequest;

        } catch (InterruptedException ex) {
            if (_logger.isWarnEnabled()) {
                _logger.warn("", ex);
            }
            return FailedSyncSpaceReplicateState.createFailedSyncState(ex);
        }
    }

    @SuppressWarnings("deprecation")
    /**
     * Starts a space copy replica request that includes only the broadcast notify templates.
     * Used during space recovery to recover the broadcast notify templates from other partition.
     * @return space copy replica state representing the current progress of the copy operation
     */
    public ISpaceCopyReplicaState spaceBroadcastNotifyTemplatesReplica(SpaceURL sourceRemoteUrl,
                                                                       int chunkSize, boolean transientOnly) throws InterruptedException {
        long recoveryStartTime = SystemTime.timeMillis();
        ISpaceCopyReplicaRequestContext context = createReplicateSpaceContext(sourceRemoteUrl,
                chunkSize,
                1,
                null,
                true,
                transientOnly,
                null,
                ReplicaType.BROADCAST_NOTIFY_TEMPLATES_COPY);

        ISpaceCopyReplicaState spaceCopyReplicaRequest = getReplicationNode().spaceCopyReplicaRequest(context);

        waitForCopyResultAndLogStatus(sourceRemoteUrl,
                recoveryStartTime,
                spaceCopyReplicaRequest,
                true);

        return spaceCopyReplicaRequest;
    }

    public void rollbackCopyReplica(ISpaceCopyResult replicaResult) {
        getReplicationNode().rollbackCopyReplica(replicaResult);

    }

    private ISpaceSynchronizeReplicaRequestContext createReplicateSpaceContext(
            SpaceURL sourceRemoteUrl, int chunkSize,
            int concurrentConsumers, ITemplatePacket template, boolean includeNotifyTemplate, boolean transientOnly,
            SpaceContext remoteSpaceContent, ReplicaType replicaType) {
        return createReplicateSpaceContext(sourceRemoteUrl,
                template,
                includeNotifyTemplate,
                transientOnly,
                transientOnly/*memory only*/,
                remoteSpaceContent,
                replicaType,
                null,
                chunkSize, concurrentConsumers);
    }

    private ISpaceSynchronizeReplicaRequestContext createReplicateSpaceContext(
            SpaceURL sourceRemoteUrl,
            ITemplatePacket template, boolean includeNotifyTemplate, boolean transientOnly,
            boolean memoryOnly, SpaceContext remoteSpaceContent,
            ReplicaType replicaType, String synchronizeGroupName, int fetchBatchSize, int concurrentConsumers) {
        return createReplicateSpaceContext(sourceRemoteUrl,
                template, includeNotifyTemplate, transientOnly, memoryOnly, remoteSpaceContent,
                replicaType, synchronizeGroupName, fetchBatchSize, concurrentConsumers, false);
    }

    private ISpaceSynchronizeReplicaRequestContext createReplicateSpaceContext(
            SpaceURL sourceRemoteUrl,
            ITemplatePacket template, boolean includeNotifyTemplate, boolean transientOnly,
            boolean memoryOnly, SpaceContext remoteSpaceContent,
            ReplicaType replicaType, String synchronizeGroupName, int fetchBatchSize, int concurrentConsumers, boolean isBlobstore) {
        return createReplicateSpaceContext(sourceRemoteUrl,
                template, includeNotifyTemplate, transientOnly, memoryOnly, remoteSpaceContent,
                replicaType, synchronizeGroupName, fetchBatchSize, concurrentConsumers, isBlobstore, null);
    }

    private ISpaceSynchronizeReplicaRequestContext createReplicateSpaceContext(
            SpaceURL sourceRemoteUrl,
            ITemplatePacket template, boolean includeNotifyTemplate, boolean transientOnly,
            boolean memoryOnly, SpaceContext remoteSpaceContent,
            ReplicaType replicaType, String synchronizeGroupName, int fetchBatchSize, int concurrentConsumers, boolean isBlobstore, DirectPersistencySyncListFetcher fetcher) {
        SpaceSynchronizeReplicaRequestContext context = new SpaceSynchronizeReplicaRequestContext();
        context.setOriginUrl(sourceRemoteUrl);
        context.setFetchBatchSize(fetchBatchSize);
        context.setConcurrentConsumers(concurrentConsumers);
        // replication progress timeout should be larger in blobstore mode
        if (isBlobstore && Long.getLong("com.gs.replication.replicaProgressTimeout") == null) {
            context.setProgressTimeout(context.getProgressTimeout() * 3);
        }
        //set parameters for the target space
        SpaceCopyReplicaParameters parameters = new SpaceCopyReplicaParameters();
        parameters.setCopyNotifyTemplates(includeNotifyTemplate);
        parameters.addTemplatePacket(template);
        parameters.setTransient(transientOnly);
        parameters.setMemoryOnly(memoryOnly);
        parameters.setIncludeEvictionReplicationMarkers(_cacheManager.requiresEvictionReplicationProtection());
        parameters.setReplicaType(replicaType);
        parameters.setSpaceContext(remoteSpaceContent);
        if (fetcher != null) {
            parameters.setSynchronizationListFetcher(fetcher);
        }
        context.setParameters(parameters);

        context.setSynchronizeGroupName(synchronizeGroupName);
        return context;
    }

    /**
     * get the basic class info from the space directory.
     */
    public ITypeDesc getClassTypeInfo(String className) {
        return getClassTypeInfo(className, false);
    }

    public ITypeDesc getClassTypeInfo(String className, boolean asVirtualType ) {
        if (className == null)
            throw new RuntimeException("getClassTypeInfo : invalid class name (null)");

        final IServerTypeDesc severTTE = _typeManager.getServerTypeDesc(className);
        if (severTTE != null) {
            ITypeDesc iTypeDesc = severTTE.getTypeDesc();
            if( asVirtualType && iTypeDesc instanceof TypeDesc ) {
                TypeDesc typeDesc = ( TypeDesc )iTypeDesc;
                return typeDesc.cloneWithoutObjectClass( typeDesc, EntryType.DOCUMENT_JAVA );
            }
            return iTypeDesc;
        }

        return null;
    }

    /**
     * returns true if the persistent space is empty.
     */
    private boolean isEmptyPersistent()
            throws SAException {
        Context context = null;
        try {
            if (!_cacheManager.getTemplatesManager().isEmpty())
                return false;  // templates in space

            ITemplateHolder tHolder = TemplateHolderFactory.createEmptyTemplateHolder(this,
                    _uidFactory.createUIDFromCounter(),
                    Long.MAX_VALUE /* expiration time*/,
                    false /*isFifo*/);

            IServerTypeDesc templateType = _typeManager.getServerTypeDesc(IServerTypeDesc.ROOT_TYPE_NAME);
            context = _cacheManager.getCacheContext();

            // iterator for entries
            ISAdapterIterator<IEntryHolder> entriesIterSA = _cacheManager.makeEntriesIter(
                    context, tHolder, templateType, 0, SystemTime.timeMillis(), false /*memoryonly*/);

            if (entriesIterSA == null)
                return true;
            IEntryHolder eh = entriesIterSA.next();
            entriesIterSA.close();
            return (eh == null);

        } // try
        finally {
            _cacheManager.freeCacheContext(context);
        }
    }


    /**
     * is sync-replication enabled for at least one member in a clustered space.
     **/
    final public boolean isSyncReplicationEnabled() {
        return _isSyncReplication;
    }

    /**
     * @return <code>true</code> if this space serves as MirrorService to receive async bulk
     * operations to be saved into persistent cache adaptor.
     **/
    final public boolean isMirrorService() {
        return _replicationManager.isMirrorService();
    }

    public boolean isTransactionalSA() {
        return false;
    }

    /**
     * If chunk-size of multiple operations reached, perform synchronize replication, otherwise do
     * nothing. In most cases this method will be invoked from multiple operations like clear with
     * null template or writeMultiple takeMultiple and updateMultiple. This should avoid OutOfMemory
     * in source space.
     *
     * NOTE: if multiple operation-chunk-size defined -1, do nothing. This method assume that will
     * be invoked only when sync-repl enabled.
     */
    private void performReplIfChunkReached(Context context) {
        if (isReplicated()) {
            final int maxMultipleOperChunkSize = _clusterPolicy.m_ReplicationPolicy.m_SyncReplPolicy.getMultipleOperationChunkSize();
            IReplicationOutContext replicationContext = context.getReplicationContext();
            if (maxMultipleOperChunkSize > 0 && replicationContext != null && replicationContext.pendingSize() >= maxMultipleOperChunkSize)
                performReplication(context);
        }
    }

    /**
     * perform  sync replication from sync-packets list or a single sync-packet contained in the
     * context.
     */
    public int performReplication(Context context) {
        if (context.isActiveBlobStoreBulk() && _cacheManager.isDirectPersistencyEmbeddedtHandlerUsed() && !context.isFromReplication()) {
            //if off-heap in case of bulk we first need to flush to the blob-store
            context.getBlobStoreBulkInfo().bulk_flush(context, false/*only_if_chunk_reached*/, false);
        }

        final IReplicationOutContext replicationContext = context.getReplicationContext();
        if (replicationContext != null) {
            return context.getReplicationContext().setCompleted(getReplicationNode().execute(replicationContext));
        }
        return 0;
    }

    /**
     * return true if entry belongs to the partition of the space, false if entry is not from space
     * partition
     *
     * @param entryHolder- to check
     * @return true if entry belong to space partition
     */
    public boolean isEntryFromPartition(IEntryHolder entryHolder) {
        final Object routingValue = entryHolder.getRoutingValue();
        if (routingValue == null)
            return false;

        if (!_clusterInfo.isPartitioned())
            return true;

        final int partitionId = PartitionedClusterUtils.getPartitionId(routingValue, _clusterInfo);
        return partitionId == _partitionId;
    }

    /**
     * return true if space is part of a partitioned cluster
     *
     * @return return true if space is part of a partition
     */
    public boolean isPartitionedSpace() {
        return _clusterInfo.isPartitioned();
    }

    /**
     * Replicate and free the cache context and log on error.
     *
     * @param context     context to replicate and free free
     * @param transaction server trnascation
     */
    private void replicateAndfreeCacheContextTxn(Context context, ServerTransaction transaction) {
        try {
            replicateAndfreeCache(context);
        } catch (RuntimeException ex) {
            if (_logger.isErrorEnabled()) {
                _logger.error("Failed to perform sync-replication on transactionId: " + transaction, ex);
            }
        }
    }

    /**
     * Replicate and free the cache context
     *
     * @param context context to replicate and free free
     */
    private int replicateAndfreeCache(Context context) {
        if (context != null) {
            try {
                /** perform sync-replication if need */
                return performReplication(context);
            } finally {
                _cacheManager.freeCacheContext(context);
            }
        }// if context != null
        return 0;
    }

    /**
     * true if external data-source is a central data-source (and not transient) arriving from
     * replication.
     *
     * @return <code>true</code> if external data-source is a central data-source (and not
     * transient); <code>false</code> otherwise.
     */
    private boolean isReplicatedFromCentralDB(Context ctx) {
        return _cacheManager.isCacheExternalDB() &&
                _cacheManager.isCentralDB() && ctx.isFromReplication();
    }

    public CacheManager getCacheManager() {
        return _cacheManager;
    }

    /**
     * why is entry removed.
     */
    public static enum EntryRemoveReasonCodes {
        TAKE, EVICT, LEASE_CANCEL, LEASE_EXPIRED
    }

    /**
     * why is entry removed.
     */
    public static enum TemplateRemoveReasonCodes {
        OPERATED_OR_TIMEDOUT, LEASE_CANCEL, LEASE_EXPIRED
    }

    public final MemoryManager getMemoryManager() {
        return _memoryManager;
    }

    /**
     * get the list of component handlers
     */
    public List<ISpaceComponentsHandler> getComponentsHandlers() {
        LinkedList<ISpaceComponentsHandler> handlers = new LinkedList<>();

        if (_filterManager != null)
            handlers.add(_filterManager);

        if (isReplicated())
            handlers.add(_replicationManager.getReplicationFilterManager());

        return handlers;
    }

    /**
     * @return the number of partitions in the static cluster.
     */
    public int getNumberOfPartitions() {
        return _clusterInfo.getNumberOfPartitions();
    }

    public PartitionRoutingInfo getPartitionRoutingInfo() {
        int partitions = getNumberOfPartitions();
        if (partitions == 0)
            return null;

        int partitionId = getPartitionIdOneBased();
        ClusterTopology topology = getClusterInfo().getTopology();
        int generation = topology != null ? topology.getGeneration() : 0;
        DynamicPartitionInfo dynamicPartitionInfo = topology != null
                ? new DynamicPartitionInfoImpl(topology.getPartitionChunks(partitionId))
                : null;

        return new PartitionRoutingInfoImpl(generation, partitions, partitionId, dynamicPartitionInfo);
    }

    /**
     * gets the number of this space in the partition. starting from 1. returns 0 in case the
     * cluster is not partitioned
     *
     * @return the number of Partition
     */
    public int getPartitionIdOneBased() {
        return _partitionId + 1;
    }

    /**
     * @return Gets the number of this space in the partition (zero based)
     */
    public int getPartitionIdZeroBased() {
        return _partitionId;
    }

    public String getSpaceName() {
        return _spaceName;
    }

    public String getFullSpaceName() {
        return _fullSpaceName;
    }

    public long getLastEntryTimestamp() {
        return _lastEntryTimestamp;
    }

    public void setLastEntryTimestamp(long timestamp) {
        this._lastEntryTimestamp = timestamp;
    }

    public void touchLastEntryTimestamp() {
        this._lastEntryTimestamp = SystemTime.timeMillis();
    }


    public boolean isColdStart() {
        return _coldStart;
    }


    public boolean isMemoryRecoveryEnabled() {
        return _memoryRecoveryEnabled;
    }

    /**
     * @return the transactionHandler
     */
    public TransactionHandler getTransactionHandler() {
        return _transactionHandler;
    }

    private ILockObject getTemplateLockObject(ITemplateHolder template) {
        return _cacheManager.getLockManager().getLockObject(template, false /*isEvictable*/);
    }

    private void freeTemplateLockObject(ILockObject lockObject) {
        _cacheManager.getLockManager().freeLockObject(lockObject);
    }

    /**
     * Returns a list of internal filters that should be used by the FilterManager
     */
    public List<FilterProvider> getInternalFilters() {
        List<FilterProvider> filters = new LinkedList<>();

        if (supportsGuaranteedNotifications())
            filters.add(new NotifyAcknowledgeFilter(this));
        return filters;
    }

    /**
     * Returns true if this cluster topology supports guaranteed notifications. It is supported only
     * for primary/backup topologies.
     */
    public boolean supportsGuaranteedNotifications() {
        return _clusterPolicy != null &&
                _clusterPolicy.isPrimaryElectionAvailable() &&
                EventSessionConfig.USE_OLD_GUARANTEED_NOTIFICATIONS;
    }


    /**
     * For local reads without xtn and timeout, used only in local cache, only with object reference
     * mode
     */
    public Object directLocalReadById(Object id, String typeName, QueryResultTypeInternal queryResultType) {
        if (!_isLocalCache)
            throw new EngineInternalSpaceException("Supported only in local cache");

        final IServerTypeDesc serverTypeDesc = _typeManager.getServerTypeDesc(typeName);
        if (serverTypeDesc == null)
            return null;

        Object res = null;
        boolean noUid = true;

        if (serverTypeDesc.isActive()) {
            if (serverTypeDesc.getTypeDesc().getIdPropertyName() == null)
                return null;

            if (serverTypeDesc.getTypeDesc().isAutoGenerateId())
                noUid = false;

            IEntryHolder entryHolder = noUid
                    ? _cacheManager.getEntryByIdFromPureCache(id, serverTypeDesc)
                    : directLocalReadByUidForClass((String) id, typeName);
            res = getUserObjectFromEntryHolder(entryHolder, queryResultType);
        }
        if (res != null || !noUid)
            return res;

        //try inheritance
        IServerTypeDesc[] subTypes = serverTypeDesc.getAssignableTypes();
        // Skip first subtype (which is actually this type):
        for (int i = 1; i < subTypes.length; i++) {
            IServerTypeDesc subType = _typeManager.getServerTypeDesc(subTypes[i].getTypeName());
            if (subType == null || subType.isInactive())
                continue;
            if (subType.getTypeDesc().getIdPropertyName() == null)
                return null;

            IEntryHolder entryHolder = _cacheManager.getEntryByIdFromPureCache(id, subType);
            res = getUserObjectFromEntryHolder(entryHolder, queryResultType);
            if (res != null)
                return res;
        }
        return null;
    }

    private IEntryHolder directLocalReadByUidForClass(String uid, String className) {
        try {
            return _cacheManager.getEntry(null, uid, className, null, false /*tryInsertToCache*/, false/*lockedEntry*/, true /*useOnlyCache*/);
        } catch (Exception ex) {
            //cannot happen
            throw new EngineInternalSpaceException(ex.toString(), ex);
        }
    }

    private Object getUserObjectFromEntryHolder(IEntryHolder entryHolder, QueryResultTypeInternal expectedResultType) {
        if (entryHolder == null)
            return null;

        IEntryData entryData = entryHolder.getTxnEntryData();
        if (entryData.getEntryDataType() != EntryDataType.USER_TYPE)
            return null;

        EntryType entryType = expectedResultType.getEntryType();
        if (entryType == null)
            entryType = entryData.getEntryTypeDesc().getTypeDesc().getObjectType();
        if (entryType != entryData.getEntryTypeDesc().getEntryType())
            return null;

        return ((UserTypeEntryData) entryData).getUserObject();
    }

    private boolean canReplicate(boolean disableReplication,
                                 boolean fromReplication) {
        return !fromReplication && !disableReplication;
    }

    public boolean shouldReplicate(ReplicationOperationType operType, IServerTypeDesc serverTypeDesc,
                                   boolean disableReplication, boolean fromReplication) {
        return shouldReplicate(operType, serverTypeDesc.getTypeDesc().isReplicable(), disableReplication, fromReplication);
    }

    public boolean shouldReplicate(ReplicationOperationType operType, boolean ofReplicatableClass,
                                   boolean disableReplication, boolean fromReplication) {
        if (!canReplicate(disableReplication, fromReplication))
            return false;
        final boolean replicableOper = _clusterPolicy.m_ReplicationPolicy.shouldReplicate(operType);
        return replicableOper && (_clusterPolicy.m_ReplicationPolicy.isFullReplication() || ofReplicatableClass);
    }

    public IReplicationNode getReplicationNode() {
        return _replicationManager.getReplicationNode();
    }

    public boolean isCleanUnusedEmbeddedGlobalXtns() {
        /** do we clean unused embedded global xtns */
        return _spaceImpl._cleanUnusedEmbeddedGlobalXtns;
    }

    public void afterSpaceModeChange(SpaceMode newMode) {
        if (_logger.isDebugEnabled())
            _logger.debug("afterSpaceModeChange(newMode=" + newMode + ")");
        if (newMode == SpaceMode.PRIMARY) {
            if (_spaceImpl.isRecovering())
                _failOverDuringRecovery = true;
            if (_memoryManager != null && _memoryManager.isRestartOnFailover()) {
                _memoryManager.close();
                _memoryManager = new MemoryManager(_spaceName, _containerName, _cacheManager, _leaseManager, _spaceImpl.isPrimary());
            }
        }
    }

    public void beforeSpaceModeChange(SpaceMode newMode) {
        if (_logger.isDebugEnabled())
            _logger.debug("beforeSpaceModeChange(newMode=" + newMode + ")");
        //Change the replication node state according to the new space mode
        switch (newMode) {
            case PRIMARY:
                _duplicateOperationIDFilter.onBecomePrimary();
                getReplicationNode().getAdmin().setActive();
                break;
            case BACKUP:
                _duplicateOperationIDFilter.onBecomeBackup();
                getReplicationNode().getAdmin().setPassive(true);
                break;
        }
    }

    public boolean isClusteredExternalDBEnabled(IStorageAdapter storageAdapter) {
        //check if replicated operations require data loading from the database
        //therefore requiring additional meta data to the replicated data
        return (storageAdapter.supportsExternalDB()
                && _clusterPolicy.m_CacheLoaderConfig.externalDataSource && !_clusterPolicy.m_CacheLoaderConfig.centralDataSource)
                || hasMirror();
    }

    public List<Throwable> getUnhealthyReasons() {
        List<Throwable> errors = new LinkedList<>();
        if (_spaceImpl.getSpaceMode() == SpaceMode.BACKUP && _replicationUnhealthyReason != null)
            errors.add(_replicationUnhealthyReason);

        if (_unhealthyReason != null)
            errors.add(_unhealthyReason);

        return errors;
    }

    public SpaceReplicationManager getReplicationManager() {
        return _replicationManager;
    }

    public int getProcessorQueueSize() {
        return _processorWG.getQueue().size();
    }

    public int getNotifierQueueSize() {
        return _dataEventManager.getQueueSize();
    }

    private String getOrCreateUid(IEntryPacket entryPacket) {
        String uid = entryPacket.getUID();
        if (uid == null) {
            final ITypeDesc typeDesc = entryPacket.getTypeDescriptor();
            final String idPropertyName = typeDesc.getIdPropertyName();
            if (idPropertyName == null || idPropertyName.length() == 0)
                uid = generateUid();
            else {
                int identifierPos = typeDesc.getIdentifierPropertyId();
                Object idValue = entryPacket.getFieldValue(identifierPos);

                if (typeDesc.isAutoGenerateId()) {
                    uid = (String) idValue;
                    if (uid == null || uid.length() == 0)
                        uid = generateUid();
                } else {
                    if (idValue != null)
                        uid = SpaceUidFactory.createUidFromTypeAndId(typeDesc, idValue);
                    else
                        throw new ConversionException("SpaceId(autogenerate=false) field value can't be null.");
                }
            }
        }

        return uid;
    }

    private static String extractMemberIdFromContainer(SpaceImpl spaceImpl) {
        if (spaceImpl.getClusterPolicy() == null)
            return null;

        final String container = spaceImpl.getContainerName();
        if (container == null || container.length() == 0)
            return null;

        // Format: qaSpace_container2_1
        final String partitionPrefix = "_container";
        int pos = container.indexOf(partitionPrefix);
        return (pos == -1) ? null : container.substring(pos + partitionPrefix.length());
    }

    public String generateUid() {
        return _uidFactory.generateUid();
    }

    public void setReplicationUnhealthyReason(Exception cause) {
        _replicationUnhealthyReason = cause;
    }

    private ConflictingOperationPolicy _conflictingOperationPolicy;

    public ConflictingOperationPolicy getConflictingOperationPolicy() {
        if (_conflictingOperationPolicy == null) {
            _conflictingOperationPolicy = _clusterPolicy != null && _clusterPolicy.getReplicationPolicy().getConflictingOperationPolicy() != null
                    ? _clusterPolicy.getReplicationPolicy().getConflictingOperationPolicy()
                    : ConflictingOperationPolicy.DEFAULT;
        }
        return _conflictingOperationPolicy;
    }

    public FifoGroupsHandler getFifoGroupsHandler() {
        return _fifoGroupsHandler;
    }

    public void enableDuplicateExecutionProtection(OperationID operationID) {
        if (operationID != null)
            _duplicateOperationIDFilter.add(operationID);
    }

    //can we disqualify the expired entry ?
    private boolean disqualifyExpiredEntry(IEntryHolder entry) {
        return !entry.isEntryUnderWriteLockXtn() && !_leaseManager.isSlaveLeaseManagerForEntries() && !(isExpiredEntryStayInSpace(entry));
    }

    public boolean isExpiredEntryStayInSpace(IEntryHolder entry) {
        return (_cacheManager.isEvictableCachePolicy() && !entry.isTransient() && !_cacheManager.isMemorySpace());
    }

    public boolean isFailOverDuringRecovery() {
        return _failOverDuringRecovery;
    }

    public boolean isLocalCache() {
        return _isLocalCache;
    }

    /**
     * Attempts to register the provided {@link ITypeDesc} instance and replicate the operation if
     * necessary.
     */
    public void registerTypeDesc(ITypeDesc typeDesc, boolean fromGateway) throws DetailedUnusableEntryException {
        final AddTypeDescResult result = getTypeManager().addTypeDesc(typeDesc);

        if (result.getResultType() == AddTypeDescResultType.NONE)
            return;

        // Replicate the new type:
        final IReplicationOutContext context = getReplicationNode().createContext();

        // If operation was executed from a gateway, specify its from gateway
        if (fromGateway)
            ((ReplicationOutContext) context).setFromGateway(true);

        try {
            getCacheManager().getStorageAdapter().introduceDataType(typeDesc);

            getReplicationNode().outDataTypeIntroduce(context, typeDesc);
            getReplicationNode().execute(context);
        } finally {
            context.release();
        }
    }

    public LocalViewRegistrations getLocalViewRegistrations() {
        return _localViewRegistrations;
    }

    public MetricRegistrator getMetricRegistrator() {
        return _metricRegistrator;
    }

    public MetricRegistrator getDataTypeMetricRegistrar(String dataTypeName) {
        return getDataTypeMetricRegistrar(dataTypeName, null);
    }

    public void clearDataTypeMetricRegistrarIfExists(String dataTypeName) {
        clearDataTypeMetricRegistrarIfExists(dataTypeName, null);
    }

    public MetricRegistrator getDataTypeMetricRegistrar(String typeName, String indexName) {
        String key = indexName == null ? typeName : typeName + '.' + indexName;
        return _dataTypesMetricRegistrators.computeIfAbsent(key, k -> {
            Map<String, String> extraTags = new HashMap<>(2);
            extraTags.put("data_type_name", typeName);
            if (indexName != null)
                extraTags.put("index", indexName);
            return extendSpaceMetricRegistrator("data", extraTags, Collections.emptyMap());
        });
    }

    public void clearDataTypeMetricRegistrarIfExists(String typeName, String indexName) {
        String key = indexName == null ? typeName : typeName + '.' + indexName;
        MetricRegistrator metricRegistrator = _dataTypesMetricRegistrators.remove(key);
        if (metricRegistrator != null)
            metricRegistrator.clear();
    }

    public void registerLocalView(ITemplatePacket[] queryPackets, Collection<SpaceQueryDetails> queryDescriptions,
                                  RouterStubHolder viewStub, int batchSize, long batchTimeout, SpaceContext spaceContext)
            throws UnusableEntryException, UnknownTypeException {
        final IReplicationNodeAdmin replicationNodeAdmin = getReplicationNode().getAdmin();

        // Register the view stub in the space replication node:
        replicationNodeAdmin.getRouterAdmin().addRemoteRouterStub(viewStub);

        final String groupName = generateGroupName();
        DynamicSourceGroupConfigHolder groupConfig = replicationNodeAdmin.getSourceGroupConfigHolder(groupName);

        for (ITemplatePacket template : queryPackets) {
            getTypeManager().loadServerTypeDesc(template);
            // If security is enabled verify read privileges for template type
            getSpaceImpl().assertAuthorizedForType(template.getTypeName(), SpacePrivilege.READ, spaceContext);
        }

        final String uniqueName = viewStub.getMyEndpointDetails().getLookupName();
        IReplicationChannelDataFilter viewFilter = new ViewReplicationChannelDataFilter(_cacheManager, groupName,
                queryPackets, getTemplateScanner().getRegexCache(), getTypeManager());

        BacklogMemberLimitationConfig memberBacklogLimitations = new BacklogMemberLimitationConfig();
        final ReplicationPolicy replicationPolicy = getClusterPolicy().getReplicationPolicy();
        final long limit = replicationPolicy.getLocalViewMaxRedologCapacity();
        memberBacklogLimitations.setLimit(limit, LimitReachedPolicy.DROP_MEMBER);
        final long localViewMaxDisconnectionTime = replicationPolicy.getLocalViewMaxDisconnectionTime();
        final long localViewMaxRedologRecoveryCapacity = replicationPolicy.getLocalViewMaxRedologRecoveryCapacity();
        memberBacklogLimitations.setLimitDuringSynchronization(localViewMaxRedologRecoveryCapacity, LimitReachedPolicy.DROP_MEMBER);
        AsyncChannelConfig config = new AsyncChannelConfig(batchSize, batchTimeout, batchSize, ReplicationMode.LOCAL_VIEW, localViewMaxDisconnectionTime);
        ViewDynamicSourceGroupMemberLifeCycle lifecycle = new ViewDynamicSourceGroupMemberLifeCycle(_localViewRegistrations, queryDescriptions,
                viewStub.getMyEndpointDetails().getConnectionDetails());
        groupConfig.replaceMember(uniqueName, viewFilter, memberBacklogLimitations, config, lifecycle);
    }

    public void unregisterLocalView(String viewStubHolderName) {
        final IReplicationNodeAdmin replicationNodeAdmin = getReplicationNode().getAdmin();
        final String groupName = generateGroupName();
        DynamicSourceGroupConfigHolder groupConfig = replicationNodeAdmin.getSourceGroupConfigHolder(groupName);
        groupConfig.removeMember(viewStubHolderName);
        // Register the view stub in the space replication node:
        replicationNodeAdmin.getRouterAdmin().removeRemoteStubHolder(viewStubHolderName);

        _localViewRegistrations.remove(viewStubHolderName);
    }

    private boolean isBlobStoreCachePolicy() {
        return _configReader.getIntSpaceProperty(CACHE_POLICY_PROP, "-1") == CACHE_POLICY_BLOB_STORE;
    }

    public boolean isBlobStorePersistent() {
        return isBlobStoreCachePolicy()
                && Boolean.parseBoolean(_spaceImpl.getCustomProperties().getProperty(FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP));
    }

    public SpaceClusterInfo getClusterInfo() {
        return _clusterInfo;
    }

    private void monitorMemoryUsage(boolean isWriteTypeOperation) {
        if (_memoryManager != null)
            _memoryManager.monitorMemoryUsage(isWriteTypeOperation);
    }

    public void closeServerIterator(UUID uuid){
        _serverIteratorsManager.closeServerIterator(uuid);
    }

    public void renewServerIteratorLease(UUID uuid){
        _serverIteratorsManager.tryRenewServerIteratorLease(uuid);
    }

    public Logger getLogger() {
        return _logger;
    }
}
