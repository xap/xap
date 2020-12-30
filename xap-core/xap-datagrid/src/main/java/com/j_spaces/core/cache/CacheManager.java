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

package com.j_spaces.core.cache;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.client.protective.ProtectiveMode;
import com.gigaspaces.client.protective.ProtectiveModeException;
import com.gigaspaces.internal.client.cache.CustomInfo;
import com.gigaspaces.internal.cluster.node.IReplicationNode;
import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationOutContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfigBuilder;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.ioImpl.DirectPersistencyBlobStoreIO;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.notification.NotificationReplicationChannelDataFilter;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.ICustomQuery;
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.query.explainplan.*;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.*;
import com.gigaspaces.internal.server.space.SpaceEngine.EntryRemoveReasonCodes;
import com.gigaspaces.internal.server.space.SpaceEngine.TemplateRemoveReasonCodes;
import com.gigaspaces.internal.server.space.eviction.*;
import com.gigaspaces.internal.server.space.metadata.IServerTypeDescListener;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.server.space.metadata.TypeDataFactory;
import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.DirectPersistencyRecoveryException;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.IStorageConsistency;
import com.gigaspaces.internal.server.space.recovery.direct_persistency.StorageConsistencyModes;
import com.gigaspaces.internal.server.storage.*;
import com.gigaspaces.internal.sync.hybrid.SyncHybridStorageAdapter;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacket;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.economy.EconomyConcurrentHashMap;
import com.gigaspaces.internal.utils.collections.economy.HashEntryHandlerSpaceEntry;
import com.gigaspaces.management.space.LocalCacheDetails;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.metadata.index.CompoundIndex;
import com.gigaspaces.metadata.index.ISpaceCompoundIndexSegment;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.MetricConstants;
import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.query.extension.QueryExtensionProvider;
import com.gigaspaces.query.extension.QueryExtensionRuntimeInfo;
import com.gigaspaces.query.extension.impl.QueryExtensionRuntimeInfoImpl;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.server.blobstore.*;
import com.gigaspaces.server.eviction.EvictableServerEntry;
import com.gigaspaces.server.eviction.SpaceEvictionManager;
import com.gigaspaces.server.eviction.SpaceEvictionStrategy;
import com.gigaspaces.server.eviction.SpaceEvictionStrategyConfig;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.*;
import com.j_spaces.core.admin.SpaceRuntimeInfo;
import com.j_spaces.core.admin.TemplateInfo;
import com.j_spaces.core.cache.TerminatingFifoXtnsInfo.FifoXtnEntryInfo;
import com.j_spaces.core.cache.blobStore.*;
import com.j_spaces.core.cache.blobStore.memory_pool.AbstractMemoryPool;
import com.j_spaces.core.cache.blobStore.memory_pool.OffHeapMemoryPool;
import com.j_spaces.core.cache.blobStore.optimizations.BlobStoreOperationOptimizations;
import com.j_spaces.core.cache.blobStore.recovery.BlobStoreRecoveryHelper;
import com.j_spaces.core.cache.blobStore.recovery.BlobStoreRecoveryHelperWrapper;
import com.j_spaces.core.cache.blobStore.sadapter.BlobStoreFifoInitialLoader;
import com.j_spaces.core.cache.blobStore.sadapter.BlobStoreStorageAdapter;
import com.j_spaces.core.cache.blobStore.sadapter.IBlobStoreStorageAdapter;
import com.j_spaces.core.cache.blobStore.storage.BlobStoreHashMock;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.cache.context.IndexMetricsContext;
import com.j_spaces.core.cache.fifoGroup.FifoGroupCacheImpl;
import com.j_spaces.core.client.*;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.exception.internal.EngineInternalSpaceException;
import com.j_spaces.core.fifo.DefaultFifoBackgroundDispatcher;
import com.j_spaces.core.fifo.FifoBackgroundDispatcher;
import com.j_spaces.core.fifo.FifoBackgroundRequest;
import com.j_spaces.core.sadapter.ISAdapterIterator;
import com.j_spaces.core.sadapter.IStorageAdapter;
import com.j_spaces.core.sadapter.SAException;
import com.j_spaces.core.sadapter.SelectType;
import com.j_spaces.core.server.processor.RemoveWaitingForInfoSABusPacket;
import com.j_spaces.jdbc.SQLFunctions;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.*;
import com.j_spaces.kernel.list.*;
import com.j_spaces.kernel.locks.*;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.space.InternalSpaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.j_spaces.core.Constants.CacheManager.*;
import static com.j_spaces.core.Constants.Engine.UPDATE_NO_LEASE;

/**
 * The Cache Manager acts as a fast intermediate storage for objects in Virtual Memory.
 * <p>
 * The maximum number of objects cached in memory is determined by a space property (cache-size).
 * <p>
 * However, according to this implementation, the following objects are always in the cache:
 * <p>
 * <ul> <li> Templates of any kind </li> <li> Active Transactions </li> <li> Entries locked under
 * Active Transactions </li> <li> Entries waiting for templates </li> </ul>
 * <p>
 * The objects in the cache are indexed.
 * <p>
 * Templates are cached in memory and are not written to the underlying SA.
 **/
@com.gigaspaces.api.InternalApi
public class CacheManager extends AbstractCacheManager
        implements SpaceEvictionManager {
    private static final Logger _logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_CACHE);
    private static final boolean _USE_UIDS_ECONOMY_HASH_MAP_FOR_BLOBSTORE_ = true;
    public final TerminatingFifoXtnsInfo _terminatingXtnsInfo;
    private final AtomicInteger _cacheSize;
    /**
     * <K:UID, V:PEntry>
     */
    private final ConcurrentMap<String, IEntryCacheInfo> _entries;

    private final SpaceEngine _engine;
    private final ClusterPolicy _clusterPolicy;
    private final SpaceTypeManager _typeManager;
    private final LocalCacheRegistrations _localCacheRegistrations;
    private final PTypeMap _typeDataMap;
    private final TypeDataFactory _typeDataFactory;
    private final CacheContextFactory _cacheContextFactory;
    private final IStorageAdapter _storageAdapter;
    private final SQLFunctions sqlFunctions;

    private LeaseManager _leaseManager;
    private PersistentGC _persistentGC;

    private final boolean _readOnlySA;

    private final IReplicationNode _replicationNode;
    private final TemplatesManager _templatesManager;
    private final TemplateExpirationManager _templateExpirationManager;

    //is true if the SA is a memorySA
    final private boolean _isMemorySA;

    // true if this cacheload-store has an external DB
    final private boolean _isCacheExternalDB;
    //true if the DB is central (common to several partitions)
    final private boolean _isCentralDB;

    // true if at least one cacheload-store defined in a cluster
    final private boolean _isClusteredExternalDBEnabled;

    //true if external-data-source supports versioning
    private final boolean _isVersionedExternalDB;

    private boolean _syncReplicationUsed;
    //any number of ext' indexed entries below this number
    // will not use indexes at all but ALL entries vector
    private int _minExtendedIndexActivationSize;

    private IBasicLockManager<IEntryHolder> _lockManager;

    private final AtomicInteger _actualCacheSize;
    private final boolean _isTimeBasedEvictionStrategy;

    private final IEntryCacheInfo _entryAlreadyInSpaceIndication = EntryCacheInfoFactory.createEntryCacheInfo(null);

    private SpaceEvictionStrategy _evictionStrategy;

    //for persistent evictable only - contains UIDs for entries recently deleted
    final private RecentDeletesRepository _recentDeletesRespository;
    //for persistent evictable only - contains UIDs for entries recently updated
    final private RecentUpdatesRepository _recentUpdatesRespository;

    final private long _recoveryLogInterval = Long.getLong(SystemProperties.CACHE_MANAGER_RECOVER_INTERVAL_LOG, SystemProperties.CACHE_MANAGER_RECOVER_INTERVAL_DEFAULT);
    final private boolean _logRecoveryProcess = Boolean.parseBoolean(System.getProperty(SystemProperties.CACHE_MANAGER_LOG_RECOVER_PROCESS, "true"));

    private boolean _partialUpdateReplication;

    private final FifoBackgroundDispatcher _fifoBackgroundDispatcher;
    private final FifoGroupCacheImpl _fifoGroupCacheImpl;

    private final IEvictionReplicationsMarkersRepository _evictionReplicationsMarkersRepository;

    //is space empty after initial load stage- used is delta-recovery considerations
    private volatile boolean _emptyAfterInitialLoadStage;

    private final boolean _directPersistencyEmbeddedtHandlerUsed;

    private BlobStoreExtendedStorageHandler _blobStoreStorageHandler;
    private BlobStoreMemoryMonitor _blobStoreMemoryMonitor;
    private IBlobStoreCacheHandler _blobStoreInternalCache;
    private final boolean _persistentBlobStore;
    private final boolean _useBlobStoreBulks;
    private final boolean _optimizedBlobStoreClear;
    private final IStorageConsistency _blobStoreRecoveryHelper;
    private final boolean _enableSyncListForBlobStore;
    private final boolean _useBlobStorePrefetch;
    private final boolean _useBlobStoreReplicationBackupBulk;

    private final Map<String, QueryExtensionIndexManagerWrapper> queryExtensionManagers;

    //TEMP FOR QA
    private boolean _blobStoreForQa;

    private final boolean _forceSpaceIdIndexIfEqual;

    private SpaceMetricsRegistrationUtils _spaceMetricsRegistrationUtils;

    public static final int MIN_SIZE_TO_PERFORM_EXPLICIT_PROPERTIES_INDEX_SCAN_ = 5;

    // GS-13953 speedup queries that combine text and standard predicates
    public static final boolean GS_13953_ENABLED = Boolean.parseBoolean(System.getProperty("com.gs.13953.enabled", "true"));

    public CacheManager(SpaceConfigReader configReader, ClusterPolicy clusterPolicy,
                        SpaceTypeManager typeManager, IReplicationNode replicationNode,
                        IStorageAdapter sa, SpaceEngine engine, Properties customProperties) throws CreateException {

        _engine = engine;
        _clusterPolicy = clusterPolicy;
        _typeManager = typeManager;
        _replicationNode = replicationNode;
        _localCacheRegistrations = new LocalCacheRegistrations();
        _typeDataMap = new PTypeMap();
        _typeDataFactory = new TypeDataFactory(engine.getConfigReader(), this);
        m_CacheSize = configReader.getIntSpaceProperty(CACHE_MANAGER_SIZE_PROP, CACHE_MANAGER_SIZE_DEFAULT);
        _spaceMetricsRegistrationUtils = new SpaceMetricsRegistrationUtils( this );

        boolean persistentBlobStore = Boolean.parseBoolean(customProperties.getProperty(FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP, "false"));

        int numNotifyFifoThreads = engine.getConfigReader().getIntSpaceProperty(
                Constants.Engine.ENGINE_FIFO_NOTIFY_THREADS_PROP, Constants.Engine.ENGINE_FIFO_NOTIFY_THREADS_DEFAULT);
        numNotifyFifoThreads = numNotifyFifoThreads != 0 ? numNotifyFifoThreads : Runtime.getRuntime().availableProcessors();

        int numNonNotifyFifoThreads = engine.getConfigReader().getIntSpaceProperty(
                Constants.Engine.ENGINE_FIFO_REGULAR_THREADS_PROP, Constants.Engine.ENGINE_FIFO_REGULAR_THREADS_DEFAULT);
        numNonNotifyFifoThreads = numNonNotifyFifoThreads != 0 ? numNonNotifyFifoThreads : Runtime.getRuntime().availableProcessors();

        _fifoBackgroundDispatcher = new DefaultFifoBackgroundDispatcher(numNotifyFifoThreads, numNonNotifyFifoThreads, this, _engine);

        // indicators which return the type of StorageAdapter this instance represents.
        boolean isCacheExternalDB = sa.supportsExternalDB();
        boolean isMemorySA = !isCacheExternalDB;

	     /* get cache policy:
         * If no value was provided or System property wasn't set,
         * the default is ALL_IN_CACHE
         * Note: this behavior was changed in 10.1 . Before that persistent space default policy was LRU
         */
        setCachePolicy(configReader.getIntSpaceProperty(CACHE_POLICY_PROP, String.valueOf(CACHE_POLICY_ALL_IN_CACHE)));
        _emptyAfterInitialLoadStage = true;

//TEMP FOR QA
//+++++++++++++++++ TEMP FOR QA
        String oh = System.getProperty("com.gs.OffHeapData");
        if (!_blobStoreForQa)
            _blobStoreForQa = (oh == null || _engine.isLocalCache()) ? false : Boolean.parseBoolean(oh);
        if (_blobStoreForQa && !isSyncHybrid()) {
            persistentBlobStore = true;
        }
        if (_blobStoreForQa && isEvictableCachePolicy()) {
            _blobStoreForQa = false;
        }
        if (_blobStoreForQa && _engine.isLocalCache()) {
            _blobStoreForQa = false;
        }
        if (_blobStoreForQa)
            setCachePolicy(CACHE_POLICY_BLOB_STORE);
        if (_blobStoreForQa && !isMemorySA && !sa.isReadOnly() && !isSyncHybrid()) {
            _blobStoreForQa = false;
            setCachePolicy(CACHE_POLICY_ALL_IN_CACHE);
        }
//------------------ TEMP FOR QA

        Boolean forceSpaceIdIndexIfEqualDefault = !isBlobStoreCachePolicy();
        _forceSpaceIdIndexIfEqual = engine.getConfigReader().getBooleanSpaceProperty(
                Constants.CacheManager.CACHE_MANAGER_FORCE_ID_INDEX_PROP, forceSpaceIdIndexIfEqualDefault.toString());
        if (_forceSpaceIdIndexIfEqual != forceSpaceIdIndexIfEqualDefault.booleanValue())
            _logger.info(Constants.CacheManager.CACHE_MANAGER_FORCE_ID_INDEX_PROP + " was set to " + _forceSpaceIdIndexIfEqual);

        if (isBlobStoreCachePolicy()) {
            _useBlobStoreBulks = Boolean.parseBoolean(System.getProperty(FULL_CACHE_MANAGER_USE_BLOBSTORE_BULKS_PROP, isSyncHybrid() ? "false" : "true"));
            _logger.info("useBlobStoreBulks=" + _useBlobStoreBulks);
            _optimizedBlobStoreClear = Boolean.parseBoolean(System.getProperty(CACHE_MANAGER_USE_BLOBSTORE_CLEAR_OPTIMIZATION_PROP, "true"));

            _enableSyncListForBlobStore = Boolean.parseBoolean(System.getProperty(SystemProperties.REPLICATION_USE_BLOBSTORE_SYNC_LIST,
                    SystemProperties.REPLICATION_USE_BLOBSTORE_SYNC_LIST_DEFAULT));
            _useBlobStorePrefetch = _useBlobStoreBulks && Boolean.parseBoolean(System.getProperty(FULL_CACHE_MANAGER_BOLBSTORE_USE_PREFETCH_PROP, "false")); /*!!!!!!!!*/
            _logger.info("useBlobStorePrefetch=" + _useBlobStorePrefetch);
            _useBlobStoreReplicationBackupBulk = _useBlobStoreBulks && _engine.isReplicatedPersistentBlobstore()
                    && Boolean.parseBoolean(System.getProperty(SystemProperties.REPLICATION_USE_BACKUP_BLOBSTORE_BULKS, SystemProperties.REPLICATION_USE_BACKUP_BLOBSTORE_BULKS_DEFAULT));
            _logger.info("useBlobStoreReplicationBackupBulk=" + _useBlobStoreReplicationBackupBulk);
        } else {
            _useBlobStoreBulks = false;
            _optimizedBlobStoreClear = false;
            _enableSyncListForBlobStore = false;
            _useBlobStorePrefetch = false;
            _useBlobStoreReplicationBackupBulk = false;
        }

        if (getCachePolicy() < 0 || getCachePolicy() > MAX_CACHE_POLICY_VALUE)
            throw new RuntimeException("invalid cache policy value specified");

        if (isBlobStoreCachePolicy() && _engine.isLocalCache())
            throw new RuntimeException("blob-store cache policy not supported in local-cache");

        if (isBlobStoreCachePolicy() && !isMemorySA && !sa.isReadOnly() && !isSyncHybrid())
            throw new RuntimeException("blob-store cache policy not supported with direct EDS");

        _persistentBlobStore = persistentBlobStore;

        if (isBlobStoreCachePolicy()) {
            IStorageAdapter curSa;
            if (isSyncHybrid()) {
                curSa = new SyncHybridStorageAdapter(this, sa, new BlobStoreStorageAdapter(_engine, _persistentBlobStore));
            } else {
                curSa = (isMemorySA) ? new BlobStoreStorageAdapter(_engine, _persistentBlobStore)
                        : new BlobStoreStorageAdapter(_engine, _persistentBlobStore, sa /*recoverysa*/);
            }
            _storageAdapter = curSa;
            _isMemorySA = false;
            _isCacheExternalDB = false;
        } else {
            _storageAdapter = sa;
            _isMemorySA = isMemorySA;
            _isCacheExternalDB = isCacheExternalDB;
        }
        _actualCacheSize = new AtomicInteger(0);
        _isTimeBasedEvictionStrategy = _evictionStrategy instanceof TimeBasedSpaceEvictionStrategy;

        //do we need to create EvictionReplicationsMarkersRepository ?
        if (!isMemorySA && isEvictableCachePolicy() && _engine.hasMirror())
            _evictionReplicationsMarkersRepository = new EvictionReplicationsMarkersRepository();
        else
            _evictionReplicationsMarkersRepository = null;

        _recentDeletesRespository = useRecentDeletes() ? new RecentDeletesRepository() : null;
        _recentUpdatesRespository = useRecentUpdatesForPinning() ? new RecentUpdatesRepository() : null;

        // Set with cache-loader cluster-wide properties
        if (engine.getClusterPolicy() != null) {
            // true if either indicated in cluster-wide property or mirror service is enabled in cluster
            _isClusteredExternalDBEnabled = engine.isClusteredExternalDBEnabled(getStorageAdapter());
            _isCentralDB = _storageAdapter.supportsExternalDB() && engine.getClusterPolicy().m_CacheLoaderConfig.centralDataSource;
        } else //set with defaults
        {
            _isClusteredExternalDBEnabled = false;
            _isCentralDB = false;
        }

        _terminatingXtnsInfo = new TerminatingFifoXtnsInfo();
        _cacheSize = new AtomicInteger(0);
        int numOfCHMSegents = Integer.getInteger(SystemProperties.CACHE_MANAGER_HASHMAP_SEGMENTS, SystemProperties.CACHE_MANAGER_HASHMAP_SEGMENTS_DEFAULT);

        _entries = (_typeDataFactory.useEconomyHashMap() || (isBlobStoreCachePolicy() && _USE_UIDS_ECONOMY_HASH_MAP_FOR_BLOBSTORE_))
                ? new EconomyConcurrentHashMap<String, IEntryCacheInfo>(16, 0.75f, numOfCHMSegents, new HashEntryHandlerSpaceEntry(IEntryCacheInfo.UID_HASH_INDICATOR))
                : new ConcurrentHashMap<String, IEntryCacheInfo>(16, 0.75f, numOfCHMSegents);

        _cacheContextFactory = new CacheContextFactory(this, engine.getFullSpaceName());
        _templatesManager = new TemplatesManager(this, numNotifyFifoThreads, numNonNotifyFifoThreads);
        _templateExpirationManager = new TemplateExpirationManager(this);
        _fifoGroupCacheImpl = new FifoGroupCacheImpl(this, _logger);

        _isVersionedExternalDB = _engine.getSpaceImpl().getJspaceAttr().isSupportsVersionEnabled();
        _readOnlySA = _storageAdapter.isReadOnly();
        if (isBlobStoreCachePolicy()) {
            BlobStoreStorageHandler driver = createBlobStoreHandlerNewInterface(configReader, customProperties);
            _blobStoreStorageHandler = new BlobStoreOperationsWrapper(this, driver);
            _blobStoreRecoveryHelper = new BlobStoreRecoveryHelperWrapper(_logger, _engine.getFullSpaceName(), driver, _engine.getNumberOfPartitions(), _engine.getClusterInfo().getNumberOfBackups());
            setOffHeapProperties(customProperties);
        } else {
            _blobStoreStorageHandler = null;
            _blobStoreRecoveryHelper = null;
        }

        _directPersistencyEmbeddedtHandlerUsed = engine.getReplicationNode() != null && engine.getReplicationNode().getDirectPesistencySyncHandler() != null
                && engine.getReplicationNode().getDirectPesistencySyncHandler().isEmbeddedListUsed();

        Object userFunctions = customProperties.get(Constants.SqlFunction.USER_SQL_FUNCTION);
        sqlFunctions = new SQLFunctions((Map<String, SqlFunction>) userFunctions);
        queryExtensionManagers = initQueryExtensionManagers(customProperties);
    }

    private void setOffHeapProperties(Properties customProperties) {
        if (customProperties.getProperty(BLOBSTORE_OFF_HEAP_MIN_DIFF_TO_ALLOCATE_PROP) != null && !hasBlobStoreOffHeapCache() && !hasBlobStoreOffHeapStore()) {
            _logger.warn(BLOBSTORE_OFF_HEAP_MIN_DIFF_TO_ALLOCATE_PROP + " is set but no off heap memory is used");
        } else {
            long minimalDiffToAllocate = StringUtils.parseStringAsBytes(customProperties.getProperty(BLOBSTORE_OFF_HEAP_MIN_DIFF_TO_ALLOCATE_PROP, BLOBSTORE_OFF_HEAP_MIN_DIFF_TO_ALLOCATE_DEFAULT_VALUE));
            if(_blobStoreStorageHandler.getOffHeapStore() != null &&  _blobStoreStorageHandler.getOffHeapStore() instanceof OffHeapMemoryPool){
                ((OffHeapMemoryPool) _blobStoreStorageHandler.getOffHeapStore()).setMinimalDiffToAllocate((int) minimalDiffToAllocate);
            }
            if (_blobStoreStorageHandler.getOffHeapCache() != null) {
                _blobStoreStorageHandler.getOffHeapCache().setMinimalDiffToAllocate((int) minimalDiffToAllocate);
            }
        }
    }

    public boolean isSyncHybrid() {
        return _engine.getConfigReader().getBooleanSpaceProperty(Constants.CacheManager.BLOBSTORE_ENABLE_UNSAFE_ENDPOINT_PROP, "false");
    }

    private Map<String, QueryExtensionIndexManagerWrapper> initQueryExtensionManagers(Properties customProperties) {
        final Map<String, QueryExtensionIndexManagerWrapper> queryExtensions = new HashMap<String, QueryExtensionIndexManagerWrapper>();
        final QueryExtensionRuntimeInfo info = new QueryExtensionRuntimeInfoImpl(
                getEngine().getSpaceImpl().getNodeName(),
                getEngine().getSpaceImpl().getDeployPath());
        // Load user-defined query extensions from config
        final SpaceInstanceConfig spaceInstanceConfig = (SpaceInstanceConfig) customProperties.get(Constants.Space.SPACE_CONFIG);
        if (spaceInstanceConfig != null && spaceInstanceConfig.getQueryExtensionProviders() != null) {
            for (QueryExtensionProvider provider : spaceInstanceConfig.getQueryExtensionProviders())
                queryExtensions.put(provider.getNamespace(), new QueryExtensionIndexManagerWrapper(provider, info));
        }
        // Load predefined query extension managers if not configured:
        addPredefinedIfAbsent(queryExtensions, info, "spatial", "org.openspaces.spatial.spi.LuceneSpatialQueryExtensionProvider");
        addPredefinedIfAbsent(queryExtensions, info, "text", "org.openspaces.textsearch.LuceneTextSearchQueryExtensionProvider");
        return queryExtensions;
    }

    private void addPredefinedIfAbsent(Map<String, QueryExtensionIndexManagerWrapper> queryExtensions, QueryExtensionRuntimeInfo info, String namespace, String providerClassName) {
        if (!queryExtensions.containsKey(namespace)) {
            try {
                Class<? extends QueryExtensionProvider> providerClass = (Class<? extends QueryExtensionProvider>) Thread.currentThread().getContextClassLoader().loadClass(providerClassName);
                QueryExtensionProvider provider = providerClass.newInstance();
                queryExtensions.put(namespace, new QueryExtensionIndexManagerWrapper(provider, info));
            } catch (Throwable e) {
                if (_logger.isDebugEnabled())
                    _logger.debug("Failed to load predefined query extension " + namespace + " - " + e);
            }
        }
    }

    public static Logger getLogger() {
        return _logger;
    }

    public void setLeaseManager(LeaseManager leaseManager) {
        this._leaseManager = leaseManager;
    }

    public void init(Properties properties) throws SAException, CreateException {
        // init real SA
        _storageAdapter.initialize();
        _typeManager.registerTypeDescListener(new TypeDescListener());

        SpaceConfigReader configReader = _engine.getConfigReader();
        // initialize the eviction strategy
        _evictionStrategy = createEvictionStrategy(configReader, properties);

        //create the lock manager
        _lockManager = isBlobStoreCachePolicy() ? new BlobStoreLockManager() : ((isAllInCachePolicy() ?
                new AllInCacheLockManager<IEntryHolder>() :
                new BasicEvictableLockManager<IEntryHolder>(configReader)));

		/* get min extd' index activation size  */
        _minExtendedIndexActivationSize = configReader.getIntSpaceProperty(
                CACHE_MANAGER_MIN_EXTENDED_INDEX_ACTIVATION_PROP,
                CACHE_MANAGER_MIN_EXTENDED_INDEX_ACTIVATION_DEFAULT);

        if (_minExtendedIndexActivationSize < 0)
            throw new RuntimeException("invalid min_ext_index_activation_size value specified");

        _syncReplicationUsed = _engine.isSyncReplicationEnabled();

        _partialUpdateReplication = configReader.getBooleanSpaceProperty(
                CACHE_MANAGER_PARTIAL_UPDATE_REPLICATION_PROP,
                CACHE_MANAGER_PARTIAL_UPDATE_REPLICATION_DEFAULT);

    }

    public boolean isTimeBasedEvictionStrategy() {
        return _isTimeBasedEvictionStrategy;
    }

    public boolean requiresEvictionReplicationProtection() {
        return _evictionReplicationsMarkersRepository != null;
    }

    public IEvictionReplicationsMarkersRepository getEvictionReplicationsMarkersRepository() {
        return _evictionReplicationsMarkersRepository;
    }

    /**
     * Init. the cache.
     * <p>
     * This method is called after init(), and after the engine Xtn table and Directory are
     * completely built, but before any other SA method is called.
     * <p>
     * This implementation preloads all active transactions and entries locked under these
     * transactions into cache.
     * <p>
     * In addition, all class info is preloaded into cache.
     */
    public void initCache(boolean loadDataFromDB, Properties properties)
            throws SAException {
        BlobStoreInternalCacheFilter blobStoreInternalCacheFilter = null;

        //for direct-per-instance primary verify that the state is consistent-dont allow destruction of data
        if (getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper() != null && getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper().isPerInstancePersistency()) {
            if (_engine.getSpaceImpl().isPrimary()) {
                if (getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper().isInconsistentStorage()) {
                    _logger.error("space " + getEngine().getFullSpaceName() + " selected to be primary but storage in inconsistent");
                    DirectPersistencyRecoveryException e = new DirectPersistencyRecoveryException("space " + getEngine().getFullSpaceName() + " selected to be primary but storage in inconsistent");
                    throw e;
                }
            } else {
                if (getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper().isMeLastPrimary()) {
                    _logger.error("space " + getEngine().getFullSpaceName() + " selected to be backup but last time was primary");
                    DirectPersistencyRecoveryException e = new DirectPersistencyRecoveryException("space " + getEngine().getFullSpaceName() + " selected to be backup last time was primary");
                    throw e;
                }
            }
        }

        SpaceEvictionStrategyConfig config = new SpaceEvictionStrategyConfig(getMaxCacheSize());
        _evictionStrategy.initialize(this, config);

        if (isBlobStoreCachePolicy()) {
            loadDataFromDB = true;
            boolean warmStart = _persistentBlobStore;

            boolean inconsistentBackup = _persistentBlobStore &&
                    (getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper() != null && !_engine.getSpaceImpl().isPrimary() && getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper().isInconsistentStorage());

            if (warmStart && inconsistentBackup && _logger.isInfoEnabled())
                _logger.info(getEngine().getFullSpaceName() + " blob-store backup space inconsistent-storage indicator, forced cold start on");
            warmStart &= (!inconsistentBackup); //inconsistent backup- clear it

            // force cold start in backup if the user explicitly disabled sync list recovery
            if (!_engine.getSpaceImpl().isPrimary() && _engine.getReplicationNode() != null) {
                warmStart &= _engine.getReplicationNode().getDirectPesistencySyncHandler() != null;
            }
            final MetricRegistrator blobstoreMetricRegistrar = _engine.getMetricRegistrator().extend(MetricConstants.BLOBSTORE_METRIC_NAME);

            properties.put("blobstoreMetricRegistrar", blobstoreMetricRegistrar);

            _blobStoreInternalCache = new BlobStoreCacheHandler(properties);

            BlobStoreConfig blobStoreConfig = new BlobStoreConfig(_engine.getFullSpaceName(),
                    _engine.getNumberOfPartitions(),
                    _engine.getClusterInfo().getNumberOfBackups(),
                    warmStart,
                    blobstoreMetricRegistrar);
            _blobStoreStorageHandler.initialize(blobStoreConfig);

            List<SQLQuery> blobStoreCacheFilterQueries = (List<SQLQuery>) properties.get(FULL_CACHE_MANAGER_BLOBSTORE_CACHE_FILTER_QUERIES_PROP);
            if (blobStoreCacheFilterQueries != null) {
                try {
                    blobStoreInternalCacheFilter = new BlobStoreInternalCacheFilter(_engine, blobStoreCacheFilterQueries);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create blobstore cache filter", e);
                }
            }

            _blobStoreInternalCache.setBlobStoreInternalCacheFilter(blobStoreInternalCacheFilter);

            Properties blobstoreProperties = new Properties();
            blobstoreProperties.setProperty(FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP, properties.getProperty(FULL_CACHE_MANAGER_BLOBSTORE_CACHE_SIZE_PROP));
            blobstoreProperties.setProperty(FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP, String.valueOf(warmStart));

            Properties implProperties = _blobStoreStorageHandler.getProperties();
            if (implProperties != null) {
                implProperties.remove(BlobStoreMemoryMonitor.MEMORY_MONITOR_PROPERTY_NAME);
                implProperties.remove(BlobStoreRecoveryHelper.BLOB_STORE_RECOVERY_HELPER_PROPERTY_NAME);
                blobstoreProperties.putAll(implProperties);
            }
            getEngine().getSpaceImpl().getConfig().setBlobStoreProperties(blobstoreProperties);

            _blobStoreMemoryMonitor = new BlobStoreMemoryMonitorWrapper(_blobStoreStorageHandler.getProperties() != null ? (BlobStoreMemoryMonitor) _blobStoreStorageHandler.getProperties().get(BlobStoreMemoryMonitor.MEMORY_MONITOR_PROPERTY_NAME) : null, this, _engine.getFullSpaceName(), properties);
            if (getEngine().getReplicationNode() != null) {
                getEngine().getReplicationNode().setBlobStoreReplicaConsumeHelper(new BlobStoreReplicaConsumeHelper(getEngine()));
                if (useBlobStoreReplicationBackupBulks()) {
                    getEngine().getReplicationNode().setBlobStoreReplicationBulkConsumeHelper(new BlobStoreReplicationBulkConsumeHelper(getEngine()));
                }
            }
        }

        if (isBlobStoreCachePolicy() && _replicationNode.getDirectPesistencySyncHandler() != null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("[" + getEngine().getFullSpaceName() + "] initializingIoHandler DirectPersistencyBlobStoreIO in DirectPersistencySyncHandler");
            }
            _replicationNode.getDirectPesistencySyncHandler().afterInitializedBlobStoreIO(new DirectPersistencyBlobStoreIO(this, _logger, _replicationNode.getDirectPesistencySyncHandler().getCurrentGenerationId()));
        }

        if (loadDataFromDB) {
            Context context = null;
            try {
                context = getCacheContext();
                loadDataFromDB(context, _engine.getConfigReader());
                _emptyAfterInitialLoadStage = _entries.isEmpty();
            } catch (Exception ex1) {
                if (getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper() != null) {
                    if (_logger.isWarnEnabled()) {
                        _logger.warn("[" + getEngine().getFullSpaceName() + "] setting storage state to Inconsistent due to failure during initial load");
                    }
                    getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper().setStorageState(StorageConsistencyModes.Inconsistent);
                }
                throw ex1 instanceof SAException ? (SAException) ex1 : new SAException(ex1);
            } finally {
                freeCacheContext(context);
            }
        }
        if (getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper() != null && _engine.getSpaceImpl().isPrimary()) {
            if (_logger.isInfoEnabled()) {
                _logger.info("[" + getEngine().getFullSpaceName() + "] setting storage state of primary to Consistent after initial load");
            }
            getEngine().getSpaceImpl().getDirectPersistencyRecoveryHelper().setStorageState(StorageConsistencyModes.Consistent);
        }
        if (isBlobStoreCachePolicy() && _replicationNode.getDirectPesistencySyncHandler() != null) {
            if (_logger.isDebugEnabled()) {
                _logger.debug("[" + getEngine().getFullSpaceName() + "] initializing DirectPersistencyBlobStoreIO in DirectPersistencySyncHandler");
            }

            if (isDirectPersistencyEmbeddedtHandlerUsed()) {
                _replicationNode.getDirectPesistencySyncHandler().getEmbeddedSyncHandler().getInitialLoadHandler().onInitialLoadEnd();
            }

            _replicationNode.getDirectPesistencySyncHandler().initialize();
        }

        //start Persistent GC (when CacheManager initialization is complete)
        _persistentGC = new PersistentGC(this, _engine.getConfigReader());
        _persistentGC.start();
    }

    /**
     * Returns an indication if Storage Adapter acts as an external data-source.
     *
     * @return <code>true</code> if Storage Adapter is an external data-source; <code>false</code>
     * otherwise.
     */
    public boolean isCacheExternalDB() {
        return _isCacheExternalDB;
    }

    public SpaceEngine getEngine() {
        return _engine;
    }

    public SpaceTypeManager getTypeManager() {
        return _typeManager;
    }

    public LeaseManager getLeaseManager() {
        return _leaseManager;
    }

    public boolean isblobStoreDataSpace() {
        return isBlobStoreCachePolicy();
    }

	/*
     * non null for off-heap space
	 */

    public BlobStoreExtendedStorageHandler getBlobStoreStorageHandler() {
        return _blobStoreStorageHandler;
    }

    public BlobStoreMemoryMonitor getBlobStoreMemoryMonitor() {
        return _blobStoreMemoryMonitor;
    }

    public IStorageConsistency getBlobStoreRecoveryHelper() {
        return _blobStoreRecoveryHelper;
    }

    public boolean useBlobStoreBulks() {
        return _useBlobStoreBulks;
    }

    public boolean useBlobStoreReplicationBackupBulks() {
        return _useBlobStoreReplicationBackupBulk;
    }

    public boolean useBlobStorePreFetch() {
        return _useBlobStorePrefetch;
    }

    public boolean isPersistentBlobStore() {
        return _persistentBlobStore;
    }

    public boolean optimizedBlobStoreClear() {
        return _optimizedBlobStoreClear;
    }

    public boolean needReReadAfterEntryLock() {
        return isEvictableCachePolicy() || isblobStoreDataSpace();
    }

    public boolean mayNeedEntriesUnpinning() {
        return isEvictableCachePolicy() || isblobStoreDataSpace();
    }

    /**
     * Returns an indication if Storage Adapter acts as an external clustered data-source.
     * <code>true</code> if either indicated by a cluster-wide property or by enabling the mirror
     * service in the cluster.
     *
     * @return <code>true</code> if Storage Adapter is an external clustered data-source;
     * <code>false</code> otherwise.
     */
    public boolean isClusteredExternalDBEnabled() {
        return _isClusteredExternalDBEnabled;
    }

    /**
     * returns an indication if the underlying DB is a central DB common to several cluster members
     *
     * @return <code>true</code> if the underlying DB is cental; <code>false</code> otherwise.
     */
    public boolean isCentralDB() {
        return _isCentralDB;
    }

    /**
     * returns an indication if the underlying data-source supports versioning.
     *
     * @return <code>true</code> if the underlying DB supports versioning; <code>false</code>
     * otherwise.
     */
    public boolean isVersionedExternalDB() {
        return _isVersionedExternalDB;
    }

    public boolean isEmptyAfterInitialLoadStage() {
        return _emptyAfterInitialLoadStage;
    }

    public boolean isDirectPersistencyEmbeddedtHandlerUsed() {
        return _directPersistencyEmbeddedtHandlerUsed;
    }

    /**
     * get the lock manager
     */
    public IBasicLockManager<IEntryHolder> getLockManager() {
        return _lockManager;
    }

    private void touch(EvictableServerEntry entry, boolean isUpdate) {
        if (!isMemorySpace() && ((EvictableEntryCacheInfo) entry).isTransient())
            return;

        if (_evictionStrategy.requiresConcurrencyProtection()) {
            if (((EvictableEntryCacheInfo) entry).permitCallingEvictionStrategy()) {
                try {
                    if (isUpdate)
                        _evictionStrategy.onUpdate(entry);
                    else
                        _evictionStrategy.onRead(entry);
                } finally {
                    ((EvictableEntryCacheInfo) entry).afterCallingEvictionStrategy();
                }
            }
        } else {
            if (isUpdate)
                _evictionStrategy.onUpdate(entry);
            else
                _evictionStrategy.onRead(entry);
        }
    }

    public void touchByEntry(Context context, IEntryHolder entry, boolean modifyOp, ITemplateHolder template, CacheOperationReason cacheOperationReason) {
        if (entry.isBlobStoreEntry()) {
            if (template.getXidOriginatedTransaction() != null && cacheOperationReason != CacheOperationReason.ON_READ)
                return;   //NOTE- for read under xtn we touch, commit is meanningless
            ((IBlobStoreEntryHolder) entry).insertOrTouchInternalCache(context, this, cacheOperationReason);
        } else {
            IEntryCacheInfo pe = getPEntryByUid(entry.getUID());
            if (pe != null && pe.getEntryHolder(this) == entry)
                touch(pe, modifyOp);
        }
    }

    private SpaceEvictionStrategy createEvictionStrategy(SpaceConfigReader configReader, Properties properties)
            throws CreateException {
        if (isAllInCachePolicy() || isBlobStoreCachePolicy())
            return new AllInCacheSpaceEvictionStrategy();

        if (getCachePolicy() == CACHE_POLICY_LRU) {
            int touchThreashold = configReader.getIntSpaceProperty(CACHE_MANAGER_LRU_TOUCH_THRESHOLD_PROP,
                    CACHE_MANAGER_LRU_TOUCH_THRESHOLD_DEFAULT);
            return new ConcurrentLruSpaceEvictionStrategy(touchThreashold, getMaxCacheSize());
        }

        //user defined injected eviction policy
        SpaceEvictionStrategy injEvictionStrategy = (SpaceEvictionStrategy) properties.get(CACHE_MANAGER_EVICTION_STRATEGY_PROP);
        if (injEvictionStrategy != null)
            return injEvictionStrategy;

        //user defined class name eviction policy
        String evictor = configReader.getSpaceProperty(CACHE_MANAGER_EVICTION_STRATEGY_CLASS_PROP, "");

        if (evictor == null || evictor.length() == 0)
            throw new RuntimeException("invalid eviction strategy value specified " + evictor);
        if (evictor.equalsIgnoreCase(DefaultTimeBasedSpaceEvictionStrategy.class.getName()))
            return new DefaultTimeBasedSpaceEvictionStrategy(configReader);

        try {
            Class<?> evClass = ClassLoaderHelper.loadClass(evictor);
            Object newInstance = evClass.newInstance();
            return (SpaceEvictionStrategy) newInstance;
        } catch (Exception e) {
            throw new CreateException("Failed to load eviction strategy class", e);
        }
    }


    private BlobStoreStorageHandler createBlobStoreHandlerNewInterface(SpaceConfigReader configReader, Properties properties)
            throws CreateException {

        BlobStoreStorageHandler res = null;
        try {
            if (!isBlobStoreCachePolicy())
                return null;

            //user defined injected blobStore handler
            res = (BlobStoreStorageHandler) properties.get(CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_PROP);
            if (res != null)
                return res;

            //user defined class name blobStore handler
            String oh = (String) properties.get(CACHE_MANAGER_BLOBSTORE_STORAGE_HANDLER_CLASS_PROP);

//TEMP for QA
            if (_blobStoreForQa && (oh == null || oh.length() == 0))
                oh = BlobStoreHashMock.class.getName();
//oh = BlobStoreNoSerializationHashMock.class.getName();

            if (oh == null || oh.length() == 0)
                throw new RuntimeException("invalid blob-store storage handler value specified " + oh);
            if (oh.indexOf("BlobStoreStorageHashMock") != -1) {
                res = new BlobStoreHashMock();
                return res;
            }

            try {
                Class<?> ohClass = ClassLoaderHelper.loadClass(oh);
                Object newInstance = ohClass.newInstance();
                res = (BlobStoreStorageHandler) newInstance;
                return res;
            } catch (Exception e) {
                throw new CreateException("Failed to load blob-store storage handler class", e);
            }
        } finally {
            if (res != null && _logger.isInfoEnabled()) {
                _logger.info("created blob-store handler class=" + res.getClass().getName() + " persistentBlobStore=" + _persistentBlobStore);
            }

        }
    }

    public IBlobStoreCacheHandler getBlobStoreInternalCache() {
        return _blobStoreInternalCache;
    }

    public boolean hasBlobStoreOffHeapCache() {
        return _blobStoreStorageHandler.getOffHeapCache() != null;
    }

    public boolean hasBlobStoreOffHeapStore() {
        return _blobStoreStorageHandler.getOffHeapStore() != null;
    }

    public String getConfigInfo() {
        return "policy=" + getPolicy() + ", persistency-mode=" + getPersistencyMode();
    }

    private String getPolicy() {
        if (isAllInCachePolicy())
            return "all-in-cache";
        if (getCachePolicy() == CACHE_POLICY_LRU)
            return ("lru(cache-size=" + m_CacheSize + ")");
        if (isPluggedEvictionPolicy())
            return "custom(cache-size=" + m_CacheSize + ")";
        return "blob-store";
    }

    private String getPersistencyMode() {
        if (_isMemorySA)
            return "memory";
        if (isBlobStoreCachePolicy())
            return "blob-store";
        if (isCacheExternalDB())
            return "external";
        return ("jdbc");
    }

    /**
     * Load the data from the database
     */
    private void loadDataFromDB(Context context, SpaceConfigReader configReader) throws SAException {
        if(this.getEngine().getSpaceImpl().getQuiesceHandler().isHorizontalScale()){
            return;
        }

        InitialLoadInfo initialLoadInfo = new InitialLoadInfo(_logger, _logRecoveryProcess, _recoveryLogInterval);
        if (isBlobStoreCachePolicy())
            initialLoadInfo.setBlobStoreFifoInitialLoader(new BlobStoreFifoInitialLoader());
        context.setInitialLoadInfo(initialLoadInfo);
        // if cache policy is ALL_IN_CACHE- load all entries to cache from SA
        if (isResidentEntriesCachePolicy()) {
            //special first call to SA- SA will return all classes, not just fifo classes
            residentEntriesInitialLoad(context, configReader, initialLoadInfo);
            if (isBlobStoreCachePolicy() && _persistentBlobStore && _engine.getSpaceImpl().isPrimary() && _entries.isEmpty()) {
                //if persistent blobstore is empty try mirror if applicable
                if (((IBlobStoreStorageAdapter) _storageAdapter).hasAnotherInitialLoadSource() && _logger.isInfoEnabled())
                    _logger.info("persistent blob store is empty, trying the mirror initial load source");
                residentEntriesInitialLoad(context, configReader, initialLoadInfo);
            }

        } //if isResidentEntriesCachePolicy

        // if cache policy is LRU- load some entries to cache from SA according to setting
        if (isEvictableCachePolicy())
            evictableEntriesInitialLoad(context, configReader, initialLoadInfo);


        if (_logger.isInfoEnabled()) {
            String formattedErrors = format(initialLoadInfo.getInitialLoadErrors());
            _logger.info("Data source recovery:\n " +
                    "\tEntries found in data source: " + initialLoadInfo.getFoundInDatabase() + ".\n" +
                    "\tEntries inserted to space: " + initialLoadInfo.getInsertedToCache() + ".\n" +
                    "\tEntries ignored: " + (initialLoadInfo.getFoundInDatabase() - initialLoadInfo.getInsertedToCache()) + ".\n" +
                    formattedErrors +
                    "\tTotal Time: " + JSpaceUtilities.formatMillis(SystemTime.timeMillis() - initialLoadInfo.getRecoveryStartTime()) + ".");
        }
        if (getBlobStoreInternalCache() != null) {
            if (getBlobStoreInternalCache().getBlobStoreInternalCacheFilter() != null) {
                if (_logger.isInfoEnabled()) {
                    _logger.info("BlobStore internal cache recovery:\n " +
                            "\tblob-store-queries: " + getBlobStoreInternalCache().getBlobStoreInternalCacheFilter().getSqlQueries() + ".\n" +
                            "\tEntries inserted to blobstore cache: " + getBlobStoreInternalCache().getBlobStoreInternalCacheFilter().getInsertedToBlobStoreInternalCacheOnInitialLoad() + ".\n");
                }
            }
        }
    }


    private void residentEntriesInitialLoad(Context context, SpaceConfigReader configReader, InitialLoadInfo initialLoadInfo)
            throws SAException {
        //special first call to SA- SA will return all classes, not just fifo classes
        boolean isFifo = true;

        ITemplateHolder th = TemplateHolderFactory.createEmptyTemplateHolder(
                _engine,
                _engine.generateUid(),
                Long.MAX_VALUE /* expiration time*/,
                isFifo);

        ISAdapterIterator<IEntryHolder> entriesIterSA = null;
        try {
            context.setInInitialLoad(true);
            entriesIterSA = _storageAdapter.initialLoad(context, th);
            if (entriesIterSA != null) {
                IServerTypeDesc serverTypeDesc = null;
                Set<String> typesIn = _persistentBlobStore ? new HashSet<String>() : null;
                while (true) {
                    IEntryHolder eh = entriesIterSA.next();
                    if (eh == null) {
                        break;
                    }
                    initialLoadInfo.incrementFoundInDatabase();
                    //Verify that entry read
                    //from the DB belongs to this partition
                    if (_engine.isPartitionedSpace() && !eh.isBlobStoreEntry()) {
                        if (serverTypeDesc == null || !serverTypeDesc.getTypeName().equals(eh.getClassName()))
                            serverTypeDesc = _typeManager.getServerTypeDesc(eh.getClassName());
                        if(!serverTypeDesc.getTypeDesc().isBroadcast()) {
                            if (eh.getRoutingValue() == null) {
                                initialLoadInfo.getInitialLoadErrors().add("Object without routing  -  [" + eh.getClassName() + ":" + eh.getUID() + "]");
                                continue;
                            }
                            if (!_engine.isEntryFromPartition(eh))
                                continue;
                        }
                    }
                    if (_entries.containsKey(eh.getUID())) {
                        initialLoadInfo.getInitialLoadErrors().add("Object with duplicate uid -  [" + eh.getClassName() + ":" + eh.getUID() + "]");
                        continue;
                    }
                    boolean entryFromBlobStore = false;
                    if (isBlobStoreCachePolicy()) {
                        if (eh.isBlobStoreEntry()) {//recovered directly from SSD/blobStore
                            entryFromBlobStore = true;
                            if (eh.getServerTypeDesc().isFifoSupported() || eh.getServerTypeDesc().getTypeDesc().getFifoGroupingPropertyPath() != null) {//fifo or f-g need to sort
                                if (initialLoadInfo.getCurTypeData() == null || initialLoadInfo.getCurDesc() != eh.getServerTypeDesc()) {
                                    initialLoadInfo.setCurTypeData(_typeDataMap.get(eh.getServerTypeDesc()));
                                    initialLoadInfo.setCurDesc(eh.getServerTypeDesc());
                                }
                                if (isDirectPersistencyEmbeddedtHandlerUsed() && !_replicationNode.getDirectPesistencySyncHandler().getEmbeddedSyncHandler().getInitialLoadHandler().onLoadingEntry(eh))
                                    continue; //embedded sync list- entry shouldnt be inserted- its a phantom

                                initialLoadInfo.getBlobStoreFifoInitialLoader().add(eh, initialLoadInfo.getCurTypeData());
                                continue;
                            }
                        } else {
                            eh = new BlobStoreEntryHolder(eh);
                            EntryCacheInfoFactory.createBlobStoreEntryCacheInfo(eh);
                        }
                    }
                    boolean insertBlobStoreEntryToCache = true;
                    // handle initial load entry with embedded sync list
                    if (eh.isBlobStoreEntry() && isDirectPersistencyEmbeddedtHandlerUsed()) {
                        insertBlobStoreEntryToCache = _replicationNode.getDirectPesistencySyncHandler().getEmbeddedSyncHandler().getInitialLoadHandler().onLoadingEntry(eh);
                    }
                    if (insertBlobStoreEntryToCache) {
                        safeInsertEntryToCache(context, eh, false /* newEntry */, null /*pType*/, false /*pin*/, entryFromBlobStore ? InitialLoadOrigin.FROM_BLOBSTORE : InitialLoadOrigin.FROM_NON_BLOBSTORE /*fromInitialLoad*/);
                    } else {
                        continue;
                    }
                    if (isBlobStoreCachePolicy()) {//kick out the entry main info after writing to blobStore if needed
                        if (!entryFromBlobStore)//no need to rewrite-it
                            ((IBlobStoreEntryHolder) eh).setDirty(this);

                        ((IBlobStoreEntryHolder) eh).getBlobStoreResidentPart().unLoadFullEntryIfPossible(this, context);
                        if (!entryFromBlobStore && typesIn != null)
                            insertMetadataTypeToBlobstoreIfNeeded(eh, typesIn);
                    }
                    initialLoadInfo.incrementInsertedToCache();
                    initialLoadInfo.setLastLoggedTime(logInsertionIfNeeded(initialLoadInfo.getRecoveryStartTime(), initialLoadInfo.getLastLoggedTime(), initialLoadInfo.getInsertedToCache()));

                } //while
            }
            //any sorted blobStore entries ?
            if (isBlobStoreCachePolicy() && !initialLoadInfo.getBlobStoreFifoInitialLoader().isEmpty()) {
                BlobStoreFifoInitialLoader blobStoreFifoInitialLoader = initialLoadInfo.getBlobStoreFifoInitialLoader();
                boolean avoidInsertToBlobStoreInternalCache = true;
                while (blobStoreFifoInitialLoader.hasNext()) {
                    IEntryHolder eh = blobStoreFifoInitialLoader.next();
                    safeInsertEntryToCache(context, eh, false /* newEntry */, null /*pType*/, false /*pin*/, InitialLoadOrigin.FROM_BLOBSTORE /*fromInitialLoad*/);

                    ((IBlobStoreEntryHolder) eh).getBlobStoreResidentPart().unLoadFullEntryIfPossible(this, context);
                    initialLoadInfo.incrementInsertedToCache();
                    initialLoadInfo.setLastLoggedTime(logInsertionIfNeeded(initialLoadInfo.getRecoveryStartTime(), initialLoadInfo.getLastLoggedTime(), initialLoadInfo.getInsertedToCache()));
                }
            }
        } finally {
            context.setInInitialLoad(false);
            if (entriesIterSA != null) {
                entriesIterSA.close();
            }
        }
    }

    //in case types loaded from mirror verify they reside in ssd
    private void insertMetadataTypeToBlobstoreIfNeeded(IEntryHolder eh, Set<String> typesIn) {
        if (!eh.getServerTypeDesc().getTypeDesc().isBlobstoreEnabled() || typesIn.contains(eh.getServerTypeDesc().getTypeDesc().getTypeName()))
            return;

        final ITypeDesc typeDescriptor = eh.getServerTypeDesc().getTypeDesc();
        final String[] superClassesNames = typeDescriptor.getRestrictSuperClassesNames();
        if (superClassesNames != null) {
            for (String superClassName : superClassesNames) {
                if (_typeManager.getServerTypeDesc(superClassName) == null)
                    throw new IllegalArgumentException("Missing super class type descriptor ["
                            + superClassName
                            + "] for type ["
                            + typeDescriptor.getTypeName() + "]");
                if (!typesIn.add(superClassName))
                    continue;
                _storageAdapter.introduceDataType(_typeManager.getServerTypeDesc(superClassName).getTypeDesc());

            }
        }
        typesIn.add(eh.getServerTypeDesc().getTypeDesc().getTypeName());
        _storageAdapter.introduceDataType(eh.getServerTypeDesc().getTypeDesc());

    }


    private void evictableEntriesInitialLoad(Context context, SpaceConfigReader configReader, InitialLoadInfo initialLoadInfo)
            throws SAException {
        int initial_lru_load = 0;
        int initial_lru_load_p = configReader.getIntSpaceProperty(CACHE_MANAGER_INITIAL_LOAD_PROP, CACHE_MANAGER_INITIAL_LOAD_DEFAULT);
        if (initial_lru_load_p > 0 && initial_lru_load_p <= 100)
            initial_lru_load = (m_CacheSize * initial_lru_load_p) / 100;
        int need_load = initial_lru_load - _cacheSize.get();

        IServerTypeDesc templateType = null;
        String initial_load_class = null;


        if (need_load > 0) //specific class desired ?
        {
            initial_load_class = configReader.getSpaceProperty(CACHE_MANAGER_INITIAL_LOAD_CLASS_PROP, "_");
            if ("_".equals(initial_load_class))
                initial_load_class = null;
            else {
                templateType = _typeManager.getServerTypeDesc(initial_load_class);
                if (templateType == null)
                    need_load = 0;
            }
        }

        if (need_load > 0) //load some
        {
            ITemplateHolder th = null;
            if (initial_load_class == null) {//null template
                th = TemplateHolderFactory.createEmptyTemplateHolder(
                        _engine,
                        _engine.generateUid(),
                        Long.MAX_VALUE /* expiration time*/,
                        false /*isFifo*/);

                templateType = _typeManager.getServerTypeDesc(IServerTypeDesc.ROOT_TYPE_NAME);
            } else {//non-null template
                ITemplatePacket ep = new TemplatePacket(templateType.getTypeDesc());

                th = TemplateHolderFactory.createTemplateHolder(templateType, ep,
                        _engine.generateUid(),
                        Long.MAX_VALUE /* expiration time*/);
            }

            ISAdapterIterator<IEntryHolder> entriesIterSA = null;
            try {

                context.setInInitialLoad(true);
                entriesIterSA = _storageAdapter.initialLoad(context, th);
                if (entriesIterSA != null) {
                    IServerTypeDesc serverTypeDesc = null;

                    while (true) {
                        IEntryHolder eh = entriesIterSA.next();
                        if (eh == null)
                            break;

                        initialLoadInfo.incrementFoundInDatabase();
                        //in case of a central DB verify that entry read
                        //from the DB belongs to this partition
                        if (_engine.isPartitionedSpace()) {
                            if (serverTypeDesc == null || !serverTypeDesc.getTypeName().equals(eh.getClassName()))
                                serverTypeDesc = _typeManager.getServerTypeDesc(eh.getClassName());

                            if (eh.getRoutingValue() == null) {
                                initialLoadInfo.getInitialLoadErrors().add("Object without routing -  [" + eh.getClassName() + ":" + eh.getUID() + "]");
                                continue;
                            }
                            if (!_engine.isEntryFromPartition(eh))
                                continue;
                        }
                        if (_entries.containsKey(eh.getUID())) {
                            initialLoadInfo.getInitialLoadErrors().add("Object with duplicate uid -  [" + eh.getClassName() + ":" + eh.getUID() + "]");
                            continue;
                        }

                        if (_engine.getMemoryManager().isEnabled() && _engine.getMemoryManager().monitorMemoryUsageWithNoEviction(true)) {
                            // evict should be called, instead just break from the while -- stop loading
                            // more elements.
                            break;
                        }
                        safeInsertEntryToCache(context, eh, false /* newEntry */,
                                null /*pType*/, false/*pin*/, InitialLoadOrigin.FROM_NON_BLOBSTORE /*fromInitialLoad*/);
                        initialLoadInfo.incrementInsertedToCache();
                        initialLoadInfo.setLastLoggedTime(logInsertionIfNeeded(initialLoadInfo.getRecoveryStartTime(), initialLoadInfo.getLastLoggedTime(), initialLoadInfo.getInsertedToCache()));
                        if (--need_load <= 0)
                            break;
                    } //while
                }
            } finally {
                context.setInInitialLoad(false);
                if (entriesIterSA != null) {
                    entriesIterSA.close();
                }
            }
        }//if (need_load > 0)

    }

    private String format(LinkedList<String> initialLoadErrors) {
        if (initialLoadErrors.isEmpty())
            return "";
        StringBuilder sb = new StringBuilder();
        sb.append("\t[\n");
        for (Iterator<String> iterator = initialLoadErrors.iterator(); iterator.hasNext(); ) {
            String error = iterator.next();
            sb.append("\t\t" + error + "\n");
        }
        sb.append("\t]\n");
        return sb.toString();
    }


    private long logInsertionIfNeeded(long startLogTime, long lastLogTime, int fetchedEntries) {
        if (_logRecoveryProcess && _logger.isInfoEnabled()) {
            long curTime = SystemTime.timeMillis();
            if (curTime - lastLogTime > _recoveryLogInterval) {
                _logger.info("Entries loaded so far: " + fetchedEntries + " [" + JSpaceUtilities.formatMillis(curTime - startLogTime) + "]");
                return curTime;
            }
        }
        return lastLogTime;
    }


    /**
     * shut down cache manager. does not throw any exception- unusable.
     */
    public void shutDown() {

        if (isBlobStoreCachePolicy() && getBlobStoreStorageHandler() != null && (hasBlobStoreOffHeapCache() || hasBlobStoreOffHeapStore())) {
            freeOffHeapCache();
        }

        if (_fifoBackgroundDispatcher != null)
            _fifoBackgroundDispatcher.close();

        if (_templateExpirationManager != null)
            _templateExpirationManager.shutDown();

        try {
            _persistentGC.shutdown();
        } catch (Exception ex) {
            if (_logger.isDebugEnabled())
                _logger.debug(ex.toString(), ex);
        } finally {
            _persistentGC = null;
        }

        try {
            _cacheContextFactory.closeAllContexts();
        } catch (Exception ex) {
            if (_logger.isDebugEnabled())
                _logger.debug(ex.toString(), ex);
        }

        try {
            _storageAdapter.shutDown();
        } catch (Exception ex) {
            if (_logger.isDebugEnabled())
                _logger.debug(ex.toString(), ex);
        }

        if (_evictionStrategy != null)
            _evictionStrategy.close();

        if (_replicationNode != null && _replicationNode.getDirectPesistencySyncHandler() != null)
            _replicationNode.getDirectPesistencySyncHandler().close();
    }

    /**
     * returns true if recentDeletes hash in engine need to be used.
     */
    public boolean useRecentDeletes() {
        return isEvictableCachePolicy() && !_isMemorySA && (!_readOnlySA || _engine.hasMirror());
    }

    public void insertToRecentDeletes(IEntryHolder entry, long duration, ServerTransaction committingXtn) {
        _recentDeletesRespository.insertToRecentDeletes(entry, duration, committingXtn);
    }

    public boolean removeFromRecentDeletes(IEntryHolder entry) {
        return _recentDeletesRespository.removeFromRecentDeletes(entry);
    }

    public Iterator<RecentDeletesRepository.RecentDeleteInfo> getRecentDeletesIterator() {
        return _recentDeletesRespository.getRecentDeletesIterator();
    }

    public RecentDeletesRepository.RecentDeleteInfo getRecentDeleteInfo(String uid) {
        return _recentDeletesRespository.getRecentDeleteInfo(uid);
    }

    public int getNumOfRecentDeletes() {
        return _recentDeletesRespository.size();
    }

    /**
     * returns true if recentUpdates hash in engine need to be used.
     */
    public boolean useRecentUpdatesForPinning() {
        return isEvictableCachePolicy() && !_isMemorySA && (!_readOnlySA || _engine.hasMirror());
    }

    public void insertToRecentUpdates(IEntryHolder entry, long duration, ServerTransaction committingXtn) {
        _recentUpdatesRespository.insertToRecentUpdates(entry, duration, committingXtn);
    }

    public void insertToRecentUpdatesIfNeeded(IEntryHolder entry, long duration, ServerTransaction committingXtn) {
        if (useRecentUpdatesForPinning())
            insertToRecentUpdates(entry, duration, committingXtn);
    }

    private boolean removeFromRecentUpdates(IEntryHolder entry) {
        return _recentUpdatesRespository.removeFromRecentUpdates(entry);
    }

    public boolean removeFromRecentUpdatesIfNeeded(IEntryHolder entry) {
        return useRecentUpdatesForPinning() ? _recentUpdatesRespository.removeFromRecentUpdates(entry) : false;
    }

    public Iterator<RecentUpdatesRepository.RecentUpdateInfo> getRecentUpdatesIterator() {
        return _recentUpdatesRespository.getRecentUpdatesIterator();
    }

    public RecentUpdatesRepository.RecentUpdateInfo getRecentUpdateInfo(String uid) {
        return _recentUpdatesRespository.getRecentUpdateInfo(uid);
    }

    public int getNumOfRecentUpdates() {
        return _recentUpdatesRespository.size();
    }

    public boolean isEntryInRecentUpdates(IEntryHolder entry) {
        return (getRecentUpdateInfo(entry.getUID()) != null);
    }


    public boolean isDummyEntry(IEntryHolder eh) {
        if (!useRecentDeletes() || !eh.isDeleted())
            return false;
        IEntryCacheInfo pEntry = getPEntryByUid(eh.getUID());
        return pEntry != null && pEntry.isRecentDelete();
    }


    /**
     * evict a quota of entries from cache.
     *
     * @return number of actual evicted
     */
    @Override
    public int evictBatch(int evictionQuota) {
        return _evictionStrategy.evict(evictionQuota);
    }


    /**
     * insert an entry to the space.
     */
    public void insertEntry(Context context, IEntryHolder entryHolder, boolean shouldReplicate, boolean origin, boolean suppliedUid)
            throws SAException, EntryAlreadyInSpaceException {
        validateEntryCanBeWrittenToCache(entryHolder);

        final TypeData typeData = _typeDataMap.get(entryHolder.getServerTypeDesc());

        if (entryHolder.getXidOriginatedTransaction() != null) {
            entryHolder.setMaybeUnderXtn(true);
            entryHolder.setWriteLockOwnerAndOperation(entryHolder.getXidOriginated(), SpaceOperations.WRITE, false /*createSnapshot*/);
        }

        if (entryHolder.getXidOriginated() != null)
            entryHolder.getXidOriginated().setOperatedUpon();

        IEntryCacheInfo pE = null;

        pE = insertEntryToCache(context, entryHolder, true /* newEntry */,
                typeData, true /*pin*/, InitialLoadOrigin.NON /*fromInitialLoad*/);

        if (pE == _entryAlreadyInSpaceIndication) {
            throw new EntryAlreadyInSpaceException(entryHolder.getUID(), entryHolder.getClassName());
        }

        if (entryHolder.getXidOriginatedTransaction() == null) {
            try {
                if (entryHolder.isBlobStoreEntry() && isDirectPersistencyEmbeddedtHandlerUsed() && context.isActiveBlobStoreBulk())
                    context.setForBulkInsert(entryHolder);
                _storageAdapter.insertEntry(context, entryHolder, origin, shouldReplicate);
                //If should be sent to cluster
                if (shouldReplicate && !context.isDelayedReplicationForbulkOpUsed())
                    handleInsertEntryReplication(context, entryHolder);
            } catch (SAException ex) {
                if (!entryHolder.isDeleted()) {
                    removeEntryFromCache(entryHolder);
                }
                context.resetRecentFifoObject();

                throw ex;
            }


        }//if (entryHolder.m_XidOriginated == null)
    }

    public void handleInsertEntryReplication(Context context, IEntryHolder entryHolder) throws SAException {
        handleInsertEntryReplication(context, entryHolder, 0);
    }

    public void handleInsertEntryReplication(Context context, IEntryHolder entryHolder, int blobstoreBulkId) throws SAException {
        final IReplicationOutContext replCtx = getReplicationContext(context);
        updateReplicationContext(replCtx, context);
        attachBlobstoreReplicationBulkIdIfNeeded(replCtx, blobstoreBulkId);
        if (requiresEvictionReplicationProtection())
            replCtx.askForMarker(getEvictionPolicyReplicationMembersGroupName());

        //Notify cluster node manager of operation
        _replicationNode.outWriteEntry(replCtx, entryHolder);

        if (requiresEvictionReplicationProtection()) {
            final IMarker marker = replCtx.getRequestedMarker();
            getEvictionReplicationsMarkersRepository().insert(entryHolder.getUID(), marker, false /*alreadyLocked*/);
        }
    }

    private void attachBlobstoreReplicationBulkIdIfNeeded(IReplicationOutContext outContext, int blobstoreBulkId) {
        boolean attachBlobstoreBulkId = useBlobStoreReplicationBackupBulks() && _engine.getSpaceImpl().isPrimary();
        outContext.setBlobstoreReplicationBulkId(attachBlobstoreBulkId ? blobstoreBulkId : 0);
    }


    public String getEvictionPolicyReplicationMembersGroupName() {
        return ReplicationNodeConfigBuilder.PERSISTENCY_GROUPING_NAME;
    }

    /**
     * update entry- if the entry should be updated under a xtn - insert to xtn lists. update entry
     * cache, call SA to perform update NOTE- if the template is under xtn associateEntryWithXtn is
     * called by the engine- and not this routine
     */

    public IEntryHolder updateEntry(Context context, IEntryHolder entry, ITemplateHolder template, boolean shouldReplicate, boolean origin)
            throws SAException {
        IEntryCacheInfo pEntry = null;
        IEntryHolder new_eh = null;

        IEntryData originalData = entry.getEntryData();
        try {

            if (entry.isBlobStoreEntry()) {
                pEntry = ((IBlobStoreEntryHolder) entry).getBlobStoreResidentPart();
                context.setBlobStoreOriginalEntryInfo(originalData, ((IBlobStoreEntryHolder) entry).getBlobStoreVersion());
            } else if (isAllInCachePolicy())
                pEntry = getPEntryByUid(entry.getUID());

            IEntryData newEntryData = template.getUpdatedEntry().getEntryData();

            pEntry = updateEntryInCache(context, pEntry, pEntry != null ? pEntry.getEntryHolder(this) : entry, newEntryData, newEntryData.getExpirationTime(), template.getOperationModifiers());
            new_eh = pEntry.getEntryHolder(this);
            if (entry.isBlobStoreEntry() && isDirectPersistencyEmbeddedtHandlerUsed() && context.isActiveBlobStoreBulk())
                context.setForBulkUpdate(new_eh, originalData, template.getMutators());
            _storageAdapter.updateEntry(context, new_eh, shouldReplicate, origin, context.getPartialUpdatedValuesIndicators());

            if (shouldReplicate && !context.isDelayedReplicationForbulkOpUsed())
                handleUpdateEntryReplication(context, new_eh, originalData, template.getMutators());
        } catch (Exception ex) {
            if (ex instanceof DuplicateIndexValueException)
                throw (RuntimeException) ex; //mending inconsistent state already tried in update-references

            try {
                if (entry.getEntryData().getVersion() == template.getUpdatedEntry().getEntryData().getVersion()) {//when RuntimeException is thrown we revert the references to the before update situation
                    // in the TypeData::updateEntryReferences
                    if (isEvictableCachePolicy()) {
                        removeEntryFromCache(entry);
                    } else {
                        updateEntryInCache(context, null, entry,
                                originalData,
                                template.getUpdatedEntry().getEntryData().getExpirationTime(),
                                template.getOperationModifiers());
                    }
                }
            } catch (Exception ex_)  //show the original cause, ignore the new
            {
                if (ex instanceof SAException)
                    ex = new SAException("Original exception=" + ex + " while trying to revert caught: " + ex_, ex);
                else
                    ex = new RuntimeException("Original exception=" + ex + " while trying to revert caught: " + ex_, ex);
            }

            if (ex instanceof SAException)
                throw (SAException) ex;

            if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            throw new RuntimeException(ex);
        }

        return new_eh;

    }

    public void handleUpdateEntryReplication(Context context, IEntryHolder new_eh, IEntryData originalData, Collection<SpaceEntryMutator> mutators) {
        handleUpdateEntryReplication(context, new_eh, originalData, mutators, 0);
    }

    public void handleUpdateEntryReplication(Context context, IEntryHolder new_eh, IEntryData originalData, Collection<SpaceEntryMutator> mutators, int blobstoreBulkId) {
        boolean isChange = mutators != null;
        final IReplicationOutContext replCtx = getReplicationContext(context);
        attachBlobstoreReplicationBulkIdIfNeeded(replCtx, blobstoreBulkId);
        if (isChange)
            updateReplicationContextForChangeEntry(replCtx,
                    originalData,
                    context,
                    mutators);
        else
            updateReplicationContextForUpdateEntry(replCtx,
                    originalData,
                    context,
                    context.getPartialUpdatedValuesIndicators());

        if (requiresEvictionReplicationProtection())
            replCtx.askForMarker(getEvictionPolicyReplicationMembersGroupName());

        //Notify cluster node manager of operation
        if (isChange)
            _replicationNode.outChangeEntry(replCtx, new_eh);
        else
            _replicationNode.outUpdateEntry(replCtx, new_eh);

        if (requiresEvictionReplicationProtection()) {
            final IMarker marker = replCtx.getRequestedMarker();
            getEvictionReplicationsMarkersRepository().insert(new_eh.getUID(), marker, false /*alreadyLocked*/);
        }
    }


    //make an entry holder containning values after operating mutators on values
    public IEntryHolder makeChangeOpEntry(Context context, ITemplateHolder template, IEntryHolder currentEntry) {
        context.setChangeResultsForCurrentEntry(null);  //reset for this entry
        //create a clone entry data
        ITransactionalEntryData edata = (ITransactionalEntryData) currentEntry.getEntryData();
        int versionID;
        if (_engine.isLocalCache() || context.isFromReplication())
            versionID = template.getEntryData().getVersion(); // local cache mode, template must contain version-id
        else
            versionID = Math.max(template.getEntryData().getVersion() + 1, 2);

        ITransactionalEntryData udata = (template.getChangeExpiration() != UPDATE_NO_LEASE) ?
                edata.createShallowClonedCopyWithSuppliedVersionAndExpiration(versionID, template.getChangeExpiration()) :
                edata.createShallowClonedCopyWithSuppliedVersion(versionID);

        if(udata instanceof HybridEntryData) {
            IEntryData cachedView = context.getCacheViewEntryDataIfNeeded(edata);
            MutableViewHybridEntryData mutableViewEntryData = new MutableViewHybridEntryData();
            if (cachedView instanceof ViewPropertiesEntryData) {
                mutableViewEntryData.view(udata, (ViewPropertiesEntryData) cachedView);
            } else {
                mutableViewEntryData.view(udata);
            }
            applyChangeMutators(context, template, mutableViewEntryData);

            if (mutableViewEntryData.isDeserialized()){
                udata.setFixedPropertyValues(mutableViewEntryData.getFixedPropertiesValues());
            } else {
                ((HybridEntryData) udata).setFixedPropertyValues(mutableViewEntryData.getPropertiesHolder().getNonSerializedProperties(),
                        mutableViewEntryData.getPropertiesHolder().getPackedSerializedProperties());
            }

        } else {
            applyChangeMutators(context, template, udata);
        }

        return EntryHolderFactory.createEntryHolder(currentEntry.getServerTypeDesc(),
                udata,
                currentEntry.getUID(),
                currentEntry.isTransient());
    }

    private void applyChangeMutators(Context context, ITemplateHolder template, ITransactionalEntryData entryData) {
        List<Object> results = null;
        try {
            int i = 0;
            for (SpaceEntryMutator mutator : template.getMutators()) {
                Object result = mutator.change(entryData);
                if (Modifiers.contains(template.getOperationModifiers(), Modifiers.RETURN_DETAILED_CHANGE_RESULT)) {
                    if (result != null) {//set returned result list on a need-to basis
                        if (results == null) {
                            results = new ArrayList<Object>(template.getMutators().size());
                            if (i > 0) {
                                for (int j = 0; j < i; j++)
                                    results.add(null);
                            }
                        }
                    }
                    if (results != null)
                        results.add(result);
                    i++;
                }
            }
        } catch (RuntimeException e) {
            throw new ChangeInternalException(e);
        }
        context.setChangeResultsForCurrentEntry(results);  //set for this entry
    }

    /**
     * insert a template to the  cache and DB.
     */
    public void insertTemplate(Context context, ITemplateHolder templateHolder, boolean shouldReplicate) {
        final NotifyTemplateHolder notifyTemplateHolder = templateHolder.isNotifyTemplate() ? (NotifyTemplateHolder) templateHolder : null;
        TemplateCacheInfo pTemplate = null;
        if (templateHolder.isNotifyTemplate()) {
            pTemplate = new TemplateCacheInfo(templateHolder);
            TemplateCacheInfo oldValue = _templatesManager.putIfAbsent(pTemplate);
            if (oldValue != null) {
                if (_logger.isWarnEnabled())
                    _logger.warn("notify template already in space, uid=" + oldValue.m_TemplateHolder.getUID() + " , in class: " + oldValue.m_TemplateHolder.getClassName());

                return;
            }
        }

        /** indication whether this template is inserted to the ~cache */
        templateHolder.setInCache();
        if (!templateHolder.isExplicitInsertionToExpirationManager())
            _templateExpirationManager.addTemplate(templateHolder);

        if (templateHolder.isNotifyTemplate() && templateHolder.getXidOriginatedTransaction() == null && shouldReplicate) {
            final IReplicationOutContext replCtx = getReplicationContext(context);
            updateReplicationContext(replCtx, context);
            //Notify cluster node manager of operation
            _replicationNode.outInsertNotifyTemplate(replCtx, notifyTemplateHolder);
        }

        if (templateHolder.getXidOriginated() != null && templateHolder.isNotifyTemplate()) {
            //set xtn as operated upon- proxy will send abort to an operation with timeout
            templateHolder.getXidOriginated().setOperatedUpon();
        }

        //BUG Here? what happens if storage adapter throws exception after update, the
        //packet is in the replication redo log already
        if (pTemplate == null) {
            pTemplate = new TemplateCacheInfo(templateHolder);
            _templatesManager.put(pTemplate);
        }

        insertTemplateToCache(context, pTemplate);

        //if sync packets for sync replication- do it
        if (!context.isSyncReplFromMultipleOperation() && shouldReplicate)
            _engine.performReplication(context);

        if (notifyTemplateHolder != null) {
            CustomInfo customInfo = notifyTemplateHolder.getNotifyInfo().getCustomInfo();
            if (customInfo != null && customInfo.isLocalCacheCustomInfo()) {
                _localCacheRegistrations.add(notifyTemplateHolder);
            }
        }
    }

    /**
     * @param lockedEntry - true if the entry is locked by the caller
     */
    public IEntryHolder getEntry(Context context, IEntryHolder entryHolder, boolean tryInsertToCache, boolean lockedEntry, boolean useOnlyCache)
            throws SAException {
        if (entryHolder.isBlobStoreEntry()) {
            //off heap entry- use the input holder to get a fresh version iff different
            context.setBlobStoreOpParams(((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart(), entryHolder, (tryInsertToCache && lockedEntry));
            return _storageAdapter.getEntry(context, entryHolder.getUID(), entryHolder.getClassName(), entryHolder);
        }
        //!!!!!NOTE- if its a off-heap entry it will not be full entry -its ok since the entry lock is not valid
        IEntryHolder entry = null;
        IEntryCacheInfo pEntry = null;
        // first we search entry in cache.
        pEntry = getPEntryByUid(entryHolder.getUID());
        if (pEntry != null) {
            if (lockedEntry) {
                if (pEntry.isDeleted())
                    return null;
            } else {
                if (pEntry.isRecentDelete())
                    return null;
            }

            if (!_isMemorySA && useOnlyCache && !lockedEntry && isEvictableCachePolicy() && !isResidentCacheEntry(pEntry))
                return null;

            if (lockedEntry && tryInsertToCache) {//locked entry
                if (isEvictableCachePolicy()) {
                    //pin entry for an intrusive op'
                    if (!pEntry.setPinned(true, !isMemorySpace() /*waitIfPendingInsertion*/))
                        pEntry = null; //failed- entry currently irrelevant
                } else {
                    if (pEntry.isBlobStoreEntry()) {//case of update/change when the entry passed is a template with only uid
                        context.setBlobStoreOpParams(pEntry, null, true);
                        return _storageAdapter.getEntry(context, entryHolder.getUID(), entryHolder.getClassName(), entryHolder);
                    }
                }
            }
            if (pEntry != null)
                return pEntry.getEntryHolder(this);
        } //if (pEntry != null)
        if (!isEvictableCachePolicy() || _isMemorySA)
            return null;   //no relevant entry found

        if (useOnlyCache)
            return null;

        //if transient, don't refer to the DB
        if (entryHolder.isTransient())
            return null;

        if (!lockedEntry && context.getPrefetchedNonBlobStoreEntries() != null)
            entry = context.getPrefetchedNonBlobStoreEntries().get(entryHolder.getUID());
            //use space uid
        else
            entry = _storageAdapter.getEntry(context, entryHolder.getUID(), entryHolder.getClassName(), entryHolder);

        if (entry == null)
            return null;


        if (tryInsertToCache) {
            pEntry = safeInsertEntryToCache(context, entry, false /* newEntry */
                    , null /*pType*/, lockedEntry /*pin*/, InitialLoadOrigin.NON /*fromInitialLoad*/);
            if (pEntry == null)
                return null;         //entry deleted
        }

        return pEntry != null ? pEntry.getEntryHolder(this) : entry;
    }

    public IEntryHolder getEntry(Context context, IEntryHolder entryHolder, boolean tryInsertToCache, boolean lockedEntry)
            throws SAException {
        return getEntry(context, entryHolder, tryInsertToCache, lockedEntry, false /*useOnlyCache*/);


    }

    /**
     * get entry by its uid, and, optionally, by its template className is passed  if known, can be
     * null template is not null if isCacheExternalDB is true: external DB, the template must
     * contain  the PK fields insertToCache- true if entry shouldbe inserted to cache
     */
    public IEntryHolder getEntry(Context context, String uid, String inputClassName, IEntryHolder template, boolean tryInsertToCache,
                                 boolean lockedEntry, boolean useOnlyCache)
            throws SAException {
        // first we search entry in cache.
        IEntryCacheInfo pEntry = getPEntryByUid(uid);
        if (pEntry != null) {
            if (lockedEntry) {
                if (pEntry.isDeleted())
                    return null;
            } else {
                if (pEntry.isRecentDelete())
                    return null;
            }
            if (!_isMemorySA && useOnlyCache && !lockedEntry && isEvictableCachePolicy() && !isResidentCacheEntry(pEntry))
                return null;

            if (lockedEntry && tryInsertToCache) {//locked entry
                if (isEvictableCachePolicy()) {
                    //pin entry for an intrusive op'
                    if (!pEntry.setPinned(true, !isMemorySpace() /*waitIfPendingInsertion*/))
                        pEntry = null; //failed- entry currently irrelevant
                } else {
                    if (pEntry.isBlobStoreEntry()) {//case of update/change when the entry passed is a template with only uid
                        context.setBlobStoreOpParams(pEntry, null, true);
                        return _storageAdapter.getEntry(context, uid, inputClassName, template);
                    }
                }
            }
            if (pEntry != null) {
                if (pEntry.isBlobStoreEntry()) {
                    boolean indexesPartOnly = (template instanceof ITemplateHolder) && BlobStoreOperationOptimizations.isConsiderOptimizedForBlobstore(_engine, context, (ITemplateHolder) template, pEntry);
                    IEntryHolder latestEntryVersion = ((IBlobStoreRefCacheInfo) pEntry).getLatestEntryVersion(this, false, null, null, indexesPartOnly);
                    return latestEntryVersion;
                } else {
                    return pEntry.getEntryHolder(this);
                }
            }

        } //if (pEntry != null)
        if (!isEvictableCachePolicy() || _isMemorySA)
            return null;

        if (useOnlyCache)
            return null;

        //if transient, don't refer to the DB
        if (template != null && template.isTransient())
            return null;

        IEntryHolder entry;
        // entry not found in cache - search in real SA
        // first see if entry was read previously from SA (multiple ids operations)
        // if its external cache DB use the template
        // else locate entry class using the UID, if its external cache DB use the template
        if (!lockedEntry && context.getPrefetchedNonBlobStoreEntries() != null)
            entry = context.getPrefetchedNonBlobStoreEntries().get(uid);
        else
            entry = _storageAdapter.getEntry(context, uid, inputClassName, template);

        if (entry == null)
            return null;

        if (tryInsertToCache) {
            pEntry = safeInsertEntryToCache(context, entry, false /* newEntry */
                    , null /*pType*/, lockedEntry /*pin*/, InitialLoadOrigin.NON /*fromInitialLoad*/);
            if (pEntry == null)
                return null;         //entry deleted
        }

        return pEntry != null ? pEntry.getEntryHolder(this) : entry;
    }

    /**
     * returns a list of all the pending templates for the given class
     */
    public List<TemplateInfo> getTemplatesInfo(String className) {
        IServerTypeDesc serverTypeDesc = _typeManager.getServerTypeDesc(className);
        TypeData typeData = _typeDataMap.get(serverTypeDesc);
        List<TemplateInfo> templatesList = new ArrayList<TemplateInfo>(0);

        // load templates info from type data
        if (typeData != null)
            typeData.fillTemplatesInfo(templatesList);


        // load durable notifications templates info from replication node
        loadDurableNotificationTemplatesInfo(serverTypeDesc, templatesList);

        return templatesList;

    }

    private void loadDurableNotificationTemplatesInfo(IServerTypeDesc typeDesc, List<TemplateInfo> templatesList) {
        String groupName = getEngine().generateGroupName();
        if (!StringUtils.hasText(groupName))
            return;
        DynamicSourceGroupConfigHolder groupConfig = getEngine().getReplicationNode().getAdmin().getSourceGroupConfigHolder(groupName);
        Map<String, IReplicationChannelDataFilter> filters = groupConfig.getConfig().getFilters();
        for (IReplicationChannelDataFilter filter : filters.values()) {
            if (filter instanceof NotificationReplicationChannelDataFilter) {
                NotificationReplicationChannelDataFilter notifyFilter = (NotificationReplicationChannelDataFilter) filter;
                if (notifyFilter.isTemplateOfType(typeDesc.getTypeName()))
                    templatesList.add(notifyFilter.createTemplateInfo());
            }
        }
    }

    public ITemplateHolder getTemplate(String uid) {
        return _templatesManager.getTemplate(uid);
    }

    //return shortest SL of matching entries
    public IScanListIterator<IEntryCacheInfo> getMatchingMemoryEntriesForScanning(Context context, IServerTypeDesc currServerTypeDesc, ITemplateHolder template, IServerTypeDesc serverTypeDesc) {
        int numOfFields = template.getClassName() != null ? serverTypeDesc.getTypeDesc().getNumOfFixedProperties() : 0;

        TypeData typeData = _typeDataMap.get(currServerTypeDesc);
        if (isBlobStoreCachePolicy() && typeData != null && !typeData.isBlobStoreClass())
            context.setDisableBlobStorePreFetching(true);

        if ( _engine.getMetricManager().getMetricFlagsState().isDataIndexHitsMetricEnabled()) {
            context.setIndexMetricsContext(new IndexMetricsContext(typeData));
        }

        IScanListIterator<IEntryCacheInfo> result;
        if (template.getExtendedMatchCodes() == null)
            result = typeData == null ? null : getScannableEntriesMinIndex(context, typeData, numOfFields, template);
        else
            result = typeData == null ? null : getScannableEntriesMinIndexExtended(context, typeData, numOfFields, template);

        return result;
    }


    /**
     * remove entry from cache & SA
     *
     * @param disableSAcall - if true don't call the underlying SA
     * @return number of completed sync replications.
     */
    public int removeEntry(Context context, IEntryHolder entryHolder, IEntryCacheInfo pEntry, boolean shouldReplicate,
                           boolean origin, EntryRemoveReasonCodes removeReason, boolean disableSAcall)
            throws SAException {
        boolean is_writing_xtn = entryHolder.getXidOriginatedTransaction() != null;
        XtnEntry xtnEntry = null;
        boolean updated_recent_deletes = false;

        if (entryHolder.hasShadow()) {    //get rid of the shadow of this entry
            TypeData typeData = _typeDataMap.get(entryHolder.getServerTypeDesc());
            if (pEntry == null)
                pEntry = getPEntryByUid(entryHolder.getUID());
            consolidateWithShadowEntry(typeData, pEntry, false /*restoreOriginalValus*/, false /*onError*/);
        }

        boolean leaseExpiration = removeReason == EntryRemoveReasonCodes.LEASE_CANCEL || removeReason == EntryRemoveReasonCodes.LEASE_EXPIRED;
        try {
            if (is_writing_xtn) {
                xtnEntry = entryHolder.getXidOriginated();
                is_writing_xtn = xtnEntry != null;
            }
            if (!is_writing_xtn) {
                if (useRecentDeletes() && !entryHolder.isTransient() && !(leaseExpiration && isCacheExternalDB())) {
                    insertToRecentDeletes(entryHolder, requiresEvictionReplicationProtection() ? Long.MAX_VALUE : 0, context.getCommittingXtn());
                    updated_recent_deletes = true;
                }
                if (!disableSAcall) {
                    if (entryHolder.isBlobStoreEntry() && isDirectPersistencyEmbeddedtHandlerUsed() && context.isActiveBlobStoreBulk())
                        context.setForBulkRemove(entryHolder, removeReason);
                    _storageAdapter.removeEntry(context, entryHolder, origin, leaseExpiration, shouldReplicate);
                    if (shouldReplicate && !context.isDelayedReplicationForbulkOpUsed())
                        handleRemoveEntryReplication(context, entryHolder, removeReason);
                }
            } else//if (!is_writing_xtn)
            {
                if (xtnEntry.m_AlreadyPrepared) {
                    if (useRecentDeletes() && !entryHolder.isTransient() && !(leaseExpiration && isCacheExternalDB())) {
                        insertToRecentDeletes(entryHolder, requiresEvictionReplicationProtection() ? Long.MAX_VALUE : 0, context.getCommittingXtn());
                        updated_recent_deletes = true;
                    }
                    boolean actualUpdateRedoLog = shouldReplicate;
                    if (!xtnEntry.m_SingleParticipant) {
                        if (xtnEntry.getStatus() != XtnStatus.COMMITED && xtnEntry.getStatus() != XtnStatus.COMMITING)
                            actualUpdateRedoLog = false;

                    }
                    if (!disableSAcall) {
                        if (entryHolder.isBlobStoreEntry() && isDirectPersistencyEmbeddedtHandlerUsed() && context.isActiveBlobStoreBulk())
                            context.setForBulkRemove(entryHolder, removeReason);
                        _storageAdapter.removeEntry(context, entryHolder, origin, leaseExpiration, actualUpdateRedoLog);

                        if (actualUpdateRedoLog && !context.isDelayedReplicationForbulkOpUsed())
                            handleRemoveEntryReplication(context, entryHolder, removeReason);
                    }
                }
            }
        } catch (Exception ex) {
            //revert
            if (updated_recent_deletes)
                removeFromRecentDeletes(entryHolder);

            if (ex instanceof SAException)
                throw (SAException) ex;

            throw new RuntimeException(ex);
        }

        RecentDeleteCodes recentDeleteUsage = updated_recent_deletes ? RecentDeleteCodes.INSERT_DUMMY : RecentDeleteCodes.NONE;

        if (!entryHolder.isBlobStoreEntry() || context.getBlobStoreBulkInfo() == null || (((IBlobStoreEntryHolder) entryHolder).getBulkInfo() == null && !context.getBlobStoreBulkInfo().wasEntryRemovedInChunk(entryHolder.getUID())))
            //in case of blob-store bulk remove the entry from cache only after the bulk op performed
            removeEntryFromCache(entryHolder, false /*initiatedByEvictionStrategy*/, true/*locked*/, pEntry/* pEntry*/, recentDeleteUsage);

        /** perform sync-repl if need */
        if (!context.isSyncReplFromMultipleOperation() && !context.isDisableSyncReplication() && shouldReplicate /* don't replicate if not needed */) {
            return _engine.performReplication(context);

        }
        return 0;
    }

    public void handleRemoveEntryReplication(Context context, IEntryHolder entryHolder, EntryRemoveReasonCodes removeReason) {
        handleRemoveEntryReplication(context, entryHolder, removeReason, 0);
    }

    public void handleRemoveEntryReplication(Context context, IEntryHolder entryHolder, EntryRemoveReasonCodes removeReason, int blobstoreBulkId) {
        IMarker marker = null;
        try {
            final IReplicationOutContext replContext = getReplicationContext(context);
            attachBlobstoreReplicationBulkIdIfNeeded(replContext, blobstoreBulkId);
            updateReplicationContext(replContext, context);
            //Notify cluster node manager of operation
            if (removeReason == EntryRemoveReasonCodes.LEASE_CANCEL) {
                _replicationNode.outCancelEntryLease(replContext,
                        entryHolder);
            } else if (removeReason == EntryRemoveReasonCodes.LEASE_EXPIRED) {
                _replicationNode.outEntryLeaseExpired(replContext,
                        entryHolder);

            } else {
                if (requiresEvictionReplicationProtection())
                    replContext.askForMarker(getEvictionPolicyReplicationMembersGroupName());

                _replicationNode.outRemoveEntry(replContext,
                        entryHolder);

                if (requiresEvictionReplicationProtection())
                    marker = replContext.getRequestedMarker();
            }
        } finally {
            if (requiresEvictionReplicationProtection() && marker != null)
                getEvictionReplicationsMarkersRepository().insert(entryHolder.getUID(), marker, false /*alreadyLocked*/);
        }

    }

    /**
     * Marks template as DELETED and send a BUS Packet to the GC. Assumes the specified template is
     * locked.
     */
    public void removeTemplate(Context context, ITemplateHolder template, boolean fromReplication, boolean origin, boolean dontReplicate) {
        removeTemplate(context, template, fromReplication, origin, dontReplicate,
                TemplateRemoveReasonCodes.OPERATED_OR_TIMEDOUT);
    }

    /**
     * Marks template as DELETED and send a BUS Packet to the GC. Assumes the specified template is
     * locked.
     */
    public void removeTemplate(Context context, ITemplateHolder template, boolean fromReplication,
                               boolean origin, boolean dontReplicate, TemplateRemoveReasonCodes removeReason) {
        // mark template as deleted
        template.setDeleted(true);

        // handle waiting for entries, if exist
        if (template.isHasWaitingFor()) {
            // create and dispatch RemoveWaitingForInfoSABusPacket
            // so that the removal of waiting for info will be done
            // asynchronously
            RemoveWaitingForInfoSABusPacket packet =
                    new RemoveWaitingForInfoSABusPacket(context.getOperationID(), null, template);
            _engine.getProcessorWG().enqueueBlocked(packet);

        } /* if (entry.isHasWaitingFor()) */


        boolean updateRedoLog = false;
        if (!dontReplicate) {
            if (template.isNotifyTemplate()) {
                boolean replicate = ((NotifyTemplateHolder) template).isReplicateNotify();
                updateRedoLog = _engine.isReplicated() && !fromReplication && replicate;
            } else
                updateRedoLog = false;
        }

        if (template.isInExpirationManager())
            _templateExpirationManager.removeTemplate(template);

        removeTemplate(context, template, updateRedoLog, origin, removeReason);
    }

    private void removeTemplate(Context context, ITemplateHolder template, boolean updateRedoLog,
                                boolean origin, TemplateRemoveReasonCodes removeReason) {
        final boolean pureTransient = template.isTransient() && !updateRedoLog && origin;
        if (template.isNotifyTemplate() && template.getXidOriginatedTransaction() == null) {
            if (!pureTransient && (!_isMemorySA || _clusterPolicy != null)) {
                if (updateRedoLog) {
                    final NotifyTemplateHolder notifyTemplateHolder = (NotifyTemplateHolder) template;
                    final IReplicationOutContext replCtx = getReplicationContext(context);
                    updateReplicationContext(replCtx, context);
                    //Notify cluster node manager of operation
                    if (removeReason == TemplateRemoveReasonCodes.LEASE_CANCEL)
                        _replicationNode.outRemoveNotifyTemplate(replCtx, notifyTemplateHolder);
                    else
                        _replicationNode.outNotifyTemplateLeaseExpired(replCtx, notifyTemplateHolder);
                }
            }
        }
        //BUG Here? what happens if storage adapter throws exception after update, the
        //packet is in the replication redo log already
        removeTemplateFromCache(template);

        if (template.isNotifyTemplate()) {
            NotifyTemplateHolder notifyTemplate = (NotifyTemplateHolder) template;
            // close the filter
            INotifyDelegatorFilter filter = notifyTemplate.getFilter();
            if (filter != null) {

                try {
                    filter.close();
                } catch (Throwable e) {
                    _logger.debug("closing user filter caused an exception", e);
                }
            }

            if (notifyTemplate.getNotifyInfo().getCustomInfo() != null) {
                CustomInfo customInfo = notifyTemplate.getNotifyInfo().getCustomInfo();
                if (customInfo != null && customInfo.isLocalCacheCustomInfo()) {
                    _localCacheRegistrations.remove(notifyTemplate);
                }
            }
        }

        //if sync packets for sync replication- do it
        if (!context.isSyncReplFromMultipleOperation() && updateRedoLog)
            _engine.performReplication(context);
    }

    public FifoBackgroundDispatcher getFifoBackgroundDispatcher() {
        return _fifoBackgroundDispatcher;
    }

    public void makeWaitForInfo(Context context, IEntryHolder entryHolder, ITemplateHolder templateHolder) throws SAException {
        entryHolder.addTemplateWaitingForEntry(templateHolder);
        templateHolder.addEntryWaitingForTemplate(entryHolder);
    }

    public void removeWaitingForInfo(Context context, IEntryHolder entryHolder, ITemplateHolder templateHolder, boolean unpinIfPossible) {
        entryHolder.removeTemplateWaitingForEntry(templateHolder);

        if (!entryHolder.isHasWaitingFor() && !entryHolder.isMaybeUnderXtn())
            entryHolder.resetEntryXtnInfo();

        templateHolder.removeEntryWaitingForTemplate(entryHolder);

        // unpin entry if relevant
        if (unpinIfPossible)
            unpinIfNeeded(context, entryHolder, null /*template*/, null /*EntryCacheInfo*/);


    }

    /**
     * tie an entry with a transaction. new_content is the new values in case of update operation
     */
    public IEntryHolder associateEntryWithXtn(Context context, IEntryHolder entryHolder, ITemplateHolder template,
                                              XtnEntry xtnEntry, IEntryHolder new_content)
            throws SAException {
        final XtnData pXtn = xtnEntry.getXtnData();

        xtnEntry.setOperatedUpon();
        TypeData typeData = _typeDataMap.get(entryHolder.getServerTypeDesc());

        IEntryCacheInfo pEntry = getPEntryByUid(entryHolder.getUID());
        if (!pEntry.isPinned())
            throw new RuntimeException("associateEntryWithXtn: internal error- entry uid =" + pEntry.getUID() + " not pinned");


        if (!pXtn.isLockedEntry(pEntry)) {
            lockEntry(pXtn, pEntry, context.getOperationID());
        } else {
            updateLock(pXtn, pEntry, context.getOperationID(), template.getTemplateOperation());
        }

        switch (template.getTemplateOperation()) {
            case SpaceOperations.READ:
            case SpaceOperations.READ_IE:
                if (pEntry.getEntryHolder(this).getWriteLockTransaction() == null ||
                        !pEntry.getEntryHolder(this).getWriteLockTransaction().equals(xtnEntry.m_Transaction)) {
                    //List<XtnEntry> readLocks = pEntry.getEntryHolder(this).getReadLockOwners();
                    if (ReadModifiers.isExclusiveReadLock(template.getOperationModifiers())) {//exclusive r-lock
                        pEntry.getEntryHolder(this).clearReadLockOwners();
                        pEntry.getEntryHolder(this).setWriteLockOwnerAndOperation(xtnEntry, template.getTemplateOperation());
                    } else //regular read
                    {
                        pEntry.getEntryHolder(this).addReadLockOwner(pXtn.getXtnEntry());
                    }
                }
                break;

            case SpaceOperations.TAKE:
            case SpaceOperations.TAKE_IE:
                // if entry was updated previously under the same xtn
                // and it has a shadow- restore shadow values in order not
                // to cause dummy WF connections
                boolean restoreShadowValues = false;
                if (pEntry.getEntryHolder(this).getWriteLockTransaction() != null && pEntry.getEntryHolder(this).hasShadow())
                    if (pEntry.getEntryHolder(this).getWriteLockOperation() == SpaceOperations.UPDATE && pEntry.getEntryHolder(this).getWriteLockTransaction().equals(xtnEntry.m_Transaction))
                        restoreShadowValues = true;

                pEntry.getEntryHolder(this).clearReadLockOwners();

                pEntry.getEntryHolder(this).setWriteLockOwnerAndOperation(xtnEntry, template.getTemplateOperation());

                if (restoreShadowValues) {
                    consolidateWithShadowEntry(typeData, pEntry, true /* restoreOriginalValus*/, false /*onError*/);
                    //remove indication of rewritten entry (if exists)
                    pXtn.removeRewrittenEntryIndication(pEntry.getUID());
                }
                pXtn.addToTakenEntriesIfNotInside(pEntry);
                break;

            case SpaceOperations.UPDATE:
                pEntry.getEntryHolder(this).clearReadLockOwners();

                boolean newShadow = true;
                if (pEntry.getEntryHolder(this).getWriteLockOperation() == SpaceOperations.WRITE &&
                        pEntry.getEntryHolder(this).getWriteLockTransaction() != null &&
                        pEntry.getEntryHolder(this).getWriteLockTransaction().equals(xtnEntry.m_Transaction))
                    newShadow = false;

                if (pEntry.getEntryHolder(this).getWriteLockOperation() == SpaceOperations.UPDATE &&
                        pEntry.getEntryHolder(this).getWriteLockTransaction() != null && //Already performed an update, so we have a shadow
                        pEntry.getEntryHolder(this).getWriteLockTransaction().equals(xtnEntry.m_Transaction) &&
                        pEntry.getEntryHolder(this).hasShadow())
                    newShadow = false;

                if ((pEntry.getEntryHolder(this).getWriteLockOperation() == SpaceOperations.TAKE ||
                        pEntry.getEntryHolder(this).getWriteLockOperation() == SpaceOperations.TAKE_IE) &&
                        pEntry.getEntryHolder(this).getWriteLockTransaction() != null &&
                        pEntry.getEntryHolder(this).getWriteLockTransaction().equals(xtnEntry.m_Transaction))
                //take + write under same xtn, signal "rewrite"
                {
                    pXtn.signalRewrittenEntry(pEntry.getUID());
                    pXtn.removeTakenEntry(pEntry); //no longer its a take
                }
                //cater for partial update

                if (newShadow) {
                    if (getTypeData(pEntry.getEntryHolder(this).getServerTypeDesc()).hasFifoGroupingIndex())
                        pXtn.addToEntriesForFifoGroupScan(pEntry.getEntryHolder(this)); //kepp the shadow value for f-g scans
                    //create a shadow entry
                    ShadowEntryHolder shadowEh = createShadowEntry(pEntry, typeData);
                    pEntry.getEntryHolder(this).setWriteLockOwnerOperationAndShadow(xtnEntry, template.getTemplateOperation(), shadowEh);
                }

                //we set this flag here before the actual update of the entry
                pEntry.getEntryHolder(this).setMaybeUnderXtn(true);
                IEntryData new_content_data = new_content.getEntryData();
                pEntry = updateEntryInCache(context, pEntry, pEntry.getEntryHolder(this), new_content_data, new_content_data.getExpirationTime(), template.getOperationModifiers());
                if (template.isChange())
                    pXtn.setInPlaceUpdatedEntry(pEntry.getEntryHolder(this), template.getMutators());
                else
                    pXtn.setUpdatedEntry(pEntry.getEntryHolder(this), context.getPartialUpdatedValuesIndicators());
                break;
        } //switch (templateOperation)

        pEntry.getEntryHolder(this).setMaybeUnderXtn(true);
        return pEntry.getEntryHolder(this);
    }


    /**
     * create a shadow entry from a master entry before its update
     */
    private ShadowEntryHolder createShadowEntry(IEntryCacheInfo pmaster, TypeData pType) {
        IEntryHolder master = pmaster.getEntryHolder(this);
        int[] backrefIndexPos = pType.createIndexBackreferenceArray(pmaster, master.getEntryData());
        return !master.isBlobStoreEntry() ? EntryHolderFactory.createShadowEntryHolder(master, pmaster.getBackRefs(), backrefIndexPos, pmaster.getLeaseManagerListRef(), pmaster.getLeaseManagerPosition())
                : EntryHolderFactory.createShadowEntryHolder(master, pmaster.getBackRefs(), backrefIndexPos, null, pmaster.getLeaseManagerPosition());

    }


    public IEntryCacheInfo getPEntryByUid(String uid) {
        IEntryCacheInfo pEntry = _entries.get(uid);
        if (pEntry != null && isEvictableCachePolicy() && pEntry.isRemovingOrRemoved())
            return null;
        return pEntry;
    }

    public IEntryHolder getEntryByUidFromPureCache(String uid) {
        IEntryCacheInfo pEntry = getPEntryByUid(uid);
        return pEntry != null ? pEntry.getEntryHolder(this) : null;
    }

    /**
     * @param uid of entry to check
     * @return <code>true</code> if this entry is in pure cache; <code>false</code> otherwise.
     */
    public boolean isEntryInPureCache(String uid) {
        return _entries.containsKey(uid);
    }


    /**
     * disconnect an entry from xtn
     *
     * @param entryHolder - entry to disconnect
     * @param xtnEnd      - true if its part of commit/rb process
     */

    public void disconnectEntryFromXtn(Context context, IEntryHolder entryHolder, XtnEntry xtnEntry, boolean xtnEnd)
            throws SAException {
        IEntryCacheInfo pEntry = getPEntryByUid(entryHolder.getUID());
        if (pEntry == null)
            return; //already disconnected (writelock replaced)
        XtnData pXtn = xtnEntry.getXtnData();

        if (!xtnEnd)
            removeLockedEntry(pXtn, pEntry);

        if ((pEntry.getEntryHolder(this).getWriteLockTransaction() == null) ||
                !pEntry.getEntryHolder(this).getWriteLockTransaction().equals(pXtn.getXtn())) {
            pEntry.getEntryHolder(this).removeReadLockOwner(pXtn.getXtnEntry());
        }

        if ((pEntry.getEntryHolder(this).getWriteLockTransaction() != null) &&
                pEntry.getEntryHolder(this).getWriteLockTransaction().equals(pXtn.getXtn())) {
            pEntry.getEntryHolder(this).resetWriteLockOwner();
        }

        if (xtnEntry == pEntry.getEntryHolder(this).getXidOriginated())
            pEntry.getEntryHolder(this).resetXidOriginated();

        pEntry.getEntryHolder(this).setMaybeUnderXtn(pEntry.getEntryHolder(this).anyReadLockXtn() || pEntry.getEntryHolder(this).getWriteLockTransaction() != null);

        // unpin entry if relevant
        if (!pEntry.getEntryHolder(this).isMaybeUnderXtn()) {
            if (!pEntry.getEntryHolder(this).isHasWaitingFor()) {
                pEntry.getEntryHolder(this).resetEntryXtnInfo();
                if (pEntry.isPinned() && xtnEnd)
                    unpinIfNeeded(context, pEntry.getEntryHolder(this), null /*template*/, pEntry);
            }
        }
    }


    public void prepare(Context context, XtnEntry xtnEntry, boolean supportsTwoPhaseReplication, boolean handleReplication, boolean handleSA) throws SAException {

        if ((_isMemorySA || _storageAdapter.isReadOnly()) && !handleReplication)
            return;

        // if transaction came from replication - don't write it to redo log
        final boolean singleParticipant = xtnEntry.m_SingleParticipant;
        // Determine if this operation needs to be replicated

        final boolean replicationIsRelevant = _engine.isReplicated() && !xtnEntry.isFromReplication();
        final boolean needsToBeReplicated = replicationIsRelevant && (singleParticipant || (supportsTwoPhaseReplication && !replicateOnePhaseCommit()));

        if (!needsToBeReplicated && _isMemorySA)
            return;
        // Set from-gateway state on context
        context.setFromGateway(xtnEntry.isFromGateway());

        ServerTransaction xtn = xtnEntry.m_Transaction;

        final XtnData pXtn = xtnEntry.getXtnData();

		/* prepare vector of locked entries*/
        final ArrayList<IEntryHolder> pLocked = createLockedEntriesList(xtnEntry, pXtn);

        if (requiresEvictionReplicationProtection())
            xtnEntry.getXtnData().setEntriesForReplicationIn2PCommit(pLocked);

        //No changes under transaction, no need to replicate or send to DB.
        if (pLocked == null || pLocked.size() == 0)
            return;

        if (handleSA && !_isMemorySA)
            _storageAdapter.prepare(context, xtn, pLocked, singleParticipant, pXtn.getUpdatedEntries(), needsToBeReplicated);

        if (!handleReplication)
            return;

        if (needsToBeReplicated) {
            final OperationID[] opIDs = createOperationIdsArray(pXtn, pLocked);
            // build updateRedoLog indicators
            final boolean[] updateRedoLog = createUpdateRedologArray(pLocked);
            final IReplicationOutContext replContext = getReplicationContext(context);
            updateReplicationContextForTransaction(replContext, context, updateRedoLog, pXtn.getUpdatedEntries(), opIDs, xtnEntry);
            if (singleParticipant) {
                if (requiresEvictionReplicationProtection())
                    replContext.askForMarker(getEvictionPolicyReplicationMembersGroupName());
                //If single participant we replicate this as a one phase commit transaction and add to replication backlog here
                _replicationNode.outTransaction(replContext, xtn, pLocked);

                if (requiresEvictionReplicationProtection()) {
                    final IMarker marker = replContext.getRequestedMarker();
                    insertTransactionMarkerToRepository(marker, pLocked, xtnEntry.m_Transaction);
                }
            } else {
                //Otherwise we insert the transaction prepare to backlog
                _replicationNode.outTransactionPrepare(replContext, xtn, pLocked);
            }
            // if SyncPackets for sync replication added-accumulate them in context
            if (_syncReplicationUsed && singleParticipant) {
                /**
                 * Only if holdTxnLock enabled, lock the transaction table until the sync-replication will
                 * be finished.
                 * NOTE: Very dangerous property! May cause to replication dead-locks.
                 **/
                if (_clusterPolicy.m_ReplicationPolicy.m_SyncReplPolicy.isHoldTxnLockUntilSyncReplication() &&
                        !context.isSyncReplFromMultipleOperation())
                    _engine.performReplication(context);
            }
        }


    }

    private boolean replicateOnePhaseCommit() {
        return _engine.getClusterPolicy().getReplicationPolicy().isReplicateOnePhaseCommit();
    }

    private boolean[] createUpdateRedologArray(
            final ArrayList<IEntryHolder> pLocked) {
        Map<String, IServerTypeDesc> typeTable = _typeManager.getSafeTypeTable();
        boolean[] updateRedoLog = new boolean[pLocked.size()];

        for (int i = 0; i < pLocked.size(); i++) {
            final IEntryHolder entryHolder = pLocked.get(i);

            if (_engine.isReplicated() && entryHolder.getWriteLockTransaction() != null) {
                // entry is write-locked by this transaction
                if (_clusterPolicy.m_ReplicationPolicy.isFullReplication()) {
                    updateRedoLog[i] = true;
                } else { // partial replication
                    IServerTypeDesc serverTypeDesc = typeTable.get(entryHolder.getClassName());
                    updateRedoLog[i] = serverTypeDesc.getTypeDesc().isReplicable();
                }
            }
        }
        return updateRedoLog;
    }

    private OperationID[] createOperationIdsArray(final XtnData pXtn,
                                                  final ArrayList<IEntryHolder> pLocked) {
        OperationID[] opIDs = new OperationID[pLocked.size()];
        for (int i = 0; i < pLocked.size(); i++) {
            opIDs[i] = pXtn.getOperationID(pLocked.get(i).getUID());
        }
        return opIDs;
    }

    private ArrayList<IEntryHolder> createLockedEntriesList(XtnEntry xtnEntry, XtnData pXtn) {
        ArrayList<IEntryHolder> pLocked = null;
        IStoredList<IEntryCacheInfo> lockedEntries = pXtn.getLockedEntries();
        if (lockedEntries != null && !lockedEntries.isEmpty()) {
            pLocked = new ArrayList<IEntryHolder>();
            for (IStoredListIterator<IEntryCacheInfo> slh = lockedEntries.establishListScan(false); slh != null; slh = pXtn.getLockedEntries().next(slh)) {
                IEntryCacheInfo pEntry = slh.getSubject();
                if (pEntry == null)
                    continue;
                IEntryHolder entryHolder = pEntry.getEntryHolder(this);

                if (entryHolder.isDeleted() ||
                        entryHolder.getWriteLockOwner() != xtnEntry ||
                        entryHolder.getWriteLockOperation() == SpaceOperations.READ || //exclusive rl- not relevant
                        entryHolder.getWriteLockOperation() == SpaceOperations.READ_IE)
                    continue;

                // Since this is a transaction, we save the entry's previous version for handling
                // version conflicts in replication targets in a case where entry's version was increased
                // by more than +1 due to several update operations within the same transaction.
                pLocked.add(new ReplicationEntryHolder(pEntry.getEntryHolder(this), xtnEntry));
            }
        }
        return pLocked;
    }

    /**
     * this method is called by the engine in order to make a "snapshot" of entries needed for
     * notify.
     */
    public void prePrepare(XtnEntry xtnEntry) {
        XtnData pXtn = xtnEntry.getXtnData();

        IStoredList<IEntryCacheInfo> locked = pXtn.getLockedEntries();
        if (locked != null && !locked.isEmpty()) {
            for (IStoredListIterator<IEntryCacheInfo> slh = locked.establishListScan(false); slh != null; slh = pXtn.getLockedEntries().next(slh)) {
                IEntryCacheInfo pEntry = slh.getSubject();
                if (pEntry == null)
                    continue;
                final IEntryHolder eh = pEntry.getEntryHolder(this);
                if (eh.isDeleted()) //not valid
                    continue;
                if (eh.isExpired() && !_leaseManager.isNoReapUnderXtnLeases() && !_leaseManager.isSlaveLeaseManagerForEntries()) //not valid
                    continue;

                //is the checked xtn write-locking this entry ??
                if (eh.getWriteLockOwner() != xtnEntry) //not locked
                    continue;
                // nothing to prepare
                if (eh.getWriteLockOperation() == SpaceOperations.READ || eh.getWriteLockOperation() == SpaceOperations.READ_IE)
                    continue;
                // destructive operations
                if (eh.getWriteLockOperation() == SpaceOperations.WRITE || eh.getWriteLockOperation() == SpaceOperations.UPDATE
                        || eh.getWriteLockOperation() == SpaceOperations.TAKE || eh.getWriteLockOperation() == SpaceOperations.TAKE_IE) {
                    if (eh.getWriteLockOperation() == SpaceOperations.UPDATE && pXtn.isReWrittenEntry(eh.getUID())) {
                        //in case entry was taken + written under same xtn, notify for both (op code is update)
                        IEntryCacheInfo pe = EntryCacheInfoFactory.createEntryCacheInfo(eh.getShadow().createCopy());
                        pe.getEntryHolder(this).setWriteLockOperation(SpaceOperations.TAKE, false /*createSnapshot*/);
                        pe.getEntryHolder(this).setUID(eh.getUID());
                        pXtn.getNeedNotifyEntries(true).add(pe);
                        pe = EntryCacheInfoFactory.createEntryCacheInfo(eh.createCopy());
                        pe.getEntryHolder(this).setWriteLockOperation(SpaceOperations.WRITE, false /*createSnapshot*/);
                        pXtn.getNeedNotifyEntries(true).add(pe);
                    } else if (eh.getWriteLockOperation() == SpaceOperations.UPDATE && (_templatesManager.anyNotifyUnmatchedTemplates()//notify for UNMATCHED operation
                            || _templatesManager.anyNotifyMatchedTemplates() || _templatesManager.anyNotifyRematchedTemplates())) {
                        IEntryCacheInfo pe = EntryCacheInfoFactory.createEntryCacheInfo(eh.createCopy());
                        IEntryHolder shadowEh = eh.getShadow().createCopy();
                        pe.getEntryHolder(this).setWriteLockOwnerOperationAndShadow(xtnEntry, eh.getWriteLockOperation(), shadowEh);
                        pXtn.getNeedNotifyEntries(true).add(pe);
                    } else //notify for operation
                    {
                        IEntryCacheInfo pe = EntryCacheInfoFactory.createEntryCacheInfo(eh.createCopy());
                        pe.getEntryHolder(this).setWriteLockOperation(eh.getWriteLockOperation(), false /*createSnapshot*/);
                        pXtn.getNeedNotifyEntries(true).add(pe);
                    }
                }
            }
        }
    }

    /**
     * xtn terminating-handle under xtn updates.
     */
    public void handleUnderXtnUpdate(Context context, ServerTransaction xtn, IEntryHolder eh, boolean isCommitting, XtnData pXtn) {
        IEntryCacheInfo pEntry = getPEntryByUid(eh.getUID());
        //pEntry cannot be null- entry is locked and under xtn
        if (pEntry == null) {
            throw new RuntimeException("handleUnderXtnUpdate:internal error-Entry is null UID=" + eh.getUID());
        }
        if (isCommitting)
            insertToRecentUpdatesIfNeeded(eh, requiresEvictionReplicationProtection() ? Long.MAX_VALUE : 0, context.getCommittingXtn());
        TypeData typeData = _typeDataMap.get(eh.getServerTypeDesc());

        consolidateWithShadowEntry(typeData, pEntry, !isCommitting /*restoreOriginalValus*/, false/*onError*/);
    }

    public void preCommit(Context context, XtnEntry xtnEntry, boolean supportsTwoPhaseReplication) {
        final boolean needsToBeReplicated = _engine.isReplicated() && !xtnEntry.isFromReplication() && (replicateOnePhaseCommit() || !supportsTwoPhaseReplication);
        if (xtnEntry.m_SingleParticipant || !needsToBeReplicated)
            return;


        ServerTransaction xtn = xtnEntry.m_Transaction;
        XtnData pXtn = xtnEntry.getXtnData();
        /* prepare vector of locked entries*/
        final ArrayList<IEntryHolder> pLocked = createLockedEntriesList(xtnEntry, pXtn);

        //No changes under transaction, no need to replicate or send to DB.
        if (pLocked == null || pLocked.size() == 0)
            return;

        final OperationID[] opIDs = createOperationIdsArray(pXtn, pLocked);
        // build updateRedoLog indicators
        final boolean[] updateRedoLog = createUpdateRedologArray(pLocked);
        final IReplicationOutContext replContext = getReplicationContext(context);
        updateReplicationContextForTransaction(replContext, context, updateRedoLog, pXtn.getUpdatedEntries(), opIDs, xtnEntry);
        if (requiresEvictionReplicationProtection())
            replContext.askForMarker(getEvictionPolicyReplicationMembersGroupName());
        _replicationNode.outTransaction(replContext, xtn, pLocked);

        if (requiresEvictionReplicationProtection()) {
            final IMarker marker = replContext.getRequestedMarker();
            insertTransactionMarkerToRepository(marker, pLocked, xtnEntry.m_Transaction);
        }

        // if SyncPackets for sync replication added-accumulate them in context
        if (_syncReplicationUsed) {
            /**
             * Only if holdTxnLock enabled, lock the transaction table until the sync-replication will
             * be finished.
             * NOTE: Very dangerous property! May cause to replication dead-locks.
             **/
            if (_clusterPolicy.m_ReplicationPolicy.m_SyncReplPolicy.isHoldTxnLockUntilSyncReplication() &&
                    !context.isSyncReplFromMultipleOperation())
                _engine.performReplication(context);
        }
    }

    //insert the replicated entries to markers repository for a given xtn
    private void insertTransactionMarkerToRepository(IMarker marker, List<IEntryHolder> pLocked, ServerTransaction transaction) {
        if (pLocked == null)
            return;

        for (IEntryHolder entryHolder : pLocked) {
            if (entryHolder == null)
                continue;
            if (entryHolder.getWriteLockTransaction() == null
                    || !entryHolder.getWriteLockTransaction()
                    .equals(transaction))
                continue;

            int op = entryHolder.getWriteLockOperation();
            if (op != SpaceOperations.WRITE && op != SpaceOperations.UPDATE && op != SpaceOperations.TAKE &&
                    op != SpaceOperations.TAKE_IE)
                continue;
            getEvictionReplicationsMarkersRepository().insert(entryHolder.getUID(), marker, false /*alreadyLocked*/);
        }

    }


    public void commit(Context context, XtnEntry xtnEntry, boolean singleParticipant, boolean anyUpdates, boolean supportsTwoPhaseReplication) throws SAException {
        ServerTransaction xtn = xtnEntry.m_Transaction;

        boolean call_sa = !singleParticipant || !_engine.isTransactionalSA();

        if (call_sa) {
            if (!_isMemorySA)
                _storageAdapter.commit(xtn, anyUpdates);
        }

        final boolean needsToBeReplicated = _engine.isReplicated() && !xtnEntry.isFromReplication();

        //No need to replicate of no work done under transaction or single participants and not prepared
        if (xtnEntry.m_SingleParticipant || !needsToBeReplicated)
            return;

        // Set from-gateway state on context
        context.setFromGateway(xtnEntry.isFromGateway());

        if (!replicateOnePhaseCommit() && supportsTwoPhaseReplication) {
            //If no operations under this transaction no need to abort
            int numOfChangesUnderTransaction = xtnEntry.getNumOfChangesUnderTransaction(this);
            if (numOfChangesUnderTransaction == 0)
                return;

            final IReplicationOutContext replContext = getReplicationContext(context);
            updateReplicationContext(replContext, context);

            if (requiresEvictionReplicationProtection())
                replContext.askForMarker(getEvictionPolicyReplicationMembersGroupName());

            _replicationNode.outTransactionCommit(replContext, xtn);

            if (requiresEvictionReplicationProtection()) {
                final IMarker marker = replContext.getRequestedMarker();
                insertTransactionMarkerToRepository(marker, xtnEntry.getXtnData().getEntriesForReplicationIn2PCommit(), xtnEntry.m_Transaction);
            }

            // if SyncPackets for sync replication added-accumulate them in context
            if (_syncReplicationUsed) {
                /**
                 * Only if holdTxnLock enabled, lock the transaction table until the sync-replication will
                 * be finished.
                 * NOTE: Very dangerous property! May cause to replication dead-locks.
                 **/
                if (_clusterPolicy.m_ReplicationPolicy.m_SyncReplPolicy.isHoldTxnLockUntilSyncReplication() &&
                        !context.isSyncReplFromMultipleOperation())
                    _engine.performReplication(context);
            }
        }

    }

    public void rollback(Context context, XtnEntry xtnEntry, boolean alreadyPrepared, boolean anyUpdates, boolean supportsTwoPhaseReplication) throws SAException {
        ServerTransaction xtn = xtnEntry.m_Transaction;
        if (alreadyPrepared) {
            if (!_isMemorySA) {
                _storageAdapter.rollback(xtn, anyUpdates);
            }
        }

        final boolean useRedoLog = _engine.isReplicated() && !xtnEntry.isFromReplication();

        //No need to replicate of no work done under transaction or single participants and not prepared
        if (xtnEntry.m_SingleParticipant || !supportsTwoPhaseReplication || !alreadyPrepared || !useRedoLog)
            return;

        //If no operations under this transaction no need to abort
        int numOfChangesUnderTransaction = xtnEntry.getNumOfChangesUnderTransaction(this);
        if (numOfChangesUnderTransaction == 0)
            return;

        final IReplicationOutContext replContext = getReplicationContext(context);
        updateReplicationContext(replContext, context);

        _replicationNode.outTransactionAbort(replContext, xtn);
        _engine.performReplication(context);
    }

    //called from lease manager
    //entry must be locked && pinned when method called
    public void extendLeasePeriod(Context context, long time, long original_time, String uid, String classname, int objectType, boolean shouldReplicate, boolean origin) {
        ILeasedEntryCacheInfo leaseCacheInfo = null;
        IEntryHolder leasedObject = null;
        switch (objectType) {
            case ObjectTypes.ENTRY:

                IEntryCacheInfo pEntry = getPEntryByUid(uid);
                leaseCacheInfo = pEntry;
                IEntryHolder eh = pEntry.getEntryHolder(this);
                leasedObject = eh;
                if (shouldReplicate) {
                    final IReplicationOutContext replContext = getReplicationContext(context);
                    updateReplicationContext(replContext, context);
                    //Notify cluster node manager of operation
                    _replicationNode.outExtendEntryLeasePeriod(replContext, eh);
                }


                pEntry.getEntryHolder(this).setExpirationTime(time);
                break;

            default: /* Notify Template */
                TemplateCacheInfo pTemplate = _templatesManager.get(uid);
                leaseCacheInfo = pTemplate;

                if (pTemplate.m_TemplateHolder.isNotifyTemplate() && pTemplate.m_TemplateHolder.getXidOriginatedTransaction() == null) {
                    if (shouldReplicate) {
                        final IReplicationOutContext replContext = getReplicationContext(context);
                        updateReplicationContext(replContext, context);
                        //Notify cluster node manager of operation
                        _replicationNode.outExtendNotifyTemplateLeasePeriod(replContext, (NotifyTemplateHolder) pTemplate.m_TemplateHolder);
                    }

                }
                leasedObject = pTemplate.m_TemplateHolder;
                pTemplate.m_TemplateHolder.setExpirationTime(time);
        }
        _leaseManager.reRegisterLease(leaseCacheInfo, leasedObject, original_time, time, objectType);
    }

    /**
     * given a class name return the SL of matching templates. This API is more efficient than
     * direct use of templates iterator
     */
    public Object findTemplatesByIndex(Context context, IServerTypeDesc typeDesc, IEntryHolder entry, MatchTarget matchTarget) {
        TypeData typeData = _typeDataMap.get(typeDesc);
        if (typeData == null)
            return null;

        return findTemplatesByIndex(context, typeData, entry, matchTarget);
    }

    /**
     * given a PEntry, return the entry.
     */
    public IEntryHolder getEntryFromCacheHolder(IEntryCacheInfo pt) {
        return pt.getEntryHolder(this);
    }

    /**
     * get PEntry for entry.
     */
    public IEntryCacheInfo getEntryCacheInfo(IEntryHolder eh) {
        return getPEntryByUid(eh.getUID());
    }

    /**
     * get PEntry for entry.
     */
    public TemplateCacheInfo getTemplateCacheInfo(ITemplateHolder th) {
        return _templatesManager.get(th.getUID());
    }

    //******************  I T E R A T O R S  ***********************//

    public ISAdapterIterator<IEntryHolder> makeEntriesIter(Context context, ITemplateHolder template,
                                                           IServerTypeDesc serverTypeDesc, long SCNFilter, long leaseFilter,
                                                           boolean memoryOnly, boolean transientOnly)
            throws SAException {
        return new EntriesIter(context, template, serverTypeDesc, this, SCNFilter, leaseFilter,
                memoryOnly, transientOnly);
    }

    public ISAdapterIterator<IEntryHolder> makeEntriesIter(Context context, ITemplateHolder template,
                                                           IServerTypeDesc serverTypeDesc, long SCNFilter, long leaseFilter, boolean memoryOnly)
            throws SAException {
        return new EntriesIter(context, template, serverTypeDesc, this, SCNFilter, leaseFilter,
                memoryOnly, false);
    }

    public IScanListIterator makeScanableEntriesIter(Context context, ITemplateHolder template,
                                                     IServerTypeDesc serverTypeDesc, long SCNFilter, long leaseFilter,
                                                     boolean memoryOnly)
            throws SAException {
        ISAdapterIterator<IEntryHolder> ei = new EntriesIter(context, template, serverTypeDesc, this, SCNFilter, leaseFilter,
                memoryOnly, false);

        return new ScanListSAIterator(ei);
    }


    public ISAdapterIterator<ITemplateHolder> makeUnderXtnTemplatesIter(Context context, XtnEntry xtnEntry) throws SAException {
        try {
            return new UnderXtnTemplatesIter(context, xtnEntry, MatchTarget.READ_TAKE_UPDATE, this);
        } catch (NullIteratorException ex) {
            // the iterator could not be constructed because of lack of data -
            // this is a valid situation, and a null iterator should be returned.
            return null;
        }
    }

    public ISAdapterIterator makeUnderXtnEntriesIter(Context context, XtnEntry xtnEntry, int selectType) throws SAException {

        try {
            return new UnderXtnEntriesIter(context, xtnEntry, selectType, this, false);
        } catch (NullIteratorException ex) {
            // the iterator could not be constructed because of lack of data -
            // this is a valid situation, and a null iterator should be returned.
            return null;
        }
    }


    public ISAdapterIterator makeUnderXtnEntriesIter(Context context, XtnEntry xtnEntry, int selectType, boolean returnPentries) throws SAException {

        XtnData pXtn = xtnEntry.getXtnData();
        ICollection<IEntryCacheInfo> entries = pXtn.getUnderXtnEntries(selectType);
        if (entries == null || entries.isEmpty())
            return null;

        try {
            return new UnderXtnEntriesIter(context, xtnEntry, selectType, this, returnPentries);
        } catch (NullIteratorException ex) {
            // the iterator could not be constructed because of lack of data -
            // this is a valid situation, and a null iterator should be returned.
            return null;
        }
    }

    // ****         END   OF   ITERATORS    METHODS       ****//

    public int count(Context context, ITemplateHolder template, final XtnEntry xtnFilter)
            throws SAException {
        final boolean memoryOnly = isCacheExternalDB() || _isMemorySA || isResidentEntriesCachePolicy() || template.isMemoryOnlySearch();
        final boolean slaveLeaseManager = _leaseManager.isSlaveLeaseManagerForEntries();

        if (xtnFilter != null)
            if (!xtnFilter.m_Active)
                throw new SAException("Transaction " + xtnFilter + " is not active");

        IServerTypeDesc serverTypeDesc = _typeManager.getServerTypeDesc(template.getClassName());

        //TODO- when SA count fixed iterateOnlyMemory should always be true
        // iterator for entries
        if (isblobStoreDataSpace()) {
            context.setBlobStoreTryNonPersistentOp(true);
            context.setOptimizedBlobStoreReadEnabled(template.isOptimizedForBlobStoreOp(this));
        }
        EntriesIter iter = (EntriesIter) makeEntriesIter(context, template, serverTypeDesc, 0, SystemTime.timeMillis(), memoryOnly);
        if (iter == null)
            return 0;
        iter.setNotRefreshCache();
        CountInfo countInfo = new CountInfo();
        HashSet<String> processedResults = new HashSet<String>();
        while (true) {
            IEntryHolder eh = null;
            Object res = iter.nextObject();
            if (res == null) {
                iter.close();
                break;
            }
            if (iter.isBringCacheInfoOnly()) {//optimize off heap dont bring entries at all
                IEntryCacheInfo pEntry = iter.getCurrentEntryCacheInfo();
                if (pEntry.isDeleted())
                    continue;
                if (pEntry.getServerTypeDesc().getTypeDesc().isBroadcast() && _engine.skipBroadcastTable(context, template))
                    continue;
                if (!pEntry.isBlobStoreEntry()) {
                    eh = pEntry.getEntryHolder(this);
                    if (eh == null)
                        continue;
                } else {
                    eh = iter.getCurrentEntryHolder();
                    if (eh == null) {
                        if (processedResults.add(pEntry.getUID()))
                            countInfo.incSaCount();
                        continue;
                    }
                }
            } else {
                eh = iter.getCurrentEntryHolder();
            }
            if (eh.isDeleted())
                continue;
            if(eh.getServerTypeDesc().getTypeDesc().isBroadcast() && _engine.skipBroadcastTable(context, template))
                continue;
            XtnStatus writelockXtnsSatus = XtnStatus.UNUSED;
            if (!memoryOnly && context.getLastMatchResult() == MatchResult.NONE) {
                //if persistent iterator is used we perform rawmatch in order to set the context fields
                _engine.getTemplateScanner().match(context, eh, template);
                if (context.getLastMatchResult() == MatchResult.NONE)
                    continue;
            }

            ITransactionalEntryData entryData = context.getLastRawMatchSnapshot();
            XtnEntry writeLock = null;
            if ((writeLock = entryData.getWriteLockOwner()) != null)
                writelockXtnsSatus = writeLock.getStatus();

            boolean useMaster = (context.getLastMatchResult() == MatchResult.MASTER_AND_SHADOW ||
                    context.getLastMatchResult() == MatchResult.MASTER);
            boolean useShadow = (context.getLastMatchResult() == MatchResult.MASTER_AND_SHADOW ||
                    context.getLastMatchResult() == MatchResult.SHADOW);

            // Filter processed uids:
            if (processedResults.add(eh.getUID())) {
                if (useMaster)
                    count_entry(context, countInfo, template, entryData, xtnFilter,
                            writeLock, memoryOnly, false, writelockXtnsSatus, slaveLeaseManager);

                if (useShadow)
                    count_entry(context, countInfo, template, entryData, xtnFilter,
                            writeLock, memoryOnly, true, writelockXtnsSatus, slaveLeaseManager);
            }

        } //while
//TBD
// if ! memory only call SA.count() and adjust using subFromPersistCount && addToPersistentCount
        return countInfo.getSaCount();
    }


    private void count_entry(Context context, CountInfo countInfo, ITemplateHolder template, ITransactionalEntryData ehData, XtnEntry xtnFilter,
                             XtnEntry xtnWriteLock, boolean memoryOnly, boolean original_shadow, XtnStatus writelockXtnsSatus, boolean slaveLeaseManager) {

        int writeLockOperation = xtnWriteLock != null ? ehData.getWriteLockOperation() : 0;

        if (ehData.isExpired() && !slaveLeaseManager)
            return; //ignore expired entries

        if (xtnWriteLock == null && !original_shadow) /* xtn not active, ignore*/ {
            countInfo.incSaCount();
            return;
        }

        if (xtnFilter != null && xtnFilter == xtnWriteLock) {
            if (writeLockOperation != SpaceOperations.TAKE && writeLockOperation != SpaceOperations.TAKE_IE && !original_shadow)
                countInfo.incSaCount();

            if (!memoryOnly) {
                switch (writeLockOperation) {
                    case SpaceOperations.WRITE:
                        countInfo.incAddedToPersistCount();
                        break;
                    case SpaceOperations.TAKE:
                    case SpaceOperations.TAKE_IE:
                        countInfo.incSubFromPersistCount();
                        break;
                    case SpaceOperations.UPDATE:
                        if (original_shadow) //original shadow
                            countInfo.incSubFromPersistCount();
                        else
                            countInfo.incAddedToPersistCount();
                        break;
                }
            }

            return;
        }
        XtnStatus xtnEntryStatus = xtnWriteLock != null ? writelockXtnsSatus : XtnStatus.UNUSED;

        boolean useDirtyRead = _engine.indicateDirtyRead(template);
        //boolean useReadCommitted = m_Engine.indicateReadCommitted(ehData,template);
        boolean useReadCommitted = template.isReadCommittedRequested();

        if (original_shadow) {
            if (!useReadCommitted) {
                if (!memoryOnly)
                    countInfo.incSubFromPersistCount();
            } else//read committed
                countInfo.incSaCount();

            return;
        }


        // (xtnEntry != null)
        if (writeLockOperation == SpaceOperations.WRITE) {
            if (xtnEntryStatus == XtnStatus.COMMITED
                    || xtnEntryStatus == XtnStatus.COMMITING
                    || (xtnEntryStatus == XtnStatus.PREPARED && xtnWriteLock.m_SingleParticipant)) {
                countInfo.incSaCount();
                return;
            }

            if (useDirtyRead) {
                countInfo.incSaCount();
                if (!memoryOnly)
                    countInfo.incAddedToPersistCount();
            }

            return;
        }
        if (writeLockOperation == SpaceOperations.TAKE || writeLockOperation == SpaceOperations.TAKE_IE) {
            if (xtnEntryStatus == XtnStatus.ROLLED
                    || xtnEntryStatus == XtnStatus.ROLLING) {
                countInfo.incSaCount();
                return;
            }
            if (useDirtyRead || useReadCommitted) {
                countInfo.incSaCount();
            } else if (!memoryOnly) {
                countInfo.incSubFromPersistCount();
            }
            return;
        }

        if (writeLockOperation == SpaceOperations.UPDATE) {
            if (useReadCommitted) {
                if (ehData.getOtherUpdateUnderXtnEntry() == null)
                    countInfo.incSaCount();

                return;
            }


            if (xtnEntryStatus == XtnStatus.COMMITED
                    || xtnEntryStatus == XtnStatus.COMMITING
                    || (xtnEntryStatus == XtnStatus.PREPARED && xtnWriteLock.m_SingleParticipant)) {
                countInfo.incSaCount();
                return;
            }
            if (xtnEntryStatus == XtnStatus.ROLLED
                    || xtnEntryStatus == XtnStatus.ROLLING) {//xtn is rolled. now verify that the original entry is counted
                //when xtn is consolidated
                if (ehData.getOtherUpdateUnderXtnEntry() == null /*xtn consolidated*/) {
                    countInfo.incSaCount();
                    return;
                }
            }
            if (useDirtyRead)
                countInfo.incSaCount();

            //handle persistent adjustment
            if (memoryOnly)
                return;

            if (useDirtyRead) {
                countInfo.incAddedToPersistCount();
            }
            return;
        }
        if (writeLockOperation == SpaceOperations.READ || writeLockOperation == SpaceOperations.READ_IE) {
            if (!xtnWriteLock.m_Active) {
                countInfo.incSaCount();
                return;
            }
            if (useDirtyRead || useReadCommitted) {
                countInfo.incSaCount();
            } else if (!memoryOnly) {
                countInfo.incSubFromPersistCount();
            }
        }

        return;
    }

    /**
     * Drop all Class entries and all its templates from the space. Calling this method will remove
     * all internal meta data related to this class stored in the space. When using persistent
     * spaced the relevant RDBMS table will be dropped. It is the caller responsibility to ensure
     * that no entries from this class are written to the space while this method is called. This
     * method is protected through the space Default Security Filter. Admin permissions required to
     * execute this request successfully.
     **/
    private void dropClass(IServerTypeDesc typeDesc, TypeData typeData) {
        if (typeData != null) {
            Context context = null;
            try {
                context = getCacheContext();
                clearEntries(typeData, context);
                clearTemplates(typeDesc.getTypeName());
                _typeDataMap.remove(typeDesc);
            } catch (SAException ex) {
                JSpaceUtilities.throwEngineInternalSpaceException(ex.getMessage(), ex);
            } finally {
                freeCacheContext(context);
            }
        }
    }

    private void clearEntries(TypeData typeData, Context context) throws SAException {
        //remove all the entries from cache
        while (true) {
            IObjectInfo<IEntryCacheInfo> oi = typeData.getEntries().getHead();
            if (oi == null)
                break;
            IEntryCacheInfo pEntry = oi.getSubject();
            if (pEntry == null) {
                typeData.getEntries().remove(oi);
                continue;
            }
            IEntryHolder entry = pEntry.getEntryHolder(this);
            if (entry.isDeleted())
                continue;
            //lock entry in order to prevent inconsistent situations with lease-expr',
            //evictions, commits etc
            ILockObject entryLock = null;
            ILockObject templateLock = null;

            try {
                entryLock = getLockManager().getLockObject(pEntry.getEntryHolder(this));
                synchronized (entryLock) {
                    IEntryHolder original = entry;
                    entry = getEntry(context, entry, true /*tryInsertToCache*/, true /*lockeEntry*/, true /*useOnlyCache*/);
                    if (entry == null)
                        continue;  //entry not there any more
                    if (isblobStoreDataSpace()) {
                        if (!entry.isSameEntryInstance(original))
                            continue; //need to relock
                    } else {
                        if (entryLock.isLockSubject() && entry != pEntry.getEntryHolder(this))
                            continue; //need to relock
                    }
                    pEntry = getEntryCacheInfo(entry);
                    //disconnect from any xtn
                    try {
                        if (pEntry.getEntryHolder(this).getWriteLockOwner() != null) {
                            disconnectEntryFromXtn(context, entry, entry.getWriteLockOwner(), false /*xtnEnd*/);
                        }

                        List<XtnEntry> readLockOwner = entry.getReadLockOwners();
                        if (readLockOwner != null) {
                            while (readLockOwner.size() > 0) {
                                XtnEntry xtn = readLockOwner.iterator().next();
                                disconnectEntryFromXtn(context, entry, xtn, false /*xtnEnd*/);
                            }
                        }
                    } catch (SAException ex) {
                        if (_logger.isDebugEnabled()) {
                            _logger.debug(ex.toString(), ex);
                        }
                    } //ignore any

                    if (entry.hasShadow()) {    //get rid of the shadow of this entry
                        consolidateWithShadowEntry(typeData, pEntry, false /*restoreOriginalValus*/, false /*onError*/);
                    }
                    entry.setDeleted(true);
                    removeEntryFromCache(entry);

                    //remove w.f. if there is
                    Collection<ITemplateHolder> waitingForTemplates = entry.getCopyOfTemplatesWaitingForEntry();
                    if (waitingForTemplates != null) {
                        for (ITemplateHolder template : waitingForTemplates) {
                            templateLock = getLockManager().getLockObject(template, false);
                            try {
                                synchronized (templateLock) {
                                    template.removeEntryWaitingForTemplate(entry);
                                }
                            } finally {
                                if (templateLock != null)
                                    getLockManager().freeLockObject(templateLock);
                                templateLock = null;

                            }
                        }
                    }

                }//synchronized(entryLock)
            } finally {
                if (entryLock != null)
                    getLockManager().freeLockObject(entryLock);
                entryLock = null;
            }

        }//while (true)
    }

    private void clearTemplates(String className) {
        ILockObject templateLock = null;
        //remove all the  templates from cache
        for (Enumeration<String> keys = _templatesManager.getAllTemplatesKeys(); keys.hasMoreElements(); ) {
            TemplateCacheInfo pt = _templatesManager.get(keys.nextElement());
            ITemplateHolder th = pt.m_TemplateHolder;
            if (th.getClassName() != null && th.getClassName().equals(className)) {
                templateLock = getLockManager().getLockObject(th, false /*isEvictable*/);
                try {
                    synchronized (templateLock) {
                        if (th.isDeleted())
                            continue;
                        th.setDeleted(true);
                        removeTemplateFromCache(th);
                    }
                } finally {
                    if (templateLock != null)
                        getLockManager().freeLockObject(templateLock);
                }
            }
        }
    }

    public TypeDataFactory getTypeDataFactory() {
        return _typeDataFactory;
    }

    public TypeData getTypeData(IServerTypeDesc typeDesc) {
        return _typeDataMap.get(typeDesc);
    }

	/*----------------------- PRIVATE/PACKAGE METHODS ------------------------------*/

    /**
     * dynamic creation of new indexes- the server-type-descriptor already reflects the new index info
     */

    /**
     * given a tppe data which indexes where added/deleted from it index the entries & templates of
     * this type. updatedTypeData contains the new indexes structures
     */

    //NOTE
    //templates are currently not handled
    //only add-index is currently suppported
    private void reindexTypeEntries(TypeData updatedTypeData, boolean indexAddition) {
        if (!indexAddition)
            throw new RuntimeException("dropindex not supported");

        //created an iterator and run over all entries in memory

        IStoredList<IEntryCacheInfo> entriesList = updatedTypeData.getEntries();

        IStoredListIterator pos = null;
        Context context = getCacheContext();
        try {
            for (pos = entriesList.establishListScan(true); pos != null; pos = entriesList.next(pos)) {
                IEntryCacheInfo pEntry = (IEntryCacheInfo) pos.getSubject();
                if (pEntry == null || (isEvictableCachePolicy() && pEntry.isRemoving()))
                    continue;

                IEntryHolder entry = pEntry.getEntryHolder(this);
                if (entry.isDeleted())
                    continue; // already deleted
                if (pEntry.isRecentDelete())
                    continue;

                //lock the entry
                ILockObject entryLock = getLockManager().getLockObject(entry);
                try {

                    boolean needUnpin = false;
                    synchronized (entryLock) {
                        try {
                            if (entry.isDeleted())
                                continue; // already deleted

                            if (isEvictableCachePolicy()) {
                                if (pEntry.setPinned(true, true /*waitIfPendingInsertion*/)) {
                                    //pin entry for an intrusive op'
                                    needUnpin = true;
                                } else
                                    continue; //failed- entry currently irrelevant
                            } else if (entry.isBlobStoreEntry()) {
                                entry = getEntry(context, entry, true, true);
                                needUnpin = true;
                            }

                            //reindex the entry
                            TypeDataIndex.reindexEntry(this, pEntry, updatedTypeData);
                        } catch (SAException ex) {
                            if (_logger.isErrorEnabled())
                                _logger.error("Reindex entry problem uid=" + entry.getUID(), ex);

                            throw new RuntimeException("Reindex problem: " + ex);
                        } finally {
                            //while entry still locked
                            if (needUnpin)
                                unpinIfNeeded(context, entry, null, null /* pEntry*/);
                        }
                    } /* synchronized(entry) */
                } finally {
                    if (entryLock != null) {
                        getLockManager().freeLockObject(entryLock);
                    }
                }
            }//for (StoredList.SLHolder pos= entries
        }//try
        finally {
            // scan ended, release resource
            entriesList.freeSLHolder(pos);
            freeCacheContext(context);
        }
    }


    /**
     * Inserts the specified entry to cache, perform memory manager check.
     */
    public IEntryCacheInfo safeInsertEntryToCache(Context context, IEntryHolder entryHolder, boolean newEntry, TypeData pType, boolean pin, InitialLoadOrigin fromInitialLoad) {
        // check memory water-mark
        _engine.getMemoryManager().monitorMemoryUsage(true);
        return insertEntryToCache(context, entryHolder, newEntry, pType, pin, fromInitialLoad);
    }


    /**
     * Inserts the specified entry to cache- if feasable. pin == rentry is locked and should be
     * pinned in cache
     */
    IEntryCacheInfo insertEntryToCache(Context context, IEntryHolder entryHolder, boolean newEntry, TypeData typeData, boolean pin, InitialLoadOrigin fromInitialLoad) {
        if (typeData == null)
            typeData = _typeDataMap.get(entryHolder.getServerTypeDesc());

        final IEntryCacheInfo pEntry = !entryHolder.isBlobStoreEntry() ? EntryCacheInfoFactory.createEntryCacheInfo(entryHolder, typeData.numberOfBackRefs(), pin, getEngine()) :
                ((IBlobStoreEntryHolder) entryHolder).getBlobStoreResidentPart();
        context.setWriteResult(null);

        try {
            if (newEntry)
                entryHolder.setunStable(true);
            //FIFO++++++++++++++++++++++++++++++++++++++++++++
            //used to order inserts to the cache and SA
            final boolean consider_fifo = (newEntry || fromInitialLoad == InitialLoadOrigin.FROM_NON_BLOBSTORE) && typeData.isFifoSupport();
            final boolean fifo_notification_for_nonfifo = newEntry && !typeData.isFifoSupport() && context.getNotifyNewEntry() != null && _templatesManager.anyNotifyFifoForNonFifoTypePerOperation(entryHolder.getServerTypeDesc(), SpaceOperations.WRITE);

            if (fromInitialLoad == InitialLoadOrigin.NON && (consider_fifo || fifo_notification_for_nonfifo)) {
                boolean nofify_fifo = fifo_notification_for_nonfifo || (_templatesManager.anyNotifyFifoWriteTemplates() && context.getNotifyNewEntry() != null);
                if (nofify_fifo)
                    context.setRecentFifoObject(new FifoBackgroundRequest(
                            context.getOperationID(),
                            true/*isNotifyRequest*/,
                            !fifo_notification_for_nonfifo/*isNonNotifyRequest*/,
                            entryHolder,
                            null,
                            context.isFromReplication(),
                            SpaceOperations.WRITE,
                            entryHolder.getXidOriginatedTransaction(),
                            context.getNotifyNewEntry()));
                else
                    context.setRecentFifoObject(new FifoBackgroundRequest(
                            context.getOperationID(),
                            false/*isNotifyRequest*/,
                            true/*isNonNotifyRequest*/,
                            entryHolder,
                            null,
                            context.isFromReplication(),
                            SpaceOperations.WRITE,
                            entryHolder.getXidOriginatedTransaction(),
                            null /*cloneEH*/));
            }
            //FIFO--------------------------------------------

            //FIFO -  order for insert anyway ++++++++++++++++++++++++++++++++++++++++++++
            if (consider_fifo || (entryHolder.isBlobStoreEntry() && newEntry && typeData.getFifoGroupingIndex() != null)) {
                typeData.setFifoOrderFieldsForEntry(entryHolder);
            }//if (newEntry && m_Engine.m_FifoSupported && !fifoTimeStampAlreadySet)
            //FIFO--------------------------------------------

            return internalInsertEntryToCache(context, entryHolder, newEntry, typeData, pEntry, pin);
        } finally {
            if (newEntry)
                entryHolder.setunStable(false);
        }
    }

    public boolean forceSpaceIdIndexIfEqual() {
        return _forceSpaceIdIndexIfEqual;
    }

    private void freeOffHeapCache() {

        AbstractMemoryPool memoryPool = hasBlobStoreOffHeapCache() ? getBlobStoreStorageHandler().getOffHeapCache() : getBlobStoreStorageHandler().getOffHeapStore();

        if (memoryPool != null && memoryPool.getUsedBytes() == 0) {
            return;
        }

        Context context = null;
        try {
            context = getCacheContext();
            if (memoryPool.isOffHeap()) {
                for (IServerTypeDesc serverTypeDesc : getTypeManager().getSafeTypeTable().values()) {
                    if (serverTypeDesc.isRootType() || serverTypeDesc.getTypeDesc().isInactive() || !serverTypeDesc.getTypeDesc().isBlobstoreEnabled())
                        continue;
                    final IScanListIterator<IEntryCacheInfo> entriesIter = getTypeData(serverTypeDesc).scanTypeEntries();
                    if (entriesIter != null) {
                        try {
                            while (entriesIter.hasNext()) {
                                IEntryCacheInfo entry = entriesIter.next();
                                if (entry != null && entry.isBlobStoreEntry()) {
                                    ((IBlobStoreRefCacheInfo) entry).freeOffHeap(this, memoryPool);
                                }
                            }
                            entriesIter.releaseScan();
                        } catch (SAException ex) {
                            _logger.warn("caught exception while cleaning offheap entries during shutdown", ex);
                        }
                    }
                }
            } else if (memoryPool.isPmem()) {
                memoryPool.close();
            }

            if (_logger.isWarnEnabled() && memoryPool != null) {
                final long count = memoryPool.getUsedBytes();
                if (count != 0) {
                    _logger.warn("offheap used bytes still consumes " + count + " bytes");
                }
            }
        } finally {
            if (context != null) {
                freeCacheContext(context);
            }
        }
    }

    public BlobStoreStatistics getBlobStoreStatistics() {
        if (!isBlobStoreCachePolicy())
            return null;
        BlobStoreStatisticsImpl result = new BlobStoreStatisticsImpl();
        result.setCacheSize(_blobStoreInternalCache.getCacheSize());
        result.setCacheHitCount(_blobStoreInternalCache.getHitCount());
        result.setCacheMissCount(_blobStoreInternalCache.getMissCount());
        result.setHotDataCacheMissCount(_blobStoreInternalCache.getHotDataCacheMiss());
        if (_blobStoreStorageHandler.getOffHeapCache() != null)
            result.setOffHeapCacheUsedBytes(_blobStoreStorageHandler.getOffHeapCache().getUsedBytes());
        BlobStoreStorageStatistics snapshot = _blobStoreStorageHandler.getStorageStatistics();
        if (snapshot != null) {
            result.setStorageStatistics(new ArrayList<BlobStoreStorageStatistics>(1));
            result.getStorageStatistics().add(snapshot);
        }
        return result;
    }

    /**
     * INITIALLOAD INFO.
     */
    public static enum InitialLoadOrigin {
        NON, FROM_NON_BLOBSTORE, FROM_BLOBSTORE
    }

    private void validateEntryCanBeWrittenToCache(IEntryHolder entryHolder) {
        ITypeDesc typeDescriptor = entryHolder.getServerTypeDesc().getTypeDesc();

        //persistent entries don't support LRU+FIFO because the EDS doesn't support fifo ordering
        if (!entryHolder.isTransient() && typeDescriptor.isFifoSupported() && isEvictableCachePolicy() && (isCacheExternalDB() || isClusteredExternalDBEnabled()))
            throw new SpaceMetadataException("Fifo class [" + typeDescriptor.getTypeName() + "] is not supported by space with LRU cache and external data source.");
    }

    /**
     * Inserts the specified entry to cache.
     */
    IEntryCacheInfo internalInsertEntryToCache(Context context, IEntryHolder entryHolder, boolean newEntry, TypeData typeData, IEntryCacheInfo pEntry, boolean pin) {
        IEntryCacheInfo res = null;
        boolean recheckedTypeData = false;
        boolean alreadyIn = false;
        boolean insertedToEvictionStrategy = false;
        boolean applySequenceNumber = newEntry && typeData.hasSequenceNumber() && !context.isFromReplication() && !_engine.isLocalCache();

        if (newEntry && typeData.isFifoSupport()) {
            long xtnNum = getLatestTTransactionTerminationNum();
            setFifoCreationXtnInfoForEntry(entryHolder, xtnNum);
            //set the xtn number for the current fifo event request
            if (context.getRecentFifoObject() != null)
                context.getRecentFifoObject().setFifoXtnNumber(xtnNum);
        }
        try {
            while (true) {
                insertedToEvictionStrategy = false;
                alreadyIn = false;
                IEntryCacheInfo oldEntry = _entries.putIfAbsent(pEntry.getUID(), pEntry);

                if (oldEntry == null)
                    break;

                //IMPORTANT NOTE- !!!!!!!
                //FOR MEMORY BASED SPACE WE INSERT THE NEW ENTRY WITHOUT PRECHECK
                //OF EXISTANCE. IF ENTRY IS ALREADY IN WE REMOVE THE NEW INSTANCE
                // FROM THE EVICTION-STRATEGY, SO REMOVAL & INSERTION IN THE EVICTIONSHOULD NOT
                //STRATEGY SHOULD NOT BE BASED ON UID BUT ON INSTANCE
                //
                alreadyIn = true;

                if (useRecentDeletes() && newEntry && oldEntry.isRecentDelete()) {// recent-deletes and new entry- remove it
                    if (_entries.replace(pEntry.getUID(), oldEntry, pEntry)) {
                        removeFromRecentDeletes(pEntry.getEntryHolder(this));
                        alreadyIn = false;
                        break;
                    }
                    throw new RuntimeException("internalInsertEntryToCache: recent deletes cannot be replaced uid=" + pEntry.getUID());
                }
                if (isEvictableCachePolicy() && oldEntry.isRemoving()) {  //entry in cache-removal process- help out
                    //first-wait for removal fromeviction strategy -provide concurrency protection
                    if (_evictionStrategy.requiresConcurrencyProtection())
                        ((EvictableEntryCacheInfo) oldEntry).verifyEntryRemovedFromStrategy();
                    _entries.remove(pEntry.getUID(), oldEntry);
                    continue;
                }

                if (useRecentDeletes() && !newEntry && oldEntry.isRecentDelete()) {
                    //if entry UID is in recent-deletes table- don't insert it in order to
                    //prevent a race in which a dead entry will reside in cache
                    return null; //deleted entry not relevant
                }
                if (newEntry)
                    return (res = _entryAlreadyInSpaceIndication);

                //try to pin the already existing entry
                if (pin && !oldEntry.setPinned(true, !isMemorySpace() /*waitIfPendingInsertion*/)) {//failed to pin, entry must be in removing process
                    continue;
                }
                return (res = oldEntry);
            }

            insertEntryReferences(context, pEntry, typeData, applySequenceNumber);
            //after inserting entry references- we recheck the type-data in order
            //to check if new index was added in order to prevent entry insertion that is missing
            // ad added index- checking a barrier
            if (!recheckedTypeData && typeData.supportsDynamicIndexing() && typeData.isTypeDataReplaced()) {
                recheckedTypeData = true;
                typeData = _typeDataMap.get(entryHolder.getServerTypeDesc());
                TypeDataIndex.reindexEntry(this, pEntry, typeData);
            }

            //if the entry is leased insert to Lease-Manager
            long expiration = pEntry.getEntryHolder(this).getEntryData().getExpirationTime();
            int version = pEntry.getEntryHolder(this).getEntryData().getVersion();
            _leaseManager.registerEntryLease(pEntry, expiration);
            context.setWriteResult(new WriteEntryResult(pEntry.getUID(), version, expiration));

            if (isEvictableCachePolicy())
                addToEvictionStrategy(pEntry, newEntry);
            insertedToEvictionStrategy = true;

            // add entry to Xtn if written under Xtn
            if (newEntry && pEntry.getEntryHolder(this).getXidOriginated() != null) {
                XtnData pXtn = pEntry.getEntryHolder(this).getXidOriginated().getXtnData();
                pXtn.getNewEntries(true/*createIfNull*/).add(pEntry);
                lockEntry(pXtn, pEntry, context.getOperationID());
                pEntry.getEntryHolder(this).getXidOriginated().setOperatedUpon();
            }

            if (isEvictableCachePolicy())
                _cacheSize.incrementAndGet();
            if (newEntry && pEntry.isBlobStoreEntry() && isDirectPersistencyEmbeddedtHandlerUsed())
                _engine.getReplicationNode().getDirectPesistencySyncHandler().getEmbeddedSyncHandler().onSpaceOpRemovePhantomIfExists(pEntry.getUID());
            return (res = pEntry);
        } catch (Exception ex) {
            //if execption thrown in process mark entry as removed
            pEntry.setRemoved();
            pEntry.getEntryHolder(this).setDeleted(true);

            try {//remove- if possible- index refs. done mainly to avoid unique values stuck in cache
                removeEntryReferences(pEntry, typeData, context.getNumOfIndexesInserted()/*onError*/);
            } catch (Exception ex1) {
            }//suppress


            _entries.remove(pEntry.getUID(), pEntry);
            if (isEvictableCachePolicy())
                ((EvictableEntryCacheInfo) pEntry).notifyWaitersOnFailure();
            if (newEntry && pEntry.getEntryHolder(this).getXidOriginated() != null) {//
                try {
                    disconnectEntryFromXtn(context, entryHolder, pEntry.getEntryHolder(this).getXidOriginated(), false/*XtnEnd*/);
                } catch (Exception ex1) {
                }
            }

            if (_logger.isErrorEnabled())
                _logger.error(" insertion entry problem uid=" + entryHolder.getUID() + " new=" + newEntry
                        + " pin=" + pin, ex);

            if (ex instanceof RuntimeException)
                throw (RuntimeException) ex;
            else
                throw new RuntimeException(ex);
        } finally {
            if (res != pEntry && insertedToEvictionStrategy)
                removeFromEvictionStrategy(pEntry);

            if (newEntry && res == _entryAlreadyInSpaceIndication && typeData.isFifoSupport())
                removeFifoXtnInfoForEntry(entryHolder);

            if (res != null && res != _entryAlreadyInSpaceIndication && !alreadyIn && !pin && !pEntry.isRemoved()) {
                if (isEvictableCachePolicy())
                    res.setInCache(!isMemorySpace()); //non-pined entry inserted
            }
        }
    }


    /**
     * Inserts the refs to cache.
     */
    public void insertEntryReferences(Context context, IEntryCacheInfo pEntry, TypeData pType, boolean applySequenceNumber) {
        context.clearNumOfIndexesInserted();
        IEntryData entryData = context.getCacheViewEntryDataIfNeeded(pEntry.getEntryHolder(this).getEntryData());
        // add entry to type info
        if(pType.getFifoGroupingIndex()!=null){
            pEntry.setMainListBackRef(((ConcurrentSegmentedStoredList<IEntryCacheInfo>)pType.getEntries()).add(pEntry, pType.getFifoGroupingIndex().getIndexValue(entryData)));
        }else{
            pEntry.setMainListBackRef(pType.getEntries().add(pEntry));
        }

        int sequenceNumPlaceHolderPos = 0;

        if (pType.hasIndexes()) {
            // add entry to indexes
            int indexBuildNumber = 0;
            final TypeDataIndex[] indexes = pType.getIndexes();
            for (TypeDataIndex index : indexes) {
                if (pType.disableIdIndexForEntries(index))
                    continue;
                if (applySequenceNumber && index == pType.getSequenceNumberIndex()) {//delay this index until all other inserted and than apply a value
                    if (pEntry.getBackRefs() != null) {
                        pEntry.getBackRefs().add(TypeDataIndex._DummyOI);
                        sequenceNumPlaceHolderPos = pEntry.getBackRefs().size() - 1;
                        if (index.isExtendedIndex())
                            pEntry.getBackRefs().add(TypeDataIndex._DummyOI);
                    } else
                        sequenceNumPlaceHolderPos = -1;   //for off-heap without backrefs
                } else
                    index.insertEntryIndexedField(pEntry, index.getIndexValue(entryData), pType);

                if (pType.supportsDynamicIndexing() && index.getIndexCreationNumber() > indexBuildNumber)
                    indexBuildNumber = index.getIndexCreationNumber();
                context.incrementNumOfIndexesInserted();
            } /* for (int pos...) */


            if (pType.supportsDynamicIndexing() && indexBuildNumber > 0 && indexBuildNumber > pEntry.getLatestIndexCreationNumber())
                pEntry.setLatestIndexCreationNumber(indexBuildNumber);


//POC  insert to foreign indexes/queries
            for (QueryExtensionIndexManagerWrapper queryExtensionIndexManager : pType.getForeignQueriesHandlers())
                queryExtensionIndexManager.insertEntry(new SpaceServerEntryImpl(pEntry, this), false /*fromTransactionalUpdate*/);
        } //if (m_AnyIndexes > 0)
        if (applySequenceNumber)
            //set the sequence number in the field
            pEntry.getEntryHolder(this).getEntryData().setFixedPropertyValue(pEntry.getEntryHolder(this).getServerTypeDesc().getTypeDesc().getSequenceNumberFixedPropertyID(), pType.getSequenceNumberGenerator().getNext());
        else if (pType.hasSequenceNumber() && !_engine.isLocalCache())
            pType.getSequenceNumberGenerator().updateIfGreater((Long) pEntry.getEntryHolder(this).getEntryData().getFixedPropertyValue(pEntry.getEntryHolder(this).getServerTypeDesc().getTypeDesc().getSequenceNumberFixedPropertyID()));


        //if the seq number is indexed its time to index it since it was kept for last indexing op
        if (sequenceNumPlaceHolderPos != 0) {
            TypeDataIndex index = pType.getSequenceNumberIndex();
            index.insertEntryIndexedField(pEntry, index.getIndexValue(pEntry.getEntryHolder(this).getEntryData()), pType);
            if (sequenceNumPlaceHolderPos > 0) {
                int curpos = index.isExtendedIndex() ? pEntry.getBackRefs().size() - 2 : pEntry.getBackRefs().size() - 1;
                pEntry.getBackRefs().set(sequenceNumPlaceHolderPos, pEntry.getBackRefs().remove(curpos));
                if (index.isExtendedIndex())
                    pEntry.getBackRefs().set(sequenceNumPlaceHolderPos + 1, pEntry.getBackRefs().remove(curpos));
            }
        }
    }


    /**
     * Inserts the specified template to cache.
     */
    private void insertTemplateToCache(Context context, TemplateCacheInfo pTemplate) {
        final ITemplateHolder template = pTemplate.m_TemplateHolder;
        final TypeData typeData = _typeDataMap.get(template.getServerTypeDesc());
        if (template.isNotifyTemplate())
            typeData.incM_NumRegularNotifyTemplatesStored();

        if (template.isFifoGroupPoll())
            _fifoGroupCacheImpl.incrementNumOfTemplates();

        final boolean anyIndexes = typeData.hasInitialIndexes();
        final boolean extendedMatch = template.getExtendedMatchCodes() != null;

        if (template.getUidToOperateBy() != null)
            insertTemplateByUid(pTemplate, typeData);
        else if (!anyIndexes)
            insertNonIndexedTemplate(pTemplate, typeData, extendedMatch);
        else
            TypeDataIndex.insertIndexedTemplate(pTemplate, typeData, extendedMatch);

        //if leased notify template insert to Lease-Manager
        if (template.isNotifyTemplate()) {
            context.setNotifyLease(_leaseManager.registerTemplateLease(pTemplate));
        }

        // add template to Xtn if written under Xtn
        if (template.getXidOriginated() != null) {
            XtnData pXtn = template.getXidOriginated().getXtnData();

            if (template.isNotifyTemplate())
                pXtn.getNTemplates(true).add(pTemplate);
            else /* READ, READ_IE, TAKE, TAKE_IE */
                pXtn.getRTTemplates(true).add(pTemplate);
        }
    }


    private void insertTemplateByUid(TemplateCacheInfo pTemplate, TypeData typeData) {
        ITemplateHolder template = pTemplate.m_TemplateHolder;

        IObjectInfo<TemplateCacheInfo> oi = null;

        //operate by uid- just index that uid
        pTemplate.initBackRefs(1);//init backrefs ArrayList with size of 1
        // add template to type info
        ConcurrentHashMap<String, IStoredList<TemplateCacheInfo>> uidTemplates;
        if (template.isNotifyTemplate())
            uidTemplates = typeData.getNotifyUidTemplates();
        else /* READ, READ_IE, TAKE, TAKE_IE */
            uidTemplates = typeData.getReadTakeUidTemplates();

        IStoredList<TemplateCacheInfo> templates = uidTemplates.get(template.getUidToOperateBy());
        for (; ; ) {
            // if found a templates SL for this UID-value
            if (templates != null) {
                oi = templates.add(pTemplate);

                //	may have been invalidated by PersistentGC
                if (oi == null) {
                    //help remove entry for key only if currently mapped to given value
                    uidTemplates.remove(template.getUidToOperateBy(), templates);
                    templates = null;
                } else
                    break; // OK - done.
            }

            // either wasn't found or we just helped remove it
            if (templates == null) {
                // create vector
                templates = StoredListFactory.createConcurrentList(true /*supportFifo*/);
                oi = templates.add(pTemplate);
                templates = uidTemplates.putIfAbsent(template.getUidToOperateBy(), templates);

                //	putIfAbsent returns null if there was no mapping for key
                if (templates == null)
                    break; // OK - done.
            }
        }//for(;;)

        pTemplate.m_BackRefs.add(oi);
    }

    private void insertNonIndexedTemplate(TemplateCacheInfo pTemplate, TypeData typeData, boolean extendedMatch) {
        IObjectInfo<TemplateCacheInfo> oi;
        pTemplate.initBackRefs(1);//init backrefs ArrayList with size of 1
        // add template to type info
        if (pTemplate.m_TemplateHolder.isNotifyTemplate()) {
            if (!extendedMatch)
                oi = typeData.getNotifyTemplates().add(pTemplate);
            else //extendedMatch
                oi = typeData.getNotifyExtendedTemplates().add(pTemplate);
            pTemplate.m_BackRefs.add(oi);
        } else  /* READ, READ_IE, TAKE, TAKE_IE */ {
            if (!extendedMatch)
                oi = typeData.getReadTakeTemplates().add(pTemplate);
            else //extendedMatch
                oi = typeData.getReadTakeExtendedTemplates().add(pTemplate);
            pTemplate.m_BackRefs.add(oi);
        }
    }

    /**
     * update entry values in cache. pEntry - the current in cache or null if unknown
     */

    private IEntryCacheInfo updateEntryInCache(Context context, IEntryCacheInfo pEntry, IEntryHolder entryHolder, IEntryData newEntryData, long newExpirationTime,
                                               int modifiers) {
        final TypeData typeData = _typeDataMap.get(entryHolder.getServerTypeDesc());
        final boolean partial_update = UpdateModifiers.isPartialUpdate(modifiers);

        if (pEntry == null)
            pEntry = getPEntryByUid(entryHolder.getUID());

        if (!pEntry.isPinned())
            throw new RuntimeException("updateEntryInCache: internal error- entry uid =" + pEntry.getUID() + " not pinned");

        try {
            pEntry.getEntryHolder(this).setunStable(true);
            //new content and reconnect to the cache
            IEntryData originalEntryData = entryHolder.getEntryData();
            //if partial update- insert the correct values to new fields array
            context.setPartialUpdatedValuesIndicators(null);
            if (partial_update) {
                int numOfFields = originalEntryData.getNumOfFixedProperties();
                boolean[] partialUpdatedValuesIndicators = new boolean[numOfFields];
                boolean anyPartial = false;
                for (int i = 0; i < numOfFields; i++) {//handle partial update
                    if (newEntryData.getFixedPropertyValue(i) == null) {
                        newEntryData.setFixedPropertyValue(i, originalEntryData.getFixedPropertyValue(i)); //supplement
                        partialUpdatedValuesIndicators[i] = true;
                        anyPartial = _partialUpdateReplication;
                    }
                }

                if (newEntryData.getDynamicProperties() == null)
                    newEntryData.setDynamicProperties(originalEntryData.getDynamicProperties());

                if (anyPartial)
                    context.setPartialUpdatedValuesIndicators(partialUpdatedValuesIndicators);
            }

            //in case of sequence number verify it havent been changed
            if (entryHolder.getServerTypeDesc().getTypeDesc().hasSequenceNumber() && !_engine.isLocalCache()) {
                int snPos = entryHolder.getServerTypeDesc().getTypeDesc().getSequenceNumberFixedPropertyID();
                Object newVal = newEntryData.getFixedPropertyValue(snPos);
                Object originalVal = originalEntryData.getFixedPropertyValue(snPos);
                if (newVal == null)
                    newEntryData.setFixedPropertyValue(snPos, originalVal);
                if (newVal != null && !TypeData.objectsEquality(originalVal, newVal))
                    throw new SequenceNumberException(entryHolder.getUID(), entryHolder.getClassName(), " sequence number altered in update/change op was=" + originalVal + " new=" + newVal);
            }

            typeData.prepareForUpdatingIndexValues(this, pEntry, newEntryData);
            long original_expiration = 0;
            if (context.isReRegisterLeaseOnUpdate())
                original_expiration = pEntry.getEntryHolder(this).getEntryData().getExpirationTime();

            entryHolder.updateEntryData(newEntryData, newExpirationTime);

            typeData.updateEntryReferences(this, entryHolder, pEntry, originalEntryData);

            if (context.isReRegisterLeaseOnUpdate())
            //need to re-register in lease manager
            {
                if (!pEntry.getEntryHolder(this).hasShadow())
                    _leaseManager.reRegisterLease(pEntry, pEntry.getEntryHolder(this), original_expiration, newExpirationTime, ObjectTypes.ENTRY);
                else {
                    pEntry.getEntryHolder(this).getShadow().incrementNumOfLeaseUpdates();
                    if (pEntry.getEntryHolder(this).getShadow().getNumOfLeaseUpdates() > 1)
                        _leaseManager.unregister(pEntry, original_expiration);//double update- remove the 1st one
                    _leaseManager.registerEntryLease(pEntry, newExpirationTime);
                }
                context.setWriteResult(new WriteEntryResult(pEntry.getUID(), newEntryData.getVersion(), newExpirationTime));
            }
            //set the deleted bit to false as a means to pass-throu a volatile (monitor)
            //in order to ensure predictable visability after update in non-blocking read
            if (!pEntry.getEntryHolder(this).isBlobStoreEntry())
                pEntry.getEntryHolder(this).setDeleted(false);

            return pEntry;
        } finally {
            pEntry.getEntryHolder(this).setunStable(false);
        }
    }


    /**
     * Removes the specified entry from cache. return true if removal ok
     */
    public boolean removeEntryFromCache(IEntryHolder entryHolder) {
        return removeEntryFromCache(entryHolder, false /*initiatedByEvictionStrategy*/, true /*locked*/, null /*pEntry*/, RecentDeleteCodes.NONE);
    }

    /**
     * Removes the specified entry from cache. return true if removal ok
     */
    public boolean removeEntryFromCache(IEntryHolder entryHolder, boolean initiatedByEvictionStrategy, boolean locked, IEntryCacheInfo pEntry, RecentDeleteCodes recentDeleteUsage) {
        boolean recentDeleteEntry = false;
        if (!locked) {
            if (!locked && pEntry == null)
                throw new RuntimeException("removeEntryFromCache: invalid usage, unlocked && pEntry is null");
            recentDeleteEntry = pEntry.isRecentDelete();
            if (recentDeleteUsage == RecentDeleteCodes.REMOVE_DUMMY && !recentDeleteEntry)
                return false;
            if (!initiatedByEvictionStrategy && !pEntry.setRemoving(false /*isPinned*/))
                return false; //someone else have removed or pinned this instance
        } else {
            if (isEvictableCachePolicy() || entryHolder.isBlobStoreEntry()) {
                if (pEntry == null)
                    pEntry = entryHolder.isBlobStoreEntry() ? ((IBlobStoreEntryHolder) (entryHolder)).getBlobStoreResidentPart() : getPEntryByUid(entryHolder.getUID());

                if (pEntry == null)
                    throw new RuntimeException("removeEntryFromCache: locked && pEntry not found uid=" + entryHolder.getUID());

                recentDeleteEntry = pEntry.isRecentDelete();
                if (recentDeleteUsage != RecentDeleteCodes.INSERT_DUMMY)
                    pEntry.setRemoving(true /*isPinned*/);
            }
        }

        if (recentDeleteUsage == RecentDeleteCodes.REMOVE_DUMMY) {
            if (pEntry == null || !recentDeleteEntry)
                return false;
        } else {
            // remove from strategy first + optional concurrency protection
            if (pEntry != null && isEvictableCachePolicy())
                removeFromEvictionStrategy(pEntry);
        }

        //NOTE- for blobstore the entry is removed by the call to SA::remove

        if (recentDeleteUsage == RecentDeleteCodes.INSERT_DUMMY) {//deleting and atomic replace by recent delete dummy
            if (pEntry == null)
                pEntry = getPEntryByUid(entryHolder.getUID());
            if (pEntry != null) {//create dummy
                IEntryHolder dummyEh = entryHolder.createDummy();
                IEntryCacheInfo dummyPe = new EvictableEntryCacheInfo(dummyEh, 0, false /*pin*/);
                dummyPe.setRecentDelete();
                _entries.replace(entryHolder.getUID(), pEntry, dummyPe);
            }
        } else {//regular delete
            if (pEntry == null)
                pEntry = _entries.remove(entryHolder.getUID());
            else
                _entries.remove(entryHolder.getUID(), pEntry);
        }
        if (pEntry == null && locked)
            throw new RuntimeException("removeEntryFromCache: locked && pEntry not found");
        if (recentDeleteUsage == RecentDeleteCodes.REMOVE_DUMMY)
            return true;

        boolean inserted = (!isEvictableCachePolicy() || (pEntry != null && pEntry.wasInserted()));
        if (!inserted)
            return true;


        TypeData typeData = _typeDataMap.get(entryHolder.getServerTypeDesc());

        removeEntryReferences(pEntry, typeData, -1);

        //unregister from lease manager
        _leaseManager.unregister(pEntry, entryHolder.getEntryData().getExpirationTime());

        if (isEvictableCachePolicy())
            _cacheSize.decrementAndGet();
        // clean Xtn reference, if exists
        XtnData pXtn = null;
        if (pEntry.getEntryHolder(this).getXidOriginatedTransaction() != null) {
            pXtn = pEntry.getEntryHolder(this).getXidOriginated().getXtnData();
            if (pXtn != null)
                pXtn.removeFromNewEntries(pEntry);
        }
        if (pEntry.getEntryHolder(this).getWriteLockTransaction() != null) {
            if (pEntry.getEntryHolder(this).getXidOriginatedTransaction() == null || !pEntry.getEntryHolder(this).getXidOriginatedTransaction().equals(pEntry.getEntryHolder(this).getWriteLockTransaction()))
                pXtn = pEntry.getEntryHolder(this).getWriteLockOwner().getXtnData();
            if (pXtn != null) {
                removeLockedEntry(pXtn, pEntry);
                pXtn.removeTakenEntry(pEntry);
            }
        }

        if (isEvictableCachePolicy())
            pEntry.setRemoved();

        return true;
    }

    public SQLFunctions getSqlFunctions() {
        return sqlFunctions;
    }

    /**
     * usage of recent deletes.
     */
    public enum RecentDeleteCodes {
        NONE, INSERT_DUMMY, REMOVE_DUMMY;
    }


    /**
     * Removes entry refs from cache.
     */
    public void removeEntryReferences(IEntryCacheInfo pEntry, TypeData pType, int numOfIndexesInsertedOnError) {
        boolean onError = numOfIndexesInsertedOnError > 0;
        int refpos = 1;
        pType.getEntries().remove(pEntry.getMainListBackRef());
        int numIndexesProcessed = 0;
        if (pType.hasIndexes()) {
            IEntryData entryData = pEntry.getEntryHolder(this).getEntryData();

            // entry  indexes
            final TypeDataIndex[] indexes = pType.getIndexes();
            for (TypeDataIndex index : indexes) {
                if (index.disableIndexUsageForOperation(pType, pEntry.getLatestIndexCreationNumber()))
                    continue;
                if (onError) {
                    if (pEntry.getBackRefs() != null) {
                        if (refpos > pEntry.getBackRefs().size() - 1)
                            break;//from error, so far have been inserted
                    } else {
                        if (numIndexesProcessed == numOfIndexesInsertedOnError)
                            break;//from error, so far have been inserted
                    }
                }
                //if (index.getIndexCreationNumber() > pEntry.getLatestIndexCreationNumber())
                //	continue;   //entry was not indexed by this one yet
                //remove the index at the pos
                refpos = index.removeEntryIndexedField_main(pEntry.getEntryHolder(this), pEntry.getBackRefs(), index.getIndexValue(entryData), refpos, true, pEntry);
                numIndexesProcessed++;
            } /* for (int pos...) */

            for (QueryExtensionIndexManagerWrapper queryExtensionIndexManager : pType.getForeignQueriesHandlers())
                queryExtensionIndexManager.removeEntry(new SpaceServerEntryImpl(pEntry, this), QueryExtensionIndexRemoveMode.NO_XTN, pEntry.getVersion());
        } /* if pType.m_AnyIndexes */

        if (pEntry.getBackRefs() != null)
            pEntry.getBackRefs().clear();
    }


    /**
     * Removes the specified template from cache.
     */
    private void removeTemplateFromCache(ITemplateHolder template) {
        // remove template from templates table
        TemplateCacheInfo pTemplate = _templatesManager.remove(template.getUID());
        if (pTemplate == null)
            return;

        boolean extendedMatch = pTemplate.m_TemplateHolder.getExtendedMatchCodes() != null;

        IObjectInfo<TemplateCacheInfo> oi = null;
        IStoredList<TemplateCacheInfo> templates;
        int refpos = 0;

        // remove template from backref vectors
        TypeData typeData = _typeDataMap.get(template.getServerTypeDesc());
        boolean anyIndexes = false;
        TypeDataIndex[] indexes = typeData.getIndexes();
        for (TypeDataIndex index : indexes) {
            if (index.getIndexCreationNumber() > pTemplate.getLatestIndexCreationNumber())
                continue;   //template was not indexed by this one yet
            if (index.isCompound())
                continue; //waiting templates currently not matched by compound
            anyIndexes = true;
            break;
        }


        // remove template from type info.
        if (template.getUidToOperateBy() == null) {
            if (!anyIndexes) {
                refpos = removeNonIndexedTemplate(pTemplate,
                        extendedMatch,
                        typeData);
            } else //if (AnyIndexes)
            {
                refpos = TypeDataIndex.removeIndexedTemplate(pTemplate,
                        extendedMatch,
                        oi,
                        refpos,
                        typeData);


            } /* if */
        }//if (templateHolder.m_UidToOperateBy == null)
        else {//if (templateHolder.m_UidToOperateBy != null)
            oi = pTemplate.m_BackRefs.get(0);
            if (pTemplate.m_TemplateHolder.isNotifyTemplate())
                templates = typeData.getNotifyUidTemplates().get(pTemplate.m_TemplateHolder.getUidToOperateBy());
            else /* READ, READ_IE, TAKE, TAKE_IE */
                templates = typeData.getReadTakeUidTemplates().get(pTemplate.m_TemplateHolder.getUidToOperateBy());
            templates.remove(oi);
        }//if (templateHolder.m_UidToOperateBy != null)

        //if template is tokened- remove its special
        //index if need to
        if (template.isNotifyTemplate())
            //decrease number of regular notify templates- optimistic
            typeData.decM_NumRegularNotifyTemplatesStored();

        pTemplate.m_BackRefs.clear();

        //unregister from lease manager
        _leaseManager.unregister(pTemplate, pTemplate.m_TemplateHolder.getExpirationTime());

        // clean Xtn reference, if exists
        if (pTemplate.m_TemplateHolder.getXidOriginated() != null) {
            XtnData pXtn = pTemplate.m_TemplateHolder.getXidOriginated().getXtnData();
            if (pXtn != null) {
                if (pTemplate.m_TemplateHolder.isNotifyTemplate())
                    pXtn.getNTemplates().removeByObject(pTemplate);
                else
                    pXtn.getRTTemplates().removeByObject(pTemplate);
            }
        }
        if (template.isFifoGroupPoll())
            _fifoGroupCacheImpl.decrementNumOfTemplates();

        // REMOVE WF OF TEMPLATE IS DONE WHEN THE ENTRY IS RELEASED FROM XTN (COMMIT, RB OR ENTRY REMOVED IN REMOVESA)
    }


    private int removeNonIndexedTemplate(TemplateCacheInfo pTemplate,
                                         boolean extendedMatch, TypeData typeData) {
        IObjectInfo<TemplateCacheInfo> oi;
        int refpos;
        oi = pTemplate.m_BackRefs.get(0);
        if (pTemplate.m_TemplateHolder.isNotifyTemplate()) {
            if (!extendedMatch)
                typeData.getNotifyTemplates().remove(oi);
            else //extendedMatch
                typeData.getNotifyExtendedTemplates().remove(oi);
        } else /* READ, READ_IE, TAKE, TAKE_IE */ {
            if (!extendedMatch)
                typeData.getReadTakeTemplates().remove(oi);
            else //extendedMatch
                typeData.getReadTakeExtendedTemplates().remove(oi);
        }
        refpos = 1;  //for token handling
        return refpos;
    }

    public IEntryCacheInfo getEntryByUniqueId(IServerTypeDesc currServerTypeDesc, Object templateValue, ITemplateHolder template) {
        TypeData typeData = _typeDataMap.get(currServerTypeDesc);

        // If template is FIFO but type is not FIFO, do not search it:
        if (template.isFifoSearch() && !typeData.isFifoSupport())
            return null;

        // If template has no fields of type has no indexes, skip index optimization:
        if (!typeData.hasIndexes())
            return null;

        int latestIndexToConsider = typeData.getLastIndexCreationNumber();

        // If type contains a primary key definition, check it first:
        TypeDataIndex<IStoredList<IEntryCacheInfo>> primaryKey = typeData.getIdField();
        if (primaryKey != null && latestIndexToConsider >= primaryKey.getIndexCreationNumber()) {
            if (typeData.disableIdIndexForEntries(primaryKey))
                return getPEntryByUid(typeData.generateUid(templateValue));

            IStoredList<IEntryCacheInfo> res = primaryKey.getUniqueEntriesStore().get(templateValue);
            if (res != null && !res.isMultiObjectCollection())
                return res.getObjectFromHead();
        }
        return null;
    }


    public IEntryCacheInfo[] getEntriesByUniqueIds(IServerTypeDesc currServerTypeDesc, Object[] ids, ITemplateHolder template) {
        IEntryCacheInfo[] res = new IEntryCacheInfo[ids.length];
        for (int i = 0; i < ids.length; i++) {
            res[i] = getEntryByUniqueId(currServerTypeDesc, ids[i], template);
        }
        return res;
    }


    /**
     * Computes the best way to reach a list of potential match of entries of the specified type.
     * <p>
     * Handles also unindexed entries.
     * <p>
     * Assumes the specified template is a superclass (in the wide sense) of the specified entry.
     *
     * @param typeData the type of entries to search for
     * @param template the template to match against
     * @return a SL   containing potential matches or <code>null</code> if there are no matches.
     */
    public IStoredList<IEntryCacheInfo> getEntriesMinIndex(Context context, TypeData typeData, int numOfFields, ITemplateHolder template) {
        // If template is FIFO but type is not FIFO, do not search it:
        if (template.isFifoSearch() && !typeData.isFifoSupport())
            return null;

        // If template has no fields of type has no indexes, skip index optimization:
        if (numOfFields == 0 || !typeData.hasIndexes())
            return typeData.getEntries();

        int latestIndexToConsider = typeData.getLastIndexCreationNumber();

        // If type contains a primary key definition, check it first:
        TypeDataIndex<IStoredList<IEntryCacheInfo>> primaryKey = typeData.getIdField();
        if (primaryKey != null && primaryKey.getPos() < numOfFields && latestIndexToConsider >= primaryKey.getIndexCreationNumber()) {
            Object templateValue = primaryKey.getIndexValueForTemplate(template.getEntryData());
            if (templateValue != null) {
                if( context.getIndexMetricsContext() != null ) {
                    context.getIndexMetricsContext().addChosenIndex( primaryKey.getUsageCounter() );
                }
                if (typeData.disableIdIndexForEntries(primaryKey))
                    return getPEntryByUid(typeData.generateUid(templateValue));
                else
                    return primaryKey.getUniqueEntriesStore().get(templateValue);
            }
        }

        context.setIndexUsed(false);
        // Initialize shortest potential match list: all entries are potential matches
        IStoredList<IEntryCacheInfo> shortestPotentialMatchList = typeData.getEntries();
        context.setIntersectionEnablment(typeData.isBlobStoreClass() && !template.isFifoGroupPoll()); //for f-g currently we dont perform index intersection
        // Look for a shorter potential match list in standard indexes:
        shortestPotentialMatchList = findShortestPotentialMatchList(context, typeData, template, shortestPotentialMatchList, numOfFields, latestIndexToConsider);
        // Look for a shorter potential match list in custom indexes:
        shortestPotentialMatchList = findShortestPotentialMatchListCustom(context, typeData, template, shortestPotentialMatchList, latestIndexToConsider);

        if (ProtectiveMode.isQueryWithoutIndexProtectionEnabled() && !context.isIndexUsed()) {
            throw new ProtectiveModeException("Cannot perform operation: The request references only unindexed fields!\n" +
                    "(you can disable this protection, though it is not recommended, by setting the following system property: " + ProtectiveMode.QUERY_WITHOUT_INDEX + "=false)");
        }

        return shortestPotentialMatchList;

    }


    private IScanListIterator<IEntryCacheInfo> getScannableEntriesMinIndex(Context context, TypeData typeData, int numOfFields, ITemplateHolder template) {
        if (context.isBlobStoreTryNonPersistentOp())
            context.setBlobStoreUsePureIndexesAccess(isRelevantUsePureIndexesAccess(context, typeData, template));
        if (template.isFifoGroupPoll())
            return _fifoGroupCacheImpl.getScannableEntriesMinIndex(context, typeData, numOfFields, template);
        IStoredList<IEntryCacheInfo> res = getEntriesMinIndex(context, typeData, numOfFields, template);
        if (res != null && context.isIndicesIntersectionEnabled() && context.getChosenIntersectedList(false) != null)
            return context.getChosenIntersectedList(true/*final*/);
        if (res != null && !res.isMultiObjectCollection())
            return res.getObjectFromHead();
        boolean fifoScan = template.isFifoTemplate() ||
                (context.isInMemoryRecovery()  && (typeData.getFifoGroupingIndex()!=null  || typeData.isFifoSupport()));
        return res != null ? new ScanSingleListIterator(res, fifoScan, template.isServerIterator()) : null;
    }


    private boolean isRelevantUsePureIndexesAccess(Context context, TypeData typeData, ITemplateHolder template) {
        if (!typeData.isBlobStoreClass() || !typeData.hasIndexes() || template.isFifoGroupPoll() || template.getXidOriginated() != null)
            return false;
        if (template.isSqlQuery())
            return template.isAllValuesIndexSqlQuery();
        if (template.getEntryData() == null || template.getEntryData().getFixedPropertiesValues() == null)
            return false; //null template
        boolean allNulls = true;
        for (int i = 0; i < template.getEntryData().getFixedPropertiesValues().length; i++) {
            {
                if (template.getEntryData().getFixedPropertiesValues()[i] != null) {
                    allNulls = false;
                    if (!typeData.getIndexesRelatedFixedProperties()[i])
                        return false;
                }
            }
        }
        return !allNulls;
    }

    public final static int MIN_SIZE_TO_CONSIDER_COMPOUND_INDICES = 5;

    private static IStoredList<IEntryCacheInfo> findShortestPotentialMatchList(Context context,
                                                                               TypeData typeData,
                                                                               ITemplateHolder template,
                                                                               IStoredList<IEntryCacheInfo> shortestPotentialMatchList,
                                                                               int numOfFields,
                                                                               int latestIndexToConsider) {
        // if there is no potential match - return immediately
        if (shortestPotentialMatchList == null)
            return shortestPotentialMatchList;
        IStoredList<IEntryCacheInfo> compound_selection = null;
        int shortestPotentialMatchListSize = shortestPotentialMatchList.size();

        Set<String> usedInSegments = null;
        MultiIntersectedStoredList<IEntryCacheInfo> intersectedList = context.getChosenIntersectedList(false);
        if (typeData.anyNonFGCompoundIndex() && !template.isFifoGroupPoll() && template.getCustomQuery() == null) {
            usedInSegments = new HashSet();
            IStoredList<IEntryCacheInfo> shortestPotentialMatchListCompound = findShortestPotentialMatchListCompound(context, typeData, template,
                    shortestPotentialMatchList, numOfFields, latestIndexToConsider, usedInSegments);
            if (shortestPotentialMatchListCompound != shortestPotentialMatchList) {
                shortestPotentialMatchList = shortestPotentialMatchListCompound;
                compound_selection = shortestPotentialMatchList;
                if (shortestPotentialMatchList == null)
                    return shortestPotentialMatchList;
                shortestPotentialMatchListSize = shortestPotentialMatchList.size();
                if (context.isIndicesIntersectionEnabled())
                    intersectedList = addToIntersectedList(context, intersectedList, shortestPotentialMatchListCompound, template.isFifoTemplate(), true/*shortest*/, typeData);
                if (shortestPotentialMatchListSize == 0 || !context.isIndicesIntersectionEnabled()) {
                    if (shortestPotentialMatchListSize < MIN_SIZE_TO_CONSIDER_COMPOUND_INDICES) {
                        if (_logger.isTraceEnabled())
                            logSearchCompoundSelection(typeData, shortestPotentialMatchList, compound_selection, null);
                        return shortestPotentialMatchList;
                    }
                }
            }
        }

        final TypeDataIndex[] indexes = typeData.getIndexes();
        TypeDataIndex shortestPotentialIndex = null;
        for (TypeDataIndex index : indexes) {
            if (index.getPos() >= numOfFields)
                break;

            if (index.disableIndexUsageForOperation(typeData, latestIndexToConsider))
                continue;   //uncompleted index

            if (usedInSegments != null && usedInSegments.contains(index.getIndexDefinition().getName()))
                continue; //already used in compound checked, skip it
            final Object templateValue = index.getIndexValueForTemplate(template.getEntryData());
            if (templateValue == null) {
                continue;
            } else {
                context.setIndexUsed(true);
            }

            final IStoredList<IEntryCacheInfo> potentialMatchList = index.getIndexEntries(templateValue);
            final int potentialMatchListSize = potentialMatchList == null ? 0 : potentialMatchList.size();
            // no entries with the corresponding template value exist - so return null
            if (potentialMatchListSize == 0) {
                return null;
            }

            if (shortestPotentialMatchListSize == 1 && !context.isIndicesIntersectionEnabled())
                continue; //already have a good match

            if (context.isIndicesIntersectionEnabled())
                intersectedList = addToIntersectedList(context, intersectedList, potentialMatchList, template.isFifoTemplate(), potentialMatchListSize <= shortestPotentialMatchListSize/*shortest*/, typeData);
            // make sure that if the list are of the same size - the indexed list is chosen instead of the all entries list
            // since indexed list has only 1 segment and its traversal is usually faster
            if (potentialMatchListSize <= shortestPotentialMatchListSize) {
                shortestPotentialMatchListSize = potentialMatchListSize;
                shortestPotentialMatchList = potentialMatchList;
                shortestPotentialIndex = index;
                if (template.isFifoGroupPoll())
                    context.setFifoGroupIndexUsedInFifoGroupScan(shortestPotentialMatchList, index);
            }

            if (!shortestPotentialMatchList.isMultiObjectCollection() && !context.isIndicesIntersectionEnabled())
                break;
        }

        if( shortestPotentialIndex != null && context.getIndexMetricsContext() != null ){
            context.getIndexMetricsContext().addChosenIndex(shortestPotentialIndex.getUsageCounter());
        }

        if (_logger.isTraceEnabled())
            logSearchCompoundSelection(typeData, shortestPotentialMatchList, compound_selection, null);

        if (context.isIndicesIntersectionEnabled())
            intersectedList = addToIntersectedList(context, intersectedList, shortestPotentialMatchList, template.isFifoTemplate(), true/*shortest*/, typeData);

        return shortestPotentialMatchList;
    }


    private static IStoredList<IEntryCacheInfo> findShortestPotentialMatchListCompound(Context context, TypeData typeData, ITemplateHolder template,
                                                                                       IStoredList<IEntryCacheInfo> shortestPotentialMatchList, int numOfFields, int latestIndexToConsider, Set<String> usedInSegments) {
        // if there is no potential match - return immediately
        if (shortestPotentialMatchList == null)
            return shortestPotentialMatchList;
        int shortestPotentialMatchListSize = shortestPotentialMatchList.size();
        if (shortestPotentialMatchListSize == 1 && !context.isIndicesIntersectionEnabled())
            return shortestPotentialMatchList; //already have a good match

        final List<TypeDataIndex> indexes = typeData.getCompoundIndexes();
        for (TypeDataIndex index : indexes) {
            if (index.disableIndexUsageForOperation(typeData, latestIndexToConsider)) {
                if (context.isBlobStoreUsePureIndexesAccess())
                    context.setBlobStoreUsePureIndexesAccess(false);
                continue;   //uncompleted index
            }

            final Object templateValue = index.getIndexValueForTemplate(template.getEntryData());
            if (templateValue == null) {
                if (context.isBlobStoreUsePureIndexesAccess()) {
                    CompoundIndex ci = (CompoundIndex) index.getIndexDefinition();
                    for (ISpaceCompoundIndexSegment seg : ci.getCompoundIndexSegments()) {
                        if (seg.getSegmentValue(template.getEntryData()) != null && typeData.getIndex(seg.getName()) == null) {
                            context.setBlobStoreUsePureIndexesAccess(false);
                            break;
                        }
                    }
                }
                continue;
            }

            final IStoredList<IEntryCacheInfo> potentialMatchList = index.getIndexEntries(templateValue);
            final int potentialMatchListSize = potentialMatchList == null ? 0 : potentialMatchList.size();
            // no entries with the corresponding template value exist - so return null
            if (potentialMatchListSize == 0)
                return null;
            if (context.isIndicesIntersectionEnabled())
                addToIntersectedList(context, context.getChosenIntersectedList(false), potentialMatchList, template.isFifoTemplate(), potentialMatchListSize <= shortestPotentialMatchListSize/*shortest*/, typeData);
            //go over this index and signal its segments not to be used as indices
            CompoundIndex ci = (CompoundIndex) index.getIndexDefinition();
            for (ISpaceCompoundIndexSegment seg : ci.getCompoundIndexSegments()) {
                usedInSegments.add(seg.getName());
            }

            // make sure that if the list are of the same size - the indexed list is chosen instead of the all entries list
            // since indexed list has only 1 segment and its traversal is usually faster
            if (potentialMatchListSize <= shortestPotentialMatchListSize) {
                shortestPotentialMatchListSize = potentialMatchListSize;
                shortestPotentialMatchList = potentialMatchList;
            }
            if (!shortestPotentialMatchList.isMultiObjectCollection() || shortestPotentialMatchListSize == 1)
                break; //good match
        }

        return shortestPotentialMatchList;
    }


    private static IStoredList<IEntryCacheInfo> findShortestPotentialMatchListCustom(Context context,
                                                                                     TypeData typeData,
                                                                                     ITemplateHolder template,
                                                                                     IStoredList<IEntryCacheInfo> shortestPotentialMatchList,
                                                                                     int latestIndexToConsider) {
        // if there is no potential match - return immediately
        if (shortestPotentialMatchList == null)
            return shortestPotentialMatchList;

        final ICustomQuery customQuery = template.getCustomQuery();
        if (customQuery == null)
            return shortestPotentialMatchList;
        final List<IQueryIndexScanner> customIndexes = customQuery.getCustomIndexes();
        if (customIndexes == null || customIndexes.isEmpty()) {
            return shortestPotentialMatchList;
        }

        IQueryIndexScanner shortestPotentialIndex = null;
        MultiIntersectedStoredList<IEntryCacheInfo> intersectedList = context.getChosenIntersectedList(false);
        // Iterate over custom indexes to find shortest potential match list:
        for (IQueryIndexScanner queryIndex : customIndexes) {
            // Get entries in space that match the indexed value in the query (a.k.a potential match list):
            IObjectsList result = queryIndex.getIndexedEntriesByType(context, typeData, template, latestIndexToConsider);

            if (result == IQueryIndexScanner.RESULT_IGNORE_INDEX) {
                context.setBlobStoreUsePureIndexesAccess(false);
                continue;
            }

            context.setIndexUsed(true);

            if (result == IQueryIndexScanner.RESULT_NO_MATCH)
                return null;

            final IStoredList<IEntryCacheInfo> potentialMatchList = (IStoredList<IEntryCacheInfo>) result;
            final int potentialMatchListSize = potentialMatchList == null ? 0 : potentialMatchList.size();

            // If the potential match list is empty, there's no need to continue:
            if (potentialMatchListSize == 0) {
                return null;
            }

            if (context.isIndicesIntersectionEnabled())
                intersectedList = addToIntersectedList(context, intersectedList, potentialMatchList, template.isFifoTemplate(), potentialMatchListSize <= shortestPotentialMatchList.size()/*shortest*/, typeData);
            // If the potential match list is shorter than the shortest match list so far, keep it:
            if (potentialMatchListSize <= shortestPotentialMatchList.size()) {
                shortestPotentialMatchList = potentialMatchList;
                shortestPotentialIndex = queryIndex;
            }

            if (!shortestPotentialMatchList.isMultiObjectCollection() && !context.isIndicesIntersectionEnabled())
                break;
        }

        if( shortestPotentialIndex != null && context.getIndexMetricsContext() != null ){
            context.getIndexMetricsContext().addChosenIndex( shortestPotentialIndex );
        }

        if (context.isIndicesIntersectionEnabled())
            intersectedList = addToIntersectedList(context, intersectedList, shortestPotentialMatchList, template.isFifoTemplate(), true/*shortest*/, typeData);

        return shortestPotentialMatchList;
    }

    /**
     * Using extended match-
     * <p>
     * Computes the best way to reach a list of potential match of entries of the specified type.
     * <p>
     * Handles also un-indexed entries.
     * <p>
     * Assumes the specified template is a superclass (in the wide sense) of the specified entry.
     *
     * @param entryType the type of entries to search for
     * @param template  the template to match against
     * @return a SL or IOrderedIndexScan containing potential matches or <code>null</code> if there
     * are no matches.
     */
    public Object getEntriesMinIndexExtended(Context context, TypeData entryType, int numOfFields, ITemplateHolder template) {
        if (template instanceof TemplateHolder && (((TemplateHolder) template).getExplainPlan() != null)) {
            SingleExplainPlan singleExplainPlan = ((TemplateHolder) template).getExplainPlan();
            singleExplainPlan.setPartitionId(Integer.toString(getEngine().getPartitionIdOneBased()));
            ExplainPlanContext explainPlanContext = new ExplainPlanContext();
            explainPlanContext.setSingleExplainPlan(singleExplainPlan);
            context.setExplainPlanContext(explainPlanContext);
        }


        //FIFO template ? consider only fifo-supported classes
        if (template.isFifoSearch() && !entryType.isFifoSupport())
            return null;

        // if there are no indexes on this class, do full-scan
        if (!entryType.hasIndexes() && (template.getCustomQuery() == null || template.getCustomQuery().getCustomIndexes() == null ||template.getCustomQuery().getCustomIndexes().isEmpty() ))
            return entryType.getEntries();
        int latestIndexToConsider = entryType.getLastIndexCreationNumber();

        //is it id  && blobStoreSpace???
        TypeDataIndex<IStoredList<IEntryCacheInfo>> primaryKey = entryType.getIdField();
        if (primaryKey != null && primaryKey.getPos() < numOfFields && latestIndexToConsider >= primaryKey.getIndexCreationNumber()
                && template.getExtendedMatchCodes()[primaryKey.getPos()] == TemplateMatchCodes.EQ) {
            Object templateValue = primaryKey.getIndexValueForTemplate(template.getEntryData());
            if (templateValue != null) {
                context.setBlobStoreUsePureIndexesAccess(false);  //no index
                IEntryCacheInfo pEntryByUid;
                if (entryType.disableIdIndexForEntries(primaryKey)) {
                    pEntryByUid = getPEntryByUid(entryType.generateUid(templateValue));
                } else {
                    pEntryByUid = primaryKey.getUniqueEntriesStore().get(templateValue);
                }
                if (context.getExplainPlanContext() != null) {
                    context.getExplainPlanContext().setMatch(new IndexChoiceNode("MATCH"));
                    context.getExplainPlanContext().getSingleExplainPlan().addScanIndexChoiceNode(entryType.getClassName(), context.getExplainPlanContext().getMatch());
                    int indexSize = pEntryByUid == null ? 0 : pEntryByUid.size();
                    IndexInfo indexInfo = new IndexInfo(entryType.getProperty(primaryKey.getPos()).getName(), indexSize, primaryKey.getIndexType(), templateValue, QueryOperator.EQ);
                    context.getExplainPlanContext().getMatch().addOption(indexInfo);
                    context.getExplainPlanContext().getMatch().setChosen(indexInfo);
                }
                if( context.getIndexMetricsContext() != null ) {
                    context.getIndexMetricsContext().addChosenIndex(primaryKey.getUsageCounter());
                }
                return pEntryByUid;

            }
        }


        Object compound_selection = null;
        String compound_name = null;

        // get index for scan
        IStoredList<IEntryCacheInfo> resultSL = null;
        IScanListIterator<IEntryCacheInfo> resultOIS = null;
        ScanUidsIterator uidsIter = null;
        final boolean ignoreOrderedIndexes = entryType.getEntries().size() < _minExtendedIndexActivationSize && !context.isBlobStoreUsePureIndexesAccess();
        context.setIntersectionEnablment(entryType.isBlobStoreClass() && !template.isFifoGroupPoll());
        MultiIntersectedStoredList<IEntryCacheInfo> intersectedList = null;   //if index intersection desired
        int uidsSize =Integer.MAX_VALUE;

        final ICustomQuery customQuery = template.getCustomQuery();
        boolean indexUsed = false;
        String nameOfChosenCustomIndex = null;
        if (customQuery != null && customQuery.getCustomIndexes() != null) {
            for (IQueryIndexScanner index : customQuery.getCustomIndexes()) {
                // Get entries in space that match the indexed value in the query (a.k.a potential match list):
                IObjectsList result = index.getIndexedEntriesByType(context, entryType, template, latestIndexToConsider);

                if (result == IQueryIndexScanner.RESULT_IGNORE_INDEX) {
                    context.setBlobStoreUsePureIndexesAccess(false);
                    continue;
                }

                if (result == IQueryIndexScanner.RESULT_NO_MATCH) {
                    return null;
                }

                if (_logger.isTraceEnabled() && index.getIndexName() != null) {
                    final TypeDataIndex idx = entryType.getIndex(index.getIndexName());
                    if (idx != null && idx.isCompound()) {
                        compound_selection = result;
                        compound_name = index.getIndexName();
                    }
                }

                //check the return type - can be extended iterator
                if (result != null && result.isIterator()) {
                    if (uidsIter == null) {
                        final IScanListIterator resultScan = (IScanListIterator<IEntryCacheInfo>) result;
                        if (index.isUidsScanner()) {
                            uidsIter = (ScanUidsIterator) resultScan;
                            uidsSize = uidsIter.size();
                        }
                        if (resultScan.hasSize()) {
                            if (resultOIS == null || resultScan.size() < resultOIS.size()) {
                                resultOIS = resultScan;
                                nameOfChosenCustomIndex = index.getIndexName();
                            }
                        } else {
                            //can't compare, override with last result (might not be the optimal approach)
                            resultOIS = resultScan;
                            nameOfChosenCustomIndex = index.getIndexName();
                        }

                        if (context.isIndicesIntersectionEnabled()) {
                            intersectedList = addToIntersectedList(context, intersectedList, resultScan, template.isFifoTemplate(), false/*shortest*/, entryType);
                        }
                        // Log index usage
                        if (_logger.isTraceEnabled()) {
                            _logger.trace("EXTENDED-INDEX '" + index.getIndexName() + "' has been used for type [" +
                                    entryType.getClassName() + "]");
                        }
                    }
                    indexUsed = true;
                    continue;
                }

                IStoredList<IEntryCacheInfo> entriesVector = (IStoredList<IEntryCacheInfo>) result;
                // Log index usage
                if (_logger.isTraceEnabled()) {
                    _logger.trace("BASIC-INDEX '" + index.getIndexName() + "' has been used for type [" +
                            entryType.getClassName() + "]");
                }

                if (entriesVector == null) {
                    return null; //no values matching the index value
                }

                indexUsed = true;

                if (context.isIndicesIntersectionEnabled()) {
                    intersectedList = addToIntersectedList(context, intersectedList, entriesVector, template.isFifoTemplate(), false/*shortest*/, entryType);
                }
                // check if the minimal index needs to be updated
                if (resultSL == null || resultSL.size() > entriesVector.size()) {
                    resultSL = entriesVector;
                    nameOfChosenCustomIndex = index.getIndexName();
                }
            }
        }

        //if single index and is a lucene index
        if (GS_13953_ENABLED && indexUsed && nameOfChosenCustomIndex != null
                && resultOIS != null && resultOIS.isExtensionIndex()) {
            //Remove already traversed paths of first custom index
            ((QueryExtensionIndexEntryIteratorWrapper) resultOIS).setAlreadyMatchedIndexPath(nameOfChosenCustomIndex);
            return resultOIS;
        }

        if (resultSL == null || (resultSL.size() > MIN_SIZE_TO_PERFORM_EXPLICIT_PROPERTIES_INDEX_SCAN_
                && uidsSize > MIN_SIZE_TO_PERFORM_EXPLICIT_PROPERTIES_INDEX_SCAN_)
                || (entryType.isBlobStoreClass() && resultSL.size() > 0)) {
            final TypeDataIndex[] indexes = entryType.getIndexes();
            TypeDataIndex selectedShortestIndex = null;
            for (TypeDataIndex<Object> index : indexes) {
                int pos = index.getPos();
                if (pos >= numOfFields)
                    break;

                if (latestIndexToConsider < index.getIndexCreationNumber())
                    continue;   //uncompleted index

                final short extendedMatchCode = template.getExtendedMatchCodes()[pos];
                final Object templateValue = index.getIndexValueForTemplate(template.getEntryData());

                // ignore indexes that don't support fifo order scanning - otherwise the results won't preserve the fifo order
                if (template.isFifoTemplate() && !TemplateMatchCodes.supportFifoOrder(extendedMatchCode))
                    continue;

                IStoredList<IEntryCacheInfo> entriesVector = null;
                //what kind of match we need on the template ?
                switch (extendedMatchCode) {
                    case TemplateMatchCodes.NOT_NULL:    //any not null will do
                    case TemplateMatchCodes.REGEX:        //unusable index for this
                        continue;
                    case TemplateMatchCodes.IS_NULL:
                        entriesVector = index.getNullEntries();
                        if (context.isIndicesIntersectionEnabled())
                            intersectedList = addToIntersectedList(context, intersectedList, entriesVector, template.isFifoTemplate(), false/*shortest*/, entryType);
                        if (resultSL == null || resultSL.size() > entriesVector.size()) {
                            if (context.getExplainPlanContext() != null) {
                                if (context.getExplainPlanContext().getMatch() == null) {
                                    context.getExplainPlanContext().setMatch(new IndexChoiceNode("MATCH"));
                                    context.getExplainPlanContext().getSingleExplainPlan().addScanIndexChoiceNode(entryType.getClassName(), context.getExplainPlanContext().getMatch());
                                }
                                int indexSize = entriesVector == null ? 0 : entriesVector.size();
                                IndexInfo indexInfo = new IndexInfo(entryType.getProperty(pos).getName(), indexSize, index.getIndexType(), templateValue, QueryOperator.IS_NULL);
                                context.getExplainPlanContext().getMatch().addOption(indexInfo);
                                context.getExplainPlanContext().getMatch().setChosen(indexInfo);
                            }
                            selectedShortestIndex = index;
                            resultSL = entriesVector;
                        }
                        break; //evaluate

                    case TemplateMatchCodes.NE:
                        if (templateValue == null)
                            continue; //TBD

                        entriesVector = index.getNonUniqueEntriesStore().get(templateValue);
                        if (entriesVector != null && entriesVector.size() == entryType.getEntries().size())
                            return null;
                        continue;  //nothing to do here

                    case TemplateMatchCodes.EQ:
                        if (templateValue == null) {
                            continue;//TBD
                        } else {
                            indexUsed = true;
                        }

                        entriesVector = index.getIndexEntries(templateValue);
                        if (entriesVector == null)
                            return null; //no values
                        if (context.isIndicesIntersectionEnabled())
                            intersectedList = addToIntersectedList(context, intersectedList, entriesVector, template.isFifoTemplate(), false/*shortest*/, entryType);
                        if (resultSL == null || resultSL.size() > entriesVector.size()) {
                            handleExplainPlanMatchCodes(true, context, entryType, index, pos, templateValue, entriesVector);
                            resultSL = entriesVector;
                            selectedShortestIndex = index;
                        } else {
                            handleExplainPlanMatchCodes(false, context, entryType, index, pos, templateValue, entriesVector);
                        }

                        break; //evaluate

                    case TemplateMatchCodes.LT:
                    case TemplateMatchCodes.LE:
                    case TemplateMatchCodes.GE:
                    case TemplateMatchCodes.GT: //for GT we must clip first value if eq to limit
                        if (templateValue == null)
                            continue; //TBD
                        if (ignoreOrderedIndexes)
                            continue; //# of entries did not reach limit

                        if (index.getExtendedIndexForScanning() == null) {
                            if (context.isBlobStoreUsePureIndexesAccess())
                                context.setBlobStoreUsePureIndexesAccess(false);
                            continue; //ordered index not defined
                        }
                        indexUsed = true;
                        if (resultOIS == null || entryType.isBlobStoreClass()) {
                            final Object rangeValue = template.getRangeValue(pos);
                            final boolean isInclusive = rangeValue == null ? false : template.getRangeInclusion(pos);
                            //range limit passed- query with "up to" range
                            //NOTE! - currently we support only range "up-to" inclusive
                            IScanListIterator<IEntryCacheInfo> originalOIS = resultOIS;
                            resultOIS = index.getExtendedIndexForScanning().establishScan(templateValue,
                                    extendedMatchCode, rangeValue, isInclusive);
                            if (resultOIS == null)
                                return null;  //no values

                            if (context.getExplainPlanContext() != null) {
                                if (context.getExplainPlanContext().getMatch() == null) {
                                    context.getExplainPlanContext().setMatch(new IndexChoiceNode("MATCH"));
                                    context.getExplainPlanContext().getSingleExplainPlan().addScanIndexChoiceNode(entryType.getClassName(), context.getExplainPlanContext().getMatch());
                                }
                                int indexSize = -1; //unknown size
                                IndexInfo indexInfo = new IndexInfo(entryType.getProperty(pos).getName(), indexSize, index.getIndexType(), templateValue, ExplainPlanUtil.getQueryOperator(extendedMatchCode));
                                context.getExplainPlanContext().getMatch().addOption(indexInfo);
                                if (resultSL == null && uidsSize == Integer.MAX_VALUE) {
                                    context.getExplainPlanContext().getMatch().setChosen(indexInfo);
                                }
                            }

                            if (resultSL == null && uidsSize == Integer.MAX_VALUE) {
                                selectedShortestIndex = index;
                            }

                            if (context.isIndicesIntersectionEnabled())
                                intersectedList = addToIntersectedList(context, intersectedList, resultOIS, template.isFifoTemplate(), false/*shortest*/, entryType);
                            if (uidsSize != Integer.MAX_VALUE)
                                resultOIS = originalOIS;
                        }
                        break; //evaluate
                }//switch
            } // for

            if (context.getIndexMetricsContext() != null && selectedShortestIndex != null) {
                context.getIndexMetricsContext().addChosenIndex(selectedShortestIndex.getUsageCounter());
            }

            if (ProtectiveMode.isQueryWithoutIndexProtectionEnabled() && !indexUsed && !ignoreOrderedIndexes) {
                throw new ProtectiveModeException("Cannot perform operation: The request references only unindexed fields!\n" +
                        "(you can disable this protection, though it is not recommended, by setting the following system property: " + ProtectiveMode.QUERY_WITHOUT_INDEX + "=false)");
            }
        }

        if (resultSL == null) {
            // the entry type is indexed, but the template has a null value for
            // every field which is indexed (or template is null), so we must return all the entries.
            if (resultOIS == null) {
                context.setBlobStoreUsePureIndexesAccess(false);

                return entryType.getEntries();
            }

            if (_logger.isTraceEnabled())
                logSearchCompoundSelection(entryType, resultOIS, compound_selection, compound_name);
            if (context.isIndicesIntersectionEnabled())
                intersectedList = addToIntersectedList(context, intersectedList, resultOIS, template.isFifoTemplate(), true/*shortest*/, entryType);
            return resultOIS;
        }

        if (resultOIS == null || (resultSL.size() < entryType.getEntries().size() && (uidsSize == Integer.MAX_VALUE || resultSL.size() <= uidsSize))) {
            if (_logger.isTraceEnabled())
                logSearchCompoundSelection(entryType, resultSL, compound_selection, compound_name);
            if (context.isIndicesIntersectionEnabled()) {
                intersectedList = addToIntersectedList(context, intersectedList, resultOIS, template.isFifoTemplate(), false/*shortest*/, entryType);
                intersectedList = addToIntersectedList(context, intersectedList, resultSL, template.isFifoTemplate(), true/*shortest*/, entryType);
            }
            return resultSL;
        }

        if (_logger.isTraceEnabled())
            CacheManager.logSearchCompoundSelection(entryType, resultOIS, compound_selection, compound_name);
        if (context.isIndicesIntersectionEnabled()) {
            intersectedList = addToIntersectedList(context, intersectedList, resultSL, template.isFifoTemplate(), false/*shortest*/, entryType);
            intersectedList = addToIntersectedList(context, intersectedList, resultOIS, template.isFifoTemplate(), true/*shortest*/, entryType);
        }
        return resultOIS;
    }

    private void handleExplainPlanMatchCodes(boolean chosen, Context context, TypeData entryType, TypeDataIndex<Object> index, int pos, Object templateValue, IStoredList<IEntryCacheInfo> entriesVector) {
        if (context.getExplainPlanContext() != null) {
            if (context.getExplainPlanContext().getMatch() == null) {
                context.getExplainPlanContext().setMatch(new IndexChoiceNode("MATCH"));
                context.getExplainPlanContext().getSingleExplainPlan().addScanIndexChoiceNode(entryType.getClassName(), context.getExplainPlanContext().getMatch());
            }
            int indexSize = entriesVector == null ? 0 : entriesVector.size();
            IndexInfo indexInfo = new IndexInfo(entryType.getProperty(pos).getName(), indexSize, index.getIndexType(), templateValue, QueryOperator.EQ);
            context.getExplainPlanContext().getMatch().addOption(indexInfo);
            if (chosen) {
                context.getExplainPlanContext().getMatch().setChosen(indexInfo);
            }
        }
    }

    private static MultiIntersectedStoredList<IEntryCacheInfo> addToIntersectedList(Context context, MultiIntersectedStoredList<IEntryCacheInfo> intersectedList, IObjectsList list, boolean fifoScan, boolean shortest, TypeData typeData) {
        if (list != null && list != typeData.getEntries()) {
            if (intersectedList == null) {
                intersectedList = new MultiIntersectedStoredList<IEntryCacheInfo>(context, list, fifoScan, typeData.getEntries(), !context.isBlobStoreUsePureIndexesAccess() /*false positive only*/);
                context.setChosenIntersectedList(intersectedList);
            } else
                intersectedList.add(list, shortest);
        }

        return intersectedList;
    }


    private static void logSearchCompoundSelection(TypeData type, Object res, Object compound_selection, String compound_name) {
        if (!_logger.isTraceEnabled() || res == null || res != compound_selection)
            return;
        if (compound_name == null)
            compound_name = " ";
        _logger.trace("COMPOUND-INDEX '" + compound_name + "' has been selected for type [" +
                type.getClassName() + "]");
    }

    IScanListIterator<IEntryCacheInfo> getScannableEntriesMinIndexExtended(Context context, TypeData entryType, int numOfFields, ITemplateHolder template) {
        if (context.isBlobStoreTryNonPersistentOp())
            context.setBlobStoreUsePureIndexesAccess(isRelevantUsePureIndexesAccess(context, entryType, template));
        if (template.isFifoGroupPoll())
            return _fifoGroupCacheImpl.getScannableEntriesMinIndexExtended(context, entryType, numOfFields, template);
        Object chosen = getEntriesMinIndexExtended(context, entryType, numOfFields, template);
        if (chosen != null && context.isIndicesIntersectionEnabled() && context.getChosenIntersectedList(false) != null)
            return context.getChosenIntersectedList(true/*final*/);
        if (chosen != null && (chosen instanceof IEntryCacheInfo))
            return (IEntryCacheInfo) chosen;
        return (chosen instanceof IStoredList) ? new ScanSingleListIterator((IStoredList) chosen, template.isFifoTemplate(), template.isServerIterator()) :
                (IScanListIterator<IEntryCacheInfo>) chosen;
    }


    public IEntryHolder getEntryByIdFromPureCache(Object id, IServerTypeDesc typeDesc) {
        TypeData typeData = _typeDataMap.get(typeDesc);
        if (typeData == null)
            return null;

        TypeDataIndex<IStoredList<IEntryCacheInfo>> idPropertyIndex = typeData.getIdField();
        if (idPropertyIndex == null || typeData.getLastIndexCreationNumber() < idPropertyIndex.getIndexCreationNumber())
            throw new EngineInternalSpaceException("No index defined for entry.");

        if (typeData.disableIdIndexForEntries(idPropertyIndex))
            return getEntryByUidFromPureCache(typeData.generateUid(id));


        IEntryCacheInfo pe = idPropertyIndex.getUniqueEntriesStore().get(id);
        if (pe == null)
            return null;

        return pe.getEntryHolder(this);
    }


    /**
     * Computes the best way to reach a list of potential match of templates of the specified type.
     * <p>
     * Handles also un-indexed templates.
     * <p>
     * Assumes the specified template is a superclass (in the wide sense) of the specified entry.
     *
     * @param templateType the type of templates to search for
     * @param entry        the entry to match against
     * @return vector array containing potential matches, or a single vector
     */
    private Object findTemplatesByIndex(Context context, TypeData templateType, IEntryHolder entry, MatchTarget matchTarget) {
        Object result = null;
        IStoredList<TemplateCacheInfo>[] t_vec;
        IStoredList<TemplateCacheInfo>[] min_array = null;
        boolean need_search = true;

        if (matchTarget == MatchTarget.NOTIFY && templateType.getM_NumRegularNotifyTemplatesStored() == 0)
            need_search = false;

        // if there are no indexes on this class, do full-scan
        final IEntryData entryData = entry.getEntryData();
        if (!templateType.hasIndexes()) {
            if (need_search)
                result = templateType.getTemplates(matchTarget);
            //get templates waiting for uid
            result = getTemplatesWaitingForUid(templateType, matchTarget, result, entry);

            if (need_search)
                result = getTemplatesExtendedSearch(templateType, matchTarget, result);

            return result;
        }

        // compute shortest vector
        int minIndexSize = 0;
        IStoredList templVector = null, nullTemplVector = null;

        int latestIndexToConsider = 0;

        if (need_search) {
            boolean anyIndex = false;
            final TypeDataIndex<?>[] indexes = templateType.getIndexes();
            for (TypeDataIndex<?> index : indexes) {
                if (index.getIndexCreationNumber() > latestIndexToConsider)
                    continue;   //irrelevant for templates index
                if (index.isCompound())
                    continue;  //currently waiting templates selection is not supported

                anyIndex = true;

                IStoredList<TemplateCacheInfo> templatesVector = null, nullTemplatesVector = null;
                t_vec = null;

                Object entryValue = index.getIndexValue(entryData);
                if (index.isMultiValuePerEntryIndex() && entryValue != null) {
                    IStoredList<TemplateCacheInfo> multiValueTemplates = new ConcurrentSegmentedStoredList<TemplateCacheInfo>(true  /* supportsFifoPerSegment*/,1, true /*padded*/);
                    nullTemplatesVector = (matchTarget == MatchTarget.READ_TAKE) ? index._RTNullTemplates : index._NNullTemplates;
                    // Collect templates for collection items
                    for (Object value : (Collection<?>) entryValue) {
                        if (matchTarget == MatchTarget.READ_TAKE) {
                            if (value != null) {
                                t_vec = index._RTTemplates.get(value);
                                if ((t_vec != null) && (!t_vec[0].isEmpty())) {
                                    templatesVector = t_vec[0];
                                    for (IStoredListIterator<TemplateCacheInfo> it = templatesVector.establishListScan(false); it != null; it = templatesVector.next(it)) {
                                        multiValueTemplates.add(it.getSubject());
                                    }
                                }
                            }
                        } else {
                            if (value != null) {
                                t_vec = index._NTemplates.get(value);
                                if ((t_vec != null) && (!t_vec[0].isEmpty())) {
                                    templatesVector = t_vec[0];
                                    for (IStoredListIterator<TemplateCacheInfo> it = templatesVector.establishListScan(false); it != null; it = templatesVector.next(it)) {
                                        TemplateCacheInfo subject = it.getSubject();

                                        if (subject != null)
                                            multiValueTemplates.add(subject);
                                    }
                                }
                            }
                        }
                    }
                    t_vec = new IStoredList[2];
                    t_vec[0] = multiValueTemplates;
                    t_vec[1] = nullTemplatesVector;
                    if (!multiValueTemplates.isEmpty())
                        templatesVector = multiValueTemplates;

                } else {
                    if (matchTarget == MatchTarget.READ_TAKE) {
                        if (entryValue != null) {
                            t_vec = index._RTTemplates.get(entryValue);
                            if ((t_vec != null) && (!t_vec[0].isEmpty()))
                                templatesVector = t_vec[0];
                        }
                        nullTemplatesVector = index._RTNullTemplates;
                    } else {
                        if (entryValue != null) {
                            t_vec = index._NTemplates.get(entryValue);
                            if ((t_vec != null) && (!t_vec[0].isEmpty()))
                                templatesVector = t_vec[0];
                        }
                        nullTemplatesVector = index._NNullTemplates;
                    }
                }

                int size = (templatesVector == null) ? nullTemplatesVector.size() :
                        templatesVector.size() + nullTemplatesVector.size();

                if (nullTemplVector == null) /* first time */ {
                    minIndexSize = size;
                    templVector = templatesVector;
                    nullTemplVector = nullTemplatesVector;
                    if (templatesVector != null)
                        min_array = t_vec;
                } else {
                    if (size < minIndexSize) /* replace Min */ {
                        minIndexSize = size;
                        templVector = templatesVector;
                        nullTemplVector = nullTemplatesVector;
                        if (templatesVector != null)
                            min_array = t_vec;
                        else
                            min_array = null;
                    }
                }
                if (size == 0)
                    break; //no use for more iterations
            } /* for */
            result = min_array;
            if (anyIndex) {
                if (templVector == null) {
                    // The entry has null value for all indexed template fields.
                    result = nullTemplVector;
                }
            } else {//no indexes
                result = templateType.getTemplates(matchTarget);
            }
        }//if (need_search)

        //get templates waiting for UID if exist
        result = getTemplatesWaitingForUid(templateType, matchTarget, result, entry);

        //add extended matches if preset according to indexes
        if (need_search)
            result = templateType.anyInitialExtendedIndex()
                    ? TypeDataIndex.getTemplatesExtendedIndexSearch(templateType, matchTarget, entry, result)
                    : getTemplatesExtendedSearch(templateType, matchTarget, result);

        return result;
    }


    /**
     * add, to the result of getTemplatesMinIndex, the extended-search templates. extended search
     * templates are stored per type and not per index-value
     */
    private Object getTemplatesExtendedSearch(TypeData templateType, MatchTarget matchTarget, Object tempResult) {
        IStoredList sl = templateType.getExtendedTemplates(matchTarget);

        if (sl.isEmpty())
            return tempResult;

        if (tempResult == null)
            return sl;

        IStoredList[] resSls;
        if (tempResult instanceof IStoredList) {
            IStoredList orgRes = (IStoredList) tempResult;
            if (orgRes.isEmpty())
                return sl;
            resSls = new IStoredList[2];
            resSls[0] = orgRes;
            resSls[1] = sl;
        } else {
            IStoredList[] sls = (IStoredList[]) tempResult;
            int dim = sls.length;
            resSls = new IStoredList[dim + 1];
            for (int i = 0; i < dim; i++) {
                resSls[i] = sls[i];
            }
            resSls[dim] = sl;
        }
        return resSls;

    }


    /**
     * add, to the result of getTemplatesMinIndex, the by-uid templates.
     */
    private Object getTemplatesWaitingForUid(TypeData templateType, MatchTarget matchTarget, Object tempResult, IEntryHolder entry) {
        IStoredList<TemplateCacheInfo> sl = templateType.getUidTemplates(matchTarget, entry.getUID());

        if (sl == null || sl.isEmpty())
            return tempResult;

        if (tempResult == null)
            return sl;

        IStoredList[] resSls;

        //add the SL
        if (tempResult instanceof IStoredList) {
            IStoredList orgRes = (IStoredList) tempResult;
            if (orgRes.isEmpty())
                return sl;
            resSls = new IStoredList[2];
            resSls[0] = orgRes;
            resSls[1] = sl;
        } else {
            IStoredList[] sls = (IStoredList[]) tempResult;
            int dim = sls.length;
            resSls = new IStoredList[dim + 1];
            for (int i = 0; i < dim; i++) {
                resSls[i] = sls[i];
            }
            resSls[dim] = sl;
        }
        return resSls;

    }

    /**
     * get permission to evict an entry, and if granted remove the entry from cache. this function
     * must be called by the eviction Strategy prior to performing an eviction of an entry. eviction
     * must be performed only if permission is granted by the space. permission can be denied if the
     * entry is currently locked by another thread that is performing a space operation on it NOTE:
     * after calling  grantEvictionPermissionAndRemove and receiving true as an answer the eviction
     * strategy should remove the entry from its internal state and call
     * reportEvictionCompletion(entry)
     *
     * @return true if permission is granted and entry was removed from cache
     */

    public boolean tryEvict(EvictableServerEntry entry) {

        EvictableEntryCacheInfo pev = (EvictableEntryCacheInfo) entry;
        IEntryHolder eh = pev.getEntryHolder();
        //check w.f. -
        if (pev.isPinned())
            return false;   //pinned, no permission

        if (requiresEvictionReplicationProtection()) {//can we evict the entry considering its replication marker ?
            Object lockObject = getEvictionReplicationsMarkersRepository().getLockObject(eh.getUID());
            try {
                synchronized (lockObject) {
                    if (!getEvictionReplicationsMarkersRepository().isEntryEvictable(eh.getUID(), true /*alreadyLocked*/))
                        return false;

                    return grantEvictionPermissionAndRemove_Impl(pev);
                }
            } finally {
                getEvictionReplicationsMarkersRepository().releaseLockObject(lockObject);
            }

        } else
            return grantEvictionPermissionAndRemove_Impl(pev);
    }

    private boolean grantEvictionPermissionAndRemove_Impl(EvictableEntryCacheInfo pev) {

        IEntryHolder eh = pev.getEntryHolder();

        if (!pev.setRemoving(false /*isPinned*/))
            return false;

        //ok- we evict the entry
        removeEntryFromCache(eh, true /*initiatedByEvictionStrategy*/, false /*locked*/, pev, RecentDeleteCodes.NONE);
        return true; //tell the eviction strategy object to remove the entry from its structure
    }

    /**
     * if relevant-unpin the entry method is called after each operation or xtn termination entry
     * should be locked when method is called
     *
     * @return null if not unpinned
     */
    public boolean unpinIfNeeded(Context context, IEntryHolder entry, ITemplateHolder template, IEntryCacheInfo pEntry) {
        if (!isEvictableCachePolicy() && !entry.isBlobStoreEntry())
            return false;

        if (template != null) {//template based op'
            if (template.isTakeOperation() && entry.isDeleted())
                return false;
        }
        if (useRecentUpdatesForPinning() && isEntryInRecentUpdates(entry))
            return false;

        if (pEntry == null)
            pEntry = entry.isBlobStoreEntry() ? ((IBlobStoreEntryHolder) entry).getBlobStoreResidentPart() : getEntryCacheInfo(entry);

        if (pEntry == null || !pEntry.isPinned())
            return false;

        if (useRecentDeletes() && pEntry.isRecentDelete())
            return false;

        if (entry.isBlobStoreEntry() && ((IBlobStoreRefCacheInfo) pEntry).isInBulk())
            return false;   //bulk termination will unpin

        // unpin entry if relevant
        if (pEntry.getEntryHolder(this).isMaybeUnderXtn() || pEntry.getEntryHolder(this).isHasWaitingFor())
            return false;

        if (isEvictableCachePolicy())
            pEntry.setPinned(false);
        else
            ((IBlobStoreRefCacheInfo) pEntry).unLoadFullEntryIfPossible(this, context);

        return true;

    }

    /*
     * eviction intiated from an api and not from the eviction strategy
	 * entry is locked && pined when api is called in order not to skip
	 * entries which are temporary locked by a trying api
	 *
	 * return true if entry is evicted
	 */
    private boolean initiatedCacheEviction(IEntryHolder entry) {
        IEntryCacheInfo pEntry = null;
        if (useRecentUpdatesForPinning() && isEntryInRecentUpdates(entry))
            return false;

        pEntry = getEntryCacheInfo(entry);

        if (pEntry == null || !pEntry.isPinned())
            return false;

        if (useRecentDeletes() && pEntry.isRecentDelete())
            return false;

        // unpin entry if relevant
        if (pEntry.getEntryHolder(this).isMaybeUnderXtn() || pEntry.getEntryHolder(this).isHasWaitingFor())
            return false;

        if (requiresEvictionReplicationProtection()) {//can we evict the entry considering its replication marker ?
            Object lockObject = getEvictionReplicationsMarkersRepository().getLockObject(entry.getUID());
            try {
                synchronized (lockObject) {
                    if (!getEvictionReplicationsMarkersRepository().isEntryEvictable(entry.getUID(), true /*alreadyLocked*/))
                        return false;

                    return initiatedCacheEviction_Impl(pEntry);
                }
            } finally {
                getEvictionReplicationsMarkersRepository().releaseLockObject(lockObject);
            }

        } else
            return initiatedCacheEviction_Impl(pEntry);
    }

    private boolean initiatedCacheEviction_Impl(IEntryCacheInfo pEntry) {

        //ok- we evict the entry
        pEntry.setRemoving(true /*pinned*/);
        removeEntryFromCache(pEntry.getEntryHolder(this), false /*initiatedByEvictionStrategy*/, true/* locked*/, pEntry, RecentDeleteCodes.NONE);

        return true;

    }


    public boolean initiatedEviction(Context context, IEntryHolder entry,
                                     boolean shouldReplicate)
            throws SAException {
        final boolean res = initiatedCacheEviction(entry);
        if (res && shouldReplicate) {//write a syncpacket to redo log for evict operation
            final IReplicationOutContext replContext = getReplicationContext(context);
            updateReplicationContext(replContext, context);
            //Notify cluster node manager of operation
            _replicationNode.outEvictEntry(replContext, entry);

            /** perform sync-repl if need */
            if (!context.isSyncReplFromMultipleOperation() && !context.isDisableSyncReplication()) {
                _engine.performReplication(context);
            }
        }

        return res;
    }


    /**
     * is this space a memory space ?
     *
     * @return true if the space is a memory space
     */
    public boolean isMemorySpace() {
        return _isMemorySA;
    }

    public boolean isResidentCacheEntry(EvictableServerEntry entry) {
        return true;
        //return _evictionStrategy.isVisible(entry);
    }

    public IReplicationOutContext getReplicationContext(Context context) {
        if (context == null)
            return null;

        IReplicationOutContext replicationContext = context.getReplicationContext();
        if (replicationContext == null) {
            replicationContext = _replicationNode.createContext();
            context.setReplicationContext(replicationContext);
        }

        return replicationContext;
    }

    /**
     * remove the shadow entry, remove the index references which are redundent returnOriginalValues
     * - false means the new values will stay and the old deleted - true means  the old values will
     * stay and the new deleted
     */
    void consolidateWithShadowEntry(TypeData pType, IEntryCacheInfo pmaster, boolean restoreOriginalValues, boolean onError) {
        ShadowEntryHolder shadowEh = pmaster.getEntryHolder(this).getShadow();
        int numIndexsesUpdated = 0;

        IEntryData deleteEntryData;
        IEntryData keptEntryData;
        ArrayList<IObjectInfo<IEntryCacheInfo>> deleteBackrefs;
        if (restoreOriginalValues) {
            deleteEntryData = pmaster.getEntryHolder(this).getEntryData();
            if (shadowEh.getNumOfLeaseUpdates() > 0 /*!pmaster.isSameLeaseManagerRef(shadowEh)*/) {
                _leaseManager.unregister(pmaster, deleteEntryData.getExpirationTime());
                pmaster.setLeaseManagerListRefAndPosition(shadowEh.getLeaseManagerListRef(), shadowEh.getLeaseManagerPosition());
            }
            pType.prepareForUpdatingIndexValues(this, pmaster, shadowEh.getEntryData());
            deleteBackrefs = pmaster.getBackRefs();
            keptEntryData = shadowEh.getEntryData();
            pmaster.setBackRefs(shadowEh.getBackRefs());
            pmaster.getEntryHolder(this).restoreUpdateXtnRollback(shadowEh.getEntryData());
        } else {
            if (shadowEh.getNumOfLeaseUpdates() > 0  /*!pmaster.isSameLeaseManagerRef(shadowEh)*/)
                _leaseManager.unregister(shadowEh, shadowEh.getEntryData().getExpirationTime());

            deleteBackrefs = shadowEh.getBackRefs();
            IEntryHolder keptEh = pmaster.getEntryHolder(this);
            keptEntryData = keptEh.getEntryData();
            deleteEntryData = shadowEh.getEntryData();
            keptEh.setOtherUpdateUnderXtnEntry(null);
        }

        int refpos = 1;
        if (pType.hasIndexes()) {
            // entry  indexes
            final TypeDataIndex[] indexes = pType.getIndexes();
            for (TypeDataIndex index : indexes) {
                if (index.disableIndexUsageForOperation(pType, pmaster.getLatestIndexCreationNumber()/*inputIndexCreationNumber*/))
                    continue;
                numIndexsesUpdated++;
                Object deleteValue = index.getIndexValue(deleteEntryData);
                boolean useOnError = onError && shadowEh.getNumOfIndexesUpdated() < numIndexsesUpdated;
                if (useOnError) {
                    try {
                        refpos = index.consolidateIndexValueOnXtnEnd(pmaster.getEntryHolder(this), pmaster, index.getIndexValue(keptEntryData), deleteValue, deleteBackrefs, refpos, true /*onError*/);
                    } catch (Exception ex) {
                    }//ignore since its an error revert
                } else
                    refpos = index.consolidateIndexValueOnXtnEnd(pmaster.getEntryHolder(this), pmaster, index.getIndexValue(keptEntryData), deleteValue, deleteBackrefs, refpos, false /*onError*/);
            } /* for (int pos...) */
            for (QueryExtensionIndexManagerWrapper queryExtensionIndexManager : pType.getForeignQueriesHandlers()) {
                try {
                    queryExtensionIndexManager.removeEntry(new SpaceServerEntryImpl(pmaster, this), restoreOriginalValues ? QueryExtensionIndexRemoveMode.ON_XTN_UPDATED_ROLLBACK : QueryExtensionIndexRemoveMode.ON_XTN_UPDATED_COMMIT,
                            deleteEntryData.getVersion());
                } catch (Exception ex) {
                    throw new RuntimeException("Remove entry to foreign index failed", ex);
                }
            }

        } /* if pType.m_AnyIndexes */


        shadowEh.setDeleted(true);
    }


    /**
     * @return true if the class  represented by enumeration a fifo class
     */
    public boolean isFromFifoClass(IServerTypeDesc typeDesc) {
        TypeData type = _typeDataMap.get(typeDesc);
        return type != null ? type.isFifoSupport() : false;
    }

//	void debug()
//	{
//		if( _logger.isInfoEnabled())
//		{
//			_logger.info("Cache Manager Entries: ");
//
//			for (Iterator<String> iter = m_Root.m_Entries.keySet().iterator(); iter.hasNext();)
//			{
//				String key = iter.next();
//				EntryCacheInfo pEntry = getPEntryByUid(key);
//
//				if (!key.equals(pEntry.getUID()))
//					_logger.info("ERROR: pEntry IEntryHolder UID is not equal to key");
//			}
//		}
//	}

    public TemplatesManager getTemplatesManager() {
        return _templatesManager;
    }

    /**
     * used by getTransactionsInfo to retrieve number of Locked Objects
     *
     * @return the number of Locked Objects by the given Transaction
     */
    public int getNumberOfLockedObjectsUnderTxn(XtnEntry xtnEntry) {
        return xtnEntry.getXtnData().getUnderXtnEntries(SelectType.ALL_ENTRIES) != null ?
                xtnEntry.getXtnData().getUnderXtnEntries(SelectType.ALL_ENTRIES).size() : 0;
    }

    public int getMinExtendedIndexActivationSize() {
        return _minExtendedIndexActivationSize;
    }

    //to be used only internaly for testing.
    public int getEnriesSize() {
        if (_entries == null) return 0;
        return _entries.size();
    }

	/*++++++++++++++++++++  FIFO RACE ++++++++++++++++++++++++++++++++++*/

    /**
     * get the Terminating fifo transaction
     */
    public long getLatestTTransactionTerminationNum() {
        return _terminatingXtnsInfo.getLatestTTransactionTerminationNum();
    }

    /**
     * get the Terminating fifo transaction //NOTE- must be called when xtns are locked !!!!!!!!
     */
    public void setLatestTransactionTerminationNum(long xtnTerminationNum) {
        _terminatingXtnsInfo.setLatestTransactionTerminationNum(xtnTerminationNum);
    }

    /**
     * set the fifo xtn number of a xtn
     */
    public void setFifoXtnNumber(XtnEntry xtnEntry, long fifoXtnNumber) {
        xtnEntry.getXtnData().setFifoXtnNumber(fifoXtnNumber);

    }

    /**
     * get the fifo xtn number of a xtn
     */
    public long getFifoXtnNumber(XtnEntry xtnEntry) {
        return xtnEntry.getXtnData().getFifoXtnNumber();
    }

    /**
     * update fifo xtn info for entry MUST be called  when entry is locked update write lock if
     * writeLock is true, else update read lock entryWritingXtn = true if this is the commit of the
     * entry write
     */
    public void updateFifoXtnInfoForEntry(IEntryHolder eh, long xtnNumber, boolean writeLock, boolean entryWritingXtn) {
        _terminatingXtnsInfo.updateFifoXtnInfoForEntry(eh, xtnNumber, writeLock, entryWritingXtn);
    }

    /**
     * create fifo xtn info for entry MUST be called  when entry is locked
     *
     * @param xtnNumber - the xtn in the system when entry  was created
     */
    public void setFifoCreationXtnInfoForEntry(IEntryHolder eh, long xtnNumber) {
        _terminatingXtnsInfo.setFifoCreationXtnInfoForEntry(eh, xtnNumber);
    }


    public TerminatingFifoXtnsInfo.FifoXtnEntryInfo getFifoEntryXtnInfo(IEntryHolder eh) {
        return _terminatingXtnsInfo.getFifoEntryXtnInfo(eh);
    }


    /**
     * remove fifo xtn info for entry MUST be called  when entry is locked
     */
    public void removeFifoXtnInfoForEntry(IEntryHolder eh) {
        _terminatingXtnsInfo.removeFifoXtnInfoForEntry(eh);

    }

    public ConcurrentHashMap<FifoXtnEntryInfo, FifoXtnEntryInfo> getTerminatingXtnsEntries() {
        return _terminatingXtnsInfo.getTerminatingXtnsEntries();
    }

    public void lockEntry(XtnData xtn, IEntryCacheInfo pEntry, OperationID operationID) {
        boolean fifo = isFromFifoClass(pEntry.getEntryHolder(this).getServerTypeDesc());
        xtn.addLockedEntry(pEntry, operationID, fifo);
    }

    public void updateLock(XtnData xtn, IEntryCacheInfo pEntry, OperationID operationID, int templateOperation) {
        boolean isReadOperation = templateOperation == SpaceOperations.READ
                || templateOperation == SpaceOperations.READ_IE;

        boolean fifo = isFromFifoClass(pEntry.getEntryHolder(this).getServerTypeDesc());
        xtn.updateLock(pEntry, operationID, isReadOperation, fifo);

    }

    public void removeLockedEntry(XtnData xtn, IEntryCacheInfo pEntry) {
        IStoredList<IEntryCacheInfo> locked = xtn.getLockedEntries();
        if (locked != null && locked.removeByObject(pEntry) && isFromFifoClass(pEntry.getEntryHolder(this).getServerTypeDesc())) {
            IStoredList<IEntryCacheInfo> flocked = xtn.getLockedFifoEntries();
            flocked.removeByObject(pEntry);
        }
    }


    /**
     * @return the sA
     */
    public IStorageAdapter getStorageAdapter() {
        return _storageAdapter;
    }

    public FifoGroupCacheImpl getFifoGroupCacheImpl() {
        return _fifoGroupCacheImpl;
    }

    private void updateReplicationContextForUpdateEntry(IReplicationOutContext replicationContext, IEntryData originalData, Context context,
                                                        boolean[] partialUpdatedValuesIndicators) {

        ReplicationOutContext typedContext = (ReplicationOutContext) replicationContext;
        typedContext.setOperationID(context.getOperationID());
        if (!_engine.getConflictingOperationPolicy().isOverride())
            //in override mode we cant rely on target holding the previous version since we always want to step over
            typedContext.setPartialUpdatedValuesIndicators(partialUpdatedValuesIndicators);
        typedContext.setFromGateway(context.isFromGateway());
        typedContext.setPreviousUpdatedEntryData(originalData);
    }


    private void updateReplicationContextForChangeEntry(
            IReplicationOutContext replicationContext, IEntryData originalData,
            Context context, Collection<SpaceEntryMutator> mutators) {
        ReplicationOutContext typedContext = (ReplicationOutContext) replicationContext;
        typedContext.setOperationID(context.getOperationID());
        typedContext.setFromGateway(context.isFromGateway());
        typedContext.setPreviousUpdatedEntryData(originalData);
        typedContext.setSpaceEntryMutators(mutators);
    }


    private void updateReplicationContext(IReplicationOutContext replicationContext,
                                          Context context) {
        ReplicationOutContext typedContext = (ReplicationOutContext) replicationContext;
        typedContext.setOperationID(context.getOperationID());
        typedContext.setFromGateway(context.isFromGateway());
        typedContext.setBackupOnly(context.isBackupOnly());
    }

    private void updateReplicationContextForTransaction(IReplicationOutContext replicationContext,
                                                        Context context,
                                                        boolean[] updateRedoLogs,
                                                        Map<String, Object> partialUpdatesAndInPlaceUpdatesInfo,
                                                        OperationID[] opIDs, XtnEntry xtnEntry) {

        ReplicationOutContext typedContext = (ReplicationOutContext) replicationContext;
        typedContext.setOperationID(context.getOperationID());
        typedContext.setShouldReplicate(updateRedoLogs);
        typedContext.setPartialUpdatesInfo(partialUpdatesAndInPlaceUpdatesInfo);
        typedContext.setOperationIDs(opIDs);
        typedContext.setFromGateway(context.isFromGateway());
        typedContext.setOverrideVersionUids(xtnEntry.getOverrideVersionUids());
    }

    private class TypeDescListener implements IServerTypeDescListener {
        /* Note: This method is invoked in a synchronized context. */
        public void onTypeAdded(IServerTypeDesc typeDesc) {
            if (typeDesc.isActive())
                CreateTypeDataIfAbsent(typeDesc);
        }

        /* Note: This method is invoked in a synchronized context. */
        public void onTypeActivated(IServerTypeDesc typeDesc) {
            CreateTypeDataIfAbsent(typeDesc);
        }

        @Override
        public void onTypeDeactivated(IServerTypeDesc typeDesc) {
            TypeData typeData = _typeDataMap.get(typeDesc);
            unregisterTypeMetrics(typeDesc, typeData);
            dropClass(typeDesc, typeData);
        }

        /* Note: This method is invoked in a synchronized context. */
        public void onTypeIndexAdded(IServerTypeDesc typeDesc) {
            final TypeData oldTypeData = _typeDataMap.get(typeDesc);
            oldTypeData.typeLock();  //only one thread is allowed to change the type
            try {
                //TBD- when engine level code is added- it should be locked from engine in order to prevent
                //types mess-up by creating a new serverTypeDesc  while performing reindexing

                //create a new type data that reflects
                TypeData newTypeData = replaceTypeData(typeDesc, TypeData.TypeDataRecreationReasons.DYNAMIC_INDEX_CREATION);
                //set the indicator to ensure that already running thread which inserts entries
                //will discover that its typedata may have been replaced- note- its a barrier
                oldTypeData.setTypeDataReplaced();
                if (newTypeData.getLastIndexPendingCreationNumber() == 0)
                    return; //no real index was created

                // Re-index type entries:
                reindexTypeEntries(newTypeData, true /*indexAddition*/);
                // Replace again- Creation is done
                newTypeData = replaceTypeData(typeDesc, TypeData.TypeDataRecreationReasons.DYNAMIC_INDEX_CREATION_COMPLETION);
            } finally {
                oldTypeData.typeUnLock();
            }
        }

        private void CreateTypeDataIfAbsent(IServerTypeDesc serverTypeDesc) {
            TypeData typeData = _typeDataMap.get(serverTypeDesc);
            if (typeData == null) {
                typeData = _typeDataFactory.createTypeData(serverTypeDesc);
                _typeDataMap.put(serverTypeDesc, typeData);

                if (!_engine.isLocalCache())
                    registerTypeMetrics(serverTypeDesc, typeData);
                if (_logger.isDebugEnabled())
                    _logger.debug("Created new TypeData for type " + serverTypeDesc.getTypeName() +
                            " [typeId=" + serverTypeDesc.getTypeId() + "]");
            }
        }

        private void registerTypeMetrics(final IServerTypeDesc serverTypeDesc, TypeData typeData) {
            short typeDescCode = serverTypeDesc.getServerTypeDescCode();
            final String typeName = serverTypeDesc.getTypeName();
            final String metricTypeName = typeName.equals(IServerTypeDesc.ROOT_TYPE_NAME) ? "total" : typeName;
            final MetricRegistrator registrator = _engine.getMetricRegistrator();
            registrator.register(registrator.toPath("data", "entries", metricTypeName), new Gauge<Integer>() {
                @Override
                public Integer getValue() throws Exception {
                    return getNumberOfEntries(typeName, true);
                }
            });
            registrator.register(registrator.toPath("data", "notify-templates", metricTypeName), new Gauge<Integer>() {
                @Override
                public Integer getValue() throws Exception {
                    return getNumberOfNotifyTemplates(typeName, true);
                }
            });

            // remomved if statement as part of GS-14342 in order to allow recording metrics when null template used ( java.lang.Object )
            // if (!typeName.equals(IServerTypeDesc.ROOT_TYPE_NAME)) {
                _spaceMetricsRegistrationUtils.registerSpaceDataTypeMetrics(serverTypeDesc, typeData, _engine.getMetricManager().getMetricFlagsState());
            //}

            if (!typeName.equals(IServerTypeDesc.ROOT_TYPE_NAME) && isBlobStoreCachePolicy()) {
                if (getBlobStoreStorageHandler().getOffHeapCache() != null)
                    getBlobStoreStorageHandler().getOffHeapCache().register(typeName, typeDescCode);
                if (getBlobStoreStorageHandler().getOffHeapStore() != null)
                    getBlobStoreStorageHandler().getOffHeapStore().register(typeName, typeDescCode);
            }
        }

        private void unregisterTypeMetrics(IServerTypeDesc typeDesc, TypeData typeData) {
            String typeName = typeDesc.getTypeName();
            final String metricTypeName = typeName.equals(IServerTypeDesc.ROOT_TYPE_NAME) ? "total" : typeName;
            final MetricRegistrator registrator = _engine.getMetricRegistrator();
            registrator.unregisterByPrefix(registrator.toPath("data", "entries", metricTypeName));
            registrator.unregisterByPrefix(registrator.toPath("data", "notify-templates", metricTypeName));

            _spaceMetricsRegistrationUtils.unregisterSpaceDataTypeMetrics(typeDesc, typeData);

            if (!typeName.equals(IServerTypeDesc.ROOT_TYPE_NAME) && isBlobStoreCachePolicy()) {
                short typeDescCode = typeDesc.getServerTypeDescCode();
                if (getBlobStoreStorageHandler().getOffHeapCache() != null) {
                    getBlobStoreStorageHandler().getOffHeapCache().unregister(typeName, typeDescCode);
                }
                if (getBlobStoreStorageHandler().getOffHeapStore() != null)
                    getBlobStoreStorageHandler().getOffHeapStore().unregister(typeName, typeDescCode);
            }
        }

        private TypeData replaceTypeData(IServerTypeDesc serverTypeDesc, TypeData.TypeDataRecreationReasons reason) {
            TypeData oldTypeData = _typeDataMap.get(serverTypeDesc);
            TypeData newTypeData = _typeDataFactory.createTypeDataOnDynamicIndexCreation(serverTypeDesc, oldTypeData, reason);
            _typeDataMap.put(serverTypeDesc, newTypeData);

            if (_logger.isDebugEnabled()) {
                _logger.debug("replaced TypeData for type [" + serverTypeDesc.getTypeName() +
                        "], typeId: " + serverTypeDesc.getTypeId() +
                        ", isInactive: " + serverTypeDesc.isInactive() +
                        " reason: " + reason);
            }

            return newTypeData;
        }
    }

    public Context getCacheContext() {
        return _cacheContextFactory.getCacheContext();
    }

    public Context freeCacheContext(Context context) {
        if (context != null) {
            if (context.isActiveBlobStoreBulk() && context.getBlobStoreBulkInfo().getException() == null)
                throw new RuntimeException("active BlobStore bulk in terminated context!!");
            _cacheContextFactory.freeCacheContext(context);
        }
        return null;
    }

    public Context viewCacheContext() {
        return _cacheContextFactory.viewCacheContext();
    }

    public void startTemplateExpirationManager() {
        _templateExpirationManager.start();
    }

    public TemplateExpirationManager getTemplateExpirationManager() {
        return _templateExpirationManager;
    }


    public boolean testAndSetFGCacheForEntry(Context context, IEntryHolder entry, ITemplateHolder template, boolean testOnly, IServerTypeDesc tte) {
        Object val = getTypeData(tte).getFifoGroupingIndex().getIndexValue(entry.getEntryData());
        return _fifoGroupCacheImpl.testAndSetFGCacheForEntry(context, entry, template, val, testOnly);
    }


    public void handleFifoGroupsCacheOnXtnEnd(Context context, XtnEntry xtnEntry) {
        _fifoGroupCacheImpl.handleFifoGroupsCacheOnXtnEnd(context, xtnEntry);
    }

    public void addToEvictionStrategy(EvictableServerEntry entry, boolean isNew) {
        int maxCacheSize = getMaxCacheSize();
        boolean actualInsert = (isMemorySpace() || !((EvictableEntryCacheInfo) entry).isTransient());
        if (maxCacheSize != Integer.MAX_VALUE && maxCacheSize != 0) {
            //try & keep cachesize by evicting surplus
            int numToEvict = _actualCacheSize.incrementAndGet() - maxCacheSize;
            if (numToEvict > 0)
                _evictionStrategy.evict(numToEvict);
        }
        if (actualInsert) {
            if (isNew)
                _evictionStrategy.onInsert(entry);
            else
                _evictionStrategy.onLoad(entry);
        }
        ((EvictableEntryCacheInfo) entry).setInEvictionStrategy();
    }

    public void removeFromEvictionStrategy(EvictableServerEntry entry) {
        if (!(((EvictableEntryCacheInfo) entry).isInEvictionStrategy()))
            return;
        boolean actualRemove = (isMemorySpace() || !((EvictableEntryCacheInfo) entry).isTransient());

        try {
            if (actualRemove) {
                if (_evictionStrategy.requiresConcurrencyProtection())
                    ((EvictableEntryCacheInfo) entry).verifyBeforeEntryRemoval();//verify no touch being called now for this entry
                _evictionStrategy.onRemove(entry);
            }
        } finally {
            ((EvictableEntryCacheInfo) entry).setRemovedFromEvictionStrategy(_evictionStrategy.requiresConcurrencyProtection());

            final int maxCacheSize = getMaxCacheSize();
            if (maxCacheSize != Integer.MAX_VALUE && maxCacheSize != 0)
                _actualCacheSize.decrementAndGet();
        }
    }

    public Map<String, LocalCacheDetails> getLocalCaches() {
        return _localCacheRegistrations.get();
    }

    public static Logger getCacheLogger() {
        return _logger;
    }

    public int getNumberOfEntries() {
        return getNumberOfEntries(_typeManager.getServerTypeDesc(IServerTypeDesc.ROOT_TYPE_NAME), true);
    }

    public int getNumberOfEntries(String typeName, boolean includeSubtypes) {
        return getNumberOfEntries(_typeManager.getServerTypeDesc(typeName), includeSubtypes);
    }

    private int getNumberOfEntries(IServerTypeDesc serverTypeDesc, boolean includeSubtypes) {
        if (serverTypeDesc == null || serverTypeDesc.isInactive())
            return -1;

        int result = 0;
        if (includeSubtypes) {
            for (IServerTypeDesc subType : serverTypeDesc.getAssignableTypes()) {
                if (subType.isInactive())
                    continue;
                TypeData typeData = _typeDataMap.get(subType);
                result += typeData == null ? 0 : typeData.getEntries().size();
            }
        } else {
            TypeData typeData = _typeDataMap.get(serverTypeDesc);
            result += typeData == null ? 0 : typeData.getEntries().size();
        }
        return result;
    }

    public int getNumberOfNotifyTemplates() {
        return getNumberOfNotifyTemplates(_typeManager.getServerTypeDesc(IServerTypeDesc.ROOT_TYPE_NAME), true);
    }

    public int getNumberOfNotifyTemplates(String typeName, boolean includeSubtypes) {
        return getNumberOfNotifyTemplates(_typeManager.getServerTypeDesc(typeName), includeSubtypes);
    }

    private int getNumberOfNotifyTemplates(IServerTypeDesc serverTypeDesc, boolean includeSubtypes) {
        if (serverTypeDesc == null || serverTypeDesc.isInactive())
            return -1;

        int result = 0;
        if (includeSubtypes) {
            for (IServerTypeDesc subType : serverTypeDesc.getAssignableTypes()) {
                if (subType.isInactive())
                    continue;
                TypeData typeData = _typeDataMap.get(subType);
                result += typeData == null ? 0 : typeData.getTotalNotifyTemplates();
            }
        } else {
            TypeData typeData = _typeDataMap.get(serverTypeDesc);
            result += typeData == null ? 0 : typeData.getTotalNotifyTemplates();
        }
        return result;
    }

    public SpaceRuntimeInfo getRuntimeInfo(String typeName) {
        final IServerTypeDesc serverTypeDesc = _typeManager.getServerTypeDesc(typeName);
        if (serverTypeDesc == null)
            throw new IllegalArgumentException("Runtime info couldn't be extracted. Unknown class [" + typeName + "] for space [" + _engine.getFullSpaceName() + "]");

        Map<String, Integer> entriesInfo;
        // APP-833 (Guy K): 18.12.2006 in order to avoid
        // Searching the DB when using all in cache/externalDB
        final boolean memoryOnlyIter = isCacheExternalDB() || isResidentEntriesCachePolicy();
        if (memoryOnlyIter && !useRecentDeletes())
            entriesInfo = null;
        else {
            boolean loadPersistent = !(isMemorySpace() || isResidentEntriesCachePolicy()) && !memoryOnlyIter;
            entriesInfo = countEntries(serverTypeDesc, loadPersistent);
        }

        return getRuntimeInfo(serverTypeDesc, entriesInfo);
    }

    private SpaceRuntimeInfo getRuntimeInfo(IServerTypeDesc serverTypeDesc, Map<String, Integer> entriesInfo) {
        final IServerTypeDesc[] subTypes = serverTypeDesc.getAssignableTypes();
        ArrayList<String> classes = new ArrayList<String>(subTypes.length);
        ArrayList<Integer> entries = new ArrayList<Integer>(subTypes.length);
        ArrayList<Integer> templates = new ArrayList<Integer>(subTypes.length);

        for (IServerTypeDesc subType : subTypes) {
            if (subType.isInactive())
                continue;
            classes.add(subType.getTypeName());
            if (entriesInfo == null)
                entries.add(getNumberOfEntries(subType, false));
            else {
                Integer count = entriesInfo.get(subType.getTypeName());
                entries.add(count != null ? count : 0);
            }
            templates.add(getNumberOfNotifyTemplates(subType, false));
        }

        return new SpaceRuntimeInfo(classes, entries, templates);
    }

    private void countPersistentEntries(Map<String, Integer> classCountMap, ITemplateHolder template, IServerTypeDesc[] subTypes) {
        Context context = null;

        try {
            context = getCacheContext();
            for (IServerTypeDesc subtype : subTypes) {
                if (subtype.isInactive())
                    continue;
                int count = _storageAdapter.count(template, new String[]{subtype.getTypeName()});
                classCountMap.put(subtype.getTypeName(), count);
            }
        } catch (SAException ex) {
            throw new InternalSpaceException(ex.getMessage(), ex);
        } finally {
            freeCacheContext(context);
        }
    }

    private Map<String, Integer> countEntries(IServerTypeDesc serverTypeDesc, boolean loadPersistent) {
        // build null Template Holder and mark it stable
        final ITemplateHolder template = TemplateHolderFactory.createEmptyTemplateHolder(_engine,
                _engine.createUIDFromCounter(), LeaseManager.toAbsoluteTime(0) /* expiration time*/, false /*isFifo*/);

        final IServerTypeDesc[] subTypes = serverTypeDesc.getAssignableTypes();
        final Map<String, Integer> entriesInfo = new HashMap<String, Integer>(subTypes.length);
        for (IServerTypeDesc subType : subTypes) {
            if (subType.isActive())
                entriesInfo.put(subType.getTypeName(), 0);
        }
        if (loadPersistent)
            countPersistentEntries(entriesInfo, template, subTypes);

        Context context = null;
        ISAdapterIterator<IEntryHolder> entriesIter = null;
        try {
            context = getCacheContext();
            entriesIter = makeEntriesIter(context, template, serverTypeDesc, 0, SystemTime.timeMillis(), true /*memoryOnlyIter*/);

            String curClass = null;
            int currCount = 0;
            if (entriesIter != null) {
                while (true) {
                    IEntryHolder entryHolder = entriesIter.next();
                    if (entryHolder == null)
                        break;
                    if(entryHolder.getServerTypeDesc().getTypeDesc().isBroadcast() && _engine.skipBroadcastTable(context, template))
                        continue;
                    // Make sure we don't count the same object twice (persistent/transient)
                    if (loadPersistent && !entryHolder.isTransient())
                        continue;
                    if (isDummyEntry(entryHolder))
                        continue;

                    if (entryHolder.getClassName().equals(curClass)) {
                        currCount++;
                    } else {
                        if (curClass != null)
                            entriesInfo.put(curClass, currCount);

                        curClass = entryHolder.getClassName();
                        Integer classCount = entriesInfo.get(entryHolder.getClassName());
                        if (classCount == null)
                            currCount = 1;
                        else
                            currCount = classCount.intValue() + 1;
                    }
                } /* while (true) */
            } /* if (entriesIter != null) */

            if (curClass != null)
                entriesInfo.put(curClass, currCount);
        } catch (SAException e) {
            throw new InternalSpaceException(e.toString(), e);
        } finally {
            try {
                if (entriesIter != null)
                    entriesIter.close();
            } catch (SAException ex) {
                freeCacheContext(context);
                context = null;
                throw new InternalSpaceException(ex.toString(), ex);
            }
            if (context != null)
                freeCacheContext(context);
        }
        return entriesInfo;
    }

    public QueryExtensionIndexManagerWrapper getQueryExtensionManager(String namespace) {
        return queryExtensionManagers.get(namespace);
    }

    public void closeQueryExtensionManagers() {
        for (QueryExtensionIndexManagerWrapper manager : queryExtensionManagers.values()) {
            try {
                manager.close();
            } catch (Throwable e) {
                if (_logger.isWarnEnabled()) {
                    _logger.warn("failure closing query extention manager " + manager.toString());
                }
            }
        }
        queryExtensionManagers.clear();
    }

}
