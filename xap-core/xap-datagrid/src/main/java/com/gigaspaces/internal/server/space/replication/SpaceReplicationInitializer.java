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

package com.gigaspaces.internal.server.space.replication;

import com.gigaspaces.cluster.replication.ReplicationFilterManager;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNodeBuilder;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationBacklogBuilder;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderBacklogBuilder;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.MultiBucketSingleFileBacklogBuilder;
import com.gigaspaces.internal.cluster.node.impl.backlog.multisourcesinglefile.MultiSourceSingleFileBacklogBuilder;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfig;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfigBuilder;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncHandler;
import com.gigaspaces.internal.cluster.node.impl.groups.AllSpaceItemGroupsExtractor;
import com.gigaspaces.internal.cluster.node.impl.handlers.AbstractSpaceReplicationEntryEventHandler;
import com.gigaspaces.internal.cluster.node.impl.handlers.ActiveActiveSpaceReplicationEntryEventHandler;
import com.gigaspaces.internal.cluster.node.impl.handlers.BlobstorePrimaryBackupSpaceReplicationEntryEventHandler;
import com.gigaspaces.internal.cluster.node.impl.handlers.PrimaryBackupSpaceReplicationEntryEventHandler;
import com.gigaspaces.internal.cluster.node.impl.handlers.PrimaryBackupSpaceReplicationEvictionProtectionEntryEventHandler;
import com.gigaspaces.internal.cluster.node.impl.handlers.SpaceReplicationEntryLeaseEventHandler;
import com.gigaspaces.internal.cluster.node.impl.handlers.SpaceReplicationMetadataEventHandler;
import com.gigaspaces.internal.cluster.node.impl.handlers.SpaceReplicationTemplateEventHandler;
import com.gigaspaces.internal.cluster.node.impl.handlers.SpaceReplicationTransactionEventHandler;
import com.gigaspaces.internal.cluster.node.impl.packets.data.BlobstoreReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.BlobstoreReplicationPacketDataProducer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataProducer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataProducer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.SpaceEngineFixFacade;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile.MultiSourceSingleFileProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceReplicaDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceReplicaDataProducerBuilder;
import com.gigaspaces.internal.extension.XapExtensions;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceEngineReplicaConsumerFacade;
import com.gigaspaces.internal.server.space.SpaceEngineReplicaDirectPersistencySyncConsumerFacade;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.sync.mirror.MirrorConfig;
import com.gigaspaces.internal.sync.mirror.MirrorService;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.concurrent.ScheduledThreadPoolAsyncHandlerProvider;
import com.j_spaces.core.Constants.Mirror;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.cluster.ReplicationProcessingType;
import com.j_spaces.core.sadapter.IStorageAdapter;
import com.j_spaces.kernel.SystemProperties;

import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_ALL_IN_CACHE;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_BLOB_STORE;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_LRU;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_PROP;
import static com.j_spaces.core.Constants.CacheManager.FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP;
import static com.j_spaces.core.Constants.CacheManager.FULL_CACHE_MANAGER_USE_BLOBSTORE_BULKS_PROP;

/**
 * Encapsulates initialization of replication-related components.
 *
 * @author Niv Ingberg
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class SpaceReplicationInitializer {
    private static final Logger _replicationNodeLogger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_REPLICATION_NODE);

    private final SpaceConfigReader _configReader;
    private final SpaceImpl _spaceImpl;
    private final SpaceTypeManager _typeManager;
    private final IStorageAdapter _storageAdapter;
    private final SpaceEngine _spaceEngine;
    private final String _fullSpaceName;
    private final SpaceClusterInfo _clusterInfo;
    private final ClusterPolicy _clusterPolicy;

    private final boolean _isReplicated;
    private final boolean _isSyncReplication;

    private final ReplicationFilterManager _replicationFilterManager;
    private final ReplicationNode _replicationNode;
    private final MirrorService _mirrorService;

    private boolean _alreadyWarnedUnsupportedConfig = false;
    // blobstore replication related flags
    private final boolean _replicationBlobstoreBackupBulks = Boolean.parseBoolean(System.getProperty(FULL_CACHE_MANAGER_USE_BLOBSTORE_BULKS_PROP, "true"))
            && Boolean.parseBoolean(System.getProperty(SystemProperties.REPLICATION_USE_BACKUP_BLOBSTORE_BULKS, SystemProperties.REPLICATION_USE_BACKUP_BLOBSTORE_BULKS_DEFAULT));
    private final boolean _syncListEnabled = Boolean.parseBoolean(System.getProperty(SystemProperties.REPLICATION_USE_BLOBSTORE_SYNC_LIST, SystemProperties.REPLICATION_USE_BLOBSTORE_SYNC_LIST_DEFAULT));

    public SpaceReplicationInitializer(SpaceConfigReader configReader, SpaceImpl spaceImpl, SpaceTypeManager typeManager,
                                       IStorageAdapter storageAdapter, SpaceEngine spaceEngine)
            throws RemoteException {

        this._configReader = configReader;
        this._spaceImpl = spaceImpl;
        this._typeManager = typeManager;
        this._storageAdapter = storageAdapter;
        this._spaceEngine = spaceEngine;
        this._fullSpaceName = configReader.getFullSpaceName();
        this._clusterInfo = spaceImpl.getClusterInfo();
        this._clusterPolicy = spaceImpl.getClusterPolicy();

        this._isReplicated = _clusterPolicy != null && _clusterPolicy.m_Replicated;
        this._isSyncReplication = _clusterPolicy != null &&
                _clusterPolicy.m_ReplicationPolicy != null &&
                _clusterPolicy.m_ReplicationPolicy.isOwnerMemberHasSyncReplication();

        boolean isMirrorService = configReader.getBooleanSpaceProperty(Mirror.MIRROR_SERVICE_ENABLED_PROP, Mirror.MIRROR_SERVICE_ENABLED_DEFAULT);
        if (isMirrorService) {
            this._mirrorService = initMirrorService();
            this._replicationFilterManager = null;
            this._replicationNode = _mirrorService.getReplicationNode();
        } else {
            this._mirrorService = null;
            if (_isReplicated) {
                this._replicationFilterManager = new ReplicationFilterManager(_clusterPolicy.getReplicationPolicy(), spaceImpl.getURL(), spaceImpl.getSingleProxy());
                this._replicationNode = createReplicationNode(_replicationFilterManager, configReader);
            } else {
                this._replicationFilterManager = null;
                this._replicationNode = createEmptyReplicationNode();
            }
        }
    }

    public boolean isReplicated() {
        return _isReplicated;
    }

    public boolean isSyncReplication() {
        return _isSyncReplication;
    }

    public ReplicationFilterManager getReplicationFilterManager() {
        return _replicationFilterManager;
    }

    public ReplicationNode getReplicationNode() {
        return _replicationNode;
    }

    public MirrorService getMirrorService() {
        return _mirrorService;
    }

    private MirrorService initMirrorService() {
        if (_clusterPolicy != null)
            throw new IllegalStateException("Mirror service cannot be deployed as a clustered topology, it should not contain any cluster schema");

        return new MirrorService(_fullSpaceName, _spaceImpl.getUuid(),
                new MirrorConfig(_configReader),
                _storageAdapter.getSynchronizationInterceptor(),
                _storageAdapter.getDataClass(),
                _typeManager, _spaceEngine.getMetricRegistrator());
    }

    public boolean isReplicatedPersistentBlobstore() {
        boolean isBlobstorePersistent = Boolean.parseBoolean(_spaceImpl.getCustomProperties().getProperty(FULL_CACHE_MANAGER_BLOBSTORE_PERSISTENT_PROP));
        boolean isReplicated = _spaceEngine.getClusterInfo().getNumberOfBackups() > 0;
        return isBlobstorePersistent && isReplicated;
    }

    private ReplicationNode createReplicationNode(ReplicationFilterManager filterManager, SpaceConfigReader configReader) {
        // Create the node builder with the proper building blocks
        ReplicationNodeBuilder nodeBuilder = new ReplicationNodeBuilder();

        nodeBuilder.setSpaceItemGroupsExtractor(new AllSpaceItemGroupsExtractor());
        nodeBuilder.setReplicationRouterBuilder(XapExtensions.getInstance().getReplicationRouterBuilderFactory().create(_spaceImpl));
        ReplicationPacketDataMediator packetDataMediator = new ReplicationPacketDataMediator();
        nodeBuilder.setReplicationProcessLogBuilder(createReplicationProcessLog(packetDataMediator));
        nodeBuilder.setReplicationBacklogBuilder(createReplicationBacklog(_storageAdapter, packetDataMediator));
        nodeBuilder.setReplicaDataProducerBuilder(new SpaceReplicaDataProducerBuilder(_spaceEngine));
        if (isReplicatedPersistentBlobstore() && _syncListEnabled) {
            if (_replicationNodeLogger.isInfoEnabled()) {
                _replicationNodeLogger.info("[" + _fullSpaceName + "]" + " initialized with direct persistency sync list recovery mode");
            }
            nodeBuilder.setReplicaDataConsumer(new SpaceReplicaDataConsumer(_typeManager, new SpaceEngineReplicaDirectPersistencySyncConsumerFacade(_spaceEngine)));
            nodeBuilder.setDirectPersistencySyncHandler(new DirectPersistencySyncHandler(_spaceEngine));
        } else {
            nodeBuilder.setReplicaDataConsumer(new SpaceReplicaDataConsumer(_typeManager, new SpaceEngineReplicaConsumerFacade(_spaceEngine)));
        }
        int corePoolSize = 4; //TODO LV: configurable
        nodeBuilder.setAsyncHandlerProvider(new ScheduledThreadPoolAsyncHandlerProvider("ReplicationNodePool-" + _fullSpaceName, corePoolSize));

        int partitionId = _spaceEngine.getPartitionIdOneBased();
        //In case we are in a non partitioned topology, the replication node should be configured with partition id 1
        //due to the reliable async replication topology
        if (partitionId == 0)
            partitionId = 1;
        // Create node configuration
        ReplicationNodeConfig replicationNodeConfig = ReplicationNodeConfigBuilder.getInstance().createConfig(_clusterInfo,
                _clusterPolicy,
                nodeBuilder,
                filterManager,
                partitionId,
                _storageAdapter,
                configReader,
                new SpaceReplicationChannelDataFilterBuilder(_spaceEngine),
                new SpaceReplicationLifeCycleBuilder(_spaceEngine));

        ReplicationNode replicationNode = new ReplicationNode(replicationNodeConfig, nodeBuilder, _fullSpaceName, _spaceEngine.getMetricRegistrator());

        AbstractSpaceReplicationEntryEventHandler entryHandler = getEntryHandler();

        replicationNode.setInEntryHandler(entryHandler);
        replicationNode.setInEvictEntryHandler(entryHandler);

        replicationNode.setInTransactionHandler(new SpaceReplicationTransactionEventHandler(_spaceEngine,
                entryHandler));

        SpaceReplicationEntryLeaseEventHandler entryLeaseHandler = new SpaceReplicationEntryLeaseEventHandler(
                _spaceEngine);
        replicationNode.setInEntryLeaseCancelledHandler(entryLeaseHandler);
        replicationNode.setInEntryLeaseExtendedHandler(entryLeaseHandler);
        replicationNode.setInEntryLeaseExpiredHandler(entryLeaseHandler);

        SpaceReplicationTemplateEventHandler templateHandler = new SpaceReplicationTemplateEventHandler(
                _spaceEngine);
        replicationNode.setInNotifyTemplateCreatedHandler(templateHandler);
        replicationNode.setInNotifyTemplateRemovedHandler(templateHandler);
        replicationNode.setInNotifyTemplateLeaseExtendedHandler(templateHandler);
        replicationNode.setInNotifyTemplateLeaseExpiredHandler(templateHandler);

        SpaceReplicationMetadataEventHandler metadataHandler = new SpaceReplicationMetadataEventHandler(
                _spaceEngine.getTypeManager(), _spaceEngine);
        replicationNode.setInDataTypeCreatedHandler(metadataHandler);
        replicationNode.setInDataTypeIndexAddedHandler(metadataHandler);

        replicationNode.getAdmin().setNodeStateListener(new ReplicationNodeStateListener(_spaceEngine));
        //Enable incomming communication of the router(though it should have been only after first copy stage of recovery, 
        //since clean does not perform recovery we want to avoid causing its router not to be available, 
        //we rely on LUS registration with replicatable=true anyhow so this has no actual affect)
        replicationNode.getAdmin().getRouterAdmin().enableIncomingCommunication();

        return replicationNode;
    }

    private AbstractSpaceReplicationEntryEventHandler getEntryHandler() {
        AbstractSpaceReplicationEntryEventHandler entryHandler;
        // we duplicate the cache manager logic here because it is null at this point
        final boolean isCentralAndExternalDB =
                _storageAdapter.supportsExternalDB() &&
                        _spaceEngine.getClusterPolicy().m_CacheLoaderConfig.centralDataSource;

        final boolean allInCache = _spaceEngine.getConfigReader().getIntSpaceProperty(CACHE_POLICY_PROP, !_storageAdapter.supportsExternalDB() ?
                String.valueOf(CACHE_POLICY_ALL_IN_CACHE) : String.valueOf(CACHE_POLICY_LRU)) == CACHE_POLICY_ALL_IN_CACHE;
        final boolean blobStore = !allInCache && _spaceEngine.getConfigReader().getIntSpaceProperty(CACHE_POLICY_PROP, !_storageAdapter.supportsExternalDB() ?
                String.valueOf(CACHE_POLICY_ALL_IN_CACHE) : String.valueOf(CACHE_POLICY_LRU)) == CACHE_POLICY_BLOB_STORE;
        final boolean requiresEvictionReplicationProtection = _spaceEngine.hasMirror() && _storageAdapter.supportsExternalDB() &&
                !allInCache && !blobStore;

        if (_clusterPolicy.isPrimaryElectionAvailable()) {
            if (requiresEvictionReplicationProtection) {
                entryHandler = new PrimaryBackupSpaceReplicationEvictionProtectionEntryEventHandler(_spaceEngine, isCentralAndExternalDB);
            } else if (isReplicatedPersistentBlobstore() && _replicationBlobstoreBackupBulks) {
                entryHandler = new BlobstorePrimaryBackupSpaceReplicationEntryEventHandler(_spaceEngine, isCentralAndExternalDB);
            } else {
                entryHandler = new PrimaryBackupSpaceReplicationEntryEventHandler(_spaceEngine, isCentralAndExternalDB);
            }
        } else {
            entryHandler = new ActiveActiveSpaceReplicationEntryEventHandler(_spaceEngine, isCentralAndExternalDB);
        }
        return entryHandler;
    }

    private ReplicationNode createEmptyReplicationNode() {

        // Create the node builder with the proper building blocks
        ReplicationNodeBuilder nodeBuilder = new ReplicationNodeBuilder();

        nodeBuilder.setReplicationRouterBuilder(XapExtensions.getInstance().getReplicationRouterBuilderFactory().createSpaceProxyReplicationRouterBuilder(_spaceImpl, true));
        nodeBuilder.setReplicaDataProducerBuilder(new SpaceReplicaDataProducerBuilder(_spaceEngine));
        nodeBuilder.setReplicaDataConsumer(new SpaceReplicaDataConsumer(_typeManager, new SpaceEngineReplicaConsumerFacade(_spaceEngine)));
        nodeBuilder.setAsyncHandlerProvider(new ScheduledThreadPoolAsyncHandlerProvider("ReplicationNodePool-" + _fullSpaceName, 1));

        ReplicationNode replicationNode = new ReplicationNode(new ReplicationNodeConfig(), nodeBuilder, _fullSpaceName, _spaceEngine.getMetricRegistrator());
        replicationNode.getAdmin().getRouterAdmin().enableIncomingCommunication();

        return replicationNode;
    }

    private IReplicationProcessLogBuilder createReplicationProcessLog(ReplicationPacketDataMediator packetDataMediator) {
        ReplicationPacketDataConsumer dataConsumer;
        if (isReplicatedPersistentBlobstore() && _replicationBlobstoreBackupBulks) {
            dataConsumer = new BlobstoreReplicationPacketDataConsumer(_typeManager, new SpaceEngineFixFacade(_spaceEngine), packetDataMediator);
        } else {
            dataConsumer = new ReplicationPacketDataConsumer(_typeManager, new SpaceEngineFixFacade(_spaceEngine), packetDataMediator);
        }
        ReplicationProcessingType processingType = _clusterPolicy.m_ReplicationPolicy.getProcessingType();
        switch (processingType) {
            case MULTIPLE_BUCKETS:
                if (supportsConcurrentReplication())
                    return new MultiBucketSingleFileProcessLogBuilder(dataConsumer);
                else
                    return new GlobalOrderProcessLogBuilder(dataConsumer);
            case MULTIPLE_SOURCES:
                if (supportsMultiSourceReplication())
                    return new MultiSourceSingleFileProcessLogBuilder(dataConsumer);
                else
                    return new GlobalOrderProcessLogBuilder(dataConsumer);
            case GLOBAL_ORDER:
                return new GlobalOrderProcessLogBuilder(dataConsumer);
            default:
                throw new IllegalArgumentException();
        }
    }

    private IReplicationBacklogBuilder createReplicationBacklog(IStorageAdapter storageAdapter, ReplicationPacketDataMediator packetDataMediator) {

        IReplicationPacketDataProducer<?> dataProducer;
        if (isReplicatedPersistentBlobstore() && _replicationBlobstoreBackupBulks) {
            dataProducer = new BlobstoreReplicationPacketDataProducer(
                    _spaceEngine,
                    _spaceEngine.isClusteredExternalDBEnabled(storageAdapter),
                    _clusterPolicy.getReplicationPolicy().isReplicateFullTake(),
                    packetDataMediator);
        } else {
            dataProducer = new ReplicationPacketDataProducer(
                    _spaceEngine,
                    _spaceEngine.isClusteredExternalDBEnabled(storageAdapter),
                    _clusterPolicy.getReplicationPolicy().isReplicateFullTake(),
                    packetDataMediator);
        }
        ReplicationProcessingType processingType = _clusterPolicy.m_ReplicationPolicy.getProcessingType();
        switch (processingType) {
            case MULTIPLE_BUCKETS:
                if (supportsConcurrentReplication())
                    return new MultiBucketSingleFileBacklogBuilder(_fullSpaceName, dataProducer);
                else
                    return new GlobalOrderBacklogBuilder(dataProducer, _fullSpaceName);
            case MULTIPLE_SOURCES:
                if (supportsMultiSourceReplication())
                    return new MultiSourceSingleFileBacklogBuilder(_fullSpaceName,
                            dataProducer);
                else
                    return new GlobalOrderBacklogBuilder(dataProducer, _fullSpaceName);
            case GLOBAL_ORDER:
                return new GlobalOrderBacklogBuilder(dataProducer, _fullSpaceName);
            default:
                throw new IllegalArgumentException();
        }
    }

    private boolean supportsMultiSourceReplication() {
        final String errorMessage = XapExtensions.getInstance().getReplicationNodeConfigBuilder().supportsMultiSourceReplication(_clusterInfo);

        if (errorMessage != null) {
            //We throw on error unless it was globally overridden with system property to concurrent replication
            final boolean throwOnError = !Boolean.getBoolean("com.gs.replication.module.multisource");
            if (throwOnError)
                throw new IllegalArgumentException(errorMessage + ", use 'cluster-config.groups.group.repl-policy.processing-type=global-order' instead");
            if (!_alreadyWarnedUnsupportedConfig) {
                if (_replicationNodeLogger.isWarnEnabled()) {
                    _replicationNodeLogger.warn(StringUtils.NEW_LINE +
                            "*********************************************************************" + StringUtils.NEW_LINE +
                            errorMessage + "," + StringUtils.NEW_LINE +
                            "reverting to Global order replication processing" + StringUtils.NEW_LINE +
                            "*********************************************************************");
                }
                _alreadyWarnedUnsupportedConfig = true;
            }
        }

        return errorMessage == null;
    }

    private boolean supportsConcurrentReplication() {
        final String errorMessage = supportsConcurrentReplication(_clusterPolicy);
        if (errorMessage != null) {
            //We throw on error unless it was globally overridden with system property to concurrent replication
            final boolean throwOnError = !Boolean.getBoolean("com.gs.replication.module.concurrent");
            if (throwOnError)
                throw new IllegalArgumentException(errorMessage + ", use 'cluster-config.groups.group.repl-policy.processing-type=global-order' instead");
            if (!_alreadyWarnedUnsupportedConfig) {
                if (_replicationNodeLogger.isWarnEnabled()) {
                    _replicationNodeLogger.warn(StringUtils.NEW_LINE +
                            "*********************************************************************" + StringUtils.NEW_LINE +
                            errorMessage + "," + StringUtils.NEW_LINE +
                            "reverting to Global order replication processing" + StringUtils.NEW_LINE +
                            "*********************************************************************");
                }
                _alreadyWarnedUnsupportedConfig = true;
            }
        }

        return errorMessage == null;
    }

    private static String supportsConcurrentReplication(ClusterPolicy clusterPolicy) {
        final boolean primaryBackupGroup = clusterPolicy.isPrimaryElectionAvailable();
        final boolean syncReplication = clusterPolicy.getReplicationPolicy().m_IsSyncReplicationEnabled;
        final boolean hasMirror = clusterPolicy.getReplicationPolicy().isMirrorServiceEnabled();
        final boolean isReliableAsync = clusterPolicy.getReplicationPolicy().isReliableAsyncRepl();
        final boolean hasExistingReplicationMembers = clusterPolicy.getReplicationPolicy().getReplicationTargetsCount() > 0;
        if (!syncReplication || !hasExistingReplicationMembers) {
            return "Async replication is not supported in Multiple bucket replication processing";
        }
        if (hasMirror && isReliableAsync && syncReplication && !primaryBackupGroup) {
            return "Mirror service with no backups is not supported in Multiple bucket replication processing";
        }

        return null;
    }
}
