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

package com.gigaspaces.internal.cluster.node.impl.config;

import com.gigaspaces.cluster.replication.ConsistencyLevel;
import com.gigaspaces.cluster.replication.ReplicationFilterManager;
import com.gigaspaces.cluster.replication.ReplicationTransmissionPolicy;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.cluster.node.impl.GroupMapping;
import com.gigaspaces.internal.cluster.node.impl.IReplicationNodeBuilder;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNodeBuilder;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig.LimitReachedPolicy;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogMemberLimitationConfig;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderBacklogConfig;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.MultiBucketSingleFileBacklogConfig;
import com.gigaspaces.internal.cluster.node.impl.backlog.multisourcesinglefile.MultiSourceSingleFileBacklogConfig;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder.IDynamicSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.GeneralReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilterBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.ReliableAsyncKeeperLruCentralDataSourceReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.async.AsyncMultiOriginReplicationTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.async.AsyncReplicationSourceGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.async.AsyncSingleOriginReplicationTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.async.AsyncSourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.consistencylevel.GroupConsistencyLevelPolicy;
import com.gigaspaces.internal.cluster.node.impl.groups.consistencylevel.SyncMembersInSyncConsistencyLevelPolicy;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.AsyncChannelConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.IDynamicSourceGroupMemberLifeCycleBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.MirrorChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncKeeperReplicationTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncReplicationSourceGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncSingleOriginReplicationTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.ReliableAsyncSourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.BacklogAdjustedThrottleControllerBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.IReplicationThrottleControllerBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.SyncMultiOriginReplicationTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.SyncReplicationSourceGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.sync.SyncSingleOriginReplicationTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.ProcessLogConfig;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessLogConfig;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessLogConfig;
import com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile.MultiSourceSingleFileProcessLogConfig;
import com.gigaspaces.internal.extension.XapExtensions;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.sync.mirror.MirrorConfig;
import com.gigaspaces.internal.utils.StringUtils;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.cluster.ReplicationProcessingType;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;
import com.j_spaces.core.sadapter.IStorageAdapter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_ALL_IN_CACHE;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_BLOB_STORE;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_LRU;
import static com.j_spaces.core.Constants.CacheManager.CACHE_POLICY_PROP;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_VERSION_DEFAULT;
import static com.j_spaces.core.Constants.DataAdapter.SUPPORTS_VERSION_PROP;

@com.gigaspaces.api.InternalApi
public class ReplicationNodeConfigBuilder {
    public static final String PRIMARY_BACKUP_RELIABLE_ASYNC = "primary-backup-reliable-async-mirror-";
    public static final String PRIMARY_BACKUP_RELIABLE_ASYNC_NO_MIRROR = "primary-backup-reliable-async-";
    public static final String ASYNC_MIRROR = "async-mirror-";
    public static final String ASYNC = "async";
    public static final String SYNC = "sync";
    public static final String PRIMARY_BACKUP_SYNC = "primary-backup-sync";
    public static final String PRIMARY_BACKUP_ASYNC = "primary-backup-async";

    public static final long UNLIMITED = ReplicationConstants.UNLIMITED;

    public static final String PERSISTENCY_GROUPING_NAME = "persistency";


    public static ReplicationNodeConfigBuilder getInstance() {
        return XapExtensions.getInstance().getReplicationNodeConfigBuilder();
    }

    public ReplicationNodeConfigBuilder() {
    }

    protected static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_REPLICATION_NODE);

    protected boolean requiresEvictionReplicationProtection(ClusterPolicy clusterPolicy,
                                                            IStorageAdapter storageAdapter,
                                                            SpaceConfigReader configReader) {
        return storageAdapter.supportsExternalDB() &&
                clusterPolicy.getReplicationPolicy().isMirrorServiceEnabled() &&
                isEvictableCachePolicy(configReader);
    }

    protected boolean reliableAsyncKeeperCannotRefillContent(ClusterPolicy clusterPolicy,
                                                             IStorageAdapter storageAdapter,
                                                             SpaceConfigReader configReader) {
        return storageAdapter.supportsExternalDB() &&
                isCentralDB(storageAdapter, clusterPolicy) &&
                isEvictableCachePolicy(configReader) &&
                !dataSourceSupportsVersioning(configReader);
    }

    public ReplicationNodeConfig createConfig(SpaceClusterInfo clusterInfo,
                                              ClusterPolicy clusterPolicy, IReplicationNodeBuilder nodeBuilder,
                                              ReplicationFilterManager filterManager,
                                              int partitionId,
                                              IStorageAdapter storageAdapter,
                                              SpaceConfigReader configReader,
                                              IReplicationChannelDataFilterBuilder filterBuilder,
                                              IDynamicSourceGroupMemberLifeCycleBuilder lifeCycleBuilder) {
        validateSupportedConfig(clusterPolicy);

        final boolean isReliableAsync = clusterPolicy.getReplicationPolicy().isReliableAsyncRepl();
        final boolean syncReplication = clusterPolicy.getReplicationPolicy().m_IsSyncReplicationEnabled;
        final boolean primaryBackupGroup = clusterPolicy.isPrimaryElectionAvailable();
        final boolean hasMirror = clusterPolicy.getReplicationPolicy().isMirrorServiceEnabled();
        final boolean hasExistingReplicationMembers = clusterPolicy.getReplicationPolicy().getReplicationTargetsCount() > 0;

        if (hasMirror) {
            if (isReliableAsync && syncReplication && primaryBackupGroup) {
                IReliableAsyncReplicationSettings reliableAsyncSettings = new ReliableAsyncReplicationSettingsAdapter(clusterPolicy.getReplicationPolicy(),
                        filterManager,
                        reliableAsyncKeeperCannotRefillContent(clusterPolicy, storageAdapter, configReader));
                return createReliableAsyncSourceWithMirrorConfig(reliableAsyncSettings,
                        nodeBuilder,
                        partitionId,
                        filterBuilder,
                        lifeCycleBuilder,
                        requiresEvictionReplicationProtection(clusterPolicy, storageAdapter, configReader));
            } else if (isReliableAsync && syncReplication) {
                if (_logger.isLoggable(Level.WARNING))
                    _logger.warning("Creating a reliable async mirrored topology without a backup is not considered reliable async");

                final MirrorReplicationSettingsAdapter mirrorSettings = new MirrorReplicationSettingsAdapter(clusterPolicy.getReplicationPolicy().getMirrorServiceConfig());
                final ISpaceReplicationSettings replicationPolicy = new ReplicationSettingsAdapter(clusterPolicy.getReplicationPolicy(), filterManager);
                return createSourceWithMirrorConfig(mirrorSettings,
                        replicationPolicy,
                        nodeBuilder,
                        partitionId,
                        requiresEvictionReplicationProtection(clusterPolicy, storageAdapter, configReader));
            } else
                throw new UnsupportedOperationException("Mirror is supported only with primary-backup synchronous replication topology.");
        } else {
            final ISpaceReplicationSettings replicationPolicy = new ReplicationSettingsAdapter(clusterPolicy.getReplicationPolicy(), filterManager);

            if (primaryBackupGroup) {
                if (syncReplication) {
                    IReliableAsyncReplicationSettings reliableAsyncSettings = new ReliableAsyncReplicationSettingsAdapter(clusterPolicy.getReplicationPolicy(),
                            filterManager,
                            reliableAsyncKeeperCannotRefillContent(clusterPolicy, storageAdapter, configReader));
                    return createReliableAsyncSourceConfig(reliableAsyncSettings,
                            nodeBuilder,
                            partitionId,
                            filterBuilder,
                            lifeCycleBuilder);
                } else
                    return createPrimaryBackupAsyncConfig(replicationPolicy,
                            nodeBuilder);
            } else if (hasExistingReplicationMembers) {
                if (storageAdapter.supportsExternalDB() && isCentralDB(storageAdapter, clusterPolicy))
                    _logger.warning("Active active replication topology with central database is deprecated - use gateway replication instead.");
                if (syncReplication)
                    return createSyncReplicationConfig(replicationPolicy,
                            nodeBuilder);
                else
                    return createAsyncReplicationConfig(replicationPolicy,
                            nodeBuilder);
            } else //Primary backup with 0 backups no mirror and no gateway.
                return createPrimaryNoBackupReplicationConfig(replicationPolicy,
                        nodeBuilder, partitionId);
        }
    }

    protected boolean dataSourceSupportsVersioning(SpaceConfigReader configReader) {
        return configReader.getBooleanSpaceProperty(SUPPORTS_VERSION_PROP, SUPPORTS_VERSION_DEFAULT);
    }

    protected boolean isEvictableCachePolicy(SpaceConfigReader configReader) {
        return (configReader.getIntSpaceProperty(CACHE_POLICY_PROP, String.valueOf(CACHE_POLICY_LRU)) != CACHE_POLICY_ALL_IN_CACHE
                && configReader.getIntSpaceProperty(CACHE_POLICY_PROP, String.valueOf(CACHE_POLICY_LRU)) != CACHE_POLICY_BLOB_STORE);
    }

    protected boolean isCentralDB(IStorageAdapter storageAdapter, ClusterPolicy clusterPolicy) {
        return storageAdapter.supportsExternalDB() && clusterPolicy.m_CacheLoaderConfig.centralDataSource;
    }

    protected void validateSupportedConfig(ClusterPolicy clusterPolicy) {
        ReplicationPolicy replicationPolicy = clusterPolicy.getReplicationPolicy();
        if (!replicationPolicy.isReplicateOriginalState()) {
            if (_logger.isLoggable(Level.WARNING))
                _logger.warning("Replicate original state = false is not supported, using true instead. For more information refer to upgrading page");
        }
        if (replicationPolicy.m_ReplMemberPolicyDescTable == null)
            return;
        Set<Map.Entry<String, ReplicationPolicy.ReplicationPolicyDescription>> entrySet = replicationPolicy.m_ReplMemberPolicyDescTable.entrySet();
        for (Map.Entry<String, ReplicationPolicy.ReplicationPolicyDescription> entry : entrySet) {
            ReplicationPolicy.ReplicationPolicyDescription policyDescription = entry.getValue();
            List<ReplicationTransmissionPolicy> transmissionPolicies = policyDescription.replTransmissionPolicies;
            if (transmissionPolicies != null) {
                for (ReplicationTransmissionPolicy replicationTransmissionPolicy : transmissionPolicies) {
                    if (replicationTransmissionPolicy.m_DisableTransmission)
                        throw new UnsupportedOperationException("Transmission policies with disabled members is not supported. For more information refer to upgrading page");
                    if (replicationTransmissionPolicy.m_RepTransmissionOperations != null) {
                        if ((replicationTransmissionPolicy.m_RepTransmissionOperations.indexOf(ReplicationTransmissionPolicy.REPLICATION_TRANSMISSION_OPERATION_NOTIFY) == -1) ||
                                (replicationTransmissionPolicy.m_RepTransmissionOperations.indexOf(ReplicationTransmissionPolicy.REPLICATION_TRANSMISSION_OPERATION_REMOVE) == -1) ||
                                (replicationTransmissionPolicy.m_RepTransmissionOperations.indexOf(ReplicationTransmissionPolicy.REPLICATION_TRANSMISSION_OPERATION_WRITE) == -1))
                            throw new UnsupportedOperationException("Transmission policies with disabled operations is not supported. For more information refer to upgrading page");
                    }
                }
            }
        }
        if (replicationPolicy.isOneWayReplication) {
            if (_logger.isLoggable(Level.WARNING))
                _logger.warning("One way replication is not supported, regular replication will be used instead.");
        }
    }

    public ReplicationNodeConfig createAsyncReplicationConfig(
            ISpaceReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder) {
        final String groupName = ASYNC;

        validateConfig(replicationPolicy);

        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();
        replicationNodeConfig.mapSourceGroups("all",
                GroupMapping.getAllMapping());

        BacklogConfig backlogConfig = createBacklogConfig(replicationPolicy,
                null,
                false,
                false);

        List<String> groupMemberNames = createGroupMemberNames(replicationPolicy);

        configureBacklogLimitations(replicationPolicy,
                backlogConfig,
                groupMemberNames);

        Map<String, IReplicationChannelDataFilter> filters = new HashMap<String, IReplicationChannelDataFilter>();
        addGeneralReplicationFilter(groupMemberNames, filters);

        AsyncSourceGroupConfig sourceGroupConfig = new AsyncSourceGroupConfig(groupName,
                backlogConfig,
                null,
                filters,
                null,
                null,
                replicationPolicy.isNetworkCompressionEnabled(),
                groupMemberNames.toArray(new String[groupMemberNames.size()]));

        for (String memberName : groupMemberNames) {
            sourceGroupConfig.setChannelConfig(memberName,
                    new AsyncChannelConfig(replicationPolicy.getBatchSize(),
                            replicationPolicy.getIdleDelay(),
                            replicationPolicy.getOperationsReplicationThreshold(),
                            ReplicationMode.ACTIVE_SPACE));
        }


        DynamicAsyncSourceGroupConfigHolder configHolder = new DynamicAsyncSourceGroupConfigHolder(sourceGroupConfig);

        // Create source async replication group
        configureAsyncSourceGroup(replicationPolicy,
                nodeBuilder,
                replicationNodeConfig,
                configHolder);

        configureAsyncReplicatedTargetGroup(replicationPolicy,
                nodeBuilder,
                replicationNodeConfig,
                groupMemberNames,
                groupName);

        setReplicationFilters(replicationPolicy, replicationNodeConfig);

        return replicationNodeConfig;
    }

    public ReplicationNodeConfig createPrimaryNoBackupReplicationConfig(
            ISpaceReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder, int partitionId) {
        final String groupName = createReplicationGroupName(partitionId,
                true,
                true,
                false,
                false);
        validateConfig(replicationPolicy);

        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();
        replicationNodeConfig.mapSourceGroups("all",
                GroupMapping.getAllMapping());

        BacklogConfig backlogConfig = createBacklogConfig(replicationPolicy,
                null,
                false,
                false);

        List<String> groupMemberNames = createGroupMemberNames(replicationPolicy);

        configureBacklogLimitations(replicationPolicy,
                backlogConfig,
                groupMemberNames);

        Map<String, IReplicationChannelDataFilter> filters = new HashMap<String, IReplicationChannelDataFilter>();
        addGeneralReplicationFilter(groupMemberNames, filters);

        AsyncSourceGroupConfig sourceGroupConfig = new AsyncSourceGroupConfig(groupName,
                backlogConfig,
                null,
                filters,
                null,
                null,
                replicationPolicy.isNetworkCompressionEnabled(),
                groupMemberNames.toArray(new String[groupMemberNames.size()]));

        for (String memberName : groupMemberNames) {
            sourceGroupConfig.setChannelConfig(memberName,
                    new AsyncChannelConfig(replicationPolicy.getBatchSize(),
                            replicationPolicy.getIdleDelay(),
                            replicationPolicy.getOperationsReplicationThreshold(),
                            ReplicationMode.ACTIVE_SPACE));
        }


        DynamicAsyncSourceGroupConfigHolder configHolder = new DynamicAsyncSourceGroupConfigHolder(sourceGroupConfig);

        // Create source async replication group
        configureAsyncSourceGroup(replicationPolicy,
                nodeBuilder,
                replicationNodeConfig,
                configHolder);

        setReplicationFilters(replicationPolicy, replicationNodeConfig);

        return replicationNodeConfig;
    }

    public ReplicationNodeConfig createSyncReplicationConfig(
            ISpaceReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder) {
        final String groupName = SYNC;

        validateConfig(replicationPolicy);

        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();
        replicationNodeConfig.mapSourceGroups("all",
                GroupMapping.getAllMapping());

        BacklogConfig backlogConfig = createBacklogConfig(replicationPolicy,
                null,
                true,
                true);

        List<String> groupMemberNames = createGroupMemberNames(replicationPolicy);

        configureBacklogLimitations(replicationPolicy,
                backlogConfig,
                groupMemberNames);

        Map<String, IReplicationChannelDataFilter> filters = new HashMap<String, IReplicationChannelDataFilter>();
        addGeneralReplicationFilter(groupMemberNames, filters);

        SourceGroupConfig sourceGroupConfig = new SourceGroupConfig(groupName,
                backlogConfig,
                null,
                filters,
                null,
                null,
                true,
                groupMemberNames.toArray(new String[groupMemberNames.size()]));

        DynamicSourceGroupConfigHolder configHolder = new DynamicSourceGroupConfigHolder(sourceGroupConfig);

        // Create source sync replication group
        configureSyncSourceGroup(replicationPolicy,
                nodeBuilder,
                replicationNodeConfig,
                configHolder,
                ReplicationMode.ACTIVE_SPACE);

        // Create active active target group

        configureSyncReplicatedTargetGroup(replicationPolicy,
                nodeBuilder,
                replicationNodeConfig,
                groupMemberNames,
                groupName,
                true);

        setReplicationFilters(replicationPolicy, replicationNodeConfig);

        return replicationNodeConfig;
    }

    public ReplicationNodeConfig createPrimaryBackupSyncConfig(
            ISpaceReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder, int partitionId) {
        final String groupName = PRIMARY_BACKUP_SYNC + "-" + partitionId;

        validateConfig(replicationPolicy);

        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();
        replicationNodeConfig.mapSourceGroups("all",
                GroupMapping.getAllMapping());

        BacklogConfig backlogConfig = createBacklogConfig(replicationPolicy,
                null,
                true,
                false);
        List<String> groupMemberNames = createGroupMemberNames(replicationPolicy);

        configureBacklogLimitations(replicationPolicy,
                backlogConfig,
                groupMemberNames);

        Map<String, IReplicationChannelDataFilter> filters = new HashMap<String, IReplicationChannelDataFilter>();
        addGeneralReplicationFilter(groupMemberNames, filters);

        SourceGroupConfig sourceGroupConfig = new SourceGroupConfig(groupName,
                backlogConfig,
                null,
                filters,
                null,
                null,
                replicationPolicy.isNetworkCompressionEnabled(),
                groupMemberNames.toArray(new String[groupMemberNames.size()]));

        DynamicSourceGroupConfigHolder configHolder = new DynamicSourceGroupConfigHolder(sourceGroupConfig);

        // Create source sync replication group
        configureSyncSourceGroup(replicationPolicy,
                nodeBuilder,
                replicationNodeConfig,
                configHolder,
                ReplicationMode.BACKUP_SPACE);

        // Create sync primary backup target group
        configureSyncPrimaryBackupTargetGroup(replicationPolicy,
                nodeBuilder,
                replicationNodeConfig,
                groupMemberNames,
                groupName,
                true,
                false);

        setReplicationFilters(replicationPolicy, replicationNodeConfig);

        return replicationNodeConfig;
    }

    public ReplicationNodeConfig createPrimaryBackupAsyncConfig(
            ISpaceReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder) {
        final String groupName = PRIMARY_BACKUP_ASYNC;

        validateConfig(replicationPolicy);

        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();
        replicationNodeConfig.mapSourceGroups("all",
                GroupMapping.getAllMapping());

        BacklogConfig backlogConfig = createBacklogConfig(replicationPolicy,
                null,
                false,
                false);
        List<String> groupMemberNames = createGroupMemberNames(replicationPolicy);

        configureBacklogLimitations(replicationPolicy,
                backlogConfig,
                groupMemberNames);

        Map<String, IReplicationChannelDataFilter> filters = new HashMap<String, IReplicationChannelDataFilter>();
        addGeneralReplicationFilter(groupMemberNames, filters);

        AsyncSourceGroupConfig sourceGroupConfig = new AsyncSourceGroupConfig(groupName,
                backlogConfig,
                null,
                filters,
                null,
                null,
                replicationPolicy.isNetworkCompressionEnabled(),
                groupMemberNames.toArray(new String[groupMemberNames.size()]));

        DynamicAsyncSourceGroupConfigHolder configHolder = new DynamicAsyncSourceGroupConfigHolder(sourceGroupConfig);

        // Create source sync replication group
        configureAsyncSourceGroup(replicationPolicy,
                nodeBuilder,
                replicationNodeConfig,
                configHolder);

        // Create sync primary backup target group
        configureAsyncPrimaryBackupTargetGroup(replicationPolicy,
                nodeBuilder,
                replicationNodeConfig,
                groupMemberNames,
                groupName);

        setReplicationFilters(replicationPolicy, replicationNodeConfig);

        return replicationNodeConfig;
    }

    public BacklogConfig createBacklogConfig(
            ISpaceReplicationSettings replicationPolicy,
            IMirrorChannelReplicationSettings mirrorPolicy,
            boolean supportsConcurrentReplication,
            boolean supportsMultiSourceReplication) {
        return createBacklogConfig(replicationPolicy, mirrorPolicy, null, supportsConcurrentReplication, supportsMultiSourceReplication);
    }

    public BacklogConfig createBacklogConfig(
            ISpaceReplicationSettings replicationPolicy,
            IMirrorChannelReplicationSettings mirrorPolicy,
            IReplicationComponentPolicy replicationComponentPolicy,
            boolean supportsConcurrentReplication,
            boolean supportsMultiSourceReplication) {
        BacklogConfig config;
        if (replicationPolicy.getProcessingType() == ReplicationProcessingType.MULTIPLE_BUCKETS
                && supportsConcurrentReplication) {
            MultiBucketSingleFileBacklogConfig multiBucketConfig = new MultiBucketSingleFileBacklogConfig();
            multiBucketConfig.setBucketCount(replicationPolicy.getBucketCount());
            config = multiBucketConfig;
        } else if (replicationPolicy.getProcessingType() == ReplicationProcessingType.MULTIPLE_SOURCES
                && supportsMultiSourceReplication) {
            config = new MultiSourceSingleFileBacklogConfig();
        } else {
            config = new GlobalOrderBacklogConfig();
        }
        config.setBackLogWeightPolicy(replicationPolicy.getBacklogWeightPolicy());

        long maxRedoLogMemoryCapacity = replicationPolicy.getMaxRedoLogMemoryCapacity();
        if (maxRedoLogMemoryCapacity == UNLIMITED) {
            config.setUnlimitedMemoryCapacity();
        } else {
            long maxReplicationComponentRedoLogCapacity = replicationComponentPolicy != null ? replicationComponentPolicy.getMaxRedoLogCapacity() : 0;

            // If all capacities are limited, we need to check if the memory
            // capacity is equal to the
            // maximum of these capacities, if so we don't need swap and
            // we can set unlimited memory capacity
            if (replicationPolicy.getMaxRedoLogCapacity() != UNLIMITED
                    && replicationPolicy.getMaxRedoLogCapacityDuringRecovery() != UNLIMITED
                    && (mirrorPolicy == null || mirrorPolicy.getMaxRedoLogCapacity() != UNLIMITED)
                    && (replicationComponentPolicy == null || maxReplicationComponentRedoLogCapacity != UNLIMITED)) {
                long maxCapacity = Math.max(replicationPolicy.getMaxRedoLogCapacity(),
                        replicationPolicy.getMaxRedoLogCapacityDuringRecovery());
                if (mirrorPolicy != null)
                    maxCapacity = Math.max(maxCapacity, mirrorPolicy.getMaxRedoLogCapacity());

                if (replicationComponentPolicy != null)
                    maxCapacity = Math.max(maxCapacity, maxReplicationComponentRedoLogCapacity);

                if (maxCapacity == maxRedoLogMemoryCapacity) {
                    config.setUnlimitedMemoryCapacity();
                } else if (maxCapacity < maxRedoLogMemoryCapacity) {
                    throw new IllegalArgumentException("the maximum between 'cluster-config.groups.group.repl-policy.redo-log-capacity="
                            + replicationPolicy.getMaxRedoLogCapacity()
                            + " 'cluster-config.groups.group.repl-policy.redo-log-recovery-capacity="
                            + replicationPolicy.getMaxRedoLogCapacityDuringRecovery()
                            + (mirrorPolicy != null ? " and 'cluster-config.mirror-service.redo-log-capacity="
                            + mirrorPolicy.getMaxRedoLogCapacity()
                            : "")
                            + (replicationComponentPolicy != null ? " and replication gateways="
                            + maxReplicationComponentRedoLogCapacity
                            : "")
                            + " cannot be less than the 'cluster-config.groups.group.repl-policy.redo-log-memory-capacity="
                            + replicationPolicy.getMaxRedoLogMemoryCapacity()
                            + "'");
                } else {
                    config.setLimitedMemoryCapacity((int) maxRedoLogMemoryCapacity);
                }
            } else {
                // We only support int memory capacity, long is provided due to
                // backwards
                config.setLimitedMemoryCapacity((int) maxRedoLogMemoryCapacity);
            }
        }

        // Set swap backlog settings
        config.setSwapBacklogConfig(replicationPolicy.getSwapBacklogSettings());

        if(mirrorPolicy != null) {
            config.setMirrorMemberName(mirrorPolicy.getMirrorMemberName());
        }

        config.setRedoLogCompaction(replicationPolicy.getRedoLogCompaction());

        return config;
    }

    private String getCapacityDisplayStr(long capacity) {
        return capacity == UNLIMITED ? "unlimited(-1)" : "" + capacity;
    }

    private void configureAsyncSourceGroup(
            IReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder,
            ReplicationNodeConfig replicationNodeConfig,
            DynamicAsyncSourceGroupConfigHolder configHolder) {
        AsyncReplicationSourceGroupBuilder sourceAsync = new AsyncReplicationSourceGroupBuilder(configHolder);
        sourceAsync.setBatchSize(replicationPolicy.getBatchSize());
        sourceAsync.setIntervalMilis(replicationPolicy.getIdleDelay());
        sourceAsync.setIntervalOperations(replicationPolicy.getOperationsReplicationThreshold());
        sourceAsync.setAsyncHandlerProvider(nodeBuilder.getAsyncHandlerProvider());
        sourceAsync.setBacklogBuilder(nodeBuilder.getReplicationBacklogBuilder());
        replicationNodeConfig.addSourceGroupBuilder(sourceAsync,
                ReplicationNodeMode.ACTIVE);
    }

    private void configureSyncSourceGroup(
            IReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder,
            ReplicationNodeConfig replicationNodeConfig,
            DynamicSourceGroupConfigHolder configHolder,
            ReplicationMode channelType) {
        SyncReplicationSourceGroupBuilder sourceSync = new SyncReplicationSourceGroupBuilder(configHolder);
        sourceSync.setAsyncStateBatchSize(replicationPolicy.getBatchSize());
        sourceSync.setAsyncHandlerProvider(nodeBuilder.getAsyncHandlerProvider());
        sourceSync.setAsyncStateIdleDelay(replicationPolicy.getIdleDelay());
        sourceSync.setThrottleController(createSyncGroupThrottleController(replicationPolicy));
        sourceSync.setBacklogBuilder(nodeBuilder.getReplicationBacklogBuilder());
        sourceSync.setChannelType(channelType);
        replicationNodeConfig.addSourceGroupBuilder(sourceSync,
                ReplicationNodeMode.ACTIVE);
    }

    public IReplicationThrottleControllerBuilder createSyncGroupThrottleController(
            IReplicationSettings replicationPolicy) {
        int maxTPWhenInactive = replicationPolicy.getSyncReplicationSettings()
                .getMaxThrottleTPWhenInactive();
        int minTPWhenActive = replicationPolicy.getSyncReplicationSettings()
                .getMinThrottleTPWhenActive();
        int threshold = replicationPolicy.getBatchSize();
        boolean throttleWhenInactive = replicationPolicy.getSyncReplicationSettings()
                .isThrottleWhenInactive();

        return new BacklogAdjustedThrottleControllerBuilder(maxTPWhenInactive,
                minTPWhenActive,
                threshold,
                throttleWhenInactive);
    }

    public List<String> createGroupMemberNames(
            ISpaceReplicationSettings settings) {
        List<String> groupMemberNames = new LinkedList<String>(settings.getGroupMemberNames());
        for (Iterator<String> iterator = groupMemberNames.iterator(); iterator.hasNext(); ) {
            String memberName = iterator.next();
            if (memberName.equals(settings.getSpaceMemberName())) {
                iterator.remove();
            }
        }
        return groupMemberNames;
    }

    public ReplicationNodeConfig createReliableAsyncSourceConfig(
            IReliableAsyncReplicationSettings reliableAsyncSettings,
            IReplicationNodeBuilder nodeBuilder, int partitionId,
            IReplicationChannelDataFilterBuilder filterBuilder,
            IDynamicSourceGroupMemberLifeCycleBuilder lifeCycleBuilder) {

        // TODO LV: properly configure reliable async interval and batch size
        // for handshake redo log completion and
        // async channel properties
        ISpaceReplicationSettings replicationPolicy = reliableAsyncSettings.getSpaceReplicationSettings();

        validateConfig(replicationPolicy);

        final String groupName = createReplicationGroupName(partitionId,
                true,
                true,
                false,
                true);

        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();
        replicationNodeConfig.mapSourceGroups("all",
                GroupMapping.getAllMapping());

        BacklogConfig backlogConfig = createBacklogConfig(replicationPolicy,
                null,
                true,
                true);

        List<String> syncGroupMemberNames = createGroupMemberNames(replicationPolicy);

        configureBacklogLimitations(replicationPolicy,
                backlogConfig,
                syncGroupMemberNames);

        // configure mirror limited backlog
        List<String> reliableAsyncMemberNames = new LinkedList<String>();

        Map<String, IReplicationChannelDataFilter> filters = new HashMap<String, IReplicationChannelDataFilter>();

        addKeepersFilters(reliableAsyncSettings,
                groupName,
                syncGroupMemberNames,
                filters,
                false);

        final GroupConsistencyLevelPolicy groupConsistencyLevelPolicy = getGroupConsistencyLevel(replicationPolicy,
                syncGroupMemberNames);

        ReliableAsyncSourceGroupConfig sourceGroupConfig = new ReliableAsyncSourceGroupConfig(groupName,
                backlogConfig,
                groupConsistencyLevelPolicy,
                filters,
                null,
                null,
                syncGroupMemberNames.toArray(new String[syncGroupMemberNames.size()]),
                reliableAsyncMemberNames.toArray(new String[reliableAsyncMemberNames.size()]), replicationPolicy.getBatchSize(), reliableAsyncSettings.getReliableAsyncCompletionNotifierInterval(), reliableAsyncSettings.getReliableAsyncCompletionNotifierPacketsThreshold(), reliableAsyncSettings.getSpaceReplicationSettings().isNetworkCompressionEnabled());

        DynamicReliableAsyncSourceGroupConfigHolder configHolder = new DynamicReliableAsyncSourceGroupConfigHolder(sourceGroupConfig);

        addKeepersListeners(filters, configHolder);

        // Create source sync replication group
        ReliableAsyncReplicationSourceGroupBuilder sourceSync = new ReliableAsyncReplicationSourceGroupBuilder(configHolder);
        sourceSync.setAsyncHandlerProvider(nodeBuilder.getAsyncHandlerProvider());
        sourceSync.setAsyncChannelBatchSize(replicationPolicy.getBatchSize());
        sourceSync.setAsyncChannelIntervalMilis(replicationPolicy.getIdleDelay());
        sourceSync.setAsyncChannelIntervalOperations(replicationPolicy.getOperationsReplicationThreshold());
        sourceSync.setSyncChannelAsyncStateBatchSize(replicationPolicy.getBatchSize());
        sourceSync.setSyncChannelIdleDelayMilis(replicationPolicy.getIdleDelay());
        sourceSync.setThrottleController(createSyncGroupThrottleController(replicationPolicy));
        sourceSync.setBacklogBuilder(nodeBuilder.getReplicationBacklogBuilder());
        replicationNodeConfig.addSourceGroupBuilder(sourceSync,
                ReplicationNodeMode.ALWAYS);

        // Create sync primary backup target group
        ReliableAsyncKeeperReplicationTargetGroupBuilder targetSyncBackup = new ReliableAsyncKeeperReplicationTargetGroupBuilder();
        TargetGroupConfig targetGroupConfig = new TargetGroupConfig(groupName,
                createProcessLogConfig(replicationPolicy,
                        true, true),
                ReplicationMode.BACKUP_SPACE,
                syncGroupMemberNames.toArray(new String[syncGroupMemberNames.size()]));
        targetSyncBackup.setGroupConfig(targetGroupConfig);
        targetSyncBackup.setProcessLogBuilder(nodeBuilder.getReplicationProcessLogBuilder());
        targetSyncBackup.setFilterBuilder(filterBuilder);
        targetSyncBackup.setLifeCycleBuilder(lifeCycleBuilder);
        replicationNodeConfig.addTargetGroupBuilder(targetSyncBackup,
                ReplicationNodeMode.PASSIVE);

        setReplicationFilters(replicationPolicy, replicationNodeConfig);

        return replicationNodeConfig;
    }

    public GroupConsistencyLevelPolicy getGroupConsistencyLevel(
            ISpaceReplicationSettings replicationPolicy,
            List<String> syncGroupMemberNames) {
        final ConsistencyLevel consistencyLevel = replicationPolicy.getSyncReplicationSettings().getConsistencyLevel();
        switch (consistencyLevel) {
            case ANY:
                return GroupConsistencyLevelPolicy.getEmptyPolicy();
            case QUORUM:
                final int groupSize = syncGroupMemberNames.size() + 1;
                final int quoromSize = (groupSize / 2) + 1;

                if (quoromSize == 1)
                    return GroupConsistencyLevelPolicy.getEmptyPolicy();
                //This is equivallent to setting all consistency level
                if (quoromSize == groupSize)
                    return new SyncMembersInSyncConsistencyLevelPolicy();

                return new SyncMembersInSyncConsistencyLevelPolicy().minNumberOfSyncMembers(quoromSize);
            case ALL:
                return new SyncMembersInSyncConsistencyLevelPolicy();
            default:
                throw new IllegalStateException("unknown consistency level " + consistencyLevel);

        }
    }

    public void addKeepersListeners(
            Map<String, IReplicationChannelDataFilter> filters,
            DynamicReliableAsyncSourceGroupConfigHolder configHolder) {
        for (IReplicationChannelDataFilter filter : filters.values()) {
            if (filter instanceof IDynamicSourceGroupStateListener)
                configHolder.addListener((IDynamicSourceGroupStateListener) filter);
        }
    }

    public void addKeepersFilters(
            IReliableAsyncReplicationSettings reliableAsyncSettings,
            final String groupName, List<String> groupMemberNames,
            Map<String, IReplicationChannelDataFilter> filters,
            boolean requiresEvictionReplicationProtection) {

        if (reliableAsyncSettings.keeperCannotRefillMissingContent()) {
            for (String groupMemberName : groupMemberNames) {
                filters.put(groupMemberName,
                        new ReliableAsyncKeeperLruCentralDataSourceReplicationChannelDataFilter(requiresEvictionReplicationProtection));
            }
        } else
            addGeneralReplicationFilter(groupMemberNames, filters);
    }

    public ReplicationNodeConfig createReliableAsyncSourceWithMirrorConfig(
            IReliableAsyncReplicationSettings reliableAsyncSettings,
            IReplicationNodeBuilder nodeBuilder, int partitionId,
            IReplicationChannelDataFilterBuilder filterBuilder,
            IDynamicSourceGroupMemberLifeCycleBuilder lifeCycleBuilder,
            boolean requiresEvictionReplicationProtection) {
        ISpaceReplicationSettings replicationPolicy = reliableAsyncSettings.getSpaceReplicationSettings();
        IMirrorChannelReplicationSettings mirrorPolicy = reliableAsyncSettings.getMirrorReplicationSettings();

        validateConfig(replicationPolicy);

        final String groupName = createReplicationGroupName(partitionId,
                true,
                true,
                true,
                true);

        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();
        replicationNodeConfig.mapSourceGroups("all",
                GroupMapping.getAllMapping());

        BacklogConfig backlogConfig = createBacklogConfig(replicationPolicy,
                mirrorPolicy,
                true,
                true);

        List<String> syncGroupMemberNames = createGroupMemberNames(replicationPolicy);
        // remove old mirror name - just a hack till mirror is properly
        // configured
        syncGroupMemberNames.remove(mirrorPolicy.getMirrorMemberName());

        configureBacklogLimitations(replicationPolicy,
                backlogConfig,
                syncGroupMemberNames);

        // configure mirror limited backlog
        List<String> reliableAsyncMemberNames = new LinkedList<String>();
        reliableAsyncMemberNames.add(mirrorPolicy.getMirrorMemberName());

        configureMirrorBacklogLimitations(mirrorPolicy,
                backlogConfig,
                reliableAsyncMemberNames);

        Map<String, IReplicationChannelDataFilter> filters = new HashMap<String, IReplicationChannelDataFilter>();

        filters.put(mirrorPolicy.getMirrorMemberName(),
                new MirrorChannelDataFilter(mirrorPolicy));

        Map<String, String[]> membersGrouping = requiresEvictionReplicationProtection ? createMembersGroupingForMirror(mirrorPolicy.getMirrorMemberName()) : null;

        addKeepersFilters(reliableAsyncSettings,
                groupName,
                syncGroupMemberNames,
                filters,
                requiresEvictionReplicationProtection);

        final GroupConsistencyLevelPolicy groupConsistencyLevelPolicy = getGroupConsistencyLevel(replicationPolicy,
                syncGroupMemberNames);

        ReliableAsyncSourceGroupConfig sourceGroupConfig = new ReliableAsyncSourceGroupConfig(groupName,
                backlogConfig,
                groupConsistencyLevelPolicy,
                filters,
                null,
                membersGrouping,
                syncGroupMemberNames.toArray(new String[syncGroupMemberNames.size()]),
                reliableAsyncMemberNames.toArray(new String[reliableAsyncMemberNames.size()]), mirrorPolicy.getBatchSize(), reliableAsyncSettings.getReliableAsyncCompletionNotifierInterval(), reliableAsyncSettings.getReliableAsyncCompletionNotifierPacketsThreshold(), reliableAsyncSettings.getSpaceReplicationSettings().isNetworkCompressionEnabled());

        sourceGroupConfig.setChannelConfig(mirrorPolicy.getMirrorMemberName(),
                new AsyncChannelConfig(mirrorPolicy.getBatchSize(),
                        mirrorPolicy.getIdleDelay(),
                        mirrorPolicy.getOperationsReplicationThreshold(),
                        ReplicationMode.MIRROR));

        DynamicReliableAsyncSourceGroupConfigHolder configHolder = new DynamicReliableAsyncSourceGroupConfigHolder(sourceGroupConfig);

        addKeepersListeners(filters, configHolder);

        // Create source sync replication group
        ReliableAsyncReplicationSourceGroupBuilder sourceSync = new ReliableAsyncReplicationSourceGroupBuilder(configHolder);
        sourceSync.setAsyncHandlerProvider(nodeBuilder.getAsyncHandlerProvider());
        sourceSync.setAsyncChannelBatchSize(mirrorPolicy.getBatchSize());
        sourceSync.setAsyncChannelIntervalMilis(mirrorPolicy.getIdleDelay());
        sourceSync.setAsyncChannelIntervalOperations(mirrorPolicy.getOperationsReplicationThreshold());
        sourceSync.setSyncChannelAsyncStateBatchSize(replicationPolicy.getBatchSize());
        sourceSync.setSyncChannelIdleDelayMilis(replicationPolicy.getIdleDelay());
        sourceSync.setThrottleController(createSyncGroupThrottleController(replicationPolicy));
        sourceSync.setBacklogBuilder(nodeBuilder.getReplicationBacklogBuilder());
        replicationNodeConfig.addSourceGroupBuilder(sourceSync,
                ReplicationNodeMode.ALWAYS);

        // Create sync primary backup target group
        ReliableAsyncKeeperReplicationTargetGroupBuilder targetSyncBackup = new ReliableAsyncKeeperReplicationTargetGroupBuilder();
        TargetGroupConfig targetGroupConfig = new TargetGroupConfig(groupName,
                createProcessLogConfig(replicationPolicy,
                        true, true),
                ReplicationMode.BACKUP_SPACE, syncGroupMemberNames.toArray(new String[syncGroupMemberNames.size()]));
        targetSyncBackup.setGroupConfig(targetGroupConfig);
        targetSyncBackup.setProcessLogBuilder(nodeBuilder.getReplicationProcessLogBuilder());
        targetSyncBackup.setLifeCycleBuilder(lifeCycleBuilder);
        targetSyncBackup.setFilterBuilder(filterBuilder);
        replicationNodeConfig.addTargetGroupBuilder(targetSyncBackup,
                ReplicationNodeMode.PASSIVE);

        setReplicationFilters(replicationPolicy, replicationNodeConfig);

        return replicationNodeConfig;
    }

    public Map<String, String[]> createMembersGroupingForMirror(String mirrorMemberName) {
        Map<String, String[]> map = new HashMap<String, String[]>();
        map.put(PERSISTENCY_GROUPING_NAME, new String[]{mirrorMemberName});
        return map;
    }

    private void addGeneralReplicationFilter(
            List<String> groupMemberNames,
            Map<String, IReplicationChannelDataFilter> filters) {
        for (String groupMemberName : groupMemberNames) {
            filters.put(groupMemberName,
                    GeneralReplicationChannelDataFilter.getInstance());
        }
    }

    public void validateConfig(
            ISpaceReplicationSettings replicationPolicy) {
        // Verify that the redo log recovery capacity as at least as high as the
        // non mirror redo log capacity
        if (replicationPolicy.getMaxRedoLogCapacityDuringRecovery() != UNLIMITED
                && (replicationPolicy.getMaxRedoLogCapacityDuringRecovery() < replicationPolicy.getMaxRedoLogCapacity() || replicationPolicy.getMaxRedoLogCapacity() == UNLIMITED))
            throw new IllegalArgumentException("'cluster-config.groups.group.repl-policy.redo-log-recovery-capacity="
                    + getCapacityDisplayStr(replicationPolicy.getMaxRedoLogCapacityDuringRecovery())
                    + "' cannot be less than the 'cluster-config.groups.group.repl-policy.redo-log-capacity="
                    + getCapacityDisplayStr(replicationPolicy.getMaxRedoLogCapacity())
                    + "'");
    }

    public ReplicationNodeConfig createSourceWithMirrorConfig(
            MirrorReplicationSettingsAdapter mirrorPolicy,
            ISpaceReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder,
            int partitionId,
            boolean requiresEvictionReplicationProtection) {
        final String groupName = createReplicationGroupName(partitionId,
                false,
                true,
                true,
                true);

        validateConfig(replicationPolicy);

        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();
        replicationNodeConfig.mapSourceGroups("all",
                GroupMapping.getAllMapping());

        BacklogConfig backlogConfig = createBacklogConfig(replicationPolicy,
                mirrorPolicy,
                false,
                false);

        // configure mirror limited backlog
        List<String> mirrorMembersNames = new LinkedList<String>();
        mirrorMembersNames.add(mirrorPolicy.getMirrorMemberName());

        configureMirrorBacklogLimitations(mirrorPolicy,
                backlogConfig,
                mirrorMembersNames);

        Map<String, String[]> membersGrouping = requiresEvictionReplicationProtection ? createMembersGroupingForMirror(mirrorPolicy.getMirrorMemberName()) : null;

        Map<String, IReplicationChannelDataFilter> filters = new HashMap<String, IReplicationChannelDataFilter>();

        filters.put(mirrorPolicy.getMirrorMemberName(),
                new MirrorChannelDataFilter(mirrorPolicy));

        AsyncSourceGroupConfig sourceGroupConfig = new AsyncSourceGroupConfig(groupName,
                backlogConfig,
                null,
                filters,
                null,
                membersGrouping,
                replicationPolicy.isNetworkCompressionEnabled(),
                mirrorMembersNames.toArray(new String[mirrorMembersNames.size()]));

        DynamicAsyncSourceGroupConfigHolder configHolder = new DynamicAsyncSourceGroupConfigHolder(sourceGroupConfig);

        // Create source replication group
        AsyncReplicationSourceGroupBuilder sourceBuilder = new AsyncReplicationSourceGroupBuilder(configHolder);

        sourceGroupConfig.setChannelConfig(mirrorPolicy.getMirrorMemberName(),
                new AsyncChannelConfig(mirrorPolicy.getBatchSize(),
                        mirrorPolicy.getIdleDelay(),
                        mirrorPolicy.getOperationsReplicationThreshold(),
                        ReplicationMode.MIRROR));

        sourceBuilder.setAsyncHandlerProvider(nodeBuilder.getAsyncHandlerProvider());
        sourceBuilder.setBatchSize(mirrorPolicy.getBatchSize());
        sourceBuilder.setIntervalMilis(mirrorPolicy.getIdleDelay());
        sourceBuilder.setIntervalOperations(mirrorPolicy.getOperationsReplicationThreshold());

        sourceBuilder.setBacklogBuilder(nodeBuilder.getReplicationBacklogBuilder());
        replicationNodeConfig.addSourceGroupBuilder(sourceBuilder,
                ReplicationNodeMode.ACTIVE);

        setReplicationFilters(replicationPolicy, replicationNodeConfig);

        return replicationNodeConfig;
    }

    public ReplicationNodeConfig createMirrorConfig(MirrorConfig mirrorConfig, ReplicationNodeBuilder nodeBuilder) {
        return createMirrorConfig(mirrorConfig.getClusterName(), nodeBuilder, mirrorConfig.getPartitionsCount(), mirrorConfig.getBackupsPerPartition());
    }

    public ReplicationNodeConfig createMirrorConfig(String spaceName,
                                                    ReplicationNodeBuilder nodeBuilder, int partitions, int backups) {
        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();

        // TODO WAN: for now still use partitions count to create groups but we
        // can remove it
        // since we got dynamic target group support
        for (int i = 0; i < partitions; i++) {
            final int partitionId = i + 1;
            String groupNameTemplate;
            if (backups > 0) {
                // We need this for backward with old group name (hot upgrade)
                String withoutGatewayMatchinTemplate = PRIMARY_BACKUP_RELIABLE_ASYNC
                        + partitionId;
                String withGatewaysMatchingTemplate = "(.*):" + spaceName + ":"
                        + PRIMARY_BACKUP_RELIABLE_ASYNC + partitionId;
                groupNameTemplate = "(" + withoutGatewayMatchinTemplate + ")|("
                        + withGatewaysMatchingTemplate + ")";
            } else {
                String withoutGatewayMatchinTemplate = ASYNC_MIRROR
                        + partitionId;
                String withGatewaysMatchingTemplate = "(.*):" + spaceName + ":"
                        + ASYNC_MIRROR + partitionId;
                groupNameTemplate = "(" + withoutGatewayMatchinTemplate + ")|("
                        + withGatewaysMatchingTemplate + ")";
            }

            List<String> groupMemberNames = new LinkedList<String>();
            // If no cluster name provided, use unbounded group
            boolean hasSourceClusterName = StringUtils.hasLength(spaceName);
            if (hasSourceClusterName) {
                String primaryMemberName = spaceName + "_container"
                        + partitionId + ":" + spaceName;
                groupMemberNames.add(primaryMemberName);
                for (int j = 0; j < backups; j++) {
                    int backupId = j + 1;
                    String backupMemberName = spaceName + "_container"
                            + partitionId + "_" + backupId + ":" + spaceName;
                    groupMemberNames.add(backupMemberName);
                }
            }
            TargetGroupConfig targetGroupConfig = new TargetGroupConfig("NOT SET",
                    null, /*
                                                                               * mirror
                                                                               * build
                                                                               * the
                                                                               * processlog
                                                                               * according
                                                                               * to
                                                                               * handshake
                                                                               */
                    ReplicationMode.MIRROR, groupMemberNames.toArray(new String[groupMemberNames.size()]));
            // If no cluster name provided, use unbounded group
            if (!hasSourceClusterName)
                targetGroupConfig.setUnbounded(true);

            ReliableAsyncSingleOriginReplicationTargetGroupBuilder mirrorGroupBuilder = new ReliableAsyncSingleOriginReplicationTargetGroupBuilder(targetGroupConfig);
            mirrorGroupBuilder.setGroupNameTemplate(groupNameTemplate);
            mirrorGroupBuilder.setProcessLogBuilder(nodeBuilder.getReplicationProcessLogBuilder());
            replicationNodeConfig.addDynamicTargetGroupBuilder(mirrorGroupBuilder,
                    ReplicationNodeMode.ACTIVE);
        }

        return replicationNodeConfig;
    }

    public ReplicationNodeConfig createLocalViewConfig(String spaceName, SpaceClusterInfo clusterInfo,
                                                       ReplicationNodeBuilder nodeBuilder,
                                                       int partitions) {
        // TODO LV: Support local view on space without replication policy.
        if (!clusterInfo.isReplicated())
            throw new UnsupportedOperationException("LocalView on a non-replicated space is currently not supported.");

        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();
        for (int i = 0; i < partitions; i++)
            createClientSideReplicationTargetGroupConfig(spaceName, clusterInfo, nodeBuilder, replicationNodeConfig, i,
                    ReplicationMode.LOCAL_VIEW);
        return replicationNodeConfig;
    }


    public ReplicationNodeConfig createNotificationConfig(String spaceName, SpaceClusterInfo clusterInfo,
                                                          ReplicationNodeBuilder nodeBuilder, int partitionIndex, int numOfPartitions) {
        int startIndex = partitionIndex == -1 ? 0 : partitionIndex;
        int endIndex = partitionIndex == -1 ? numOfPartitions : partitionIndex + 1;

        ReplicationNodeConfig replicationNodeConfig = new ReplicationNodeConfig();
        for (int i = startIndex; i < endIndex; i++)
            createClientSideReplicationTargetGroupConfig(spaceName, clusterInfo, nodeBuilder, replicationNodeConfig, i,
                    ReplicationMode.DURABLE_NOTIFICATION);
        return replicationNodeConfig;
    }

    private void createClientSideReplicationTargetGroupConfig(
            String spaceName, SpaceClusterInfo clusterInfo, ReplicationNodeBuilder nodeBuilder,
            ReplicationNodeConfig replicationNodeConfig,
            int partitionIndex,
            ReplicationMode groupChannelType) {
        {
            final int partitionId = partitionIndex + 1;
            String groupName = createReplicationGroupName(partitionId, spaceName, clusterInfo, clusterInfo.getNumberOfBackups() > 0);

            List<String> groupMemberNames = new LinkedList<String>();
            // If no cluster name provided, use unbounded group
            String primaryMemberName = spaceName + "_container" + partitionId
                    + ":" + spaceName;
            groupMemberNames.add(primaryMemberName);
            for (int j = 0; j < clusterInfo.getNumberOfBackups(); j++) {
                int backupId = j + 1;
                String backupMemberName = spaceName + "_container"
                        + partitionId + "_" + backupId + ":" + spaceName;
                groupMemberNames.add(backupMemberName);
            }
            TargetGroupConfig targetGroupConfig = new TargetGroupConfig(groupName,
                    null, /*
                                                                               * building
                                                                               * the
                                                                               * processlog
                                                                               * according
                                                                               * to
                                                                               * handshake
                                                                               */
                    groupChannelType,
                    groupMemberNames.toArray(new String[groupMemberNames.size()]));

            ReliableAsyncSingleOriginReplicationTargetGroupBuilder groupBuilder = new ReliableAsyncSingleOriginReplicationTargetGroupBuilder(targetGroupConfig);
            groupBuilder.setProcessLogBuilder(nodeBuilder.getReplicationProcessLogBuilder());
            replicationNodeConfig.addTargetGroupBuilder(groupBuilder,
                    ReplicationNodeMode.ACTIVE);
        }
    }

    public String getSynchronizeGroupName(SpaceClusterInfo clusterInfo, int partitionId, String spaceMemberName) {
        return createReplicationGroupName(partitionId, spaceMemberName, clusterInfo, clusterInfo.isPrimaryElectionAvailable());
    }

    public String createReplicationGroupName(int partitionId, boolean primaryBackupGroup,
                                             boolean syncReplication, boolean hasMirror, boolean hasExistingReplicationMembers) {
        if (hasMirror)
            return primaryBackupGroup ? PRIMARY_BACKUP_RELIABLE_ASYNC + partitionId : ASYNC_MIRROR + partitionId;
        if (primaryBackupGroup || !hasExistingReplicationMembers)
            return (syncReplication ? PRIMARY_BACKUP_SYNC : PRIMARY_BACKUP_ASYNC) + "-" + partitionId;
        return syncReplication ? SYNC : ASYNC;
    }

    public String createReplicationGroupName(int partitionId, String spaceMemberName, SpaceClusterInfo clusterInfo,
                                             boolean primaryBackupGroup) {
        return createReplicationGroupName(partitionId, primaryBackupGroup,
                clusterInfo.isSyncReplicationEnabled(),
                clusterInfo.isMirrorServiceEnabled(),
                clusterInfo.hasReplicationTargets());
    }

    public void configureBacklogLimitations(
            ISpaceReplicationSettings replicationPolicy,
            BacklogConfig backlogConfig, List<String> groupMemberNames) {
        // configure limited backlog settings
        long nonMirrorMaxRedoLogCapacity = replicationPolicy.getMaxRedoLogCapacity();
        for (String memberName : groupMemberNames) {
            BacklogMemberLimitationConfig memberLimitationConfig = new BacklogMemberLimitationConfig();
            if (nonMirrorMaxRedoLogCapacity == -1) {
                memberLimitationConfig.setUnlimited();
            } else {
                LimitReachedPolicy limitReachedPolicy = replicationPolicy.getLimitReachedPolicy();
                memberLimitationConfig.setLimit(nonMirrorMaxRedoLogCapacity,
                        limitReachedPolicy);

                long maxRedoLogCapacityDuringRecovery = replicationPolicy.getMaxRedoLogCapacityDuringRecovery();
                if (maxRedoLogCapacityDuringRecovery == -1) {
                    memberLimitationConfig.setUnlimitedDuringSynchronization();
                } else {
                    memberLimitationConfig.setLimitDuringSynchronization(maxRedoLogCapacityDuringRecovery, LimitReachedPolicy.BLOCK_NEW);
                }
            }
            backlogConfig.setMemberBacklogLimitation(memberName, memberLimitationConfig);
        }
    }

    public void configureMirrorBacklogLimitations(
            IReplicationSettings mirrorSettings, BacklogConfig backlogConfig,
            List<String> mirrorMembersNames) {
        long mirrorMaxRedoLogCapacity = mirrorSettings.getMaxRedoLogCapacity();

        for (String memberName : mirrorMembersNames) {
            BacklogMemberLimitationConfig memberLimitationConfig = new BacklogMemberLimitationConfig();
            if (mirrorMaxRedoLogCapacity == UNLIMITED) {
                memberLimitationConfig.setUnlimited();
            } else {
                memberLimitationConfig.setLimit(mirrorMaxRedoLogCapacity,
                        mirrorSettings.getLimitReachedPolicy());
            }
            backlogConfig.setMemberBacklogLimitation(memberName, memberLimitationConfig);
        }
    }

    private void configureAsyncReplicatedTargetGroup(
            ISpaceReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder,
            ReplicationNodeConfig replicationNodeConfig,
            List<String> groupMemberNames, final String groupName) {
        AsyncMultiOriginReplicationTargetGroupBuilder targetAsyncGroup = new AsyncMultiOriginReplicationTargetGroupBuilder();
        TargetGroupConfig targetGroupConfig = new TargetGroupConfig(groupName,
                createProcessLogConfig(replicationPolicy,
                        false, false),
                ReplicationMode.ACTIVE_SPACE,
                groupMemberNames.toArray(new String[groupMemberNames.size()]));
        targetAsyncGroup.setGroupConfig(targetGroupConfig);
        targetAsyncGroup.setProcessLogBuilder(nodeBuilder.getReplicationProcessLogBuilder());
        replicationNodeConfig.addTargetGroupBuilder(targetAsyncGroup,
                ReplicationNodeMode.ACTIVE);
    }

    private void configureSyncReplicatedTargetGroup(
            ISpaceReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder,
            ReplicationNodeConfig replicationNodeConfig,
            List<String> groupMemberNames, final String groupName,
            boolean supportConcurrent) {
        SyncMultiOriginReplicationTargetGroupBuilder targetSync = new SyncMultiOriginReplicationTargetGroupBuilder();
        TargetGroupConfig targetGroupConfig = new TargetGroupConfig(groupName,
                createProcessLogConfig(replicationPolicy,
                        supportConcurrent, false),
                ReplicationMode.ACTIVE_SPACE,
                groupMemberNames.toArray(new String[groupMemberNames.size()]));
        targetSync.setGroupConfig(targetGroupConfig);
        targetSync.setProcessLogBuilder(nodeBuilder.getReplicationProcessLogBuilder());
        replicationNodeConfig.addTargetGroupBuilder(targetSync,
                ReplicationNodeMode.ACTIVE);
    }

    private void configureSyncPrimaryBackupTargetGroup(
            ISpaceReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder,
            ReplicationNodeConfig replicationNodeConfig,
            List<String> groupMemberNames, final String groupName,
            boolean supportConcurrent, boolean supportsMultiSource) {
        SyncSingleOriginReplicationTargetGroupBuilder targetSyncBackup = new SyncSingleOriginReplicationTargetGroupBuilder();
        TargetGroupConfig targetGroupConfig = new TargetGroupConfig(groupName,
                createProcessLogConfig(replicationPolicy,
                        supportConcurrent, supportsMultiSource),
                ReplicationMode.BACKUP_SPACE,
                groupMemberNames.toArray(new String[groupMemberNames.size()]));
        targetSyncBackup.setGroupConfig(targetGroupConfig);
        targetSyncBackup.setProcessLogBuilder(nodeBuilder.getReplicationProcessLogBuilder());
        replicationNodeConfig.addTargetGroupBuilder(targetSyncBackup,
                ReplicationNodeMode.PASSIVE);
    }

    private void configureAsyncPrimaryBackupTargetGroup(
            ISpaceReplicationSettings replicationPolicy,
            IReplicationNodeBuilder nodeBuilder,
            ReplicationNodeConfig replicationNodeConfig,
            List<String> groupMemberNames, String groupName) {
        TargetGroupConfig targetGroupConfig = new TargetGroupConfig(groupName,
                createProcessLogConfig(replicationPolicy,
                        false, false),
                ReplicationMode.BACKUP_SPACE,
                groupMemberNames.toArray(new String[groupMemberNames.size()]));
        AsyncSingleOriginReplicationTargetGroupBuilder targetAsyncBackup = new AsyncSingleOriginReplicationTargetGroupBuilder(targetGroupConfig);
        targetAsyncBackup.setProcessLogBuilder(nodeBuilder.getReplicationProcessLogBuilder());
        replicationNodeConfig.addTargetGroupBuilder(targetAsyncBackup,
                ReplicationNodeMode.PASSIVE);
    }

    public ProcessLogConfig createProcessLogConfig(
            ISpaceReplicationSettings replicationPolicy,
            boolean supportsConcurrentReplication, boolean supportsMultiSource) {
        if (replicationPolicy.getProcessingType() == ReplicationProcessingType.MULTIPLE_BUCKETS
                && supportsConcurrentReplication) {
            MultiBucketSingleFileProcessLogConfig config = new MultiBucketSingleFileProcessLogConfig();
            config.setBucketsCount(replicationPolicy.getBucketCount());
            config.setBatchProcessingThreshold(replicationPolicy.getBatchParallelThreshold());
            config.setBatchParallelFactor(replicationPolicy.getBatchParallelFactor());
            config.setConsumeTimeout(replicationPolicy.getConsumeTimeout());
            return config;
        } else if (replicationPolicy.getProcessingType() == ReplicationProcessingType.MULTIPLE_SOURCES
                && supportsMultiSource) {
            MultiSourceSingleFileProcessLogConfig config = new MultiSourceSingleFileProcessLogConfig();
            config.setConsumeTimeout(replicationPolicy.getConsumeTimeout());
            return config;
        }
        GlobalOrderProcessLogConfig config = new GlobalOrderProcessLogConfig();
        config.setConsumeTimeout(replicationPolicy.getConsumeTimeout());
        return config;
    }

    public void setReplicationFilters(
            final IReplicationSettings replicationPolicy,
            ReplicationNodeConfig replicationNodeConfig) {
        replicationNodeConfig.setReplicationInFilter(replicationPolicy.getInFilter());
        replicationNodeConfig.setSpaceCopyReplicaInFilter(replicationPolicy.getSpaceCopyInFilter());

        replicationNodeConfig.setReplicationOutFilter(replicationPolicy.getOutFilter());
        replicationNodeConfig.setSpaceCopyReplicaOutFilter(replicationPolicy.getSpaceCopyOutFilter());
    }

    public String supportsMultiSourceReplication(SpaceClusterInfo clusterInfo) {
        // sync replicated cluster is not supported.
        final boolean primaryBackupGroup = clusterInfo.isPrimaryElectionAvailable();
        final boolean hasMirror = clusterInfo.isMirrorServiceEnabled();
        final boolean hasExistingReplicationMembers = clusterInfo.hasReplicationTargets();

        if (!hasMirror && !primaryBackupGroup && hasExistingReplicationMembers) {
            final boolean syncReplication = clusterInfo.isSyncReplicationEnabled();
            return (syncReplication ? "Sync" : "Async") + " replication is not supported in Multiple sources replication processing";
        }
        return null;
    }
}
