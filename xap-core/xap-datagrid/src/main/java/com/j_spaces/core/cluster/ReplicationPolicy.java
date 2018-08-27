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


package com.j_spaces.core.cluster;

import com.gigaspaces.cluster.activeelection.LeaderSelector;
import com.gigaspaces.cluster.activeelection.core.ActiveElectionState;
import com.gigaspaces.cluster.replication.MirrorServiceConfig;
import com.gigaspaces.cluster.replication.ReplicationTransmissionPolicy;
import com.gigaspaces.cluster.replication.sync.SyncReplPolicy;
import com.gigaspaces.internal.cluster.node.impl.config.MultiBucketReplicationPolicy;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.j_spaces.core.ISpaceState;
import com.j_spaces.core.JSpaceState;
import com.j_spaces.core.client.SpaceURL;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ReplicationPolicy implements Serializable, Externalizable {
    private static final long serialVersionUID = 1L;

    final static public String UNICAST_COMMUNICATION_MODE = "unicast";
    final static public String MULTICAST_COMMUNICATION_MODE = "multicast";
    final static public int UNICAST = 0;
    final static public int MULTICAST = 1;

    // replication mode
    public final static String ASYNC_REPLICATION_MODE = "async";
    public final static String SYNC_REPLICATION_MODE = "sync";
    public final static String SYNC_REC_ACK_REPLICATION_MODE = "sync-rec-ack";

    // Redo log capacity exceeded mode
    public final static String BLOCK_OPERATIONS_MODE = "block-operations";
    public final static String DROP_OLDEST_MODE = "drop-oldest";

    // Missing packets mode
    public final static String RECOVER_MODE = "recover";
    public final static String IGNORE_MODE = "ignore";

    // Processing type mode
    public final static String GLOBAL_ORDER_MODE = "global-order";
    public final static String MULTI_BUCKET_MODE = "multi-bucket";
    public final static String MULTI_SOURCE_MODE = "multi-source";

    // replication common parameters
    public final static int FULL_REPLICATION = 0;
    public final static int PARTIAL_REPLICATION = 1;

    // asynchronous replication parameters
    public final static String DEFAULT_REPL_ORIGINAL_STATE = "false";
    final static public int DEFAULT_REPL_CHUNK_SIZE = 500;
    final static public int DEFAULT_REPL_INTERVAL_MILLIS = 3000;
    final static public int DEFAULT_REPL_INTERVAL_OPERS = 500;
    final static public int DEFAULT_REPL_SPACE_FINDER_TIMEOUT = 1000 * 5; // 5 sec;
    final static public int DEFAULT_REPL_SPACE_FINDER_REPORT_INTERVAL = 30000; //print report every 30 sec
    final static public int DEFAULT_SYNC_ON_COMMIT_TIMEOUT = 60 * 1000 * 5; // 5 minutes
    final static public long DEFAULT_ASYNC_CHANNEL_SHUTDOWN_TIMEOUT = 5 * 60 * 1000; // 5 minutes
    final static public boolean DEFAULT_REPL_NETWORK_COMPRESSION = true;

    final static public long DEFAULT_MAX_REDO_LOG_CPACITY = -1;
    final static public RedoLogCapacityExceededPolicy DEFAULT_REDO_LOG_CAPACITY_EXCEEDED = RedoLogCapacityExceededPolicy.BLOCK_OPERATIONS;
    final static public int DEFAULT_RECOVERY_CHUNK_SIZE = 200;
    final static public int DEFAULT_RECOVERY_THREAD_POOL_SIZE = 4;
    public static final int DEFAULT_BLOBSTORE_RECOVERY_THREAD_POOL_SIZE = 8;

    final static public MissingPacketsPolicy DEFAULT_MISSING_PACKETS = MissingPacketsPolicy.IGNORE;

    final static public boolean DEFAULT_REPL_FULL_TAKE = false;

    final static public boolean DEFAULT_OLD_MODULE = false;

    final static public ReplicationProcessingType DEFAULT_PROCESSING_TYPE = ReplicationProcessingType.GLOBAL_ORDER;

    final static public long DEFAULT_LOCALVIEW_REDOLOG_CAPACITY = 150000L;

    final static public long DEFAULT_LOCALVIEW_MAX_DISCONNECTION_TIME = 300000L;

    final static public long DEFAULT_LOCALVIEW_REDOLOG_RECOVERY_CAPACITY = 1000000L;

    // TODO durable notifications : Decide defaults
    final static public long DEFAULT_DURABLE_NOTIFICATION_REDOLOG_CAPACITY = 150000L;
    final static public long DEFAULT_DURABLE_NOTIFICATION_MAX_DISCONNECTION_TIME = 300000L;

    final static public long DEFAULT_RELIABLE_ASYNC_STATE_NOTIFY_INTERVAL = 500;
    final static public long DEFAULT_RELIABLE_ASYNC_STATE_NOTIFY_PACKETS = 500;

    final static public boolean DEFAULT_REPLICATE_ONE_PHASE_COMMIT = false;

    public static final int DEFAULT_CONNECTION_MONITOR_THREAD_POOL_SIZE = 4;
    public static final String DEFAULT_BACKLOG_WEIGHT_POLICY = "accumulated";
    public static final RedoLogCompaction DEFAULT_REDO_LOG_COMPACTION = RedoLogCompaction.MIRROR;
    public static final float DEFAULT_DISCARDED_PACKET_WEIGHT_LOAD_FACTOR = Float.valueOf(System.getProperty("com.j_spaces.repl.default.discarded.load-factor", "0.01"));
    public static final float DEFAULT_REDO_LOG_COMPACTION_BATCH_SIZE = Float.valueOf(System.getProperty("com.j_spaces.repl.default.compaction.batch-size", "100"));

    public String m_OwnMemberName;
    public String m_ReplicationGroupName;
    public List<String> m_ReplicationGroupMembersNames;  // A list of "container-name:space-name"
    public List<SpaceURL> m_ReplicationGroupMembersURLs;  // A list of space URLs
    public boolean m_ReplicateNotifyTemplates = false;
    public boolean m_TriggerNotifyTemplates = false;
    private boolean replicateLeaseExpirations = false;

    // true means save the original state of the entry
    private boolean m_ReplicateOriginalState = false;
    public boolean m_SyncOnCommit = false;
    public String m_ReplicationMode;//= ASYNC_REPLICATION_MODE;
    public int m_PolicyType = FULL_REPLICATION;
    public int m_ReplicationChunkSize = DEFAULT_REPL_CHUNK_SIZE;
    public long m_ReplicationIntervalMillis = DEFAULT_REPL_INTERVAL_MILLIS;
    public int m_ReplicationIntervalOperations = DEFAULT_REPL_INTERVAL_OPERS;
    public long m_SpaceFinderTimeout = DEFAULT_REPL_SPACE_FINDER_TIMEOUT;
    public long m_SpaceFinderReportInterval = DEFAULT_REPL_SPACE_FINDER_REPORT_INTERVAL;
    public long m_SyncOnCommitTimeout = DEFAULT_SYNC_ON_COMMIT_TIMEOUT;
    private long _asyncChannelShutdownTimeout = DEFAULT_ASYNC_CHANNEL_SHUTDOWN_TIMEOUT;
    public boolean m_ReplicationNetworkCompression = DEFAULT_REPL_NETWORK_COMPRESSION;


    private boolean _reliableAsyncRepl = false;
    private Set<ReplicationOperationType> permittedOperations = new HashSet<ReplicationOperationType>();

    // memory recovery
    public boolean m_Recovery = false;  // memory/DB recovery for entire group

    /**
     * replication policy description table: key: memberName, value: ReplicationPolicyDescription.
     */
    public Hashtable<String, ReplicationPolicyDescription> m_ReplMemberPolicyDescTable;

    /**
     * fast one way replication
     */
    public boolean isOneWayReplication = false;

    /**
     * true if sync-replication enabled, otherwise async-repl
     */
    public boolean m_IsSyncReplicationEnabled;

    /**
     * group's sync-replication information, if null, not sync replication defined.
     */
    public SyncReplPolicy m_SyncReplPolicy;

    /**
     * max redo log capacity, if -1 unlimited.
     */
    private long maxRedoLogCapacity = DEFAULT_MAX_REDO_LOG_CPACITY;

    /**
     * max redo log memory capacity, if -1 unlimited.
     */
    private long maxRedoLogMemoryCapacity = DEFAULT_MAX_REDO_LOG_CPACITY;

    /**
     * back log wight Policy
     */
    private String backlogWeightPolicy = DEFAULT_BACKLOG_WEIGHT_POLICY;

    /**
     * max redo log recovery capacity, if -1 unlimited.
     */
    private long maxRedoLogRecoveryCapacity = DEFAULT_MAX_REDO_LOG_CPACITY;

    /**
     * dictates behavior when the redo log capacity is exceeded
     */
    private RedoLogCapacityExceededPolicy onRedoLogCapacityExceeded = DEFAULT_REDO_LOG_CAPACITY_EXCEEDED;

    /**
     * specify whether missing packets can be tolerated or replication is considered broken when
     * this occur
     */
    private MissingPacketsPolicy onMissingPackets = DEFAULT_MISSING_PACKETS;

    /**
     * specify how conflicting replications packets are handled
     */
    private ConflictingOperationPolicy conflictingOperationPolicy;

    /**
     * memory recovery chunk-size - default 1000.
     */
    private int recoveryChunkSize = DEFAULT_RECOVERY_CHUNK_SIZE;

    // number of threads that perform the memory recovery
    private int recoveryThreadPoolSize = DEFAULT_RECOVERY_THREAD_POOL_SIZE;

    private boolean replicateFullTake = DEFAULT_REPL_FULL_TAKE;

    /**
     * the name of the cluster this replication policy belongs to
     */
    public String clusterName;

    /**
     * not null if mirror-service enabled
     */
    private MirrorServiceConfig _mirrorServiceConfig;

    private ReplicationProcessingType processingType = DEFAULT_PROCESSING_TYPE;

    private MultiBucketReplicationPolicy multiBucketReplicationPolicy = null;

    private SwapBacklogConfig swapRedologPolicy = null;

    private Long localViewMaxRedologCapacity = DEFAULT_LOCALVIEW_REDOLOG_CAPACITY;

    private long localViewMaxDisconnectionTime = DEFAULT_LOCALVIEW_MAX_DISCONNECTION_TIME;

    private Long localViewMaxRedologRecoveryCapacity = DEFAULT_LOCALVIEW_REDOLOG_RECOVERY_CAPACITY;

    private Long durableNotificationMaxRedologCapacity = DEFAULT_DURABLE_NOTIFICATION_REDOLOG_CAPACITY;
    private long durableNotificationMaxDisconnectionTime = DEFAULT_DURABLE_NOTIFICATION_MAX_DISCONNECTION_TIME;

    private long reliableAsyncCompletionNotifierInterval = DEFAULT_RELIABLE_ASYNC_STATE_NOTIFY_INTERVAL;
    private long reliableAsyncCompletionNotifierPacketsThreshold = DEFAULT_RELIABLE_ASYNC_STATE_NOTIFY_PACKETS;

    private boolean replicateOnePhaseCommit = DEFAULT_REPLICATE_ONE_PHASE_COMMIT;

    private int connectionMonitorThreadPoolSize = DEFAULT_CONNECTION_MONITOR_THREAD_POOL_SIZE;

    private transient LeaderSelector _primarySpaceSelector;

    final public static String DEFAULT_MIRROR_SERVICE_NAME = "MIRROR_SERVICE";
    final public static String DEFAULT_MIRROR_SERVICE_CONNECTOR_NAME = "Mirror Service Connector";
    private transient RedoLogCompaction redoLogCompaction;

    public void setRedoLogCompaction(RedoLogCompaction redoLogCompaction) {
        this.redoLogCompaction = redoLogCompaction;
    }

    public RedoLogCompaction getRedoLogCompaction() {
        return redoLogCompaction;
    }


    private interface BitMap {

        long OWN_MEMBER_NAME = 1 << 0;
        long REPLICATION_GROUP_NAME = 1 << 1;
        long REPLICATION_GROUP_MEMBERS_NAMES = 1 << 2;
        long REPLICATION_GROUP_MEMBERS_URLS = 1 << 3;
        long REPLICATE_NOTIFY_TEMPLATES = 1 << 4;
        long TRIGGER_NOTIFY_TEMPLATES = 1 << 5;
        long REPLICATE_ORIGINAL_STATE = 1 << 6;
        long SYNC_ON_COMMIT = 1 << 7;
        long REPLICATION_MODE = 1 << 8;
        long POLICY_TYPE = 1 << 9;
        long REPLICATION_CHUNK_SIZE = 1 << 10;
        long REPLICATION_INTERVAL_MILLIS = 1 << 11;
        long REPLICATION_INTERVAL_OPERATIONS = 1 << 12;
        long SPACE_FINDER_TIMEOUT = 1 << 13;
        long SPACE_FINDER_REPORT_INTERVAL = 1 << 14;
        long SYNC_ON_COMMIT_TIMEOUT = 1 << 15;
        long RELIABLE_ASYNC_REPL = 1 << 16;
        long PERMITTED_OPERATIONS = 1 << 17;
        long RECOVERY = 1 << 18;
        long REP_MEMBER_POLICY_DEC_TABLE = 1 << 19;
        long ONE_WAY_REPLICATION = 1 << 20;
        long SYNC_REPLICATION_ENABLED = 1 << 21;
        long SYNC_REPLICATION_POLICY = 1 << 22;
        //long PROTOCOL_ADAPTER_CLASS             = 1 << 23;
        long MAX_REDO_LOG_CAPACITY = 1 << 25;
        //long COMMUNICATION_MODE                 = 1 << 24;
        long RECOVERY_CHUNK_SIZE = 1 << 26;
        long RECOVERY_THREAD_POOL_SIZE = 1 << 27;
        long CLUSTER_NAME = 1 << 28;
        long MIRROR_SERVICE_CONFIG = 1 << 29;
        long ASYNC_CHANNEL_SHUTDOWN_TIMEOUT = 1 << 30;
        long MAX_REDO_LOG_MEMORY_CAPACITY = 1L << 31;
        long ON_REDO_LOG_CAPACITY_EXCEEDED = 1L << 32;
        long TOLERATE_MISSING_PACKETS = 1L << 33;
        long REPL_FULL_TAKE = 1L << 34;
        long MAX_REDO_LOG_RECOVERY_CAPACITY = 1L << 35;
        long PROCESSING_TYPE = 1L << 36;
        long MULTI_BUCKET_REPLICATION_POLICY = 1L << 37;
        long SWAP_REDOLOG_POLICY = 1L << 38;
        long ON_CONFLICTING_PACKETS = 1L << 39;
        long RESERVED_BACKWARDS = 1L << 40;
        long REPLICATE_LEASE_EXPIRATIONS = 1L << 41;
        long LOCALVIEW_MAX_REDOLOG_CAPACITY = 1L << 42;
        long RELIABLE_ASYNC_COMPLETION_NOTIFIER_INTERVAL = 1L << 43;
        long RELIABLE_ASYNC_COMPLETION_NOTIFIER_PACKETS_THRESHOLD = 1L << 44;
        long LOCALVIEW_MAX_DISCONNECTION_TIME = 1L << 45;
        long REPLICATE_ONE_PHASE_COMMIT = 1L << 46;
        long NOTIFICATION_MAX_REDOLOG_CAPACITY = 1L << 47;
        long NOTIFICATION_MAX_DISCONNECTION_TIME = 1L << 48;
        long LOCALVIEW_MAX_REDOLOG_RECOVERY_CAPACITY = 1L << 49;
        long BACKLOG_WEIGHT_POLICY = 1L << 50;
    }
    public void writeExternal(ObjectOutput out) throws IOException {
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();

        long flags = buildFlags(version);
        out.writeLong(flags);

        if (m_OwnMemberName != null)
            out.writeObject(m_OwnMemberName);
        if (m_ReplicationGroupName != null)
            out.writeObject(m_ReplicationGroupName);
        if (m_ReplicationGroupMembersNames != null) {
            out.writeInt(m_ReplicationGroupMembersNames.size());
            for (String name : m_ReplicationGroupMembersNames)
                out.writeObject(name);
        }
        if (m_ReplicationGroupMembersURLs != null) {
            out.writeInt(m_ReplicationGroupMembersURLs.size());
            for (SpaceURL url : m_ReplicationGroupMembersURLs) {
                // we don't do this, since we want to share SpaceURL across cluser policy
                //url.writeExternal(out);
                out.writeObject(url);
            }
        }
        if (m_ReplicationMode != null)
            out.writeObject(m_ReplicationMode);
        if (m_PolicyType != FULL_REPLICATION)
            out.writeInt(m_PolicyType);
        if (m_ReplicationChunkSize != DEFAULT_REPL_CHUNK_SIZE)
            out.writeInt(m_ReplicationChunkSize);
        if (m_ReplicationIntervalMillis != DEFAULT_REPL_INTERVAL_MILLIS)
            out.writeLong(m_ReplicationIntervalMillis);
        if (m_ReplicationIntervalOperations != DEFAULT_REPL_INTERVAL_OPERS)
            out.writeInt(m_ReplicationIntervalOperations);
        if (m_SpaceFinderTimeout != DEFAULT_REPL_SPACE_FINDER_TIMEOUT)
            out.writeLong(m_SpaceFinderTimeout);
        if (m_SpaceFinderReportInterval != DEFAULT_REPL_SPACE_FINDER_REPORT_INTERVAL)
            out.writeLong(m_SpaceFinderReportInterval);
        if (m_SyncOnCommitTimeout != DEFAULT_SYNC_ON_COMMIT_TIMEOUT)
            out.writeLong(m_SyncOnCommitTimeout);
        if (permittedOperations != null) {
            Set<ReplicationOperationType> tempCopy = new HashSet<ReplicationOperationType>(permittedOperations);
            if (version.lessThan(PlatformLogicalVersion.v9_5_0))
                tempCopy.remove(ReplicationOperationType.CHANGE);
            out.writeObject(tempCopy);
        }
        if (m_ReplMemberPolicyDescTable != null) {
            out.writeInt(m_ReplMemberPolicyDescTable.size());
            for (Map.Entry<String, ReplicationPolicyDescription> entry : m_ReplMemberPolicyDescTable.entrySet()) {
                out.writeObject(entry.getKey());
                entry.getValue().writeExternal(out);
            }
        }
        if (m_SyncReplPolicy != null)
            out.writeObject(m_SyncReplPolicy);
        if (maxRedoLogCapacity != DEFAULT_MAX_REDO_LOG_CPACITY)
            out.writeLong(maxRedoLogCapacity);
        if (recoveryChunkSize != DEFAULT_RECOVERY_CHUNK_SIZE)
            out.writeInt(recoveryChunkSize);
        if (recoveryThreadPoolSize != DEFAULT_RECOVERY_THREAD_POOL_SIZE)
            out.writeInt(recoveryThreadPoolSize);
        if (clusterName != null)
            out.writeObject(clusterName);
        if (_mirrorServiceConfig != null)
            out.writeObject(_mirrorServiceConfig);
        if (_asyncChannelShutdownTimeout != DEFAULT_ASYNC_CHANNEL_SHUTDOWN_TIMEOUT)
            out.writeLong(_asyncChannelShutdownTimeout);
        if (maxRedoLogMemoryCapacity != DEFAULT_MAX_REDO_LOG_CPACITY)
            out.writeLong(maxRedoLogMemoryCapacity);
        if (onRedoLogCapacityExceeded != DEFAULT_REDO_LOG_CAPACITY_EXCEEDED)
            out.writeObject(onRedoLogCapacityExceeded);
        if (onMissingPackets != DEFAULT_MISSING_PACKETS)
            out.writeObject(onMissingPackets);
        if (replicateFullTake != DEFAULT_REPL_FULL_TAKE)
            out.writeBoolean(replicateFullTake);
        if (maxRedoLogRecoveryCapacity != DEFAULT_MAX_REDO_LOG_CPACITY)
            out.writeLong(maxRedoLogRecoveryCapacity);
        if (processingType != DEFAULT_PROCESSING_TYPE)
            out.writeObject(processingType);
        if (multiBucketReplicationPolicy != null)
            out.writeObject(multiBucketReplicationPolicy);
        if (swapRedologPolicy != null)
            out.writeObject(swapRedologPolicy);
        if (conflictingOperationPolicy != ConflictingOperationPolicy.DEFAULT)
            out.writeObject(conflictingOperationPolicy);
        if (localViewMaxRedologCapacity != DEFAULT_LOCALVIEW_REDOLOG_CAPACITY)
            out.writeObject(localViewMaxRedologCapacity);
        if (reliableAsyncCompletionNotifierInterval != DEFAULT_RELIABLE_ASYNC_STATE_NOTIFY_INTERVAL)
            out.writeLong(reliableAsyncCompletionNotifierInterval);
        if (reliableAsyncCompletionNotifierPacketsThreshold != DEFAULT_RELIABLE_ASYNC_STATE_NOTIFY_PACKETS)
            out.writeLong(reliableAsyncCompletionNotifierPacketsThreshold);
        if (localViewMaxDisconnectionTime != DEFAULT_LOCALVIEW_MAX_DISCONNECTION_TIME)
            out.writeLong(localViewMaxDisconnectionTime);
        if (replicateOnePhaseCommit != DEFAULT_REPLICATE_ONE_PHASE_COMMIT)
            out.writeBoolean(replicateOnePhaseCommit);
        if (durableNotificationMaxRedologCapacity != DEFAULT_DURABLE_NOTIFICATION_REDOLOG_CAPACITY)
            out.writeLong(durableNotificationMaxRedologCapacity);
        if (durableNotificationMaxDisconnectionTime != DEFAULT_DURABLE_NOTIFICATION_MAX_DISCONNECTION_TIME)
            out.writeLong(durableNotificationMaxDisconnectionTime);
        if((flags & BitMap.BACKLOG_WEIGHT_POLICY) != 0){
                out.writeObject(backlogWeightPolicy);
        }
        if (version.greaterOrEquals(PlatformLogicalVersion.v9_7_0)) {
            if (localViewMaxRedologRecoveryCapacity == null || localViewMaxRedologRecoveryCapacity.longValue() != DEFAULT_LOCALVIEW_REDOLOG_RECOVERY_CAPACITY)
                out.writeObject(localViewMaxRedologRecoveryCapacity);
        }
    }

    private long buildFlags(PlatformLogicalVersion version) {
        long flags = 0;

        if (m_OwnMemberName != null)
            flags |= BitMap.OWN_MEMBER_NAME;
        if (m_ReplicationGroupName != null)
            flags |= BitMap.REPLICATION_GROUP_NAME;
        if (m_ReplicationGroupMembersNames != null)
            flags |= BitMap.REPLICATION_GROUP_MEMBERS_NAMES;
        if (m_ReplicationGroupMembersURLs != null)
            flags |= BitMap.REPLICATION_GROUP_MEMBERS_URLS;
        if (m_ReplicateNotifyTemplates)
            flags |= BitMap.REPLICATE_NOTIFY_TEMPLATES;
        if (m_TriggerNotifyTemplates)
            flags |= BitMap.TRIGGER_NOTIFY_TEMPLATES;
        if (m_ReplicateOriginalState)
            flags |= BitMap.REPLICATE_ORIGINAL_STATE;
        if (m_SyncOnCommit)
            flags |= BitMap.SYNC_ON_COMMIT;
        if (m_ReplicationMode != null)
            flags |= BitMap.REPLICATION_MODE;
        if (m_PolicyType != FULL_REPLICATION)
            flags |= BitMap.POLICY_TYPE;
        if (m_ReplicationChunkSize != DEFAULT_REPL_CHUNK_SIZE)
            flags |= BitMap.REPLICATION_CHUNK_SIZE;
        if (m_ReplicationIntervalMillis != DEFAULT_REPL_INTERVAL_MILLIS)
            flags |= BitMap.REPLICATION_INTERVAL_MILLIS;
        if (m_ReplicationIntervalOperations != DEFAULT_REPL_INTERVAL_OPERS)
            flags |= BitMap.REPLICATION_INTERVAL_OPERATIONS;
        if (m_SpaceFinderTimeout != DEFAULT_REPL_SPACE_FINDER_TIMEOUT)
            flags |= BitMap.SPACE_FINDER_TIMEOUT;
        if (m_SpaceFinderReportInterval != DEFAULT_REPL_SPACE_FINDER_REPORT_INTERVAL)
            flags |= BitMap.SPACE_FINDER_REPORT_INTERVAL;
        if (m_SyncOnCommitTimeout != DEFAULT_SYNC_ON_COMMIT_TIMEOUT)
            flags |= BitMap.SYNC_ON_COMMIT_TIMEOUT;
        if (_reliableAsyncRepl)
            flags |= BitMap.RELIABLE_ASYNC_REPL;
        if (permittedOperations != null)
            flags |= BitMap.PERMITTED_OPERATIONS;
        if (m_Recovery)
            flags |= BitMap.RECOVERY;
        if (m_ReplMemberPolicyDescTable != null)
            flags |= BitMap.REP_MEMBER_POLICY_DEC_TABLE;
        if (isOneWayReplication)
            flags |= BitMap.ONE_WAY_REPLICATION;
        if (m_IsSyncReplicationEnabled)
            flags |= BitMap.SYNC_REPLICATION_ENABLED;
        if (m_SyncReplPolicy != null)
            flags |= BitMap.SYNC_REPLICATION_POLICY;
        if (maxRedoLogCapacity != DEFAULT_MAX_REDO_LOG_CPACITY)
            flags |= BitMap.MAX_REDO_LOG_CAPACITY;
        if (recoveryChunkSize != DEFAULT_RECOVERY_CHUNK_SIZE)
            flags |= BitMap.RECOVERY_CHUNK_SIZE;
        if (recoveryThreadPoolSize != DEFAULT_RECOVERY_THREAD_POOL_SIZE)
            flags |= BitMap.RECOVERY_THREAD_POOL_SIZE;
        if (clusterName != null)
            flags |= BitMap.CLUSTER_NAME;
        if (_mirrorServiceConfig != null)
            flags |= BitMap.MIRROR_SERVICE_CONFIG;
        if (_asyncChannelShutdownTimeout != DEFAULT_ASYNC_CHANNEL_SHUTDOWN_TIMEOUT)
            flags |= BitMap.ASYNC_CHANNEL_SHUTDOWN_TIMEOUT;
        if (maxRedoLogMemoryCapacity != DEFAULT_MAX_REDO_LOG_CPACITY)
            flags |= BitMap.MAX_REDO_LOG_MEMORY_CAPACITY;
        if (onRedoLogCapacityExceeded != DEFAULT_REDO_LOG_CAPACITY_EXCEEDED)
            flags |= BitMap.ON_REDO_LOG_CAPACITY_EXCEEDED;
        if (onMissingPackets != DEFAULT_MISSING_PACKETS)
            flags |= BitMap.TOLERATE_MISSING_PACKETS;
        if (replicateFullTake != DEFAULT_REPL_FULL_TAKE)
            flags |= BitMap.REPL_FULL_TAKE;
        if (maxRedoLogRecoveryCapacity != DEFAULT_MAX_REDO_LOG_CPACITY)
            flags |= BitMap.MAX_REDO_LOG_RECOVERY_CAPACITY;
        if (processingType != DEFAULT_PROCESSING_TYPE)
            flags |= BitMap.PROCESSING_TYPE;
        if (multiBucketReplicationPolicy != null)
            flags |= BitMap.MULTI_BUCKET_REPLICATION_POLICY;
        if (swapRedologPolicy != null)
            flags |= BitMap.SWAP_REDOLOG_POLICY;
        if (conflictingOperationPolicy != ConflictingOperationPolicy.DEFAULT)
            flags |= BitMap.ON_CONFLICTING_PACKETS;
        if (replicateLeaseExpirations)
            flags |= BitMap.REPLICATE_LEASE_EXPIRATIONS;
        if (localViewMaxRedologCapacity != DEFAULT_LOCALVIEW_REDOLOG_CAPACITY)
            flags |= BitMap.LOCALVIEW_MAX_REDOLOG_CAPACITY;
        if (reliableAsyncCompletionNotifierInterval != DEFAULT_RELIABLE_ASYNC_STATE_NOTIFY_INTERVAL)
            flags |= BitMap.RELIABLE_ASYNC_COMPLETION_NOTIFIER_INTERVAL;
        if (reliableAsyncCompletionNotifierPacketsThreshold != DEFAULT_RELIABLE_ASYNC_STATE_NOTIFY_PACKETS)
            flags |= BitMap.RELIABLE_ASYNC_COMPLETION_NOTIFIER_PACKETS_THRESHOLD;
        if (localViewMaxDisconnectionTime != DEFAULT_LOCALVIEW_MAX_DISCONNECTION_TIME)
            flags |= BitMap.LOCALVIEW_MAX_DISCONNECTION_TIME;
        if (replicateOnePhaseCommit != DEFAULT_REPLICATE_ONE_PHASE_COMMIT)
            flags |= BitMap.REPLICATE_ONE_PHASE_COMMIT;
        if (durableNotificationMaxRedologCapacity != DEFAULT_DURABLE_NOTIFICATION_REDOLOG_CAPACITY)
            flags |= BitMap.NOTIFICATION_MAX_REDOLOG_CAPACITY;
        if (durableNotificationMaxDisconnectionTime != DEFAULT_DURABLE_NOTIFICATION_MAX_DISCONNECTION_TIME)
            flags |= BitMap.NOTIFICATION_MAX_DISCONNECTION_TIME;
        if (localViewMaxRedologRecoveryCapacity == null || localViewMaxRedologRecoveryCapacity.longValue() != DEFAULT_LOCALVIEW_REDOLOG_RECOVERY_CAPACITY)
            flags |= BitMap.LOCALVIEW_MAX_REDOLOG_RECOVERY_CAPACITY;
        if(!backlogWeightPolicy.equals(DEFAULT_BACKLOG_WEIGHT_POLICY) && version.greaterOrEquals(PlatformLogicalVersion.v12_1_0))
            flags |= BitMap.BACKLOG_WEIGHT_POLICY;
        return flags;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        long flags = in.readLong();
        if ((flags & BitMap.OWN_MEMBER_NAME) != 0) {
            m_OwnMemberName = (String) in.readObject();
        }
        if ((flags & BitMap.REPLICATION_GROUP_NAME) != 0) {
            m_ReplicationGroupName = (String) in.readObject();
        }
        if ((flags & BitMap.REPLICATION_GROUP_MEMBERS_NAMES) != 0) {
            int size = in.readInt();
            m_ReplicationGroupMembersNames = new ArrayList<String>(size);
            for (int i = 0; i < size; i++) {
                m_ReplicationGroupMembersNames.add((String) in.readObject());
            }
        }
        if ((flags & BitMap.REPLICATION_GROUP_MEMBERS_URLS) != 0) {
            int size = in.readInt();
            m_ReplicationGroupMembersURLs = new ArrayList<SpaceURL>(size);
            for (int i = 0; i < size; i++) {
                SpaceURL url = (SpaceURL) in.readObject();
                m_ReplicationGroupMembersURLs.add(url);
            }
        }
        m_ReplicateNotifyTemplates = ((flags & BitMap.REPLICATE_NOTIFY_TEMPLATES) != 0);
        m_TriggerNotifyTemplates = ((flags & BitMap.TRIGGER_NOTIFY_TEMPLATES) != 0);
        m_ReplicateOriginalState = ((flags & BitMap.REPLICATE_ORIGINAL_STATE) != 0);
        m_SyncOnCommit = ((flags & BitMap.SYNC_ON_COMMIT) != 0);
        if ((flags & BitMap.REPLICATION_MODE) != 0) {
            m_ReplicationMode = (String) in.readObject();
        }
        if ((flags & BitMap.POLICY_TYPE) != 0) {
            m_PolicyType = in.readInt();
        } else {
            m_PolicyType = FULL_REPLICATION;
        }
        if ((flags & BitMap.REPLICATION_CHUNK_SIZE) != 0) {
            m_ReplicationChunkSize = in.readInt();
        } else {
            m_ReplicationChunkSize = DEFAULT_REPL_CHUNK_SIZE;
        }
        if ((flags & BitMap.REPLICATION_INTERVAL_MILLIS) != 0) {
            m_ReplicationIntervalMillis = in.readLong();
        } else {
            m_ReplicationIntervalMillis = DEFAULT_REPL_INTERVAL_MILLIS;
        }
        if ((flags & BitMap.REPLICATION_INTERVAL_OPERATIONS) != 0) {
            m_ReplicationIntervalOperations = in.readInt();
        } else {
            m_ReplicationIntervalOperations = DEFAULT_REPL_INTERVAL_OPERS;
        }
        if ((flags & BitMap.SPACE_FINDER_TIMEOUT) != 0) {
            m_SpaceFinderTimeout = in.readLong();
        } else {
            m_SpaceFinderTimeout = DEFAULT_REPL_SPACE_FINDER_TIMEOUT;
        }
        if ((flags & BitMap.SPACE_FINDER_REPORT_INTERVAL) != 0) {
            m_SpaceFinderReportInterval = in.readLong();
        } else {
            m_SpaceFinderReportInterval = DEFAULT_REPL_SPACE_FINDER_REPORT_INTERVAL;
        }
        if ((flags & BitMap.SYNC_ON_COMMIT_TIMEOUT) != 0) {
            m_SyncOnCommitTimeout = in.readLong();
        } else {
            m_SyncOnCommitTimeout = DEFAULT_SYNC_ON_COMMIT_TIMEOUT;
        }
        _reliableAsyncRepl = ((flags & BitMap.RELIABLE_ASYNC_REPL) != 0);
        if ((flags & BitMap.PERMITTED_OPERATIONS) != 0) {
            permittedOperations = (Set<ReplicationOperationType>) in.readObject();
        }
        m_Recovery = ((flags & BitMap.RECOVERY) != 0);
        if ((flags & BitMap.REP_MEMBER_POLICY_DEC_TABLE) != 0) {
            int size = in.readInt();
            m_ReplMemberPolicyDescTable = new Hashtable<String, ReplicationPolicyDescription>();
            for (int i = 0; i < size; i++) {
                String key = (String) in.readObject();
                ReplicationPolicyDescription value = new ReplicationPolicyDescription();
                value.readExternal(in);
                m_ReplMemberPolicyDescTable.put(key, value);
            }
        }
        isOneWayReplication = ((flags & BitMap.ONE_WAY_REPLICATION) != 0);
        m_IsSyncReplicationEnabled = ((flags & BitMap.SYNC_REPLICATION_ENABLED) != 0);
        if ((flags & BitMap.SYNC_REPLICATION_POLICY) != 0) {
            m_SyncReplPolicy = (SyncReplPolicy) in.readObject();
        }
        if ((flags & BitMap.MAX_REDO_LOG_CAPACITY) != 0) {
            maxRedoLogCapacity = in.readLong();
        } else {
            maxRedoLogCapacity = DEFAULT_MAX_REDO_LOG_CPACITY;
        }
        if ((flags & BitMap.RECOVERY_CHUNK_SIZE) != 0) {
            recoveryChunkSize = in.readInt();
        } else {
            recoveryChunkSize = DEFAULT_RECOVERY_CHUNK_SIZE;
        }
        if ((flags & BitMap.RECOVERY_THREAD_POOL_SIZE) != 0) {
            recoveryThreadPoolSize = in.readInt();
        } else {
            recoveryThreadPoolSize = DEFAULT_RECOVERY_THREAD_POOL_SIZE;
        }
        if ((flags & BitMap.CLUSTER_NAME) != 0) {
            clusterName = (String) in.readObject();
        }
        if ((flags & BitMap.MIRROR_SERVICE_CONFIG) != 0) {
            _mirrorServiceConfig = (MirrorServiceConfig) in.readObject();
        }

        if ((flags & BitMap.ASYNC_CHANNEL_SHUTDOWN_TIMEOUT) != 0) {
            _asyncChannelShutdownTimeout = in.readLong();
        } else {
            _asyncChannelShutdownTimeout = DEFAULT_ASYNC_CHANNEL_SHUTDOWN_TIMEOUT;
        }
        if ((flags & BitMap.MAX_REDO_LOG_MEMORY_CAPACITY) != 0) {
            maxRedoLogMemoryCapacity = in.readLong();
        } else {
            maxRedoLogMemoryCapacity = DEFAULT_MAX_REDO_LOG_CPACITY;
        }
        if ((flags & BitMap.ON_REDO_LOG_CAPACITY_EXCEEDED) != 0) {
            onRedoLogCapacityExceeded = (RedoLogCapacityExceededPolicy) in.readObject();
        } else {
            onRedoLogCapacityExceeded = DEFAULT_REDO_LOG_CAPACITY_EXCEEDED;
        }
        if ((flags & BitMap.TOLERATE_MISSING_PACKETS) != 0) {
            MissingPacketsPolicy policy = (MissingPacketsPolicy) in.readObject();
            setOnMissingPackets(policy);
        } else {
            setOnMissingPackets(DEFAULT_MISSING_PACKETS);
        }
        if ((flags & BitMap.REPL_FULL_TAKE) != 0) {
            replicateFullTake = in.readBoolean();
        } else {
            replicateFullTake = DEFAULT_REPL_FULL_TAKE;
        }

        if ((flags & BitMap.MAX_REDO_LOG_RECOVERY_CAPACITY) != 0) {
            maxRedoLogRecoveryCapacity = in.readLong();
        } else {
            maxRedoLogRecoveryCapacity = DEFAULT_MAX_REDO_LOG_CPACITY;
        }

        if ((flags & BitMap.PROCESSING_TYPE) != 0) {
            processingType = (ReplicationProcessingType) in.readObject();
        } else {
            processingType = DEFAULT_PROCESSING_TYPE;
        }

        if ((flags & BitMap.MULTI_BUCKET_REPLICATION_POLICY) != 0) {
            multiBucketReplicationPolicy = (MultiBucketReplicationPolicy) in.readObject();
        }

        if ((flags & BitMap.SWAP_REDOLOG_POLICY) != 0) {
            swapRedologPolicy = (SwapBacklogConfig) in.readObject();
        }

        if ((flags & BitMap.ON_CONFLICTING_PACKETS) != 0) {
            conflictingOperationPolicy = (ConflictingOperationPolicy) in.readObject();
        } else {
            conflictingOperationPolicy = ConflictingOperationPolicy.DEFAULT;
        }
        if ((flags & BitMap.RESERVED_BACKWARDS) != 0) {
            throw new IllegalStateException("Unsupported object in this context: " + BitMap.RESERVED_BACKWARDS);
        }
        replicateLeaseExpirations = ((flags & BitMap.REPLICATE_LEASE_EXPIRATIONS) != 0);
        if ((flags & BitMap.LOCALVIEW_MAX_REDOLOG_CAPACITY) != 0) {
            localViewMaxRedologCapacity = (Long) in.readObject();
        } else {
            localViewMaxRedologCapacity = DEFAULT_LOCALVIEW_REDOLOG_CAPACITY;
        }
        if ((flags & BitMap.RELIABLE_ASYNC_COMPLETION_NOTIFIER_INTERVAL) != 0) {
            reliableAsyncCompletionNotifierInterval = in.readLong();
        }
        if ((flags & BitMap.RELIABLE_ASYNC_COMPLETION_NOTIFIER_PACKETS_THRESHOLD) != 0) {
            reliableAsyncCompletionNotifierPacketsThreshold = in.readLong();
        }
        if ((flags & BitMap.LOCALVIEW_MAX_DISCONNECTION_TIME) != 0) {
            localViewMaxDisconnectionTime = in.readLong();
        }
        if ((flags & BitMap.REPLICATE_ONE_PHASE_COMMIT) != 0) {
            replicateOnePhaseCommit = in.readBoolean();
        } else {
            replicateOnePhaseCommit = DEFAULT_REPLICATE_ONE_PHASE_COMMIT;
        }
        if ((flags & BitMap.NOTIFICATION_MAX_REDOLOG_CAPACITY) != 0) {
            durableNotificationMaxRedologCapacity = in.readLong();
        }
        if ((flags & BitMap.NOTIFICATION_MAX_DISCONNECTION_TIME) != 0) {
            durableNotificationMaxDisconnectionTime = in.readLong();
        }
        if ((flags & BitMap.BACKLOG_WEIGHT_POLICY) != 0) {
            backlogWeightPolicy = (String) in.readObject();
        }
        if ((flags & BitMap.LOCALVIEW_MAX_REDOLOG_RECOVERY_CAPACITY) != 0) {
            localViewMaxRedologRecoveryCapacity = (Long) in.readObject();
        } else {
            localViewMaxRedologRecoveryCapacity = DEFAULT_LOCALVIEW_REDOLOG_RECOVERY_CAPACITY;
        }
    }

    /**
     * Just for Externalizable.
     */
    public ReplicationPolicy() {
        m_ReplicationGroupName = null;
        m_ReplicationGroupMembersNames = null;
        m_ReplicationGroupMembersURLs = null;
        m_ReplMemberPolicyDescTable = null;
        m_SyncReplPolicy = null;
        multiBucketReplicationPolicy = null;
        swapRedologPolicy = null;
        clusterName = null;
    }

    // constructor

    public ReplicationPolicy(String clusterName, String replicationGroupName, List<String> replicationGroupMembersNames,
                             List<SpaceURL> replicationGroupMembersURLs, String ownMemberName,
                             Hashtable<String, ReplicationPolicyDescription> replMemberPolicyDescTable, SyncReplPolicy syncReplPolicy, MultiBucketReplicationPolicy multiBucketReplicationPolicy, SwapBacklogConfig swapRedologPolicy) {
        this.clusterName = clusterName;
        m_ReplicationGroupName = replicationGroupName;
        m_ReplicationGroupMembersNames = replicationGroupMembersNames;
        m_ReplicationGroupMembersURLs = replicationGroupMembersURLs;
        m_OwnMemberName = ownMemberName;
        m_ReplMemberPolicyDescTable = replMemberPolicyDescTable;
        m_SyncReplPolicy = syncReplPolicy;
        this.multiBucketReplicationPolicy = multiBucketReplicationPolicy;
        this.swapRedologPolicy = swapRedologPolicy;
    }
    public void setMirrorServiceConfig(MirrorServiceConfig mirrorServiceConfig) {
        _mirrorServiceConfig = mirrorServiceConfig;
    }

    /**
     * @return <code>null</code> if the mirror-service disabled
     */
    public MirrorServiceConfig getMirrorServiceConfig() {
        return _mirrorServiceConfig;
    }

    /**
     * @return if the mirror service enabled for entire replication group
     */
    public boolean isMirrorServiceEnabled() {
        return _mirrorServiceConfig != null;
    }


    /**
     * @return the async-replication 'reliable' indicator; default false
     */
    public boolean isReliableAsyncRepl() {
        return _reliableAsyncRepl;
    }

    public boolean shouldReplicate(ReplicationOperationType operType) {
        return permittedOperations.contains(operType);
    }

    public void setPermittedOperations(List<ReplicationOperationType> permittedOperations) {
        if (permittedOperations.size() == ReplicationOperationType.getFullPermissions().size()) {
            this.permittedOperations = ReplicationOperationType.getFullPermissions();
        } else {
            this.permittedOperations.addAll(permittedOperations);
        }
    }

    /**
     * Sets the async-replication 'reliable' indicator; default false
     *
     * @param reliableAsyncRepl true if reliable-async-replication is enabled; false otherwise.
     */
    public void setReliableAsyncRepl(boolean reliableAsyncRepl) {
        this._reliableAsyncRepl = reliableAsyncRepl;
    }

    public void setMaxRedoLogCapacity(long capacity) {
        maxRedoLogCapacity = capacity;
    }

    public long getMaxRedoLogCapacity() {
        return maxRedoLogCapacity;
    }

    public void setMaxRedoLogMemoryCapacity(long capacity) {
        maxRedoLogMemoryCapacity = capacity;
    }

    public void setBacklogWeightPolicy(String backlogWeightPolicy) {
        this.backlogWeightPolicy = backlogWeightPolicy;
    }

    public long getMaxRedoLogMemoryCapacity() {
        return maxRedoLogMemoryCapacity;
    }

    public String getBacklogWeightPolicy() {
        return backlogWeightPolicy;
    }

    public long getMaxRedoLogRecoveryCapacity() {
        return maxRedoLogRecoveryCapacity;
    }

    public void setMaxRedoLogRecoveryCapacity(long capacity) {
        maxRedoLogRecoveryCapacity = capacity;
    }

    public void setLocalViewMaxRedologCapacity(Long localViewMaxRedologCapacity) {
        this.localViewMaxRedologCapacity = localViewMaxRedologCapacity;
    }

    public long getLocalViewMaxRedologCapacity() {
        if (localViewMaxRedologCapacity != null)
            return localViewMaxRedologCapacity.longValue();

        return maxRedoLogMemoryCapacity;
    }

    public void setLocalViewMaxDisconnectionTime(
            long localViewMaxDisconnectionTime) {
        this.localViewMaxDisconnectionTime = localViewMaxDisconnectionTime;
    }

    public long getLocalViewMaxDisconnectionTime() {
        return localViewMaxDisconnectionTime;
    }

    public void setLocalViewMaxRedologRecoveryCapacity(
            Long localViewMaxRedologRecoveryCapacity) {
        this.localViewMaxRedologRecoveryCapacity = localViewMaxRedologRecoveryCapacity;
    }

    public long getLocalViewMaxRedologRecoveryCapacity() {
        if (localViewMaxRedologRecoveryCapacity != null)
            return localViewMaxRedologRecoveryCapacity.longValue();

        return maxRedoLogMemoryCapacity;
    }

    public void setOnRedoLogCapacityExceeded(
            RedoLogCapacityExceededPolicy policy) {
        onRedoLogCapacityExceeded = policy;
    }

    public RedoLogCapacityExceededPolicy getOnRedoLogCapacityExceeded() {
        return onRedoLogCapacityExceeded;
    }

    /**
     * returns <code>true</code> if this member has sync-replication to at least one target.
     */
    public boolean isOwnerMemberHasSyncReplication() {
        boolean isSyncReplEnabled = m_IsSyncReplicationEnabled;

        if (!isSyncReplEnabled) {
            ReplicationPolicy.ReplicationPolicyDescription rp = m_ReplMemberPolicyDescTable.get(m_OwnMemberName);
            if (rp != null && rp.replTransmissionPolicies != null) {
                for (ReplicationTransmissionPolicy rtp : rp.replTransmissionPolicies) {
                    if (!rtp.m_DisableTransmission && rtp.isSyncReplication()) {
                        isSyncReplEnabled = true;
                        break;
                    }
                }
            }
        }

        return isSyncReplEnabled;
    }

    public void setPrimarySpaceSelector(LeaderSelector primarySpaceSelector) {
        _primarySpaceSelector = primarySpaceSelector;
    }

    public LeaderSelector getPrimarySpaceSelector() {
        return _primarySpaceSelector;
    }

    /**
     * Finds all the recovery targets in cluster. The targets are ordered according to their
     * reliability. First all primary targets are added. Then the sync targets, and only then all
     * the others.
     *
     * @param excludeTargets targets that should be excluded from search
     * @return a list of recoverable members SpaceURLs. returns empty list if recovery is disabled.
     */
    public List<SpaceURL> getRecoverableTargets(List<String> excludeTargets) {
        LinkedList<SpaceURL> recoverableTargets = new LinkedList<SpaceURL>();

        // First get all primary spaces in the replication group
        // (even the spaces that are in a different fail over group)

        if (_primarySpaceSelector != null) {
            List<SpaceURL> primaryTargets = getPrimaryTargets(excludeTargets);

            recoverableTargets.addAll(primaryTargets);

        }
        // Add all sync targets in replication group
        List<SpaceURL> syncTargets = getSyncTargets(excludeTargets);

        recoverableTargets.addAll(syncTargets);

        // Finally add all possible targets in replication group
        List<SpaceURL> allTargets = getAllTargets(excludeTargets);

        recoverableTargets.addAll(allTargets);

        return recoverableTargets;
    }

    /**
     * Get all valid targets in space replication group.
     *
     * If jini urls are used, the first target will always be multicast url. only if it fails the
     * unicast and rmi urls will be used.
     */
    private LinkedList<SpaceURL> getAllTargets(List<String> excludeTargets) {
        boolean foundJini = false;
        SpaceURL multicastUrl = null;

        ReplicationPolicyDescription rd = m_ReplMemberPolicyDescTable.get(m_OwnMemberName);

        LinkedList<SpaceURL> allTargets = new LinkedList<SpaceURL>();


        for (int i = 0; i < m_ReplicationGroupMembersNames.size(); i++) {
            String memberName = m_ReplicationGroupMembersNames.get(i);
            if (memberName.equals(m_OwnMemberName))
                continue;

            if (excludeTargets.contains(memberName))
                continue;

			/* must clone to prevent direct reference change */
            SpaceURL remoteSpaceURL = (SpaceURL) m_ReplicationGroupMembersURLs.get(i)
                    .clone();

            /** not <code>null</code> if defined specific source memory for recovery */
            if (rd.sourceMemberRecovery != null) {
                if (!rd.sourceMemberRecovery.equalsIgnoreCase(memberName))
                    continue; // only specific space to upload from
                else {
                    /** returns entrySet with specific sourceMemeber */
                    allTargets.add(remoteSpaceURL);
                    return allTargets;
                }
            } else {
                /** if at least one jini url found, we do memory-recovery by cluster-name & replication-group attributes */
                if (remoteSpaceURL.isJiniProtocol()) {

                    //use multicast only if exclude list is empty
                    if (!foundJini && excludeTargets.isEmpty()) {
                        // If jini was used.
                        // if so, add the multicast url to the head of list
                        // for lookup optimization
                        foundJini = true;
                        multicastUrl = (SpaceURL) remoteSpaceURL.clone();
                        multicastUrl.setPropertiesPrefix(SpaceURL.JINI_PROTOCOL,
                                SpaceURL.ANY,
                                SpaceURL.ANY,
                                SpaceURL.ANY);
                        multicastUrl.setProperty(SpaceURL.CLUSTER_NAME, clusterName);
                        multicastUrl.setProperty(SpaceURL.CLUSTER_GROUP,
                                m_ReplicationGroupName);
                        multicastUrl.setProperty(SpaceURL.STATE,
                                JSpaceState.convertToString(ISpaceState.STARTED));

                        // if no reliable targets, add as first in list;
                        // otherwise add after reliable targets but before any other URL
                        allTargets.addFirst(multicastUrl);
                    }

                    // Only if this is a unicast url - add it
                    if (remoteSpaceURL.getProperty(SpaceURL.HOST_NAME) != null
                            && !SpaceURL.ANY.equals(remoteSpaceURL.getProperty(SpaceURL.HOST_NAME))) {

                        remoteSpaceURL.setProperty(SpaceURL.CLUSTER_NAME, clusterName);
                        remoteSpaceURL.setProperty(SpaceURL.CLUSTER_GROUP,
                                m_ReplicationGroupName);
                        remoteSpaceURL.setProperty(SpaceURL.STATE,
                                JSpaceState.convertToString(ISpaceState.STARTED));

                        /** keep target member-name with space-url */
                        allTargets.add(remoteSpaceURL);
                    }
                }// if jini://
                else {
                    /** if we got here we don't have specific source-recovery member and no jini:// url found */
                    allTargets.add(remoteSpaceURL);
                }
            }

        }

        return allTargets;
    }

    /**
     * Returns a list of URLs or an empty list of reliable targets to recover from. A 'reliable'
     * target is one that replicates synchronously to 'this' source. <p> This differs from {@link
     * #getRecoverableTargets(java.util.List)} in that: <br> 1. all URLs are returned and there is
     * no multicast URL - We need specific members. <br> 2. The target is chosen only if it
     * sync-replicates to 'this', in contrast to returning a target which 'this' replicates to.
     *
     * @return a list of URLs or an empty list of reliable targets to recover from. Never returns
     * null.
     */
    public List<SpaceURL> getSyncTargets(List<String> excludeTargets) {
        LinkedList<SpaceURL> reliableTargets = new LinkedList<SpaceURL>();

        for (int indexOfMemberInList = 0; indexOfMemberInList < m_ReplicationGroupMembersNames.size(); ++indexOfMemberInList) {
            String transmissionSource = m_ReplicationGroupMembersNames.get(indexOfMemberInList);
            if (transmissionSource.equals(m_OwnMemberName))
                continue; //skip transmission to self

            if (excludeTargets.contains(transmissionSource))
                continue;

            //extract sync source which replicates to 'this' (as target)
            ReplicationPolicyDescription transmissionDescriptor = m_ReplMemberPolicyDescTable.get(transmissionSource);
            if (transmissionDescriptor == null)
                continue; //skip if no transmission defined
            ReplicationTransmissionPolicy transmissionMatrix = transmissionDescriptor.getTargetTransmissionMatrix(m_OwnMemberName);


            // Add this target to the list of reliable targets to recover from -
            // if exists a sync replicated transmission from source to 'this' target
            // OR
            // if no transmission defined but full sync replication is defined
            if ((transmissionMatrix != null && transmissionMatrix.isSyncReplication())
                    || (transmissionMatrix == null && m_IsSyncReplicationEnabled)) {
                // extract spaceURL (clone to prevent direct reference change)
                SpaceURL remoteSpaceURL = (SpaceURL) m_ReplicationGroupMembersURLs.get(indexOfMemberInList).clone();
                reliableTargets.add(remoteSpaceURL);
            }
        }

        return reliableTargets;
    }

    /**
     * Returns a list of URLs or an empty list of primary  targets .
     *
     * @return a list of URLs or an empty list of primary targets. Never returns null.
     */
    public List<SpaceURL> getPrimaryTargets(List<String> excludeTargets) {
        LinkedList<SpaceURL> primaryTargets = new LinkedList<SpaceURL>();

        for (int indexOfMemberInList = 0; indexOfMemberInList < m_ReplicationGroupMembersNames.size(); ++indexOfMemberInList) {
            String transmissionSource = m_ReplicationGroupMembersNames.get(indexOfMemberInList);
            if (transmissionSource.equals(m_OwnMemberName))
                continue; //skip transmission to self

            if (excludeTargets.contains(transmissionSource))
                continue;


            //extract sync source which replicates to 'this' (as target)
            ReplicationPolicyDescription transmissionDescriptor = m_ReplMemberPolicyDescTable.get(transmissionSource);
            if (transmissionDescriptor == null)
                continue; //skip if no transmission defined
            ReplicationTransmissionPolicy transmissionMatrix = transmissionDescriptor.getTargetTransmissionMatrix(m_OwnMemberName);


            // Add this target to the list of reliable targets to recover from -
            // if exists a sync replicated transmission from source to 'this' target
            // OR
            // if no transmission defined but full sync replication is defined
            if ((transmissionMatrix != null && transmissionMatrix.isSyncReplication())
                    || (transmissionMatrix == null && m_IsSyncReplicationEnabled)) {
                // extract spaceURL (clone to prevent direct reference change)
                SpaceURL remoteSpaceURL = (SpaceURL) m_ReplicationGroupMembersURLs.get(indexOfMemberInList)
                        .clone();

                remoteSpaceURL.setElectionState(ActiveElectionState.State.ACTIVE.name());

                primaryTargets.add(remoteSpaceURL);
            }

        }

        return primaryTargets;
    }


    /**
     * Replication policy description for every member.
     */
    public static class ReplicationPolicyDescription implements Externalizable {

        private static final long serialVersionUID = 2L;
        // transmission policy matrix for this member value = ReplicationTransmissionPolicy

        public List<ReplicationTransmissionPolicy> replTransmissionPolicies;  // A list of replicationTransmissionPolicy objects
        public String sourceMemberRecovery; // Source member name for Memory/DB recovery
        public boolean memberRecovery; // overwrite to the group recovery parameter
        // replication filters (input and output) for this member

        public String inputReplicationFilterClassName;
        public String inputReplicationFilterParamUrl;
        public String outputReplicationFilterClassName;
        public String outputReplicationFilterParamUrl;
        public boolean activeWhenBackup = true;
        public boolean shutdownSpaceOnInitFailure;
        private interface BitMap {

            int POLICIES = 1 << 0;
            int SOURCE_MEMBER_RECOVERY = 1 << 1;
            int INPUT_REPLICATION_FILTER_CLASSNAME = 1 << 2;
            int INPUT_REPLICATION_FILTER_PARAM_URL = 1 << 3;
            int OUTPUT_REPLICATION_FILTER_CLASSNAME = 1 << 4;
            int OUTPUT_REPLICATION_FILTER_PARAM_URL = 1 << 5;
            int MEMBER_RECOVERY = 1 << 6;
            int ACTIVE_WHEN_BACKUP = 1 << 7;
            int SHUTDOWN_SPACE_ON_INIT_FAILURE = 1 << 8;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            // build flags
            int flags = 0;
            if (sourceMemberRecovery != null) {
                flags |= BitMap.SOURCE_MEMBER_RECOVERY;
            }
            if (inputReplicationFilterClassName != null) {
                flags |= BitMap.INPUT_REPLICATION_FILTER_CLASSNAME;
            }
            if (inputReplicationFilterParamUrl != null) {
                flags |= BitMap.INPUT_REPLICATION_FILTER_PARAM_URL;
            }
            if (outputReplicationFilterClassName != null) {
                flags |= BitMap.OUTPUT_REPLICATION_FILTER_CLASSNAME;
            }
            if (outputReplicationFilterParamUrl != null) {
                flags |= BitMap.OUTPUT_REPLICATION_FILTER_PARAM_URL;
            }
            if (memberRecovery) {
                flags |= BitMap.MEMBER_RECOVERY;
            }
            if (activeWhenBackup) {
                flags |= BitMap.ACTIVE_WHEN_BACKUP;
            }
            if (shutdownSpaceOnInitFailure) {
                flags |= BitMap.SHUTDOWN_SPACE_ON_INIT_FAILURE;
            }
            out.writeInt(flags);
            if (sourceMemberRecovery != null) {
                out.writeUTF(sourceMemberRecovery);
            }
            if (inputReplicationFilterClassName != null) {
                out.writeUTF(inputReplicationFilterClassName);
            }
            if (inputReplicationFilterParamUrl != null) {
                out.writeUTF(inputReplicationFilterParamUrl);
            }
            if (outputReplicationFilterClassName != null) {
                out.writeUTF(outputReplicationFilterClassName);
            }
            if (outputReplicationFilterParamUrl != null) {
                out.writeUTF(outputReplicationFilterParamUrl);
            }
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            int flags = in.readInt();
            if ((flags & BitMap.POLICIES) != 0) {
                int size = in.readInt();
                replTransmissionPolicies = new ArrayList<ReplicationTransmissionPolicy>(size);
                for (int i = 0; i < size; i++) {
                    ReplicationTransmissionPolicy policy = new ReplicationTransmissionPolicy();
                    policy.readExternal(in);
                    replTransmissionPolicies.add(policy);
                }
            }
            if ((flags & BitMap.SOURCE_MEMBER_RECOVERY) != 0) {
                sourceMemberRecovery = in.readUTF();
            }
            if ((flags & BitMap.INPUT_REPLICATION_FILTER_CLASSNAME) != 0) {
                inputReplicationFilterClassName = in.readUTF();
            }
            if ((flags & BitMap.INPUT_REPLICATION_FILTER_PARAM_URL) != 0) {
                inputReplicationFilterParamUrl = in.readUTF();
            }
            if ((flags & BitMap.OUTPUT_REPLICATION_FILTER_CLASSNAME) != 0) {
                outputReplicationFilterClassName = in.readUTF();
            }
            if ((flags & BitMap.OUTPUT_REPLICATION_FILTER_PARAM_URL) != 0) {
                outputReplicationFilterParamUrl = in.readUTF();
            }
            memberRecovery = ((flags & BitMap.MEMBER_RECOVERY) != 0);
            activeWhenBackup = ((flags & BitMap.ACTIVE_WHEN_BACKUP) != 0);
            shutdownSpaceOnInitFailure = ((flags & BitMap.SHUTDOWN_SPACE_ON_INIT_FAILURE) != 0);
        }

        /**
         * Extracts the transmission policy from this source to the requested target. Note the
         * ambiguity: returns null if no transmission policy is defined or if the parameter is not a
         * replicated target by this source member.
         *
         * @param targetMember the replicated target to extract the transmission policy for.
         * @return null if not a replicated target from this source or if no transmission policy
         * defined.
         */
        public ReplicationTransmissionPolicy getTargetTransmissionMatrix(String targetMember) {
            if (!transmissionPolicyDefined())
                return null;

            for (ReplicationTransmissionPolicy targetTransMatrix : replTransmissionPolicies) {
                if (targetTransMatrix.m_TargetSpace.equals(targetMember))
                    return targetTransMatrix;
            }

            return null;
        }

        /**
         * If transmission policy was defined in cluster xml, then <code>true</code> is returned;
         * otherwise <code>false</code> indicating that there is no transmission policy.
         *
         * @return <code>true</code> if transmission policy was defined; <code>false</code>
         * otherwise.
         */
        public boolean transmissionPolicyDefined() {
            return replTransmissionPolicies != null;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\n ReplTransmissionPolicies -\t" + replTransmissionPolicies);
            sb.append("\n Source Member Recovery -\t" + sourceMemberRecovery);
            sb.append("\n Member Recovery -\t" + memberRecovery);
            sb.append("\n Input Replication Filter Class Name -\t" + inputReplicationFilterClassName);
            sb.append("\n Input Replication Filter Param Url -\t" + inputReplicationFilterParamUrl);
            sb.append("\n Output Replication Filter Class Name -\t" + outputReplicationFilterClassName);
            sb.append("\n Output Replication Filter Param Url -\t" + outputReplicationFilterParamUrl);
            sb.append("\n Active when backup -\t" + activeWhenBackup);
            sb.append("\n Shutdown space on init failure -\t" + shutdownSpaceOnInitFailure);

            return sb.toString();
        }

    }/*end ReplicationPolicyDescription class */
    /**
     * This method returns the relative position of the specified space member in the replication
     * group, or -1 if the space member is not found.
     *
     * Positions are counted from 0 to #nodes - 1.
     *
     * The position can be used as a unique identifier of the space within its group.
     */
    public int getSpaceMemberPosition(String spaceMemberName) {
        int pos = 0;

        for (Iterator iter = m_ReplicationGroupMembersNames.iterator();
             iter.hasNext(); ) {
            if (iter.next().equals(spaceMemberName)) {
                return pos;
            }

            pos++;
        }

        return -1;
    }

    final public int getRecoveryChunkSize() {
        return recoveryChunkSize;
    }

    public void setRecoveryChunkSize(int recoveryChunkSize) {
        this.recoveryChunkSize = recoveryChunkSize;
    }

    final public boolean isReplicateOriginalState() {
        return m_IsSyncReplicationEnabled || m_ReplicateOriginalState;
    }

    public void setReplicatedOriginalState(boolean isReplOriginalState) {
        m_ReplicateOriginalState = isReplOriginalState;
    }

    public ConflictingOperationPolicy getConflictingOperationPolicy() {
        return conflictingOperationPolicy;
    }

    public void setConflictingOperationPolicy(
            ConflictingOperationPolicy conflictingOperationPolicy) {
        this.conflictingOperationPolicy = conflictingOperationPolicy;
    }

    public String toString() {
        StringBuffer strBuffer = new StringBuffer();
        strBuffer.append("\n------------ Replication Policy ---------\n");
        strBuffer.append("Member Name -\t" + m_OwnMemberName + "\n");
        strBuffer.append("Replication Group Name -\t" + m_ReplicationGroupName + "\n");
        strBuffer.append("Replication Group Members Name List -\t" + m_ReplicationGroupMembersNames + "\n");
        strBuffer.append("Replication Group Members URL -\t" + m_ReplicationGroupMembersURLs + "\n");
        strBuffer.append("Is Replicate Notify Templates -\t" + m_ReplicateNotifyTemplates + "\n");
        strBuffer.append("Is Trigger Notify Templates -\t" + m_TriggerNotifyTemplates + "\n");
        strBuffer.append("Is Replicate Original State -\t" + m_ReplicateOriginalState + "\n");
        strBuffer.append("Is Sync On Commit -\t" + m_SyncOnCommit + "\n");
        strBuffer.append("Is Replicate Lease Expirations -\t" + replicateLeaseExpirations + "\n");
        strBuffer.append("Replication Mode -\t" + m_ReplicationMode + "\n");
        strBuffer.append("Policy Type -\t");
        if (m_PolicyType == FULL_REPLICATION)
            strBuffer.append("Full Replication\n");
        else if (m_PolicyType == PARTIAL_REPLICATION)
            strBuffer.append("Partial Replication\n");
        else
            strBuffer.append("UNKNOWN\n");

        strBuffer.append("Replication Chunk Size -\t" + m_ReplicationChunkSize + "\n");
        strBuffer.append("Replication Interval Millis -\t" + m_ReplicationIntervalMillis + "\n");
        strBuffer.append("Replication Interval Operations -\t" + m_ReplicationIntervalOperations + "\n");
        strBuffer.append("Space Finder Timeout -\t" + m_SpaceFinderTimeout + "\n");
        strBuffer.append("SpaceFinder Report Interval -\t" + m_SpaceFinderReportInterval + "\n");
        strBuffer.append("Sync On Commit Timeout -\t" + m_SyncOnCommitTimeout + "\n");
        strBuffer.append("Is recovery -\t" + m_Recovery + "\n");
        strBuffer.append("Cluster Name -\t" + clusterName + "\n");
        strBuffer.append("Max Redo Log Capacity -\t" + maxRedoLogCapacity + "\n");
        strBuffer.append("Max Redo Log Memory Capacity -\t" + maxRedoLogMemoryCapacity + "\n");
        strBuffer.append("On Redo Log Capacity Exceeded -\t" + onRedoLogCapacityExceeded + "\n");
        strBuffer.append("Recovery Chunk Size -\t" + recoveryChunkSize + "\n");
        strBuffer.append("Is One Way Replication -\t" + isOneWayReplication + "\n");
        strBuffer.append("Is SyncReplication Enabled -\t" + m_IsSyncReplicationEnabled + "\n");
        strBuffer.append("Sync Replication Policy -\n\t" + m_SyncReplPolicy + "\n");
        strBuffer.append("Replication Policy Desc Table -\n\t" + m_ReplMemberPolicyDescTable + "\n");
        strBuffer.append("Mirror Service Config -\t" + _mirrorServiceConfig + "\n");
        strBuffer.append("Reliable async replication -\t" + _reliableAsyncRepl + "\n");
        strBuffer.append("Async replication shutdown timeout-\t" + _asyncChannelShutdownTimeout + "\n");

        return strBuffer.toString();
    }

    public int getRecoveryThreadPoolSize() {
        return recoveryThreadPoolSize;
    }

    public void setRecoveryThreadPoolSize(int recoveryThreadPoolSize) {
        this.recoveryThreadPoolSize = recoveryThreadPoolSize;
    }

    public long getAsyncChannelShutdownTimeout() {
        return _asyncChannelShutdownTimeout;
    }

    public void setAsyncChannelShutdownTimeout(long shutdownTimeout) {
        _asyncChannelShutdownTimeout = shutdownTimeout;
    }

    public void setOnMissingPackets(MissingPacketsPolicy onMissingPackets) {
        this.onMissingPackets = onMissingPackets;
    }

    public MissingPacketsPolicy getOnMissingPackets() {
        return onMissingPackets;
    }

    public void setReplicateFullTake(boolean replicateFullTake) {
        this.replicateFullTake = replicateFullTake;
    }

    public boolean isReplicateFullTake() {
        return replicateFullTake;
    }

    public SpaceURL getSpaceReplicationUrl(String fullSpaceName) {
        int spaceMemberPosition = getSpaceMemberPosition(fullSpaceName);
        return m_ReplicationGroupMembersURLs.get(spaceMemberPosition);

    }

    public ReplicationProcessingType getProcessingType() {
        //Override for simplicity change of default during testing, should be removed
        if (Boolean.getBoolean("com.gs.replication.module.concurrent"))
            return ReplicationProcessingType.MULTIPLE_BUCKETS;
        if (Boolean.getBoolean("com.gs.replication.module.gloablorder"))
            return ReplicationProcessingType.GLOBAL_ORDER;
        if (Boolean.getBoolean("com.gs.replication.module.multisource"))
            return ReplicationProcessingType.MULTIPLE_SOURCES;
        return processingType;
    }

    public void setProcessingType(ReplicationProcessingType processingType) {
        this.processingType = processingType;
    }

    public MultiBucketReplicationPolicy getMultiBucketReplicationPolicy() {
        return multiBucketReplicationPolicy;
    }

    public SwapBacklogConfig getSwapRedologPolicy() {
        return swapRedologPolicy;
    }

    public boolean isFullReplication() {
        return m_PolicyType == FULL_REPLICATION;
    }

    public boolean isReplicateLeaseExpirations() {
        return replicateLeaseExpirations;
    }

    public void setReplicateLeaseExpirations(boolean value) {
        replicateLeaseExpirations = value;
    }

    public int getReplicationTargetsCount() {
        if (m_ReplicationGroupMembersNames == null)
            return 0;

        boolean containSelf = m_ReplicationGroupMembersNames.contains(m_OwnMemberName);
        return m_ReplicationGroupMembersNames.size() - (containSelf ? 1 : 0);
    }

    public void setReliableAsyncCompletionNotifierInterval(
            long reliableAsyncCompletionNotifierInterval) {
        this.reliableAsyncCompletionNotifierInterval = reliableAsyncCompletionNotifierInterval;
    }

    public long getReliableAsyncCompletionNotifierInterval() {
        return reliableAsyncCompletionNotifierInterval;
    }

    public long getReliableAsyncCompletionNotifierPacketsThreshold() {
        return reliableAsyncCompletionNotifierPacketsThreshold;
    }

    public void setReliableAsyncCompletionNotifierPacketsThreshold(
            long reliableAsyncCompletionNotifierPacketsThreshold) {
        this.reliableAsyncCompletionNotifierPacketsThreshold = reliableAsyncCompletionNotifierPacketsThreshold;
    }

    public boolean isReplicateOnePhaseCommit() {
        return replicateOnePhaseCommit;
    }

    public void setReplicateOnePhaseCommit(boolean replicateOnePhaseCommit) {
        this.replicateOnePhaseCommit = replicateOnePhaseCommit;
    }

    public long getDurableNotificationMaxRedologCapacity() {
        return durableNotificationMaxRedologCapacity;
    }

    public void setDurableNotificationMaxRedologCapacity(
            Long notificationMaxRedologCapacity) {
        this.durableNotificationMaxRedologCapacity = notificationMaxRedologCapacity;
    }

    public long getDurableNotificationMaxDisconnectionTime() {
        return durableNotificationMaxDisconnectionTime;
    }

    public void setDurableNotificationMaxDisconnectionTime(
            long notificationMaxDisconnectionTime) {
        this.durableNotificationMaxDisconnectionTime = notificationMaxDisconnectionTime;
    }

    public void setConnectionMonitorThreadPoolSize(int connectionMonitorThreadPoolSize) {
        this.connectionMonitorThreadPoolSize = connectionMonitorThreadPoolSize;
    }

    public int getConnectionMonitorThreadPoolSize() {
        return connectionMonitorThreadPoolSize;
    }

    public boolean isNetworkCompressionEnabled() {
        return m_ReplicationNetworkCompression;
    }
}