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

package com.gigaspaces.internal.cluster;

import com.gigaspaces.cluster.replication.MirrorServiceConfig;
import com.gigaspaces.internal.extension.CustomSerializer;
import com.gigaspaces.internal.extension.XapExtensions;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageConfig;
import com.gigaspaces.internal.server.space.tiered_storage.TieredStorageTableConfig;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.server.SpaceCustomComponent;
import com.j_spaces.core.Constants;
import com.j_spaces.core.JSpaceAttributes;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.cluster.ClusterXML;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.*;

/**
 * @author Niv Ingberg
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceClusterInfo implements Externalizable {
    private static final long serialVersionUID = 1L;

    private static final Set<String> supportedSchemas = initSupportedSchemas();

    private static Set<String> initSupportedSchemas() {
        HashSet<String> result = new HashSet<String>();
        result.add(ClusterXML.CLUSTER_SCHEMA_NAME_SYNC_REPLICATED);
        result.add(ClusterXML.CLUSTER_SCHEMA_NAME_ASYNC_REPLICATED);
        result.add(ClusterXML.CLUSTER_SCHEMA_NAME_PRIMARY_BACKUP);
        result.add(ClusterXML.CLUSTER_SCHEMA_NAME_PARTITIONED);
        result.add(ClusterXML.CLUSTER_SCHEMA_NAME_PARTITIONED_SYNC2BACKUP);
        return result;
    }

    private boolean clustered;
    private String clusterName;
    private String clusterSchema;
    private ClusterTopology topology;
    private boolean replicated;
    private boolean primaryElectionAvailable;
    private boolean activeActive;
    private List<String> membersNames;
    private boolean broadcastDisabled;
    private int numOfPartitions;
    private int numOfBackups;
    private transient SpaceClusterPartitionInfo[] partitions;
    private SpaceProxyLoadBalancerType loadBalancerType;
    private Map<String, SpaceCustomComponent> customComponents = new HashMap<>();
    private boolean syncReplication;
    private MirrorServiceConfig mirrorServiceConfig;
    private boolean hasReplicationTargets;

    /**
     * Required for Externalizable
     */
    public SpaceClusterInfo() {
    }

    public SpaceClusterInfo(JSpaceAttributes spaceAttributes, String defaultMemberName) {
        final ClusterPolicy clusterPolicy = spaceAttributes != null ? spaceAttributes.getClusterPolicy() : null;
        this.clustered = clusterPolicy != null;
        this.clusterName = clusterPolicy != null ? clusterPolicy.m_ClusterName : null;
        this.clusterSchema = clusterPolicy != null ? clusterPolicy.m_ClusterSchemaName : null;
        if (clusterSchema != null && !supportedSchemas.contains(clusterSchema))
            throw new IllegalArgumentException("Unsupported cluster schema - " + clusterSchema);
        this.replicated = clusterPolicy != null && clusterPolicy.isReplicated();
        this.primaryElectionAvailable = clusterPolicy != null && clusterPolicy.isPrimaryElectionAvailable();
        this.activeActive = clusterPolicy != null && clusterPolicy.isActiveActive();
        this.syncReplication = clusterPolicy != null && clusterPolicy.getReplicationPolicy() != null && clusterPolicy.getReplicationPolicy().m_IsSyncReplicationEnabled;
        this.mirrorServiceConfig = replicated ? clusterPolicy.getReplicationPolicy().getMirrorServiceConfig() : null;
        this.hasReplicationTargets = clusterPolicy != null && clusterPolicy.getReplicationPolicy() != null && clusterPolicy.getReplicationPolicy().getReplicationTargetsCount() > 0;
        final Map<String, SpaceCustomComponent> customComponents = spaceAttributes != null ? spaceAttributes.getCustomComponents() : null;
        if (customComponents != null) {
            for (SpaceCustomComponent component : customComponents.values()) {
                if (component instanceof Serializable)
                    this.customComponents.put(component.getSpaceComponentKey(), component);
            }
        }
        this.membersNames = clusterPolicy != null ? Collections.unmodifiableList(clusterPolicy.m_AllClusterMemberList) : Collections.singletonList(defaultMemberName);
        this.broadcastDisabled = clusterPolicy != null && clusterPolicy.m_LoadBalancingPolicy != null && clusterPolicy.m_LoadBalancingPolicy.m_DefaultPolicy.getBroadcastCondition().isBroadcastNever();

        final String routingPolicy = getLoadBalancingPolicy(clusterPolicy);
        this.numOfPartitions = getNumOfPartitions(clusterPolicy, routingPolicy);
        this.numOfBackups = getNumberOfBackups(clusterPolicy, numOfPartitions);
        this.loadBalancerType = StringUtils.hasLength(routingPolicy) && routingPolicy.equals("round-robin")
                ? SpaceProxyLoadBalancerType.ROUND_ROBIN : SpaceProxyLoadBalancerType.STICKY;

        initialize();
    }

    public SpaceClusterInfo(SpaceClusterInfo clusterInfo) {
        this.clustered = clusterInfo.clustered;
        this.clusterName = clusterInfo.clusterName;
        this.clusterSchema = clusterInfo.clusterSchema;
        this.replicated = clusterInfo.replicated;
        this.primaryElectionAvailable = clusterInfo.primaryElectionAvailable;
        this.activeActive = clusterInfo.activeActive;
        this.broadcastDisabled= clusterInfo.broadcastDisabled;
        this.loadBalancerType = clusterInfo.loadBalancerType;
        this.syncReplication = clusterInfo.syncReplication;
        this.hasReplicationTargets = clusterInfo.hasReplicationTargets;
        this.numOfBackups = clusterInfo.numOfBackups;
        this.customComponents = clusterInfo.customComponents;
        this.mirrorServiceConfig = clusterInfo.mirrorServiceConfig;
    }


    private void initialize() {
        this.partitions = generatePartitionsInfo(clusterName, numOfPartitions, numOfBackups);
    }

    public boolean isClustered() {
        return clustered;
    }

    public List<String> getMembersNames() {
        return membersNames;
    }

    public boolean isBroadcastDisabled() {
        return broadcastDisabled;
    }

    public SpaceClusterInfo cloneAndUpdate(ClusterTopology newChunksMap){
        SpaceClusterInfo newClusterInfo = new SpaceClusterInfo(this);
        newClusterInfo.topology = newChunksMap;
        newClusterInfo.numOfPartitions = newChunksMap.getNumberOfInstances();
        newClusterInfo.partitions = generatePartitionsInfo(newClusterInfo.clusterName, newClusterInfo.numOfPartitions, newClusterInfo.numOfBackups);
        newClusterInfo.membersNames = new ArrayList<>();
        for (SpaceClusterPartitionInfo partition : newClusterInfo.partitions) {
            newClusterInfo.membersNames.addAll(partition.members);
        }
        return newClusterInfo;
    }

    /**
     * @return Gets whether the cluster is partitioned. True for "partitioned_sync2backup"
     * topologies.
     */
    public boolean isPartitioned() {
        return numOfPartitions != 0;
    }

    /**
     * @return Gets the number of partitions in a partitioned topology, otherwise returns 0.
     */
    public int getNumberOfPartitions() {
        return numOfPartitions;
    }

    public ClusterTopology getTopology() {
        return topology;
    }

    public int getNumberOfBackups() {
        return numOfBackups;
    }

    public SpaceProxyLoadBalancerType getLoadBalancerType() {
        return loadBalancerType;
    }

    /**
     * @return Gets a group members list for the provided partition id - relevant only for
     * partitioned clusters.
     */
    public List<String> getPartitionMembersNames(int partitionId) {
        return partitions[partitionId].members;
    }

    public int getPartitionOfMember(String memberName) {
        for (int i = 0; i < numOfPartitions; i++)
            if (partitions[i].members.contains(memberName))
                return i;
        return -1;
    }

    public boolean isReplicated() {
        return replicated;
    }

    public boolean isActiveActive() {
        return activeActive;
    }

    public <T> T getCustomComponent(String key) {
        return (T) customComponents.get(key);
    }

    public Map<String, SpaceCustomComponent> getCustomComponents() {
        return customComponents;
    }

    public void setCustomComponent(SpaceCustomComponent component) {
        customComponents.put(component.getSpaceComponentKey(), component);
    }

    public void setNumOfPartitions(int numOfPartitions) {
        this.numOfPartitions = numOfPartitions;
    }

    public boolean isSyncReplicationEnabled() {
        return syncReplication;
    }

    public boolean isMirrorServiceEnabled() {
        return mirrorServiceConfig != null;
    }

    public MirrorServiceConfig getMirrorServiceConfig() {
        return mirrorServiceConfig;
    }

    public boolean hasReplicationTargets() {
        return hasReplicationTargets;
    }

    public boolean isPrimaryElectionAvailable() {
        return primaryElectionAvailable;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getClusterSchema() {
        return clusterSchema;
    }

    public void setTopology(ClusterTopology topology) {
        this.topology = topology;
    }

    private static String getLoadBalancingPolicy(ClusterPolicy clusterPolicy) {
        if (clusterPolicy == null || clusterPolicy.m_LoadBalancingPolicy == null)
            return null;
        return clusterPolicy.m_LoadBalancingPolicy.m_DefaultPolicy.m_PolicyType;
    }

    private static int getNumOfPartitions(ClusterPolicy clusterPolicy, String routingPolicy) {
        final boolean isPartitioned = StringUtils.hasLength(routingPolicy) && routingPolicy.equals("hash-based");
        if (!isPartitioned)
            return 0;

        final Map<String, List<String>> backups = clusterPolicy.m_FailOverPolicy == null ? null : clusterPolicy.m_FailOverPolicy.m_DefaultFOPolicy.m_BackupMemberNames;
        if (backups != null) {
            // groups contains all group names in the correct order + backup members
            // so we need to keep that order when building group names & members per partition
            return backups.size();
        }
        // backups is null for "partitioned" schema - groups contains primary members only
        return clusterPolicy.m_LoadBalancingPolicy.loadBalanceGroupMembersNames.size();
    }

    private static SpaceClusterPartitionInfo[] generatePartitionsInfo(String spaceName, int numOfPartitions, int numOfBackups) {
        SpaceClusterPartitionInfo[] partitions = new SpaceClusterPartitionInfo[numOfPartitions];
        for (int partitionId = 1; partitionId <= numOfPartitions; partitionId++) {
            List<String> members = new ArrayList<String>();
            for (int backupId = 0; backupId <= numOfBackups; backupId++)
                members.add(toMemberName(spaceName, partitionId, backupId));
            partitions[partitionId - 1] = new SpaceClusterPartitionInfo(members);
        }
        return partitions;
    }

    private static int getNumberOfBackups(ClusterPolicy clusterPolicy, int numOfPartitions) {
        if (clusterPolicy == null ||
                clusterPolicy.m_FailOverPolicy == null ||
                clusterPolicy.m_FailOverPolicy.m_DefaultFOPolicy == null ||
                clusterPolicy.m_FailOverPolicy.m_DefaultFOPolicy.m_BackupOnly == null)
            return 0;

        int numOfBackups = clusterPolicy.m_FailOverPolicy.m_DefaultFOPolicy.m_BackupOnly.size();
        return numOfPartitions == 0 ? numOfBackups : numOfBackups / numOfPartitions;
    }

    private static String toMemberName(String spaceName, int partitionId, int backupId) {
        String member = backupId == 0 ? String.valueOf(partitionId) : partitionId + "_" + backupId;
        return spaceName + "_container" + member + ":" + spaceName;
    }

    public boolean isTieredStorage(){
        return customComponents.get(Constants.TieredStorage.SPACE_CLUSTER_INFO_TIERED_STORAGE_COMPONENT_NAME) != null;
    }

    public Map<String, TieredStorageTableConfig> tieredStorageConfigString (){
        TieredStorageConfig tieredStorageConfig = (TieredStorageConfig) customComponents.get(Constants.TieredStorage.SPACE_CLUSTER_INFO_TIERED_STORAGE_COMPONENT_NAME);
       return tieredStorageConfig.toMapConfigurationForDisplay();
    }

    @Override
    public String toString() {
        if (!clustered)
            return "SpaceClusterInfo [clustered=false]";
        return "SpaceClusterInfo [" +
                "clusterName=" + clusterName +
                ", clusterSchema=" + clusterSchema +
                ", numOfPartitions=" + numOfPartitions +
                ", numOfBackups=" + numOfBackups +
                "]";
    }

    private static final short FLAG_CLUSTERED = 1 << 0;
    private static final short FLAG_REPLICATED = 1 << 1;
    private static final short FLAG_PRIMARY_ELECTION = 1 << 2;
    private static final short FLAG_ACTIVE_ACTIVE = 1 << 3;
    private static final short FLAG_BROADCAST_DISABLED = 1 << 4;
    private static final short FLAG_SYNC_REPLICATION = 1 << 5;
    private static final short FLAG_MIRROR_CONFIG = 1 << 6;
    private static final short FLAG_HAS_REPLICATION_TARGETS = 1 << 7;
    private static final short FLAG_PARTITIONED = 1 << 8;
    private static final short FLAG_HAS_BACKUPS = 1 << 9;
    private static final short FLAG_HAS_CUSTOM_COMPONENTS = 1 << 10;
    private static final short FLAG_LOAD_BALANCER_TYPE = 1 << 11;

    private int buildFlags() {
        int flags = 0;
        if (clustered)
            flags |= FLAG_CLUSTERED;
        if (replicated)
            flags |= FLAG_REPLICATED;
        if (primaryElectionAvailable)
            flags |= FLAG_PRIMARY_ELECTION;
        if (activeActive)
            flags |= FLAG_ACTIVE_ACTIVE;
        if (broadcastDisabled)
            flags |= FLAG_BROADCAST_DISABLED;
        if (syncReplication)
            flags |= FLAG_SYNC_REPLICATION;
        if (mirrorServiceConfig != null)
            flags |= FLAG_MIRROR_CONFIG;
        if (hasReplicationTargets)
            flags |= FLAG_HAS_REPLICATION_TARGETS;
        if (numOfPartitions != 0)
            flags |= FLAG_PARTITIONED;
        if (numOfBackups != 0)
            flags |= FLAG_HAS_BACKUPS;
        if (!customComponents.isEmpty())
            flags |= FLAG_HAS_CUSTOM_COMPONENTS;
        if (loadBalancerType != SpaceProxyLoadBalancerType.STICKY)
            flags |= FLAG_LOAD_BALANCER_TYPE;
        return flags;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        out.writeInt(buildFlags());
        IOUtils.writeString(out, clusterName);
        IOUtils.writeString(out, clusterSchema);
        IOUtils.writeListString(out, membersNames);
        if (numOfPartitions != 0)
            out.writeInt(numOfPartitions);
        if (numOfBackups != 0)
            out.writeInt(numOfBackups);
        if (mirrorServiceConfig != null)
            mirrorServiceConfig.writeExternal(out);
        if (!customComponents.isEmpty()) {
            CustomSerializer<SpaceClusterInfo> customSerializer = XapExtensions.getInstance().getCustomSerializer(
                    SpaceClusterInfo.class, version);
            if (customSerializer != null) {
                customSerializer.writeExternal(this, out, version);
            } else {
                IOUtils.writeObject(out, customComponents);
            }
        }
        if (loadBalancerType != SpaceProxyLoadBalancerType.STICKY)
            out.writeByte(loadBalancerType.getCode());
        if(version.greaterOrEquals(PlatformLogicalVersion.v15_5_0)) {
            IOUtils.writeObject(out, topology);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final PlatformLogicalVersion version = LRMIInvocationContext.getEndpointLogicalVersion();
        final int flags = in.readInt();
        this.clusterName = IOUtils.readString(in);
        this.clusterSchema = IOUtils.readString(in);
        this.membersNames = IOUtils.readListString(in);
        this.numOfPartitions = ((flags & FLAG_PARTITIONED) != 0) ? in.readInt() : 0;
        this.numOfBackups = (flags & FLAG_HAS_BACKUPS) != 0 ? in.readInt() : 0;
        if ((flags & FLAG_MIRROR_CONFIG) != 0) {
            this.mirrorServiceConfig = new MirrorServiceConfig();
            this.mirrorServiceConfig.readExternal(in);
        }
        if ((flags & FLAG_HAS_CUSTOM_COMPONENTS) != 0) {
            CustomSerializer<SpaceClusterInfo> customSerializer = XapExtensions.getInstance().getCustomSerializer(
                    SpaceClusterInfo.class, version);
            if (customSerializer != null) {
                customSerializer.readExternal(this, in, version);
            } else {
                Map<String, SpaceCustomComponent> componentMap = IOUtils.readObject(in);
                this.customComponents.putAll(componentMap);
            }
        }
        if ((flags & FLAG_LOAD_BALANCER_TYPE) != 0)
            this.loadBalancerType = SpaceProxyLoadBalancerType.fromCode(in.readByte());
        else
            this.loadBalancerType = SpaceProxyLoadBalancerType.STICKY;

        this.clustered = (flags & FLAG_CLUSTERED) != 0;
        this.replicated = (flags & FLAG_REPLICATED) != 0;
        this.primaryElectionAvailable = (flags & FLAG_PRIMARY_ELECTION) != 0;
        this.activeActive = (flags & FLAG_ACTIVE_ACTIVE) != 0;
        this.broadcastDisabled = (flags & FLAG_BROADCAST_DISABLED) != 0;
        this.syncReplication = (flags & FLAG_SYNC_REPLICATION) != 0;
        this.hasReplicationTargets = (flags & FLAG_HAS_REPLICATION_TARGETS) != 0;
        if(version.greaterOrEquals(PlatformLogicalVersion.v15_5_0)) {
            this.topology = IOUtils.readObject(in);
        }
        initialize();
    }

    public boolean isChunksRouting() {
        return this.topology != null;
    }

    public int getPartitionId(int routingValue) {
        return this.topology.getPartitionId(routingValue);
    }

    public int getPartitionId(long routingValue) {
        return this.topology.getPartitionId(routingValue);
    }
}
