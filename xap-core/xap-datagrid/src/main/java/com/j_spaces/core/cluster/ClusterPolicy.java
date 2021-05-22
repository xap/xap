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

import com.gigaspaces.cluster.loadbalance.LoadBalancingPolicy;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.JSpaceAttributes;
import com.j_spaces.kernel.JSpaceUtilities;
import com.j_spaces.kernel.SystemProperties;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Holds the cluster policy data. <b>This API will not be available in future versions</b>
 */
@com.gigaspaces.api.InternalApi
public class ClusterPolicy implements SmartExternalizable {
    private static final long serialVersionUID = 2L;

    public ClusterPolicy() {
        int foo = 1;
    }

    public String m_ClusterSchemaName;
    public String m_ClusterName;
    // recovery in load balancing group
    public boolean m_NotifyRecovery;
    public CacheLoaderConfig m_CacheLoaderConfig;
    public String m_ClusterGroupMember;
    // Is member of replication
    public boolean m_Replicated;
    public ReplicationPolicy m_ReplicationPolicy;
    public FailOverPolicy m_FailOverPolicy;
    public LoadBalancingPolicy m_LoadBalancingPolicy;
    public List<ReplicationPolicy> m_ReplicationGroups;
    // Cluster members' names
    public List<String> m_AllClusterMemberList;
    // Holds for each member its properties
    public HashMap<String, Properties> m_ClusterMembersProperties;

    /**
     * DCache configuration.
     *
     * @deprecated No longer in the cluster policy. Included in a space configuration. In order to
     * get the attributes use: ((JSpaceAttributes)JProperties.getSpaceProperties(getServiceName())).getDCacheProperties().
     */
    @Deprecated
    public JSpaceAttributes m_DCacheAttributes;

    /**
     * DCache configuration name.
     *
     * @deprecated No longer in the cluster policy. Included in a space configuration. In order to
     * get the configuration name use: ((JSpaceAttributes)JProperties.getSpaceProperties(getServiceName())).getDCacheConfigName().
     */
    @Deprecated
    public String m_DCacheConfigName;

    @Deprecated
    public Properties m_jmsProperties;

    @Deprecated
    public String m_jmsConfigName;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\n\n------------- CLUSTER POLICY RUNTIME CONFIGURATIONS REPORT ----------\n\n");
        sb.append("Cluster schema name -\t" + m_ClusterSchemaName + "\n");
        sb.append("Cluster name -\t" + m_ClusterName + "\n");
        sb.append("Is notify recovery -\t" + m_NotifyRecovery + "\n");
        sb.append("Cluster group member -\t" + m_ClusterGroupMember + "\n");
        sb.append("Is replicated -\t" + m_Replicated + "\n");
        sb.append("DCache config name -\t" + m_DCacheConfigName + "\n");
        sb.append("JMS config name -\t" + m_jmsConfigName + "\n");
        sb.append("JMS properties -\n\t" + JSpaceUtilities.getPropertiesPresentation(m_jmsProperties) + "\n");
        sb.append("DCache attributes -\n\t" + JSpaceUtilities.getPropertiesPresentation(m_DCacheAttributes) + "\n");
        sb.append("Cluster members list -\t" + m_AllClusterMemberList + "\n");
        sb.append("Replication groups -\t" + m_ReplicationGroups + "\n");
        sb.append("CacheLoader config -\t" + m_CacheLoaderConfig + "\n");
        sb.append("Replication Policy -\t" + m_ReplicationPolicy + "\n");
        sb.append("FailOver Policy -\t" + m_FailOverPolicy + "\n");
        sb.append("Load Balancing Policy -\t" + m_LoadBalancingPolicy + "\n");
        sb.append("\n\n------------- END REPORT OF CLUSTER POLICY RUNTIME CONFIGURATIONS ----------\n");
        return sb.toString();
    }

    public boolean isPersistentStartupEnabled() {
        return m_Replicated && Boolean.getBoolean(SystemProperties.SPACE_STARTUP_STATE_ENABLED);
    }

    public boolean isFailbackEnabled() {
        return m_FailOverPolicy.isFailBackEnabled();
    }

    public boolean isReplicated() {
        return m_Replicated;
    }

    public ReplicationPolicy getReplicationPolicy() {
        return m_ReplicationPolicy;
    }

    public boolean isPrimaryElectionAvailable() {
        return m_FailOverPolicy != null &&
                m_FailOverPolicy.getElectionGroupName() != null &&
                !isPersistentStartupEnabled() &&
                !m_FailOverPolicy.isFailBackEnabled();
    }

    public boolean isActiveActive() {
        return m_ReplicationPolicy != null
                && !isPrimaryElectionAvailable()
                && m_ReplicationPolicy.getReplicationTargetsCount() != 0
                && !m_ReplicationPolicy.isMirrorServiceEnabled();
    }

    public static final class CacheLoaderConfig implements SmartExternalizable {
        private static final long serialVersionUID = 1L;

        public CacheLoaderConfig() {
        }

        /**
         * true if cluster holds at least one external data-source; default: false.
         */
        public boolean externalDataSource = externalDataSourceDefault();

        /**
         * true if cluster interacts with a central-data-source; default: false.
         */
        public boolean centralDataSource = centralDataSourceDefault();

        public static boolean externalDataSourceDefault() {
            return true;
        }

        public static boolean centralDataSourceDefault() {
            return true;
        }

        @Override
        public String toString() {
            return "\n-----CacheLoaderConfig------\n" +
                    "External Data Source -\t" + externalDataSource + "\n" +
                    "Central Data Source -\t" + centralDataSource + "\n";
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(externalDataSource);
            out.writeBoolean(centralDataSource);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            externalDataSource = in.readBoolean();
            centralDataSource = in.readBoolean();
        }
    }

    public int getNumOfPartitions() {
        return m_ReplicationGroups != null ? m_ReplicationGroups.size() : m_AllClusterMemberList.size();
    }

    private static final class BitMap {
        private static final int CLUSTER_SCHEMANAME = 1 << 0;
        private static final int CLUSTER_NAME = 1 << 1;
        private static final int CLUSTER_GROUP_MEMBER = 1 << 2;
        private static final int REPLICATION_GROUPS = 1 << 3;
        private static final int ALLCLUSTER_MEMBERLIST = 1 << 4;
        private static final int CLUSTER_MEMBERS_PROPERTIES = 1 << 5;
        private static final int DCACHE_CONFIGNAME = 1 << 6;
        private static final int JMS_PROPERTIES = 1 << 7;
        private static final int JMS_CONFIGNAME = 1 << 8;
    }

    private int buildFlags() {
        int flags = 0;

        if (m_ClusterSchemaName != null)
            flags |= BitMap.CLUSTER_SCHEMANAME;
        if (m_ClusterName != null)
            flags |= BitMap.CLUSTER_NAME;
        if (m_ClusterGroupMember != null)
            flags |= BitMap.CLUSTER_GROUP_MEMBER;
        if (m_ReplicationGroups != null)
            flags |= BitMap.REPLICATION_GROUPS;
        if (m_AllClusterMemberList != null)
            flags |= BitMap.ALLCLUSTER_MEMBERLIST;
        if (m_ClusterMembersProperties != null)
            flags |= BitMap.CLUSTER_MEMBERS_PROPERTIES;
        if (m_DCacheConfigName != null)
            flags |= BitMap.DCACHE_CONFIGNAME;
        if (m_jmsProperties != null)
            flags |= BitMap.JMS_PROPERTIES;
        if (m_jmsConfigName != null)
            flags |= BitMap.JMS_CONFIGNAME;
        return flags;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(buildFlags());
        if (m_ClusterSchemaName != null)
            out.writeObject(m_ClusterSchemaName);
        if (m_ClusterName != null)
            out.writeObject(m_ClusterName);
        out.writeBoolean(m_NotifyRecovery);
        out.writeObject(m_CacheLoaderConfig);
        if (m_ClusterGroupMember != null)
            out.writeObject(m_ClusterGroupMember);
        out.writeBoolean(m_Replicated);
        out.writeObject(m_ReplicationPolicy);
        out.writeObject(m_FailOverPolicy);
        out.writeObject(m_LoadBalancingPolicy);
        if (m_ReplicationGroups != null)
            out.writeObject(m_ReplicationGroups);
        if (m_AllClusterMemberList != null)
            out.writeObject(m_AllClusterMemberList);
        if (m_ClusterMembersProperties != null)
            out.writeObject(m_ClusterMembersProperties);
        out.writeObject(m_DCacheAttributes);
        if (m_DCacheConfigName != null)
            out.writeUTF(m_DCacheConfigName);
        if (m_jmsProperties != null)
            out.writeObject(m_jmsProperties);
        if (m_jmsConfigName != null)
            out.writeUTF(m_jmsConfigName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int flags = in.readInt();
        if ((flags & BitMap.CLUSTER_SCHEMANAME) != 0)
            m_ClusterSchemaName = (String) in.readObject();
        if ((flags & BitMap.CLUSTER_NAME) != 0)
            m_ClusterName = (String) in.readObject();
        m_NotifyRecovery = in.readBoolean();
        m_CacheLoaderConfig = (CacheLoaderConfig) in.readObject();
        if ((flags & BitMap.CLUSTER_GROUP_MEMBER) != 0)
            m_ClusterGroupMember = (String) in.readObject();
        m_Replicated = in.readBoolean();
        m_ReplicationPolicy = (ReplicationPolicy) in.readObject();

        m_FailOverPolicy = (FailOverPolicy) in.readObject();
        if (m_FailOverPolicy != null)
            m_FailOverPolicy.buildElectionGroups(m_ClusterGroupMember);

        m_LoadBalancingPolicy = (LoadBalancingPolicy) in.readObject();
        if ((flags & BitMap.REPLICATION_GROUPS) != 0)
            m_ReplicationGroups = (List<ReplicationPolicy>) in.readObject();
        if ((flags & BitMap.ALLCLUSTER_MEMBERLIST) != 0)
            m_AllClusterMemberList = (List<String>) in.readObject();
        if ((flags & BitMap.CLUSTER_MEMBERS_PROPERTIES) != 0)
            m_ClusterMembersProperties = (HashMap<String, Properties>) in.readObject();
        m_DCacheAttributes = (JSpaceAttributes) in.readObject();
        if ((flags & BitMap.DCACHE_CONFIGNAME) != 0)
            m_DCacheConfigName = in.readUTF();
        if ((flags & BitMap.JMS_PROPERTIES) != 0)
            m_jmsProperties = (Properties) in.readObject();
        if ((flags & BitMap.JMS_CONFIGNAME) != 0)
            m_jmsConfigName = in.readUTF();
    }
}
