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

package com.gigaspaces.cluster.loadbalance;

import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.ISpaceState;
import com.j_spaces.core.JSpaceState;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.cluster.ClusterXML;
import com.j_spaces.kernel.JSpaceUtilities;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class LoadBalancingPolicy implements Serializable, SmartExternalizable {
    private static final long serialVersionUID = 2L;

    public static final int DEFAULT_BROADCAST_THREADPOOL_MIN_SIZE = 4;
    public static final int DEFAULT_BROADCAST_THREADPOOL_MAX_SIZE = 64;

    public String m_GroupName;
    public List<String> loadBalanceGroupMembersNames; // A list of "container-name:space-name"
    public List<SpaceURL> loadBalanceGroupMembersURLs;  // A list of SpaceURL URLs
    //when m_ApplyOwnership is true the operation will be directed to the owner space (or
    // its backup) if UID (which contains the creator space) is supplied for relevant operations (take, read, update, updateMultiple)
    // regardless of the defined load-balancing policy
    public boolean m_ApplyOwnership;

    //when m_DisableParallelScattering is true parallel scattering is
    //disabled in the clustered-proxy/LB
    public boolean m_DisableParallelScattering;

    //Parallel operations clustered proxy thread pool (WorkingGroup)
    public int m_broadcastThreadpoolMinSize = DEFAULT_BROADCAST_THREADPOOL_MIN_SIZE;
    public int m_broadcastThreadpoolMaxSize = DEFAULT_BROADCAST_THREADPOOL_MAX_SIZE;

    public LoadBalancingPolicyDescription m_WriteOperationsPolicy;
    public LoadBalancingPolicyDescription m_ReadOperationsPolicy;
    public LoadBalancingPolicyDescription m_TakeOperationsPolicy;
    public LoadBalancingPolicyDescription m_NotifyOperationsPolicy;
    public LoadBalancingPolicyDescription m_DefaultPolicy;

    private interface BitMap {
        int GROUP_NAME = 1 << 0;
        int LOAD_BALANCE_GROUP_MEMBER_NAMES = 1 << 1;
        int LOAD_BALANCE_GROUP_MEMBER_URLS = 1 << 2;
        int APPLY_OWNERSHIP = 1 << 3;
        int DISABLE_PARALLEL_SCATERING = 1 << 4;
        int BROADCAST_TP_MIN_SIZE = 1 << 5;
        int BROADCAST_TP_MAX_SIZE = 1 << 6;
        int WRITE_OP_POLICY = 1 << 7;
        int READ_OP_POLICY = 1 << 8;
        int TAKE_OP_POLICY = 1 << 9;
        int NOTIFY_OP_POLICY = 1 << 10;
        int DEFAULT_OP_POLICY = 1 << 11;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        int flags = buildFlags();
        out.writeInt(flags);

        if (m_GroupName != null)
            out.writeObject(m_GroupName);
        if (loadBalanceGroupMembersNames != null) {
            out.writeInt(loadBalanceGroupMembersNames.size());
            for (String s : loadBalanceGroupMembersNames)
                out.writeObject(s);
        }

        if (loadBalanceGroupMembersURLs != null) {
            out.writeInt(loadBalanceGroupMembersURLs.size());
            for (SpaceURL u : loadBalanceGroupMembersURLs)
                out.writeObject(u);
        }

        if (m_broadcastThreadpoolMinSize != DEFAULT_BROADCAST_THREADPOOL_MIN_SIZE)
            out.writeInt(m_broadcastThreadpoolMinSize);
        if (m_broadcastThreadpoolMinSize != DEFAULT_BROADCAST_THREADPOOL_MAX_SIZE)
            out.writeInt(m_broadcastThreadpoolMaxSize);
        if (m_WriteOperationsPolicy != null)
            out.writeObject(m_WriteOperationsPolicy);
        if (m_ReadOperationsPolicy != null)
            out.writeObject(m_ReadOperationsPolicy);
        if (m_TakeOperationsPolicy != null)
            out.writeObject(m_TakeOperationsPolicy);
        if (m_NotifyOperationsPolicy != null)
            out.writeObject(m_NotifyOperationsPolicy);
        if (m_DefaultPolicy != null)
            out.writeObject(m_DefaultPolicy);
    }

    private int buildFlags() {
        int flags = 0;

        if (m_GroupName != null)
            flags |= BitMap.GROUP_NAME;
        if (loadBalanceGroupMembersNames != null)
            flags |= BitMap.LOAD_BALANCE_GROUP_MEMBER_NAMES;
        if (loadBalanceGroupMembersURLs != null)
            flags |= BitMap.LOAD_BALANCE_GROUP_MEMBER_URLS;
        if (m_ApplyOwnership)
            flags |= BitMap.APPLY_OWNERSHIP;
        if (m_DisableParallelScattering)
            flags |= BitMap.DISABLE_PARALLEL_SCATERING;
        if (m_broadcastThreadpoolMinSize != DEFAULT_BROADCAST_THREADPOOL_MIN_SIZE)
            flags |= BitMap.BROADCAST_TP_MIN_SIZE;
        if (m_broadcastThreadpoolMinSize != DEFAULT_BROADCAST_THREADPOOL_MAX_SIZE)
            flags |= BitMap.BROADCAST_TP_MAX_SIZE;
        if (m_WriteOperationsPolicy != null)
            flags |= BitMap.WRITE_OP_POLICY;
        if (m_ReadOperationsPolicy != null)
            flags |= BitMap.READ_OP_POLICY;
        if (m_TakeOperationsPolicy != null)
            flags |= BitMap.TAKE_OP_POLICY;
        if (m_NotifyOperationsPolicy != null)
            flags |= BitMap.NOTIFY_OP_POLICY;
        if (m_DefaultPolicy != null)
            flags |= BitMap.DEFAULT_OP_POLICY;

        return flags;
    }

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        int flags = in.readInt();

        if ((flags & BitMap.GROUP_NAME) != 0)
            m_GroupName = (String) in.readObject();
        if ((flags & BitMap.LOAD_BALANCE_GROUP_MEMBER_NAMES) != 0) {
            int size = in.readInt();
            loadBalanceGroupMembersNames = new ArrayList<String>(size);
            for (int i = 0; i < size; i++)
                loadBalanceGroupMembersNames.add((String) in.readObject());
        }
        if ((flags & BitMap.LOAD_BALANCE_GROUP_MEMBER_URLS) != 0) {
            int size = in.readInt();
            loadBalanceGroupMembersURLs = new ArrayList<SpaceURL>(size);
            for (int i = 0; i < size; i++)
                loadBalanceGroupMembersURLs.add((SpaceURL) in.readObject());
        }
        m_ApplyOwnership = ((flags & BitMap.APPLY_OWNERSHIP) != 0);
        m_DisableParallelScattering = ((flags & BitMap.DISABLE_PARALLEL_SCATERING) != 0);
        if ((flags & BitMap.BROADCAST_TP_MIN_SIZE) != 0)
            m_broadcastThreadpoolMinSize = in.readInt();
        else
            m_broadcastThreadpoolMinSize = DEFAULT_BROADCAST_THREADPOOL_MIN_SIZE;
        if ((flags & BitMap.BROADCAST_TP_MAX_SIZE) != 0)
            m_broadcastThreadpoolMaxSize = in.readInt();
        else
            m_broadcastThreadpoolMaxSize = DEFAULT_BROADCAST_THREADPOOL_MAX_SIZE;

        if ((flags & BitMap.WRITE_OP_POLICY) != 0)
            m_WriteOperationsPolicy = (LoadBalancingPolicyDescription) in.readObject();
        if ((flags & BitMap.READ_OP_POLICY) != 0)
            m_ReadOperationsPolicy = (LoadBalancingPolicyDescription) in.readObject();
        if ((flags & BitMap.TAKE_OP_POLICY) != 0)
            m_TakeOperationsPolicy = (LoadBalancingPolicyDescription) in.readObject();
        if ((flags & BitMap.NOTIFY_OP_POLICY) != 0)
            m_NotifyOperationsPolicy = (LoadBalancingPolicyDescription) in.readObject();
        if ((flags & BitMap.DEFAULT_OP_POLICY) != 0)
            m_DefaultPolicy = (LoadBalancingPolicyDescription) in.readObject();
    }


    public static class LoadBalancingPolicyDescription implements Serializable, SmartExternalizable {
        private static final long serialVersionUID = 2L;

        public String m_PolicyType;
        public Properties m_Properties;
        //on what condition does broadcast depend for this policy?
        private BroadcastCondition m_BroadcastCondition;

        // the actual string value that was given in configuration
        // this is needed to support the old values with the new values
        private String _broadcastConditionDescription;

        private interface BitMap {
            byte POLICY_TYPE = 1 << 0;
            byte PROPERTIES = 1 << 1;
            byte BROADCAST_CONDITION = 1 << 2;
            byte BROADCAST_CONDITION_DESC = 1 << 3;
        }


        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            byte flags = 0;
            if (m_PolicyType != null) {
                flags |= BitMap.POLICY_TYPE;
            }
            if (m_Properties != null) {
                flags |= BitMap.PROPERTIES;
            }
            if (m_BroadcastCondition != null) {
                flags |= BitMap.BROADCAST_CONDITION;
            }
            if (_broadcastConditionDescription != null) {
                flags |= BitMap.BROADCAST_CONDITION_DESC;
            }
            out.writeByte(flags);
            if (m_PolicyType != null) {
                out.writeUTF(m_PolicyType);
            }
            if (m_Properties != null) {
                out.writeInt(m_Properties.size());
                for (Iterator it = m_Properties.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry entry = (Map.Entry) it.next();
                    out.writeUTF((String) entry.getKey());
                    out.writeUTF((String) entry.getValue());
                }
            }
            if (m_BroadcastCondition != null) {
                out.writeObject(m_BroadcastCondition);
            }
            if (_broadcastConditionDescription != null) {
                out.writeObject(_broadcastConditionDescription);
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            byte flags = in.readByte();
            if ((flags & BitMap.POLICY_TYPE) != 0) {
                m_PolicyType = in.readUTF();
            }
            if ((flags & BitMap.PROPERTIES) != 0) {
                int size = in.readInt();
                m_Properties = new Properties();
                for (int i = 0; i < size; i++) {
                    m_Properties.put(in.readUTF(), in.readUTF());
                }
            }
            if ((flags & BitMap.BROADCAST_CONDITION) != 0) {
                m_BroadcastCondition = (BroadcastCondition) in.readObject();
            }
            if ((flags & BitMap.BROADCAST_CONDITION_DESC) != 0) {
                _broadcastConditionDescription = (String) in.readObject();
            }
        }

        public String getBroadcastConditionDescription() {
            return _broadcastConditionDescription;
        }

        public void setBroadcastConditionDescription(
                String broadcastConditionDescription) {
            _broadcastConditionDescription = broadcastConditionDescription;

            setBroadcastCondition(BroadcastCondition.getBroadcastConditionValue(_broadcastConditionDescription));
        }

        public void setBroadcastCondition(BroadcastCondition broadcastCondition) {
            this.m_BroadcastCondition = broadcastCondition;

            if (_broadcastConditionDescription == null)
                _broadcastConditionDescription = broadcastCondition.getDescription();
        }


        public BroadcastCondition getBroadcastCondition() {
            return m_BroadcastCondition;
        }

        @Override
        public String toString() {
            return "\n------------Load Balancing Policy Description---------\n" +
                    "Policy Type -\t" + m_PolicyType + '\n' +
                    "Properties -\t" + JSpaceUtilities.getPropertiesPresentation(m_Properties) + '\n' +
                    "Broadcast Condition -\t" + getBroadcastConditionDescription() + '\n';
        }

    }

    public static enum BroadcastCondition {
        /**
         * on what condition does broadcast depend ?
         */
        ROUTING_INDEX_IS_NULL(ClusterXML.BC_BROADCAST_IF_ROUTING_INDEX_IS_NULL) /* null fields , for hash based- null index*/, ALWAYS(ClusterXML.BC_BROADCAST_ALWAYS) /* use it any way*/, NEVER(ClusterXML.BC_BROADCAST_NEVER),
        // old values
        IF_NULL_VALUES(ClusterXML.BC_BROADCAST_IF_NULL_VALUES) /* null fields , for hash based- null index*/, UNCONDITIONAL(ClusterXML.BC_BROADCAST_UNCONDITIONAL) /* use it any way*/, DISABLED(ClusterXML.BC_BROADCAST_DISABLED);
      /* disable broadcast*/

        private final String _description;

        private BroadcastCondition(String description) {
            _description = description;
        }

        public boolean isBroadcastNever() {
            return this == NEVER || this == DISABLED;
        }

        public String getDescription() {
            return _description;
        }

        public static BroadcastCondition getBroadcastConditionValue(String val) {
            if (ClusterXML.BC_BROADCAST_NEVER.equals(val))
                return NEVER;
            if (ClusterXML.BC_BROADCAST_ALWAYS.equals(val))
                return ALWAYS;

            if (ClusterXML.BC_BROADCAST_IF_ROUTING_INDEX_IS_NULL.equals(val))
                return ROUTING_INDEX_IS_NULL;

            if (ClusterXML.BC_BROADCAST_DISABLED.equals(val))
                return DISABLED;

            if (ClusterXML.BC_BROADCAST_UNCONDITIONAL.equals(val))
                return UNCONDITIONAL;

            if (ClusterXML.BC_BROADCAST_IF_NULL_VALUES.equals(val))
                return IF_NULL_VALUES;

            // default value
            return ROUTING_INDEX_IS_NULL;
        }
    }


    /**
     * Finds all the recovery targets in cluster. If jini URLs are used, the first target will
     * always be multicast url. only if it fails the unicast and rmi URLs will be used.
     *
     * @param clusterName    the cluster to recover in.
     * @param myMemberName   the member of the cluster that needs to be recovered.
     * @param excludeTargets targets that should be excluded from recovery
     * @return a list of recoverable members SpaceURLs. returns empty list if recovery is disabled.
     */
    public List<SpaceURL> getRecoverableTargets(String clusterName,
                                                String myMemberName, List<String> excludeTargets) {

        LinkedList<SpaceURL> recoverableTargets = new LinkedList<SpaceURL>();
        boolean foundJini = false;

        // Go over all load balancing members
        for (int i = 0; i < loadBalanceGroupMembersNames.size(); i++) {
            String memberName = loadBalanceGroupMembersNames.get(i);

            if (memberName.equals(myMemberName))
                continue;

            if (excludeTargets.contains(memberName))
                continue;

	 		/* must clone to prevent direct reference change */
            SpaceURL remoteSpaceURL = loadBalanceGroupMembersURLs.get(i).clone();

            if (remoteSpaceURL == null)
                continue;

            /**
             * if at least one jini url found, we do memory-recovery by
             * cluster-name & replication-group attributes
             */
            if (remoteSpaceURL.isJiniProtocol()) {
                // Check if multicast url was already found
                if (!foundJini) {
                    //	Create a multicast url
                    SpaceURL anySpaceURL = remoteSpaceURL.clone();

                    anySpaceURL.setPropertiesPrefix(SpaceURL.JINI_PROTOCOL, SpaceURL.ANY, SpaceURL.ANY, SpaceURL.ANY);
                    anySpaceURL.setProperty(SpaceURL.CLUSTER_NAME, clusterName);
                    anySpaceURL.setProperty(SpaceURL.STATE, JSpaceState.convertToString(ISpaceState.STARTED));

                    // Add the multicast url as the first target only once
                    recoverableTargets.addFirst(anySpaceURL);
                    foundJini = true;
                }


                //	If unicast - add the url as well
                if (remoteSpaceURL.getProperty(SpaceURL.HOST_NAME) != null && !SpaceURL.ANY.equals(remoteSpaceURL.getProperty(SpaceURL.HOST_NAME))) {
                    remoteSpaceURL.setProperty(SpaceURL.CLUSTER_NAME, clusterName);
                    remoteSpaceURL.setProperty(SpaceURL.STATE, JSpaceState.convertToString(ISpaceState.STARTED));

                    recoverableTargets.add(remoteSpaceURL);
                }

            } else {
                /** returns entrySet with specific sourceMemeber */
                recoverableTargets.add(remoteSpaceURL);

            }
        }

        return recoverableTargets;
    }

    @Override
    public String toString() {
        return "\n------------ LoadBalancing Policy ---------\n" +
                "Group Name -\t" + m_GroupName + '\n' +
                "Load Balancing Group Members Name -\t" + loadBalanceGroupMembersNames + '\n' +
                "Load Balancing Group Members URL -\t" + loadBalanceGroupMembersURLs + '\n' +
                "Is apply ownership -\t" + m_ApplyOwnership + '\n' +
                "Is disable Parallel Scattering -\t" + m_DisableParallelScattering + '\n' +
                "Broadcast Thread Pool Min Size -\t" + m_broadcastThreadpoolMinSize + '\n' +
                "Broadcast Thread Pool Max Size -\t" + m_broadcastThreadpoolMaxSize + '\n' +
                '\n' + "Write Operations Policy -\t" + m_WriteOperationsPolicy + '\n' +
                '\n' + "Read Operations Policy -\t" + m_ReadOperationsPolicy + '\n' +
                '\n' + "Take Operations Policy -\t" + m_TakeOperationsPolicy + '\n' +
                '\n' + "Notify Operations Policy -\t" + m_NotifyOperationsPolicy + '\n' +
                '\n' + "Default Policy -\t" + m_DefaultPolicy + '\n';
    }
}