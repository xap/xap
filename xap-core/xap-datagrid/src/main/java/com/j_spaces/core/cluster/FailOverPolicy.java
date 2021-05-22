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

/*
 * @(#)FailOverPolicy.java 1.0   25/12/2001  14:56AM
 */


package com.j_spaces.core.cluster;

import com.gigaspaces.cluster.activeelection.core.ActiveElectionConfig;
import com.gigaspaces.cluster.activeelection.core.ActiveElectionState;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.client.SpaceURL;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class FailOverPolicy implements Serializable, SmartExternalizable {
    private static final long serialVersionUID = 2L;

    /**
     * For dynamic clustering - if >0
     */
    public int _numberOfBackups;

    public String failOverGroupName; // group member name

    public List<String> failOverGroupMembersNames; // A list of "container-name:space-name"
    public List<SpaceURL> failOverGroupMembersURLs;  // A list of SpaceURL URLs
    public long spaceFinderTimeout = DEFAULT_FAILOVER_SPACE_FINDER_TIMEOUT;
    //public long spaceFinderReportInterval;


    public FailOverPolicyDescription m_WriteFOPolicy;
    public FailOverPolicyDescription m_ReadFOPolicy;
    public FailOverPolicyDescription m_TakeFOPolicy;
    public FailOverPolicyDescription m_NotifyFOPolicy;
    public FailOverPolicyDescription m_DefaultFOPolicy;

    /**
     * always do failback to primary from the backup, when primary came to be alive
     */
    //public boolean isFailBackEnabled = true;
    private boolean isFailBackEnabled;

    // Election group definitions

    // All the elections groups in fail-over group (they are built dynamically after serialization)
    private transient HashMap<String, List<String>> _electionGroups;

    // The election group of the space member (they are built dynamically after serialization)
    private transient String _electionGroupName;
    // values for m_policyType :
    //fail in group to an available member according to policy
    public final static int POLICY_TYPE_FAIL_TO_AVAILABLE = 0;
    // fail to the backup member(s) of the failing member
    public final static int POLICY_TYPE_FAIL_TO_BACKUP = 1;

    public final static int DEFAULT_FAILOVER_SPACE_FINDER_TIMEOUT = 1000 * 2; // retry every 2 seconds

    // active election manager config
    private ActiveElectionConfig _activeElectConfig;


    private interface BitMap {
        int NUMBER_OF_BACKUPS = 1 << 0;
        int FAILOVER_GROUP_NAME = 1 << 2;
        int FAILOVER_GROUP_MEMBER_NAMES = 1 << 3;
        int FAILOVER_GROUP_MEMBER_URLS = 1 << 4;
        int SPACE_FINDER_TIMEOUT = 1 << 5;
        int WRITE_FO_POLICY = 1 << 9;
        int READ_FO_POLICY = 1 << 10;
        int TAKE_FO_POLICY = 1 << 11;
        int NOTIFY_FO_POLICY = 1 << 12;
        int DEFAULT_FO_POLICY = 1 << 13;
        int FAILBACK_ENABLED = 1 << 14;
        int ACTIVE_ELECTION_CONFIG = 1 << 15;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        int flags = 0;
        if (_numberOfBackups != 0) {
            flags |= BitMap.NUMBER_OF_BACKUPS;
        }

        if (failOverGroupName != null) {
            flags |= BitMap.FAILOVER_GROUP_NAME;
        }
        if (failOverGroupMembersNames != null) {
            flags |= BitMap.FAILOVER_GROUP_MEMBER_NAMES;
        }
        if (failOverGroupMembersURLs != null) {
            flags |= BitMap.FAILOVER_GROUP_MEMBER_URLS;
        }
        if (spaceFinderTimeout != DEFAULT_FAILOVER_SPACE_FINDER_TIMEOUT) {
            flags |= BitMap.SPACE_FINDER_TIMEOUT;
        }

        if (m_WriteFOPolicy != null) {
            flags |= BitMap.WRITE_FO_POLICY;
        }
        if (m_ReadFOPolicy != null) {
            flags |= BitMap.READ_FO_POLICY;
        }
        if (m_TakeFOPolicy != null) {
            flags |= BitMap.TAKE_FO_POLICY;
        }
        if (m_NotifyFOPolicy != null) {
            flags |= BitMap.NOTIFY_FO_POLICY;
        }
        if (m_DefaultFOPolicy != null) {
            flags |= BitMap.DEFAULT_FO_POLICY;
        }
        if (isFailBackEnabled) {
            flags |= BitMap.FAILBACK_ENABLED;
        }
        if (_activeElectConfig != null) {
            flags |= BitMap.ACTIVE_ELECTION_CONFIG;
        }
        out.writeInt(flags);
        if (_numberOfBackups != 0) {
            out.writeInt(_numberOfBackups);
        }

        if (failOverGroupName != null) {
            out.writeObject(failOverGroupName);
        }
        if (failOverGroupMembersNames != null) {
            out.writeInt(failOverGroupMembersNames.size());
            for (String s : failOverGroupMembersNames) {
                out.writeObject(s);
            }
        }
        if (failOverGroupMembersURLs != null) {
            out.writeInt(failOverGroupMembersURLs.size());
            for (SpaceURL u : failOverGroupMembersURLs) {
                out.writeObject(u);
            }
        }
        if (spaceFinderTimeout != DEFAULT_FAILOVER_SPACE_FINDER_TIMEOUT) {
            out.writeLong(spaceFinderTimeout);
        }

        if (m_WriteFOPolicy != null) {
            m_WriteFOPolicy.writeExternal(out);
        }
        if (m_ReadFOPolicy != null) {
            m_ReadFOPolicy.writeExternal(out);
        }
        if (m_TakeFOPolicy != null) {
            m_TakeFOPolicy.writeExternal(out);
        }
        if (m_NotifyFOPolicy != null) {
            m_NotifyFOPolicy.writeExternal(out);
        }
        if (m_DefaultFOPolicy != null) {
            m_DefaultFOPolicy.writeExternal(out);
        }
        if (_activeElectConfig != null) {
            _activeElectConfig.writeExternal(out);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int flags = in.readInt();
        if ((flags & BitMap.NUMBER_OF_BACKUPS) != 0) {
            _numberOfBackups = in.readInt();
        } else {
            _numberOfBackups = 0;
        }

        if ((flags & BitMap.FAILOVER_GROUP_NAME) != 0) {
            failOverGroupName = (String) in.readObject();
        }
        if ((flags & BitMap.FAILOVER_GROUP_MEMBER_NAMES) != 0) {
            int size = in.readInt();
            failOverGroupMembersNames = new ArrayList<String>(size);
            for (int i = 0; i < size; i++) {
                failOverGroupMembersNames.add((String) in.readObject());
            }
        }
        if ((flags & BitMap.FAILOVER_GROUP_MEMBER_URLS) != 0) {
            int size = in.readInt();
            failOverGroupMembersURLs = new ArrayList<SpaceURL>(size);
            for (int i = 0; i < size; i++) {
                failOverGroupMembersURLs.add((SpaceURL) in.readObject());
            }
        }
        if ((flags & BitMap.SPACE_FINDER_TIMEOUT) != 0) {
            spaceFinderTimeout = in.readLong();
        } else {
            spaceFinderTimeout = DEFAULT_FAILOVER_SPACE_FINDER_TIMEOUT;
        }

        if ((flags & BitMap.WRITE_FO_POLICY) != 0) {
            m_WriteFOPolicy = new FailOverPolicyDescription();
            m_WriteFOPolicy.readExternal(in);
        }
        if ((flags & BitMap.READ_FO_POLICY) != 0) {
            m_ReadFOPolicy = new FailOverPolicyDescription();
            m_ReadFOPolicy.readExternal(in);
        }
        if ((flags & BitMap.TAKE_FO_POLICY) != 0) {
            m_TakeFOPolicy = new FailOverPolicyDescription();
            m_TakeFOPolicy.readExternal(in);
        }
        if ((flags & BitMap.NOTIFY_FO_POLICY) != 0) {
            m_NotifyFOPolicy = new FailOverPolicyDescription();
            m_NotifyFOPolicy.readExternal(in);
        }
        if ((flags & BitMap.DEFAULT_FO_POLICY) != 0) {
            m_DefaultFOPolicy = new FailOverPolicyDescription();
            m_DefaultFOPolicy.readExternal(in);
        }
        isFailBackEnabled = (flags & BitMap.FAILBACK_ENABLED) != 0;
        if ((flags & BitMap.ACTIVE_ELECTION_CONFIG) != 0) {
            _activeElectConfig = new ActiveElectionConfig();
            _activeElectConfig.readExternal(in);
        }
    }

    // default constractor
    public FailOverPolicy() {
        spaceFinderTimeout = DEFAULT_FAILOVER_SPACE_FINDER_TIMEOUT;
    }

    public static class FailOverPolicyDescription implements SmartExternalizable {
        private static final long serialVersionUID = -2885295494848244093L;

        public int m_PolicyType = -1;      // policy type
        // the following fields are relevant only for policy type = POLICY_TYPE_FAIL_TO_BACKUP
        // the following list contains all the members which are back-up only, not
        // active participants in the load balancing of the operation
        public List<String> m_BackupOnly;
        // the following hashcontains for each member a list of its backup members
        public HashMap<String, List<String>> m_BackupMemberNames; // key=member-name value=list of backup members

        private interface BitMap {
            byte POLICY_TYPE = 1 << 0;
            byte BACKUP_ONLY = 1 << 3;
            byte BACKUP_MEMBER_NAMES = 1 << 4;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            byte flags = 0;
            if (m_PolicyType != -1) {
                flags |= BitMap.POLICY_TYPE;
            }

            if (m_BackupOnly != null) {
                flags |= BitMap.BACKUP_ONLY;
            }
            if (m_BackupMemberNames != null) {
                flags |= BitMap.BACKUP_MEMBER_NAMES;
            }
            out.writeByte(flags);

            if (m_PolicyType != -1) {
                out.writeInt(m_PolicyType);
            }
            if (m_BackupOnly != null) {
                out.writeInt(m_BackupOnly.size());
                for (String s : m_BackupOnly) {
                    out.writeObject(s);
                }
            }
            if (m_BackupMemberNames != null) {
                out.writeInt(m_BackupMemberNames.size());
                for (Map.Entry<String, List<String>> entry : m_BackupMemberNames.entrySet()) {
                    out.writeObject(entry.getKey());
                    out.writeInt(entry.getValue().size());
                    for (String s : entry.getValue()) {
                        out.writeObject(s);
                    }
                }
            }
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            byte flags = in.readByte();
            if ((flags & BitMap.POLICY_TYPE) != 0) {
                m_PolicyType = in.readInt();
            } else {
                m_PolicyType = -1;
            }

            if ((flags & BitMap.BACKUP_ONLY) != 0) {
                int size = in.readInt();
                m_BackupOnly = new ArrayList<String>(size);
                for (int i = 0; i < size; i++) {
                    m_BackupOnly.add((String) in.readObject());
                }
            }
            if ((flags & BitMap.BACKUP_MEMBER_NAMES) != 0) {
                int size = in.readInt();
                m_BackupMemberNames = new HashMap<String, List<String>>();
                for (int i = 0; i < size; i++) {
                    String key = (String) in.readObject();
                    int listSize = in.readInt();
                    List<String> value = new ArrayList<String>(listSize);
                    for (int j = 0; j < listSize; j++) {
                        value.add((String) in.readObject());
                    }
                    m_BackupMemberNames.put(key, value);
                }
            }
        }

        @Override
        public String toString() {
            StringBuffer strBuffer = new StringBuffer();
            strBuffer.append("\n------------FailOverPolicyDescription------\n");
            strBuffer.append("Policy type -\t" + ClusterXML.getFailOverPolicies().get(m_PolicyType) + "\n");
            strBuffer.append("Backup only members -\t" + m_BackupOnly + "\n");
            strBuffer.append("Backup members names -\t" + m_BackupMemberNames + "\n");

            return strBuffer.toString();
        }
    }

    public boolean isFailBackEnabled() {
        return isFailBackEnabled;
    }

    public void setFailBackEnabled(boolean isFailBackEnabled) {
        this.isFailBackEnabled = isFailBackEnabled;
    }

    @Override
    public String toString() {
        StringBuffer strBuffer = new StringBuffer();
        strBuffer.append("\n------------ FailOver Policy ------\n");
        strBuffer.append("Number of backups -\t" + _numberOfBackups + "\n");
        strBuffer.append("FailOver group name -\t" + failOverGroupName + "\n");
        strBuffer.append("Space finder timeout -\t" + spaceFinderTimeout + "\n");
        strBuffer.append("FailOver members name -\t" + failOverGroupMembersNames + "\n");
        strBuffer.append("FailOver members URL -\t" + failOverGroupMembersURLs + "\n");
        strBuffer.append("Is fail back enabled -\t" + isFailBackEnabled + "\n");
        strBuffer.append("Write FO Policy -\t" + m_WriteFOPolicy + "\n");
        strBuffer.append("Read FO Policy -\t" + m_ReadFOPolicy + "\n");
        strBuffer.append("Take FO Policy -\t" + m_TakeFOPolicy + "\n");
        strBuffer.append("Notify FO Policy -\t" + m_NotifyFOPolicy + "\n");
        strBuffer.append("Default FO Policy -\t" + m_DefaultFOPolicy + "\n");

        return strBuffer.toString();
    }


    /**
     * Build all the election groups in fail-over group. Election group is a group of spaces that
     * contains the primary space and all of its backup-only members
     */
    public void buildElectionGroups(String spaceName) {
        if ((_electionGroups = buildElectionGroups(m_WriteFOPolicy)) == null &&
                (_electionGroups = buildElectionGroups(m_TakeFOPolicy)) == null &&
                (_electionGroups = buildElectionGroups(m_ReadFOPolicy)) == null &&
                (_electionGroups = buildElectionGroups(m_NotifyFOPolicy)) == null &&
                (_electionGroups = buildElectionGroups(m_DefaultFOPolicy)) == null) {
            return;
        }

        for (Map.Entry<String, List<String>> entry : _electionGroups.entrySet()) {
            String primary = entry.getKey();
            List<String> electionGroup = entry.getValue();

            // Set the election group of given space
            if (electionGroup.contains(spaceName))
                _electionGroupName = primary;
        }
    }


    /**
     * Build all the election groups in fail-over group. Election group is a group of spaces that
     * contains the primary space and all of its backup-only members
     */
    private HashMap<String, List<String>> buildElectionGroups(FailOverPolicyDescription polDesc) {
        // Election groups can be built only for groups that have backup only members
        if (polDesc == null || polDesc.m_BackupOnly == null || polDesc.m_BackupOnly.isEmpty() || polDesc.m_BackupMemberNames == null)
            return null;

        HashMap<String, List<String>> electionGroups = new HashMap<String, List<String>>();

        for (Iterator<Map.Entry<String, List<String>>> iter = polDesc.m_BackupMemberNames.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, List<String>> entry = iter.next();
            List<String> backupOnly = new LinkedList<String>();
            String primary = entry.getKey();

            for (Iterator<String> iterator = entry.getValue().iterator(); iterator.hasNext(); ) {
                String backup = iterator.next();

                if (polDesc.m_BackupOnly.contains(backup)) {
                    backupOnly.add(backup);
                }
            }

            if (backupOnly.size() > 0) {
                backupOnly.add(primary);
                electionGroups.put(primary, backupOnly);
            }
        }

        return electionGroups;
    }

    /**
     * @return the election group of give space
     */
    public String getElectionGroupName() {
        return _electionGroupName;
    }

    /**
     * Set active election configuration.
     */
    public void setActiveElectionConfig(ActiveElectionConfig activeElectConfig) {
        _activeElectConfig = activeElectConfig;
    }

    /**
     * @return active election configuration
     */
    public ActiveElectionConfig getActiveElectionConfig() {
        return _activeElectConfig != null ? _activeElectConfig : new ActiveElectionConfig() /*default*/;
    }


    /**
     * Returns a list of URLs or an empty list of primary  targets for given member.
     *
     * @param memberName member that looks for a list of primary targets.
     * @return a list of URLs or an empty list of primary targets. Never returns null.
     */
    public List<SpaceURL> getRecoverableTargets(String memberName) {
        LinkedList<SpaceURL> primaryTargets = new LinkedList<SpaceURL>();


        List<String> members = getElectionGroupMembers();

        for (int indexOfMemberInList = 0; indexOfMemberInList < failOverGroupMembersNames.size(); ++indexOfMemberInList) {
            String transmissionSource = failOverGroupMembersNames.get(indexOfMemberInList);

            if (transmissionSource.equals(memberName))
                continue; //skip transmission to self

            if (!members.contains(transmissionSource))
                continue;

            // extract spaceURL (clone to prevent direct reference change)
            SpaceURL remoteSpaceURL = (SpaceURL) failOverGroupMembersURLs.get(indexOfMemberInList)
                    .clone();

            if (!isFailBackEnabled())
                remoteSpaceURL.setElectionState(ActiveElectionState.State.ACTIVE.name());

            primaryTargets.add(remoteSpaceURL);
        }

        return primaryTargets;
    }

    /**
     * Get election group members
     *
     * @return list of group members
     */
    public List<String> getElectionGroupMembers() {
        if (_electionGroups == null)
            return new LinkedList<String>();

        List<String> members = new LinkedList<String>(_electionGroups.get(_electionGroupName));

        //fail-back=true --> make sure primary space is used  first (same name as the election group)
        if (isFailBackEnabled()) {
            members.remove(_electionGroupName);
            members.add(0, _electionGroupName);
        }

        return members;
    }
}