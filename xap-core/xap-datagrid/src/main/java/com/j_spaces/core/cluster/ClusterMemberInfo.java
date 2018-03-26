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

import com.j_spaces.core.client.SpaceURL;

import net.jini.core.lookup.ServiceID;

import java.io.Serializable;
import java.util.List;
import java.util.Vector;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

public class ClusterMemberInfo
        implements Cloneable {
    public String memberName;
    public SpaceURL memberURL;
    public Vector paramData;
    public boolean isBackup;
    private String clusterName;
    private String groupName;
    private ServiceID _spaceID;
    private boolean _isMirror;

    public ClusterMemberInfo(String memberName, SpaceURL memberUrl,
                             String clusterName, String groupName, ServiceID spaceID, boolean isMirror) {
        this(memberName, memberUrl, clusterName,
                groupName, null, false, spaceID, isMirror);
    }


    public ClusterMemberInfo(String memberName, SpaceURL memberUrl,
                             String clusterName, String groupName,
                             Vector paramData, ServiceID spaceID) {
        this(memberName, memberUrl, clusterName, groupName,
                paramData, false, spaceID, false);
    }

    public ClusterMemberInfo(String memberName, SpaceURL memberUrl,
                             String clusterName, String groupName, Vector paramData,
                             boolean isBackup, ServiceID spaceID, boolean isMirror) {
        this.memberName = memberName;
        this.memberURL = memberUrl;
        this.paramData = paramData;
        this.isBackup = isBackup;
        this.groupName = groupName;
        this.clusterName = clusterName;
        this._spaceID = spaceID;
        this._isMirror = isMirror;
    }

    @Override
    public String toString() {
       /*String str = memberName;
       if( m_replTransmissionPolicies != null )
   		str += " TP:" + m_replTransmissionPolicies.size();
   	if( m_replFilters != null )
   		str += " F";
   	if( m_replRecovery != null )
   		str += " R";
   	return str;
   	*/
        return memberName;
    }

    /**
     * Add in version 2.5, for support group member attributes
     */
    public List m_replTransmissionPolicies;
    public ReplFilters m_replFilters;
    public ReplRecovery m_replRecovery;

    /* Replication member attributes */
    public static class ReplFilters implements Serializable {
        private static final long serialVersionUID = -306798340661866409L;

        public String inputReplicationFilterClassName = "";
        public String inputReplicationFilterParamUrl = "";
        public String outputReplicationFilterClassName = "";
        public String outputReplicationFilterParamUrl = "";

        public ReplFilters(String inputClassName, String inputParamURL,
                           String outputClassName, String outputParamURL) {
            this.inputReplicationFilterClassName = inputClassName;
            this.inputReplicationFilterParamUrl = inputParamURL;
            this.outputReplicationFilterClassName = outputClassName;
            this.outputReplicationFilterParamUrl = outputParamURL;
        }

        public ReplFilters() {
        }
    }

    public static class ReplRecovery implements Serializable {
        private static final long serialVersionUID = -8107984596854514381L;

        public String sourceMemberRecovery; // Source member name for Memory/DB recovery
        public boolean recoveryEnabled;      // enable/disable recovery DB/Memory

        public ReplRecovery(boolean enabled, String sourceMemberRecovery) {
            this.recoveryEnabled = enabled;
            this.sourceMemberRecovery = sourceMemberRecovery;
        }
    }

    // create new replication member attributes
    public void createClusterMemberAttributes(List transmPolicy, ReplFilters filters, ReplRecovery rec) {
        m_replTransmissionPolicies = transmPolicy;
        m_replFilters = filters;
        m_replRecovery = rec;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof ClusterMemberInfo))
            return false;
        else
            return ((ClusterMemberInfo) obj).memberName.equals(memberName);
    }

    @Override
    public int hashCode() {
        return memberName.hashCode();
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ex) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }


    public String getClusterName() {
        return clusterName;
    }

    public boolean isMirror() {
        return _isMirror;
    }

    public String getGroupName() {
        return groupName;
    }


    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }


    public ServiceID getSpaceID() {
        return _spaceID;
    }

    public void setSpaceID(ServiceID spaceServiceID) {
        _spaceID = spaceServiceID;
    }
} /* end class ClusterMemberInfo */