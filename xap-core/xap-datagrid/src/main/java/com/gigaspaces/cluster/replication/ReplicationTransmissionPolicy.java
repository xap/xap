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

package com.gigaspaces.cluster.replication;

import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.cluster.ReplicationPolicy.ReplicationPolicyDescription;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This class describes the transmission policy from source space to a target space.
 *
 * @author Yechiel Fefer
 * @version 1.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationTransmissionPolicy
        implements Cloneable, SmartExternalizable {
    private static final long serialVersionUID = 2L;

    final static public char REPLICATION_TRANSMISSION_OPERATION_WRITE = 'W'; //includes lease-extend
    final static public char REPLICATION_TRANSMISSION_OPERATION_REMOVE = 'T'; //includes take
    final static public char REPLICATION_TRANSMISSION_OPERATION_NOTIFY = 'N';

    public static final String OPER_WRITE = "write";
    public static final String OPER_TAKE = "take";
    public static final String OPER_NOTIFY = "notify";


    public String m_SourceSpace;
    public String m_TargetSpace;
    // a combination of REPLICATION_TRANSMISSION_OPERATION_XXX values.
    // if null- all allowed
    public String m_RepTransmissionOperations;
    public boolean m_SyncOnCommit;
    public boolean m_DisableTransmission; //if true, transmission disabled

    /**
     * <code>true</code> if sync replication to target member enabled, otherwise <code>false</code>
     */
    private boolean _isSyncReplication = false;

    /**
     * fast one way replication
     */
    private boolean _isOneWayReplication = false;

    /**
     * true if this transmission policy represents mirror connection
     */
    private boolean _mirrorConnector;

    private interface BitMap {
        int TRANSMISSION_OPERATIONS = 1 << 0;
        int SYNC_ON_COMMIT = 1 << 1;
        int DISABLE_TRANSMISSION = 1 << 2;
        int SYNC_REPLICATION = 1 << 3;
        int ONE_WAY_REPLICATION = 1 << 4;
        //int PROTOCOL_ADAPTER_CLASS = 1 << 5;
        int MIRROR_CONNECTOR = 1 << 6;
        int SOURCE_SPACE = 1 << 7;
        int TARGET_SPACE = 1 << 8;
        //int COMMUNICATION_MODE = 1 << 9;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        int flags = 0;
        if (m_RepTransmissionOperations != null) {
            flags |= BitMap.TRANSMISSION_OPERATIONS;
        }
        if (m_SyncOnCommit) {
            flags |= BitMap.SYNC_ON_COMMIT;
        }
        if (m_DisableTransmission) {
            flags |= BitMap.DISABLE_TRANSMISSION;
        }
        if (_isSyncReplication) {
            flags |= BitMap.SYNC_REPLICATION;
        }
        if (_isOneWayReplication) {
            flags |= BitMap.ONE_WAY_REPLICATION;
        }
        if (_mirrorConnector) {
            flags |= BitMap.MIRROR_CONNECTOR;
        }
        if (m_SourceSpace != null) {
            flags |= BitMap.SOURCE_SPACE;
        }
        if (m_TargetSpace != null) {
            flags |= BitMap.TARGET_SPACE;
        }
        out.writeInt(flags);

        if (m_RepTransmissionOperations != null) {
            out.writeUTF(m_RepTransmissionOperations);
        }
        if (m_SourceSpace != null) {
            out.writeUTF(m_SourceSpace);
        }
        if (m_TargetSpace != null) {
            out.writeUTF(m_TargetSpace);
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        int flags = in.readInt();

        if ((flags & BitMap.TRANSMISSION_OPERATIONS) != 0) {
            m_RepTransmissionOperations = in.readUTF();
        }
        if ((flags & BitMap.SOURCE_SPACE) != 0) {
            m_SourceSpace = in.readUTF();
        }
        if ((flags & BitMap.TARGET_SPACE) != 0) {
            m_TargetSpace = in.readUTF();
        }

        m_SyncOnCommit = ((flags & BitMap.SYNC_ON_COMMIT) != 0);
        m_DisableTransmission = ((flags & BitMap.DISABLE_TRANSMISSION) != 0);
        _isSyncReplication = ((flags & BitMap.SYNC_REPLICATION) != 0);
        _isOneWayReplication = ((flags & BitMap.ONE_WAY_REPLICATION) != 0);
        _mirrorConnector = ((flags & BitMap.MIRROR_CONNECTOR) != 0);
    }

    /**
     * Constructor just for externlizable. Should not be used.
     */
    public ReplicationTransmissionPolicy() {
    }

    public ReplicationTransmissionPolicy(String source, String target,
                                         String operations, boolean SyncOnCommit,
                                         boolean disableTransmission) {
        m_SourceSpace = source;
        m_TargetSpace = target;
        m_RepTransmissionOperations = operations;
        m_SyncOnCommit = SyncOnCommit;
        m_DisableTransmission = disableTransmission;
    }

    public ReplicationTransmissionPolicy(String source, String target,
                                         String operations, boolean SyncOnCommit, //String replicationMode,
                                         String communicationMode, boolean disableTransmission) {
        m_SourceSpace = source;
        m_TargetSpace = target;
        m_RepTransmissionOperations = operations;
        m_SyncOnCommit = SyncOnCommit;

        m_DisableTransmission = disableTransmission;
    }

    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException ex) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }

    public ReplicationTransmissionPolicy(String source, String target) {
        m_SourceSpace = source;
        m_TargetSpace = target;
    }

    public ReplicationTransmissionPolicy(String source, String target, ReplicationPolicy replPolicy) {
        m_SourceSpace = source;
        m_TargetSpace = target;
        m_SyncOnCommit = replPolicy.m_SyncOnCommit;

        /** check if this target is ~mirror target */
        if (replPolicy.getMirrorServiceConfig() != null && replPolicy.getMirrorServiceConfig().isMirrorTarget(target)) {
            setupMirror(target, replPolicy);
        } else {
            setOneWayReplication(replPolicy.isOneWayReplication);
      
      /*
       * determine if a sync or async target
       */
            ReplicationPolicyDescription replicationPolicyDescription = replPolicy.m_ReplMemberPolicyDescTable.get(source);
            ReplicationTransmissionPolicy transmissionMatrix = replicationPolicyDescription.getTargetTransmissionMatrix(target);
            if (transmissionMatrix == null)
                setSyncReplication(replPolicy.m_IsSyncReplicationEnabled);
            else
                setSyncReplication(transmissionMatrix.isSyncReplication());
        }
    }


    /**
     * setup mirror connector
     */
    private void setupMirror(String targetName, ReplicationPolicy replPolicy) {
        /** the mirror service always async-repl */
        setSyncReplication(false);
        setMirrorConnector(true);
    }


    public String getSourceName() {
        return m_SourceSpace;
    }

    public String getTargetName() {
        return m_TargetSpace;
    }

    public void setOneWayReplication(boolean isOneWay) {
        _isOneWayReplication = isOneWay;
    }

    public boolean isOneWayReplication() {
        return _isOneWayReplication;
    }

    /**
     * @return Returns the isSyncReplication.
     */
    public boolean isSyncReplication() {
        return _isSyncReplication;
    }

    /**
     * @param isSyncReplication The isSyncReplication to set.
     */
    public void setSyncReplication(boolean isSyncReplication) {
        this._isSyncReplication = isSyncReplication;
    }

    public void setMirrorConnector(boolean isMirrorConnector) {
        _mirrorConnector = isMirrorConnector;
    }

    public boolean isMirrorConnector() {
        return _mirrorConnector;
    }

    public String toString() {
        String str = "[ Source: " + m_SourceSpace
                + ", Target: " + m_TargetSpace
                + ", RepTransmissionOperations: " + m_RepTransmissionOperations
                + ", SyncOnCommit: " + m_SyncOnCommit
                + ", DisableTransmission: " + m_DisableTransmission
                + ", isSyncReplication: " + _isSyncReplication + " ]";
        return str;
    }
}