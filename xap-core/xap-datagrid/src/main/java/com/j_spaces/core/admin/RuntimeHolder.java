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

package com.j_spaces.core.admin;

import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.serialization.SmartExternalizable;
import com.gigaspaces.server.space.suspend.SuspendInfo;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.core.ISpaceState;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Space runtime state holds the state of the space instace, for example, primary backup and
 * replication status.
 *
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class RuntimeHolder implements SmartExternalizable {

    static final long serialVersionUID = -6308647788360370371L;

    private SpaceMode spaceMode;

    private Object[] replicationStatus = null;

    private Integer spaceState = null;

    private SuspendInfo suspendInfo;

    public RuntimeHolder() {
    }

    public RuntimeHolder(SpaceMode spaceMode, Object[] replicationStatus, int spaceState, SuspendInfo suspendInfo) {
        this.spaceMode = spaceMode;
        this.replicationStatus = replicationStatus;
        this.spaceState = spaceState;
        this.suspendInfo = suspendInfo;
    }

    public SpaceMode getSpaceMode() {
        return spaceMode;
    }

    public Object[] getReplicationStatus() {
        return replicationStatus;
    }

    /**
     * @return ISpaceState or null if working against old XAP client.
     * @see ISpaceState
     */
    public Integer getSpaceState() {
        return spaceState;
    }

    public SuspendInfo getSuspendInfo() {
        return suspendInfo;
    }

    public void setSuspendInfo(SuspendInfo suspendInfo) {
        this.suspendInfo = suspendInfo;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(spaceMode);
        if (replicationStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeObject(replicationStatus);
        }

        if (spaceState == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeInt(spaceState);
        }

        PlatformLogicalVersion version = PlatformLogicalVersion.getLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v14_0_0)) {
            if (suspendInfo == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeObject(suspendInfo);
            }
        }
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        spaceMode = (SpaceMode) in.readObject();
        if (in.readBoolean()) {
            replicationStatus = (Object[]) in.readObject();
        }

        if (in.readBoolean()) {
            spaceState = in.readInt();
        }

        PlatformLogicalVersion version = PlatformLogicalVersion.getLogicalVersion();
        if (version.greaterOrEquals(PlatformLogicalVersion.v14_0_0)) {
            if (in.readBoolean()) {
                suspendInfo = (SuspendInfo) in.readObject();
            }
        }
    }
}
