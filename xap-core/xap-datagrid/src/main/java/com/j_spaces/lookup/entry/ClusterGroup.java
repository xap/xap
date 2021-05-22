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

package com.j_spaces.lookup.entry;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

@com.gigaspaces.api.InternalApi
public class ClusterGroup extends com.j_spaces.lookup.entry.GenericEntry implements SmartExternalizable {
    private static final long serialVersionUID = -3820632312122408764L;

    public String electionGroup;
    public String replicationGroup;
    public String loadBalancingGroup;

    public ClusterGroup() {
    }

    public ClusterGroup(String electionGroup, String replicationGroup, String loadBalancingGroup) {
        super();
        if (electionGroup != null)
            this.electionGroup = electionGroup.intern();

        this.replicationGroup = replicationGroup;
        this.loadBalancingGroup = loadBalancingGroup;
    }

    public ClusterGroup(String replicationGroup) {
        // To keep backward compatibility
        this.replicationGroup = replicationGroup;
    }

    public net.jini.core.entry.Entry fromString(String group) {
        return new ClusterGroup(group);
    }

    private interface BitMap {
        byte ELECTION_GROUP = 1 << 0;
        byte REPLICATION_GROUP = 1 << 1;
        byte LOAD_BALANCING_GROUP = 1 << 2;
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        final byte flags = buildFlags();
        out.writeByte(flags);

        if (flags == 0)
            return;

        if (electionGroup != null)
            out.writeObject(electionGroup);
        if (replicationGroup != null)
            out.writeObject(replicationGroup);
        if (loadBalancingGroup != null)
            out.writeObject(loadBalancingGroup);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final byte flags = in.readByte();
        if (flags == 0)
            return;

        if ((flags & BitMap.ELECTION_GROUP) != 0)
            electionGroup = (String) in.readObject();
        if ((flags & BitMap.REPLICATION_GROUP) != 0)
            replicationGroup = (String) in.readObject();
        if ((flags & BitMap.LOAD_BALANCING_GROUP) != 0)
            loadBalancingGroup = (String) in.readObject();
    }

    private byte buildFlags() {
        byte flags = 0;
        if (electionGroup != null)
            flags |= BitMap.ELECTION_GROUP;
        if (replicationGroup != null)
            flags |= BitMap.REPLICATION_GROUP;
        if (loadBalancingGroup != null)
            flags |= BitMap.LOAD_BALANCING_GROUP;
        return flags;
    }
}
