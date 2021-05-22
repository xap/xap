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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.cluster.RedoLogCapacityExceededPolicy;
import com.j_spaces.core.cluster.ReplicationPolicy;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * mirror service configuration
 */
final public class MirrorServiceConfig implements SmartExternalizable {
    private static final long serialVersionUID = 2L;

    private static final Long DEFAULT_MIRROR_REDO_LOG_CAPACITY = null;

    /**
     * mirror service definition
     */
    public SpaceURL serviceURL;
    public String memberName;
    public int bulkSize = 100;
    public long intervalMillis = 2000;
    public int intervalOpers = 100;
    public RedoLogCapacityExceededPolicy onRedoLogCapacityExceeded = ReplicationPolicy.DEFAULT_REDO_LOG_CAPACITY_EXCEEDED;
    public Long maxRedoLogCapacity = DEFAULT_MIRROR_REDO_LOG_CAPACITY;
    public boolean supportsPartialUpdate = false;
    public Set<String> supportedChangeOperations = null;

    private interface BitMap {
        byte SERVICE_URL = 1 << 0;
        byte MEMBER_NAME = 1 << 1;
        byte BULK_SIZE = 1 << 2;
        byte INTERVAL_MILLIS = 1 << 3;
        byte INTERVAL_OPERS = 1 << 4;
        byte ON_REDO_LOG_CAPACITY_EXCEEDED = 1 << 5;
        byte MAX_REDO_LOG_CAPACITY = 1 << 6;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        byte flags = 0;
        if (serviceURL != null)
            flags |= BitMap.SERVICE_URL;
        if (memberName != null)
            flags |= BitMap.MEMBER_NAME;
        if (bulkSize != ReplicationPolicy.DEFAULT_REPL_CHUNK_SIZE)
            flags |= BitMap.BULK_SIZE;
        if (intervalMillis != ReplicationPolicy.DEFAULT_REPL_INTERVAL_MILLIS)
            flags |= BitMap.INTERVAL_MILLIS;
        if (intervalOpers != ReplicationPolicy.DEFAULT_REPL_INTERVAL_OPERS)
            flags |= BitMap.INTERVAL_OPERS;
        if (onRedoLogCapacityExceeded != ReplicationPolicy.DEFAULT_REDO_LOG_CAPACITY_EXCEEDED)
            flags |= BitMap.ON_REDO_LOG_CAPACITY_EXCEEDED;
        if (maxRedoLogCapacity != DEFAULT_MIRROR_REDO_LOG_CAPACITY)
            flags |= BitMap.MAX_REDO_LOG_CAPACITY;

        out.writeByte(flags);

        if (serviceURL != null)
            serviceURL.writeExternal(out);
        if (memberName != null)
            out.writeUTF(memberName);
        if (bulkSize != ReplicationPolicy.DEFAULT_REPL_CHUNK_SIZE)
            out.writeInt(bulkSize);
        if (intervalMillis != ReplicationPolicy.DEFAULT_REPL_INTERVAL_MILLIS)
            out.writeLong(intervalMillis);
        if (intervalOpers != ReplicationPolicy.DEFAULT_REPL_INTERVAL_OPERS)
            out.writeInt(intervalOpers);
        if (onRedoLogCapacityExceeded != ReplicationPolicy.DEFAULT_REDO_LOG_CAPACITY_EXCEEDED)
            out.writeObject(onRedoLogCapacityExceeded);
        if (maxRedoLogCapacity != DEFAULT_MIRROR_REDO_LOG_CAPACITY)
            out.writeObject(maxRedoLogCapacity);

        out.writeBoolean(supportsPartialUpdate);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_5_0)) {
            final boolean supportChange = supportedChangeOperations != null;
            out.writeBoolean(supportChange);
            if (supportChange)
                IOUtils.writeStringSet(out, supportedChangeOperations);
        }

    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte flags = in.readByte();
        if ((flags & BitMap.SERVICE_URL) != 0) {
            serviceURL = new SpaceURL();
            serviceURL.readExternal(in);
        }
        if ((flags & BitMap.SERVICE_URL) != 0) {
            memberName = in.readUTF();
        }
        if ((flags & BitMap.BULK_SIZE) != 0) {
            bulkSize = in.readInt();
        } else {
            bulkSize = ReplicationPolicy.DEFAULT_REPL_CHUNK_SIZE;
        }
        if ((flags & BitMap.INTERVAL_MILLIS) != 0) {
            intervalMillis = in.readLong();
        } else {
            intervalMillis = ReplicationPolicy.DEFAULT_REPL_INTERVAL_MILLIS;
        }
        if ((flags & BitMap.INTERVAL_OPERS) != 0) {
            intervalOpers = in.readInt();
        } else {
            intervalOpers = ReplicationPolicy.DEFAULT_REPL_INTERVAL_OPERS;
        }
        if ((flags & BitMap.ON_REDO_LOG_CAPACITY_EXCEEDED) != 0) {
            onRedoLogCapacityExceeded = (RedoLogCapacityExceededPolicy) in.readObject();
        } else {
            onRedoLogCapacityExceeded = ReplicationPolicy.DEFAULT_REDO_LOG_CAPACITY_EXCEEDED;
        }
        if ((flags & BitMap.MAX_REDO_LOG_CAPACITY) != 0) {
            maxRedoLogCapacity = (Long) in.readObject();
        } else {
            maxRedoLogCapacity = DEFAULT_MIRROR_REDO_LOG_CAPACITY;
        }

        supportsPartialUpdate = in.readBoolean();
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_5_0)) {
            final boolean supportsChange = in.readBoolean();
            if (!supportsChange)
                supportedChangeOperations = null;
            else
                supportedChangeOperations = IOUtils.readStringSet(in);
        }
    }

    /**
     * @return true if the targetMember is ~Mirror member
     */
    public boolean isMirrorTarget(String targetMember) {
        return memberName.equals(targetMember);
    }

    @Override
    public String toString() {
        StringBuilder strBuffer = new StringBuilder("-----------MirrorServiceConfig-------------\n");
        strBuffer.append("Service URL -\t" + serviceURL + "\n");
        strBuffer.append("Member Name -\t" + memberName + "\n");
        strBuffer.append("Bulk Size -\t" + bulkSize + "\n");
        strBuffer.append("Interval Millis -\t" + intervalMillis + "\n");
        strBuffer.append("Interval Operations -\t" + intervalOpers + "\n");
        strBuffer.append("Max Redo Log Capacity -\t" + maxRedoLogCapacity + "\n");
        strBuffer.append("On Redo Log Capacity Exceeded -\t" + onRedoLogCapacityExceeded + "\n");
        strBuffer.append("Supports partial update -\t" + supportsPartialUpdate + "\n");

        String supportedChangeOperation = (supportedChangeOperations != null) ? supportedChangeOperations.toString() : "None";

        strBuffer.append("Supported change Operation -\t" + supportedChangeOperation + "\n");

        return strBuffer.toString();
    }
}
