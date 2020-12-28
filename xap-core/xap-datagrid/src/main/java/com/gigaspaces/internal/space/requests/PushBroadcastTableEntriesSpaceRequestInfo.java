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
package com.gigaspaces.internal.space.requests;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.PUSH_ENTRIES;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class PushBroadcastTableEntriesSpaceRequestInfo extends BroadcastTableSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    private IEntryPacket[] entryPackets;
    private long lease;
    private long[] leases;
    private long timeout;
    private int modifiers;

    public PushBroadcastTableEntriesSpaceRequestInfo() {
    }

    public PushBroadcastTableEntriesSpaceRequestInfo(IEntryPacket[] entryPackets, long lease, long[] leases, long timeout, int modifiers) {
        this.entryPackets = entryPackets;
        this.lease = lease;
        this.leases = leases;
        this.timeout = timeout;
        this.modifiers = modifiers;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObjectArray(out, entryPackets);
        IOUtils.writeLong(out, lease);
        IOUtils.writeLongArray(out, leases);
        IOUtils.writeLong(out, timeout);
        IOUtils.writeInt(out, modifiers);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        entryPackets = IOUtils.readEntryPacketArray(in);
        lease = IOUtils.readLong(in);
        leases = IOUtils.readLongArray(in);
        timeout = IOUtils.readLong(in);
        modifiers = IOUtils.readInt(in);
    }

    public IEntryPacket[] getEntryPackets() {
        return entryPackets;
    }

    public long getLease() {
        return lease;
    }

    public long[] getLeases() {
        return leases;
    }

    public int getModifiers() {
        return modifiers;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public Action getAction() {
        return PUSH_ENTRIES;
    }
}
