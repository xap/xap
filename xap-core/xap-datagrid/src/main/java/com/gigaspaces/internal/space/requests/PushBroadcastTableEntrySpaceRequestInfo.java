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

import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.PUSH_ENTRY;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class PushBroadcastTableEntrySpaceRequestInfo extends BroadcastTableSpaceRequestInfo {
    private static final long serialVersionUID = 1L;
    private IEntryPacket entryPacket;
    private long lease;
    private boolean isUpdate;
    private long timeout;
    private int modifiers;

    public PushBroadcastTableEntrySpaceRequestInfo() {
    }

    public PushBroadcastTableEntrySpaceRequestInfo(IEntryPacket entryPacket, long lease, boolean isUpdate, long timeout, int modifiers) {
        this.entryPacket = entryPacket;
        this.lease = lease;
        this.isUpdate = isUpdate;
        this.timeout = timeout;
        this.modifiers = modifiers;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, entryPacket);
        IOUtils.writeLong(out, lease);
        out.writeBoolean(isUpdate);
        IOUtils.writeLong(out, timeout);
        IOUtils.writeInt(out, modifiers);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        entryPacket = IOUtils.readObject(in);
        lease = IOUtils.readLong(in);
        isUpdate = in.readBoolean();
        timeout = IOUtils.readLong(in);
        modifiers = IOUtils.readInt(in);
    }

    public IEntryPacket getEntry() {
        return entryPacket;
    }

    public long getLease() {
        return lease;
    }

    public boolean isUpdate() {
        return isUpdate;
    }

    public int getModifiers() {
        return modifiers;
    }

    public long getTimeout() {
        return timeout;
    }

    @Override
    public Action getAction() {
        return PUSH_ENTRY;
    }
}
