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

package com.gigaspaces.client.iterator.internal;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Niv Ingberg
 * @since 10.1
 */
@com.gigaspaces.api.InternalApi
public class SpaceIteratorAggregatorPartitionResult implements SmartExternalizable,ISpaceIteratorAggregatorPartitionResult{

    private static final long serialVersionUID = 1L;

    private int partitionId;
    private List<IEntryPacket> entries;
    private List<String> uids;

    /**
     * Required for Externalizable
     */
    public SpaceIteratorAggregatorPartitionResult() {
    }

    public SpaceIteratorAggregatorPartitionResult(int partitionId) {
        this.partitionId = partitionId;
        this.entries = new ArrayList<IEntryPacket>();
    }

    public List<IEntryPacket> getEntries() {
        return entries;
    }

    @Override
    public void addUID(String typeName, String uid) {
        if (uids == null)
            uids= new ArrayList<>();
        uids.add(uid);
    }

    public void setEntries(List<IEntryPacket> entries) {
        this.entries = entries;
    }

    public List<String> getUids() {
        return uids;
    }

    public void setUids(List<String> uids) {
        this.uids = uids;
    }

    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(partitionId);
        IOUtils.writeList(out, entries);
        IOUtils.writeListString(out, uids);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.partitionId = in.readInt();
        this.entries = IOUtils.readList(in);
        this.uids = IOUtils.readListString(in);
    }
}
