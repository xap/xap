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

package com.gigaspaces.client.iterator.internal.tiered_storage;

import com.gigaspaces.client.iterator.internal.ISpaceIteratorAggregatorPartitionResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.transport.IEntryPacket;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Yael nahon
 * @since 16.0.0
 */
@com.gigaspaces.api.InternalApi
public class TieredSpaceIteratorAggregatorPartitionResult implements Externalizable, ISpaceIteratorAggregatorPartitionResult {

    private static final long serialVersionUID = 1L;

    private int partitionId;
    private List<IEntryPacket> entries;
    private Map<String, List<String>> uids;

    /**
     * Required for Externalizable
     */
    public TieredSpaceIteratorAggregatorPartitionResult() {
    }

    public TieredSpaceIteratorAggregatorPartitionResult(int partitionId) {
        this.partitionId = partitionId;
        this.entries = new ArrayList<>();
    }


    public List<IEntryPacket> getEntries() {
        return entries;
    }

    @Override
    public void addUID(String typeName, String uid) {
        if (uids == null)
            uids = new HashMap<>();
        uids.computeIfAbsent(typeName, k -> new ArrayList<>()).add(uid);
    }

    public void setEntries(List<IEntryPacket> entries) {
        this.entries = entries;
    }

    public Map<String, List<String>> getUids() {
        return uids;
    }

    public TieredSpaceIteratorAggregatorPartitionResult setUids(Map<String, List<String>> uids) {
        this.uids = uids;
        return this;
    }


    public int getPartitionId() {
        return partitionId;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(partitionId);
        IOUtils.writeList(out, entries);
        out.writeInt(uids == null ? -1 : uids.size());
        if (uids != null) {
            for (Map.Entry<String, List<String>> entry : uids.entrySet()) {
                IOUtils.writeString(out, entry.getKey());
                IOUtils.writeListString(out, entry.getValue());
            }
        }
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        partitionId = in.readInt();
        entries = IOUtils.readList(in);
        int size = in.readInt();
        if (size == -1) {
            return;
        }
        uids = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            uids.put(IOUtils.readString(in), IOUtils.readListString(in));
        }
    }
}
