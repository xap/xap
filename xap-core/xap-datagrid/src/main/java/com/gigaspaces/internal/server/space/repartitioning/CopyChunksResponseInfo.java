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
package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class CopyChunksResponseInfo implements SpaceResponseInfo {

    private int partitionId;
    private volatile IOException exception;
    private Map<Short, AtomicInteger> movedToPartition;


    @SuppressWarnings("WeakerAccess")
    public CopyChunksResponseInfo() {
    }

    CopyChunksResponseInfo(int partitionId, Set<Integer> keys) {
        this.partitionId = partitionId;
        this.movedToPartition = new HashMap<>(keys.size());
        for (int key : keys) {
            this.movedToPartition.put((short) key, new AtomicInteger(0));
        }
    }

    public IOException getException() {
        return exception;
    }

    public void setException(IOException exception) {
        this.exception = exception;
    }

    public Map<Short, AtomicInteger> getMovedToPartition() {
        return movedToPartition;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public String toString() {
        return "CopyChunksResponseInfo{" +
                "partitionId=" + partitionId +
                ", exception=" + exception +
                ", movedToPartition=" + movedToPartition +
                '}';
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeInt(out, partitionId);
        IOUtils.writeObject(out, exception);
        IOUtils.writeShort(out, (short) movedToPartition.size());
        for (Map.Entry<Short, AtomicInteger> entry : movedToPartition.entrySet()) {
            IOUtils.writeShort(out, entry.getKey());
            IOUtils.writeInt(out, entry.getValue().get());
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.partitionId = IOUtils.readInt(in);
        this.exception = IOUtils.readObject(in);
        int size = IOUtils.readShort(in);
        if (size > 0) {
            this.movedToPartition = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                this.movedToPartition.put(IOUtils.readShort(in), new AtomicInteger(IOUtils.readInt(in)));
            }
        }
    }
}
