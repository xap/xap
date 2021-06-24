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
package com.gigaspaces.internal.cluster;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.cluster.DynamicPartitionInfo;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * @author Niv Ingberg
 * @since 15.5
 */
@InternalApi
public class DynamicPartitionInfoImpl implements DynamicPartitionInfo, SmartExternalizable {
    private static final long serialVersionUID = 1L;

    private Collection<Integer> chunks;

    public DynamicPartitionInfoImpl() {
    }

    public DynamicPartitionInfoImpl(Collection<Integer> chunks) {
        this.chunks = chunks;
    }

    @Override
    public Collection<Integer> getChunks() {
        return chunks;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (chunks == null)
            out.writeShort(-1);
        else {
            out.writeShort(chunks.size());
            for (Integer chunk : chunks) {
                out.writeShort(chunk);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        short numOfChunks = in.readShort();
        if (numOfChunks != -1) {
            chunks = new LinkedHashSet<>(numOfChunks);
            for (short i = 0 ; i < numOfChunks ; i++) {
                chunks.add((int) in.readShort());
            }
        }
    }
}
