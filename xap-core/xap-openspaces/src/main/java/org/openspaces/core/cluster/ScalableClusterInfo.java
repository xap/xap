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
package org.openspaces.core.cluster;

import com.gigaspaces.internal.io.IOUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.LinkedHashSet;

public class ScalableClusterInfo extends ClusterInfo implements Externalizable {
    private static final long serialVersionUID = 1L;

    private short generation;
    private Collection<Integer> chunks;

    /**
     * Constructs a new cluster info with null values on all the fields
     */
    public ScalableClusterInfo() {
    }



    /**
     * Constructs a new Scalable Cluster info
     *
     * @param schema            The cluster schema
     * @param instanceId        The instance id
     * @param backupId          The backupId (can be <code>null</code>)
     * @param numberOfInstances Number of instances
     * @param numberOfBackups   Number Of backups (can be <code>null</code>)
     */
    public ScalableClusterInfo(String schema, Integer instanceId, Integer backupId, Integer numberOfInstances, Integer numberOfBackups) {
        super(schema, instanceId, backupId, numberOfInstances, numberOfBackups);
    }

    @Override
    public boolean supportsHorizontalScale() {
        return true;
    }

    @Override
    public ScalableClusterInfo getScalableClusterInfo() {
        return this;
    }

    public short getGeneration() {
        return generation;
    }

    public void setGeneration(short generation) {
        this.generation = generation;
    }

    public Collection<Integer> getChunks() {
        return chunks;
    }

    public void setChunks(Collection<Integer> chunks) {
        this.chunks = chunks;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, getName());
        IOUtils.writeString(out, getSchema());
        writeNullableInt(out, getInstanceId());
        writeNullableInt(out, getBackupId());
        writeNullableInt(out, getNumberOfInstances());
        writeNullableInt(out, getNumberOfBackups());
        out.writeShort(generation);
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
        setName(IOUtils.readString(in));
        setSchema(IOUtils.readString(in));
        setInstanceId(readNullableInt(in));
        setBackupId(readNullableInt(in));
        setNumberOfInstances(readNullableInt(in));
        setNumberOfBackups(readNullableInt(in));
        this.generation = in.readShort();
        short numOfChunks = in.readShort();
        if (numOfChunks != -1) {
            chunks = new LinkedHashSet<>(numOfChunks);
            for (short i = 0 ; i < numOfChunks ; i++) {
                chunks.add(Integer.valueOf(in.readShort()));
            }
        }
    }

    private static final int INT_NULL_VALUE = -1;

    private static void writeNullableInt(ObjectOutput out, Integer value) throws IOException {
        out.writeInt(value != null ? value.intValue() : INT_NULL_VALUE);
    }

    private static Integer readNullableInt(ObjectInput in) throws IOException {
        int value = in.readInt();
        return value != INT_NULL_VALUE ? Integer.valueOf(value) : null;
    }
}
