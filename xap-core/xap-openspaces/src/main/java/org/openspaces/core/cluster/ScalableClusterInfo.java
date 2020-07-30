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

    private int generation;
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

    public void initChunksInfo(int generation, Collection<Integer> chunks) {
        this.generation = generation;
        this.chunks = chunks;
    }

    public int getGeneration() {
        return generation;
    }

    public Collection<Integer> getChunks() {
        return chunks;
    }

    public ScalableClusterInfo copy(){
        ScalableClusterInfo clusterInfo = new ScalableClusterInfo();
        clusterInfo.setBackupId(getBackupId());
        clusterInfo.setInstanceId(getInstanceId());
        clusterInfo.setNumberOfBackups(getNumberOfBackups());
        clusterInfo.setNumberOfInstances(getNumberOfInstances());
        clusterInfo.setSchema(getSchema());
        clusterInfo.setName(getName());
        return clusterInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, getName());
        IOUtils.writeString(out, getSchema());
        writeNullableInt(out, getInstanceId());
        writeNullableInt(out, getBackupId());
        writeNullableInt(out, getNumberOfInstances());
        writeNullableInt(out, getNumberOfBackups());
        out.writeInt(generation);
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
        this.generation = in.readInt();
        short numOfChunks = in.readShort();
        if (numOfChunks != -1) {
            chunks = new LinkedHashSet<>(numOfChunks);
            for (short i = 0 ; i < numOfChunks ; i++) {
                chunks.add((int) in.readShort());
            }
        }
    }

    private static final int INT_NULL_VALUE = -1;

    private static void writeNullableInt(ObjectOutput out, Integer value) throws IOException {
        out.writeInt(value != null ? value : INT_NULL_VALUE);
    }

    private static Integer readNullableInt(ObjectInput in) throws IOException {
        int value = in.readInt();
        return value != INT_NULL_VALUE ? value : null;
    }
}
