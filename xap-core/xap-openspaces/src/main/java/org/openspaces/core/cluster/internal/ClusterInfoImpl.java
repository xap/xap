package org.openspaces.core.cluster.internal;

import com.gigaspaces.internal.io.IOUtils;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoBuilder;
import org.openspaces.core.cluster.DynamicPartitionInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ClusterInfoImpl extends ClusterInfo implements Externalizable {
    private static final long serialVersionUID = 1L;

    private int generation;
    private DynamicPartitionInfo dynamicPartitionInfo;

    /**
     * Constructs a new cluster info with null values on all the fields
     */
    public ClusterInfoImpl() {
    }

    public ClusterInfoImpl(ClusterInfoBuilder builder) {
        super(builder);
        this.generation = builder.getGeneration();
        this.dynamicPartitionInfo = builder.getDynamicPartitionInfo();
    }

    @Override
    public DynamicPartitionInfo getDynamicPartitionInfo() {
        return dynamicPartitionInfo;
    }

    @Override
    public int getGeneration() {
        return generation;
    }

    @Override
    public ClusterInfo copy(){
        ClusterInfoImpl copy = new ClusterInfoImpl();
        copy.generation = this.generation;
        copy.dynamicPartitionInfo = this.dynamicPartitionInfo;
        return copy(copy);
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
        IOUtils.writeObject(out, dynamicPartitionInfo);
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
        this.dynamicPartitionInfo = IOUtils.readObject(in);
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
