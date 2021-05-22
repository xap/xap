package com.gigaspaces.cluster.replication;

import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.1
 */
public class ReplicationTargetInfo implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    long weight;

    public ReplicationTargetInfo() {
    }

    public ReplicationTargetInfo(long weight) {
        this.weight = weight;
    }

    public long getWeight() {
        return weight;
    }

    public void setWeight(long weight) {
        this.weight = weight;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeLong(weight);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        this.weight = objectInput.readLong();
    }
}
