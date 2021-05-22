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
