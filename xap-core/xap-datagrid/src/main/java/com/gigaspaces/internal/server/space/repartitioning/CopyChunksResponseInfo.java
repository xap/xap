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

    CopyChunksResponseInfo(Set<Integer> keys) {
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
