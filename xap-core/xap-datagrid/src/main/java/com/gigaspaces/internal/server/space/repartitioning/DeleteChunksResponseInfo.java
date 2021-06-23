package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.atomic.AtomicInteger;

public class DeleteChunksResponseInfo implements SpaceResponseInfo {
    private static final long serialVersionUID = 131713841051965917L;
    private int partitionId;
    private volatile IOException exception;
    private AtomicInteger deleted;


    @SuppressWarnings("WeakerAccess")
    public DeleteChunksResponseInfo() {
    }

    DeleteChunksResponseInfo(int partitionId) {
        this.partitionId = partitionId;
        this.deleted = new AtomicInteger(0);
    }

    public IOException getException() {
        return exception;
    }

    public void setException(IOException exception) {
        this.exception = exception;
    }

    public AtomicInteger getDeleted() {
        return deleted;
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
        IOUtils.writeInt(out, deleted.get());
        IOUtils.writeObject(out, exception);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.partitionId = IOUtils.readInt(in);
        this.deleted = new AtomicInteger(IOUtils.readInt(in));
        this.exception = IOUtils.readObject(in);
    }
}
