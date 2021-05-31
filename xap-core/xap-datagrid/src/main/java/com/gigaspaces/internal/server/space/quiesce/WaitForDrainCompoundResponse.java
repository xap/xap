package com.gigaspaces.internal.server.space.quiesce;

import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.internal.utils.collections.ConcurrentHashSet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class WaitForDrainCompoundResponse implements SpaceResponseInfo {
    private final Map<Integer, Exception> exceptionMap = new ConcurrentHashMap<>();
    private final Set<Integer> successfulPartitions = new ConcurrentHashSet<>();

    public WaitForDrainCompoundResponse() {

    }

    public void addException(int partition, Exception e) {
        exceptionMap.put(partition, e);
    }

    public void addSuccessfulPartition(int partition) {
        successfulPartitions.add(partition);
    }

    public Map<Integer, Exception> getExceptionMap() {
        return exceptionMap;
    }

    public Collection<Integer> getSuccessfulPartitions() {
        return successfulPartitions;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(exceptionMap.size());
        for (Map.Entry<Integer, Exception> entry : exceptionMap.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeObject(entry.getValue());
        }


        out.writeInt(successfulPartitions.size());
        for (Integer partition : successfulPartitions) {
            out.writeInt(partition);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        final int exceptionMapSize = in.readInt();
        for (int i = 0; i < exceptionMapSize; i++) {
            exceptionMap.put(in.readInt(), (Exception) in.readObject());
        }
        final int successfulPartitionsSize = in.readInt();
        for (int i = 0; i < successfulPartitionsSize; i++) {
            successfulPartitions.add(in.readInt());
        }
    }


}
