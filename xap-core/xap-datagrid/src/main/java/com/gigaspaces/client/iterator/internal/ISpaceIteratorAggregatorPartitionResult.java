package com.gigaspaces.client.iterator.internal;

import com.gigaspaces.internal.transport.IEntryPacket;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

public interface ISpaceIteratorAggregatorPartitionResult extends Externalizable {
    List<IEntryPacket> getEntries();

    int getPartitionId();

    @Override
    void writeExternal(ObjectOutput out) throws IOException;

    @Override
    void readExternal(ObjectInput in) throws IOException, ClassNotFoundException;
}
