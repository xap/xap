package com.gigaspaces.client.iterator.internal;

import com.gigaspaces.internal.transport.IEntryPacket;

import java.io.Serializable;
import java.util.List;

public interface ISpaceIteratorAggregatorPartitionResult extends Serializable {
    List<IEntryPacket> getEntries();
    void addUID(String typeName, String uid);
}
