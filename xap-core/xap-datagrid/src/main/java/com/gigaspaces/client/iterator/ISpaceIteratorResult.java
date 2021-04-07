package com.gigaspaces.client.iterator;

import com.gigaspaces.client.iterator.internal.ISpaceIteratorAggregatorPartitionResult;
import com.gigaspaces.internal.transport.IEntryPacket;

public interface ISpaceIteratorResult<T extends ISpaceIteratorAggregatorPartitionResult> {

    void close();

    Iterable<IEntryPacket> getEntries();

    void addPartition(T partitionResult);

    int size();
}
