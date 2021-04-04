package com.gigaspaces.client.iterator.internal;

import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.UidQueryPacket;

import java.util.List;

public interface ISpaceIteratorResult {
    void addPartition(ISpaceIteratorAggregatorPartitionResult partitionResult);

    List<IEntryPacket> getEntries();

    void close();

    UidQueryPacket buildQueryPacket(ISpaceProxy spaceProxy, int batchSize, QueryResultTypeInternal resultType);

    int size();
}
