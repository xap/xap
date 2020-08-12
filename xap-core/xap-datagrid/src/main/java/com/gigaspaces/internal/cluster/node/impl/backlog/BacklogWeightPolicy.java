package com.gigaspaces.internal.cluster.node.impl.backlog;

import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;

/**
 * @author yael nahon
 * @since 12.1
 */
public interface BacklogWeightPolicy {

    int calculateWeight(IReplicationPacketData<?> data);

    int predictWeightBeforeOperation(OperationWeightInfo info);

    int getDefaultPacketWeight();
}
