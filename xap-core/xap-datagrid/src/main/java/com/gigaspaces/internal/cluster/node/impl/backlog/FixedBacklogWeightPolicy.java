package com.gigaspaces.internal.cluster.node.impl.backlog;

import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;

/**
 * @author yael nahon
 * @since 12.1
 */
public class FixedBacklogWeightPolicy implements BacklogWeightPolicy {

    private final int defaultPacketWeight = 1;

    @Override
    public int calculateWeight(IReplicationPacketData<?> data) {
        return 1;
    }

    @Override
    public int predictWeightBeforeOperation(OperationWeightInfo info) {
        if(info.getNumOfOperations() > 1 && info.getType() != WeightInfoOperationType.PREPARE){
            return info.getNumOfOperations();
        }
        else
            return defaultPacketWeight;
    }

    @Override
    public int getDefaultPacketWeight() {
        return defaultPacketWeight;
    }
}
