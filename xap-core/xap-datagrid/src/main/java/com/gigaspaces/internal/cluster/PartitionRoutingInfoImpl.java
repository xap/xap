package com.gigaspaces.internal.cluster;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.cluster.DynamicPartitionInfo;
import com.gigaspaces.cluster.PartitionRoutingInfo;

@InternalApi
public class PartitionRoutingInfoImpl implements PartitionRoutingInfo {
    private final int generation;
    private final int numOfPartitions;
    private final int partitionId;
    private final DynamicPartitionInfo dynamicPartitionInfo;

    public PartitionRoutingInfoImpl(int generation, int numOfPartitions, int partitionId, DynamicPartitionInfo dynamicPartitionInfo) {
        this.generation = generation;
        this.numOfPartitions = numOfPartitions;
        this.partitionId = partitionId;
        this.dynamicPartitionInfo = dynamicPartitionInfo;
    }

    @Override
    public int getGeneration() {
        return generation;
    }

    @Override
    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public DynamicPartitionInfo getDynamicPartitionInfo() {
        return dynamicPartitionInfo;
    }
}
