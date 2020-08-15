package com.gigaspaces.cluster;

/**
 * @author Niv Ingberg
 * @since 15.5
 */
public interface PartitionRoutingInfo {
    /**
     * Gets the current cluster topology generation
     */
    int getGeneration();

    /**
     * Gets the number of partitions in the current cluster topology
     */
    int getNumOfPartitions();

    /**
     * Gets the current partition id (one-based)
     */
    int getPartitionId();

    /**
     * Gets dynamic partition info (if exists)
     */
    DynamicPartitionInfo getDynamicPartitionInfo();
}
