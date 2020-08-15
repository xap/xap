package com.gigaspaces.cluster;

import java.io.Serializable;
import java.util.Collection;

/**
 * @author Niv Ingberg
 * @since 15.5
 */
public interface DynamicPartitionInfo extends Serializable {
    /**
     * Returns the chunks of a partition
     */
    Collection<Integer> getChunks();
}
