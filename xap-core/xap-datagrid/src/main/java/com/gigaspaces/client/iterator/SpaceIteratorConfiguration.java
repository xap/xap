package com.gigaspaces.client.iterator;

import com.gigaspaces.client.ReadModifiers;

import java.time.Duration;

/**
 * Configuration class for {@link SpaceIterator}
 * @author Alon Shoham
 * @since 15.2.0
 */
public class SpaceIteratorConfiguration {
    public static int getDefaultBatchSize() {
        return 1000;
    }
    public static Duration getDefaultMaxInactiveDuration() { return Duration.ofMinutes(1);}

    private SpaceIteratorType iteratorType;
    private int batchSize = getDefaultBatchSize();
    private Duration maxInactiveDuration = null;
    private ReadModifiers readModifiers;

    /**
     * Constructs a new configuration with default values
     */
    public SpaceIteratorConfiguration() {
    }

    /**
     *
     * @return The space iterator type being used. For more information see {@link SpaceIteratorType}
     */
    public SpaceIteratorType getIteratorType() {
        return iteratorType;
    }

    /**
     * Sets the space iterator type being used. For more information see {@link SpaceIteratorType}
     * @param iteratorType
     * @return updated iterator configuration
     */
    public SpaceIteratorConfiguration setIteratorType(SpaceIteratorType iteratorType) {
        this.iteratorType = iteratorType;
        return this;
    }

    /**
     *
     * @return iterator batch size retrieved from the space
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the iterator batch size retrieved from the space
     * @param batchSize
     * @return updated iterator configuration
     */
    public SpaceIteratorConfiguration setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * @return max duration of an inactive iterator, before it's removed from the space.
     * iterator activity is automatically renewed, as long as the iterator is not explicitly closed.
     * Note: relevant only to {@link SpaceIteratorType#CURSOR}
     */
    public Duration getMaxInactiveDuration() {
        return maxInactiveDuration;
    }

    /**
     * Sets the max duration of an inactive iterator. Note: relevant only to {@link SpaceIteratorType#CURSOR}
     * @param maxInactiveDuration
     * @return updated configuration
     */
    public SpaceIteratorConfiguration setMaxInactiveDuration(Duration maxInactiveDuration) {
        this.maxInactiveDuration = maxInactiveDuration;
        return this;
    }

    /**
     *
     * @return {@link ReadModifiers} which allows to programmatically control the isolation level this iteration
     */
    public ReadModifiers getReadModifiers() {
        return readModifiers;
    }

    /**
     * Sets the {@link ReadModifiers} which allows to programmatically control the isolation level this iteration
     *      * operation will be performed under.
     * @param readModifiers
     * @return updated iterator configuration
     */
    public SpaceIteratorConfiguration setReadModifiers(ReadModifiers readModifiers) {
        this.readModifiers = readModifiers;
        return this;
    }
}
