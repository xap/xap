package com.j_spaces.core.cluster.startup;

/**
 * @author Yael Nahon
 * @since 12.3
 */
public class CompactionResult {
    private long discardedCount = 0;
    private long weightRemoved = 0;

    public CompactionResult() {
    }

    public CompactionResult(long discardedCount, long weightRemoved) {
        this.discardedCount = discardedCount;
        this.weightRemoved = weightRemoved;
    }

    public long getDiscardedCount() {
        return discardedCount;
    }

    public long getWeightRemoved() {
        return weightRemoved;
    }

    public void increaseDiscardedCount(int discardedCount) {
        this.discardedCount += discardedCount;
    }

    public void increaseWeightRemoved(int weightRemoved) {
        this.weightRemoved += weightRemoved;
    }

    public boolean isEmpty() {
        return this.discardedCount == 0 && this.weightRemoved == 0;
    }

    public void appendResult(CompactionResult other) {
        this.discardedCount += other.getDiscardedCount();
        this.weightRemoved += other.getWeightRemoved();
    }
}
