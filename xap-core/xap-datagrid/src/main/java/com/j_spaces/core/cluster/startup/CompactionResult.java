package com.j_spaces.core.cluster.startup;

/**
 * @author Yael Nahon
 * @since 12.3
 */
public class CompactionResult {
    private long discardedCount = 0;
    private int deletedFromTxn = 0;

    public CompactionResult() {
    }

    public CompactionResult(long discardedCount, int deletedFromTxn) {
        this.discardedCount = discardedCount;
        this.deletedFromTxn = deletedFromTxn;
    }

    public long getDiscardedCount() {
        return discardedCount;
    }

    public void increaseDiscardedCount(int discardedCount) {
        this.discardedCount += discardedCount;
    }

    public void setDiscardedCount(long discardedCount) {
        this.discardedCount = discardedCount;
    }

    public int getDeletedFromTxn() {
        return deletedFromTxn;
    }

    public void increaseDeletedFromTxnCount(int deletedFromTxn) {
        this.deletedFromTxn += deletedFromTxn;
    }

    public void setDeletedFromTxn(int deletedFromTxn) {
        this.deletedFromTxn = deletedFromTxn;
    }

    public boolean isEmpty() {
        return this.discardedCount == 0 && this.deletedFromTxn == 0;
    }

    public void appendResult(CompactionResult other) {
        this.discardedCount += other.getDiscardedCount();
        this.deletedFromTxn += other.getDeletedFromTxn();
    }
}
