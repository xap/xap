/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
