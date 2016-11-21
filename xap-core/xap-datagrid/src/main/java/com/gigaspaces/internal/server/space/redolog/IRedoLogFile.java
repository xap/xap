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

package com.gigaspaces.internal.server.space.redolog;

import com.gigaspaces.cluster.replication.IRedoLogFileStatistics;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterable;
import com.gigaspaces.internal.utils.collections.ReadOnlyIterator;

/**
 * Acts as the redo log packets storage {@link MemoryRedoLog} Implementor should support concurrent
 * readers or a single writer, in other words, the implementor can assume access to this structure
 * are guarded with a reader writer lock according to the operation type
 *
 * An exception is {@link #getApproximateSize()} method which should not assume a reader lock is
 * held.
 *
 * @author eitany
 * @since 7.1
 */
public interface IRedoLogFile<T> extends Iterable<T>, ReadOnlyIterable<T>, IRedoLogFileStatistics {
    /**
     * Remove and returns the oldest replication packet in the file
     *
     * @return returns the oldest replication packet in the file
     */
    T removeOldest();

    /**
     * @return Gets the oldest replication packet in the file
     */
    T getOldest();

    /**
     * Add a replication packet to the file as the latest packet
     *
     * @param replicationPacket packet to add
     */
    void add(T replicationPacket);

    /**
     * @return the number of replication packets held in the file
     */
    long size();

    /**
     * Gets an approximation of the number of replication packets held in the file, implementation
     * of this method should not assume a reader lock is held and hence can return a result which is
     * not accurate if it cannot do so without a lock.
     *
     * @return an approximation of the replication packets held in the file
     */
    long getApproximateSize();

    /**
     * @return true if the file has no replication packets stored
     */
    boolean isEmpty();

    /**
     * @param fromIndex index to start from
     * @return read only iterator over the packets in the file that will start from the given index,
     * where 0 specified the oldest packet
     */
    ReadOnlyIterator<T> readOnlyIterator(long fromIndex);

    /**
     * Deletes the oldest packets, starting from the oldest up until the specified batch size
     *
     * @param batchSize number of oldest packets to delete
     */
    void deleteOldestBatch(long batchSize);

    /**
     * Validates the integrity of the redo log file
     */
    void validateIntegrity() throws RedoLogFileCompromisedException;

    /**
     * Closes the redo log file
     */
    void close();

    long getWeight();

}
