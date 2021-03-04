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
package com.gigaspaces.client.iterator;

/**
 * Enum of {@link SpaceIterator} implementation types
 * @author Alon Shoham
 * @since 15.2.0
 */
public enum SpaceIteratorType {
    /**
     * This type iterates over entries by maintaining an iterator within each partition (similar to database cursors).
     * Since getting entries one-by-one is extremely inefficient, this iterator offers the following optimizations:
     * In theory, each hasNext() invocation would require at least one remote call, which would be extremely inefficient.
     * In practice, the iterator uses the following techniques to improve performance:
     * 1. The server-side iterator returns a batch of entries instead of a single one. That batch is used implicitly to
     * optimize hasNext()/next(). Users can control the batch size using SpaceIteratorConfiguration.setBatchSize().
     * 2. When the iterator is initialized, it asynchronously requests a batch from all partitions. That means that
     * once any of the partitions returns a result, the iterator can start serving entries. It also means that once
     * that batch is consumed, the iterator can continue serving entries from other partitions batches which meanwhile
     * arrived.
     * 3. When the iterator starts consuming entries from a partition batch, it implicitly sends an asynchronous request
     * in the background to that partition for the next batch, which further reduces the time waiting for entries.
     * With each partition batch consumption, the next batch is fetched in the background from that specific partition
     * Advantages (compared to PREFETCH_UIDS):
     *  1. Short latency till first entry is served (independent of number of matching entries).
     *  2. Small memory footprint on client side (independent of number of matching entries).
     *  3. Small workload on space: single batch is fetched from each partition on any given time
     * Disadvantages:
     *  1. Intolerant for primary-backup failover in a partition:
     *      If a primary space fails, entries will not be fetched from that partition. Iterator will continue to
     *      serve entries from other partitions.
     */
    CURSOR,
    /**
     * This type iterates over entries by first fetching all matching entries UIDs, then fetching the actual entries in
     * batches by uids.
     * Advantages (compared to CURSOR):
     *  1. Tolerant to primary space failure in any partition
     * Disadvantages:
     *  1. Long latency till first entry is served (independent of number of matching entries).
     *  2. Large memory footprint on client side (independent of number of matching entries).
     *  3. Large workload on space: all template matching entries need to be gathered on init
     *
     */
    PREFETCH_UIDS
}
