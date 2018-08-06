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

package com.j_spaces.kernel;

import com.gigaspaces.internal.utils.collections.economy.ConcurrentSegmentedStoredListHashmapEntry;
import com.j_spaces.kernel.list.*;

import java.util.concurrent.locks.ReadWriteLock;


/**
 * StoredListFactory returns a {@link IStoredList} implementation, according to the Java version the
 * sources were compiled. This is due to performance profiling conducted on both JDK1.4 and JDK1.5 -
 * Thus, using 1.4 will return {@link IStoredList}, and using 1.5 will return {@link
 * SimpleLockStoredList}.
 *
 * @author moran
 * @version 1.0
 * @since 4.1
 */
public final class StoredListFactory {
    // a shared locks pool - used to reduce the memory footprint of the stored lists
    final private static IReusableResourcePool<ReadWriteLock> _locksPool = new RwlLocksPool(Integer.getInteger(SystemProperties.ENGINE_STORED_LIST_LOCKS,
            SystemProperties.ENGINE_STORED_LIST_LOCKS_DEFAULT));

    /**
     * Returns a StoredList instance according to the JVM Version. Equivalent to calling
     * <code>getStoredList(false,false);</code>
     *
     * @return StoredList instance with random and SLHolder-Factory set to false.
     */
    public static <T> IStoredList<T> createList(boolean reuseLocks) {
        return createStoredList(false, reuseLocks);
    }

    /**
     * Creates a StoredList that can be scanned from random position
     *
     * @return createStoredList
     */
    public static <T> IStoredList<T> createRandomScanList(boolean reuseLocks) {
        return createStoredList(true, reuseLocks);
    }

    /**
     * Creates an ordered list.
     *
     * @return ExternallyLockedStoredList
     */
    public static <T> IOrderedList<T> createOrderedList() {
        return new ExternallyLockedStoredList<T>(false/*random*/);
    }

    /**
     * Creates a segmented stored list - used for highly concurrent lists.
     *
     * @return RwlSegmentedStoredLis
     */
    public static <T> IStoredList<T> createSegmentedList() {
        return new RwlSegmentedStoredList<T>(null /*locks-pool*/);
    }

    /**
     * Creates a concurrent stored list - used for highly concurrent lists.
     *
     * @return IStoredList
     */
    public static <T> IStoredList<T> createConcurrentList(boolean supportsFifo) {
        return new ConcurrentStoredList<T>(false /* segmented*/, supportsFifo);
    }

    /**
     * Creates a concurrent segmented stored list - used for highly concurrent lists.
     *
     * @return IStoredList
     */
    public static <T> IStoredList<T> createConcurrentSegmentedList(boolean supportsFifoPerSegment) {
        return createConcurrentSegmentedList(supportsFifoPerSegment,0 /*numOfSegments*/,true /* padded*/);
    }
    /**
     * Creates a concurrent segmented stored list - used for highly concurrent lists.
     *
     * @return IStoredList
     */
    public static <T> IStoredList<T> createConcurrentSegmentedList(boolean supportsFifoPerSegment,int numOfSegments,boolean padded) {
        return new ConcurrentSegmentedStoredList<T>(supportsFifoPerSegment,numOfSegments,padded);
    }

    /**
     * Creates a segmented stored list - used for highly concurrent lists. this SL supports serving
     * as a EconomyConcurrentHashMap HashEntry for storing an index value
     *
     * @return oncurrentSegmentedStoredListHashmapEntry
     */
    public static <T> IStoredList<T> createConcurrentSegmentedList(boolean segmented, boolean supportFifoPerSegment, Object StoredIndexValueInHashmap) {
        return new ConcurrentSegmentedStoredListHashmapEntry<T>(segmented, supportFifoPerSegment, StoredIndexValueInHashmap);
    }

    /**
     * @return StoredList instance according to the given parameters and JVM Version.
     */
    public static <T> AbstractStoredList<T> createStoredList(boolean supportsRandomScans, boolean reuseLocks) {
        //check if locks can be reused for this stored list
        IReusableResourcePool<ReadWriteLock> locksPool = reuseLocks ? _locksPool : null;

        return new SimpleLockStoredList<T>(supportsRandomScans, locksPool);

    }


    /**
     * Create Hashed StoredList
     *
     * @return HashedSimpleLockIStoredList
     */
    public static <T> IStoredList<T> createHashList() {
        return new HashedSimpleLockIStoredList<T>(false);
    }

}
