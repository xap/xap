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


package com.gigaspaces.internal.utils.collections.economy;

import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.ConcurrentSegmentedStoredList;
import com.j_spaces.kernel.list.ConcurrentStoredList;

/**
 * TODO	add Javadoc
 *
 * @author Yechiel Fefer
 * @version 1.0
 * @since 7.1
 */

// stored list will be a value + HashHetry in an EconomyConcurrentHashMap 
//public class ConcurrentSegmentedStoredListHashmapEntry<K,T>
@com.gigaspaces.api.InternalApi
public class ConcurrentSegmentedStoredListHashmapEntry<T>
        extends ConcurrentStoredList<T>
//		implements IStoredListHashmapEntry<K, T>
{

    final Object _key;

    public ConcurrentSegmentedStoredListHashmapEntry(boolean segmented, boolean supportFifoPerSegment, Object key) {
        super(false,supportFifoPerSegment);
        _key = key;
    }

    public int getHashCode(int id) {
        return _key.hashCode();
    }

    public Object getKey(int id) {
        return _key;
    }

    public IStoredList<T> getValue(int id) {
        return this;
    }

    public boolean isNativeHashEntry() {
        return false;
    }

}
