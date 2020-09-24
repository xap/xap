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

package com.j_spaces.core.cache;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/

import com.j_spaces.kernel.IStoredList;

import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by yechielf on 09/08/2015.
 */
public interface IExtendedEntriesIndex<K, V> extends IExtendedIndex<K, V> {

    IStoredList<IEntryCacheInfo> getIndexEntries(K indexValue);

//    ConcurrentMap<Object, IStoredList<IEntryCacheInfo>> getNonUniqueEntriesStore();
//
//    ConcurrentMap<Object, IEntryCacheInfo> getUniqueEntriesStore();
//
//    FastConcurrentSkipListMap<Object, IStoredList<IEntryCacheInfo>> getOrderedStore();

    Map<Object, IStoredList<IEntryCacheInfo>> getNonUniqueEntriesStore();

    Map<Object, IEntryCacheInfo> getUniqueEntriesStore();

    NavigableMap<Object, IStoredList<IEntryCacheInfo>> getOrderedStore();


    void onUpdate(IEntryCacheInfo eci);

    void onUpdateEnd(IEntryCacheInfo eci);

    void onRemove(IEntryCacheInfo eci);

    int reapExpired();

}
