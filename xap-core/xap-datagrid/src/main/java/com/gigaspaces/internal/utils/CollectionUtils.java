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

package com.gigaspaces.internal.utils;

import com.gigaspaces.internal.metadata.SpaceTypeInfo;
import com.gigaspaces.internal.metadata.SpaceTypeInfoRepository;

import java.util.*;

/**
 * Miscellaneous collection utility methods. Mainly for internal use within the framework.
 *
 * @author kimchy
 */
public abstract class CollectionUtils {
    /**
     * Return <code>true</code> if the supplied <code>Collection</code> is null or empty. Otherwise,
     * return <code>false</code>.
     *
     * @param collection the <code>Collection</code> to check
     */
    public static boolean isEmpty(Collection<?> collection) {
        return (collection == null || collection.isEmpty());
    }

    /**
     * Converts the supplied array into a List.
     *
     * @param source the original array
     * @return the converted List result
     */
    public static <T> List<T> toList(T... items) {
        if (items == null)
            return null;

        List<T> list = new ArrayList<T>(items.length);
        for (T item : items)
            list.add(item);

        return list;
    }

    public static <T> List<T> toUnmodifiableList(T... items) {
        List<T> result = toList(items);
        return  result == null ? null : Collections.unmodifiableList(result);
    }

    /**
     * Converts the supplied array into a Set.
     *
     * @param source the original array
     * @return the converted List result
     */
    public static <T> Set<T> toSet(T... items) {
        if (items == null)
            return null;

        Set<T> set = new HashSet<T>(items.length);
        for (T item : items)
            set.add(item);

        return set;
    }

    public static <T> Set<T> toUnmodifiableSet(T... items) {
        Set<T> result = toSet(items);
        return  result == null ? null : Collections.unmodifiableSet(result);
    }

    public static <T> boolean equals(Collection<T> c1, Collection<T> c2) {
        if (c1 == c2)
            return true;
        if (c1 == null || c2 == null)
            return false;
        if (c1.size() != c2.size())
            return false;

        Iterator<T> i1 = c1.iterator();
        Iterator<T> i2 = c2.iterator();

        while (i1.hasNext() && i2.hasNext()) {
            T o1 = i1.next();
            T o2 = i2.next();
            if (!ObjectUtils.equals(o1, o2))
                return false;
        }

        if (i1.hasNext() || i2.hasNext())
            return false;

        return true;
    }

    public static <T> List<T> cloneList(List<T> list) {
        if (list instanceof ArrayList)
            return (List<T>) ((ArrayList<T>) list).clone();
        if (list instanceof LinkedList)
            return (List<T>) ((LinkedList<T>) list).clone();
        if (list.getClass().getName().equals("java.util.Arrays$ArrayList"))
            return new ArrayList<T>(list);

        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(list.getClass());
        List<T> result = (List<T>) typeInfo.createInstance();
        result.addAll(list);
        return result;
    }

    public static <T> Collection<T> cloneCollection(Collection<T> collection) {
        if (collection instanceof List)
            return cloneList((List<T>) collection);

        if (collection instanceof HashSet)
            return (Collection<T>) ((HashSet<T>) collection).clone();

        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(collection.getClass());
        Collection<T> result = (Collection<T>) typeInfo.createInstance();
        result.addAll(collection);
        return result;
    }

    public static <K, V> Map<K, V> cloneMap(Map<K, V> map) {
        if (map instanceof HashMap)
            return (Map<K, V>) ((HashMap<K, V>) map).clone();
        if (map instanceof TreeMap)
            return (Map<K, V>) ((TreeMap<K, V>) map).clone();

        SpaceTypeInfo typeInfo = SpaceTypeInfoRepository.getTypeInfo(map.getClass());
        Map<K, V> result = (Map<K, V>) typeInfo.createInstance();
        result.putAll(map);
        return result;
    }

    public static <T> T first(Collection<T> collection) {
        Iterator<T> iterator = collection.iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }
}
