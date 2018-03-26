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
     * Return <code>true</code> if the supplied <code>Map</code> is null or empty. Otherwise, return
     * <code>false</code>.
     *
     * @param map the <code>Map</code> to check
     */
    public static boolean isEmpty(Map<?, ?> map) {
        return (map == null || map.isEmpty());
    }

    /**
     * Check whether the given Iterator contains the given element.
     *
     * @param iterator the Iterator to check
     * @param element  the element to look for
     * @return <code>true</code> if found, <code>false</code> else
     */
    public static <T> boolean contains(Iterator<T> iterator, T element) {
        if (iterator != null) {
            while (iterator.hasNext()) {
                T candidate = iterator.next();
                if (ObjectUtils.nullSafeEquals(candidate, element))
                    return true;
            }
        }
        return false;
    }

    /**
     * Check whether the given Enumeration contains the given element.
     *
     * @param enumeration the Enumeration to check
     * @param element     the element to look for
     * @return <code>true</code> if found, <code>false</code> else
     */
    public static <T> boolean contains(Enumeration<T> enumeration, T element) {
        if (enumeration != null) {
            while (enumeration.hasMoreElements()) {
                T candidate = enumeration.nextElement();
                if (ObjectUtils.nullSafeEquals(candidate, element))
                    return true;
            }
        }

        return false;
    }

    /**
     * Determine whether the given collection only contains a single unique object.
     *
     * @param collection the collection to check
     * @return <code>true</code> if the collection contains a single reference or multiple
     * references to the same instance, <code>false</code> else
     */
    public static <T> boolean hasUniqueObject(Collection<T> collection) {
        if (isEmpty(collection))
            return false;

        boolean hasCandidate = false;
        T candidate = null;
        for (T elem : collection) {
            if (!hasCandidate) {
                hasCandidate = true;
                candidate = elem;
            } else if (candidate != elem)
                return false;
        }

        return true;
    }

    /**
     * Find a value of the given type in the given collection.
     *
     * @param collection the collection to search
     * @param type       the type to look for
     * @return a value of the given type found, or <code>null</code> if none
     * @throws IllegalArgumentException if more than one value of the given type found
     */
    public static <T> T findValueOfType(Collection<T> collection, Class<? extends T> type) throws IllegalArgumentException {
        if (isEmpty(collection))
            return null;

        Class<?> typeToUse = (type != null ? type : Object.class);
        T value = null;
        for (T obj : collection) {
            if (typeToUse.isInstance(obj)) {
                if (value != null)
                    throw new IllegalArgumentException("More than one value of type [" + typeToUse.getName() + "] found");

                value = obj;
            }
        }
        return value;
    }

    /**
     * Find a value of one of the given types in the given collection: searching the collection for
     * a value of the first type, then searching for a value of the second type, etc.
     *
     * @param collection the collection to search
     * @param types      the types to look for, in prioritized order
     * @return a of one of the given types found, or <code>null</code> if none
     * @throws IllegalArgumentException if more than one value of the given type found
     */
    public static <T> T findValueOfType(Collection<T> collection, Class<? extends T>[] types) {
        if (isEmpty(collection) || ObjectUtils.isEmpty(types))
            return null;

        for (Class<? extends T> type : types) {
            T value = findValueOfType(collection, type);
            if (value != null)
                return value;
        }
        return null;
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

    /**
     * Merge the given Properties instance into the given Map, copying all properties (key-value
     * pairs) over. <p>Uses <code>Properties.propertyNames()</code> to even catch default properties
     * linked into the original Properties instance.
     *
     * @param props the Properties instance to merge (may be <code>null</code>)
     * @param map   the target Map to merge the properties into
     */
    public static void mergePropertiesIntoMap(Properties props, Map<String, String> map) {
        if (map == null) {
            throw new IllegalArgumentException("Map must not be null");
        }
        if (props != null) {
            for (Enumeration<?> en = props.propertyNames(); en.hasMoreElements(); ) {
                String key = (String) en.nextElement();
                map.put(key, props.getProperty(key));
            }
        }
    }

    /**
     * Return <code>true</code> if any element in '<code>candidates</code>' is contained in
     * '<code>source</code>'; otherwise returns <code>false</code>.
     */
    public static <T> boolean containsAny(Collection<T> source, Collection<T> candidates) {
        if (isEmpty(source) || isEmpty(candidates))
            return false;

        for (T candidate : candidates)
            if (source.contains(candidate))
                return true;

        return false;
    }

    /**
     * Return the first element in '<code>candidates</code>' that is contained in
     * '<code>source</code>'. If no element in '<code>candidates</code>' is present in
     * '<code>source</code>' returns <code>null</code>. Iteration order is {@link Collection}
     * implementation specific.
     */
    public static <T> T findFirstMatch(Collection<T> source, Collection<T> candidates) {
        if (isEmpty(source) || isEmpty(candidates))
            return null;

        for (T candidate : candidates)
            if (source.contains(candidate))
                return candidate;

        return null;
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
