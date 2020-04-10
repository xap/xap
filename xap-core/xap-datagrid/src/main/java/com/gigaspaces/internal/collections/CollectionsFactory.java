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

package com.gigaspaces.internal.collections;

import com.gigaspaces.internal.collections.standard.StandardCollectionsFactory;
import com.gigaspaces.internal.collections.standard.StandardObjectShortMap;
import com.gigaspaces.internal.collections.standard.StandardShortLongMap;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.reflection.ReflectionUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 12.0
 */
public abstract class CollectionsFactory {
    private static final Logger _logger = LoggerFactory.getLogger(CollectionsFactory.class.getName());

    private static final CollectionsFactory instance = initialize();

    private static CollectionsFactory initialize() {
        final Class<?> factoryClass = com.gigaspaces.internal.collections.trove.TroveCollectionsFactory.class;
        CollectionsFactory factory = (CollectionsFactory) ReflectionUtil.createInstanceWithOptionalDependencies(factoryClass);
        if (factory == null) {
            _logger.warn("Failed to create collections factory [" + factoryClass.getName() + "], falling back to standard factory instead");
            factory = new StandardCollectionsFactory();
        }
        return factory;
    }

    public static CollectionsFactory getInstance() {
        return instance;
    }

    public abstract <K, V> Map<K, V> createMap();

    public abstract <K, V> Map<K, V> createMap(int initialCapacity);

    public abstract <K, V> Map<K, V> createMap(int initialCapacity, float loadFactor);

    public abstract <K, V> Map<K, V> createMap(Map<K, V> map);

    public abstract ShortList createShortList();

    public abstract IntegerList createIntegerList();

    public abstract IntegerSet createIntegerSet();

    public abstract IntegerSet createIntegerSet(int initialCapacity);

    public abstract ShortLongMap createShortLongMap();

    public ShortLongMap deserializeShortLongMap(ObjectInput in) throws IOException, ClassNotFoundException {
        Object map = IOUtils.readObject(in);
        if (map instanceof java.util.Map)
            return new StandardShortLongMap((Map<Short, Long>) map);
        return deserializeShortLongMap(map);
    }

    protected abstract ShortLongMap deserializeShortLongMap(Object map);

    public abstract <V> ShortObjectMap<V> createShortObjectMap();

    public abstract <V> ShortObjectMap<V> createShortObjectMap(int initialCapacity);

    public abstract <K> ObjectShortMap<K> createObjectShortMap();

    public abstract <V> LongObjectMap<V> createLongObjectMap();

    public abstract <K> ObjectLongMap<K> createObjectLongMap();

    public <K> ObjectShortMap<K> deserializeObjectShortMap(ObjectInput in) throws IOException, ClassNotFoundException {
        Object map = in.readObject();
        if (map instanceof java.util.Map)
            return new StandardObjectShortMap((Map<K, Short>) map);
        return deserializeObjectShortMap(map);
    }

    protected abstract <K> ObjectShortMap<K> deserializeObjectShortMap(Object map);

    public abstract <V> IntegerObjectMap<V> createIntegerObjectMap();

    public abstract <K> ObjectIntegerMap<K> createObjectIntegerMap();

    public <K, V> boolean forEachEntry(Map<K, V> map, MapProcedure<K, V> procedure) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            if (!procedure.execute(entry.getKey(), entry.getValue()))
                return false;
        }
        return true;
    }
}
