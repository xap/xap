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

package com.gigaspaces.internal.classloader;

import com.gigaspaces.internal.exceptions.IllegalArgumentNullException;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.j_spaces.kernel.ClassLoaderHelper;

import java.util.Map;

public abstract class AbstractClassRepository<T> implements IClassLoaderCacheStateListener {
    private final CopyOnUpdateMap<Long, Map<String, T>> _cache;

    protected AbstractClassRepository() {
        this._cache = new CopyOnUpdateMap<Long, Map<String, T>>();
        ClassLoaderCache.getCache().registerCacheStateListener(this);
    }

    public void onClassLoaderRemoved(Long classLoaderKey, boolean explicit) {
        _cache.remove(classLoaderKey);
    }

    public T getByType(Class<?> type) {
        if (type == null)
            throw new IllegalArgumentNullException("type");

        return get(type, type.getName(), null, true, true);
    }

    public T getByName(String typeName) {
        return get(null, typeName, null, true, false);
    }

    public T getByName(String typeName, Object context) {
        return get(null, typeName, context, false, false);
    }

    public T getByNameIfExists(String typeName) {
        return get(null, typeName, null, false, true);
    }

    private T get(Class<?> type, String typeName, Object context, boolean throwIfNotExists, boolean localOnly) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        // Get class loader key:
        final Long classLoaderKey = classLoader != null
                ? ClassLoaderCache.getCache().putClassLoader(classLoader)
                : -1;

        // Get Class loader map:
        Map<String, T> classLoaderMap = this._cache.get(classLoaderKey);
        if (classLoaderMap == null) {
            Map<String, T> temp = new CopyOnUpdateMap<String, T>();
            classLoaderMap = _cache.putIfAbsent(classLoaderKey, temp);
            if (classLoaderMap == null)
                classLoaderMap = temp;
        }

        // Get result from class loader cache, if exists:
        T result = classLoaderMap.get(typeName);
        if (result != null)
            return result;

        synchronized (classLoaderMap) {
            return safeGet(type, typeName, context, classLoaderMap, throwIfNotExists, localOnly);
        }
    }

    private T safeGet(Class<?> type, String typeName, Object context,
                      Map<String, T> classLoaderMap, boolean throwIfNotExists, boolean localOnly) {
        T result = classLoaderMap.get(typeName);
        if (result != null)
            return result;

        // Load type by name if not provided:
        if (type == null) {
            try {
                type = ClassLoaderHelper.loadClass(typeName, localOnly);
            } catch (ClassNotFoundException e) {
                if (throwIfNotExists)
                    throw new SpaceMetadataException("Unable to load type [" + typeName + "]", e);
                return null;
            }
        }

        // Get super type info recursively:
        Class<?> superType = type.getSuperclass();
        T superTypeInfo = superType != null ? safeGet(superType, superType.getName(), context, classLoaderMap, true, localOnly) : null;

        // Create type info using type and super type info:
        result = create(type, superTypeInfo, context);
        // Put in cache for next time:
        classLoaderMap.put(typeName, result);
        // Return result:
        return result;
    }

    protected abstract T create(Class<?> type, T superTypeInfo, Object context);
}
