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

package com.j_spaces.map;

import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.j_spaces.core.Constants;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceInitializationException;
import com.j_spaces.core.client.cache.map.MapCache;
import com.j_spaces.javax.cache.EvictionStrategy;
import com.j_spaces.kernel.ClassLoaderHelper;
import com.j_spaces.kernel.log.JProperties;
import com.j_spaces.map.eviction.AbstractEvictionStrategy;
import com.j_spaces.map.eviction.NoneEvictionStrategy;

import java.rmi.RemoteException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapFactory {

    public static IMap createMapWithCache(IJSpace space, Integer updateMode) throws FinderException {
        SpaceConfigReader dcacheConfigReader = getDCacheConfigReader(space);
        boolean putFirst = dcacheConfigReader.getBooleanSpaceProperty(Constants.DCache.PUT_FIRST_PROP, Constants.DCache.PUT_FIRST_DEFUALT);
        int compression = dcacheConfigReader.getIntSpaceProperty(Constants.DCache.COMPRESSION_PROP, Constants.DCache.COMPRESSION_DEFUALT);
        boolean versioned = dcacheConfigReader.getBooleanSpaceProperty(Constants.DCache.VERSIONED_PROP, Constants.DCache.VERSIONED_DEFUALT);
        int sizeLimit = dcacheConfigReader.getIntSpaceProperty(Constants.CacheManager.CACHE_MANAGER_SIZE_PROP, Constants.CacheManager.CACHE_MANAGER_SIZE_DEFAULT);
        EvictionStrategy evictionStrategy = loadEvictionStrategy(dcacheConfigReader);

        if (updateMode == null)
            updateMode = dcacheConfigReader.getIntSpaceProperty(Constants.DCache.UPDATE_MODE_PROP, Constants.DCache.UPDATE_MODE_DEFUALT);

        space.setOptimisticLocking(versioned);
        try {
            return new MapCache(space, versioned, updateMode, evictionStrategy, putFirst, sizeLimit, compression);
        } catch (RemoteException e) {
            throw new FinderException("Failed to create MapCache", e);
        }
    }

    public static IMap createMap(IJSpace space) throws FinderException {
        SpaceConfigReader dCacheConfigReader = getDCacheConfigReader(space);
        int compression = dCacheConfigReader.getIntSpaceProperty(Constants.DCache.COMPRESSION_PROP, Constants.DCache.COMPRESSION_DEFUALT);
        return new GSMapImpl(space, compression);
    }

    private static EvictionStrategy loadEvictionStrategy(SpaceConfigReader dcacheConfigReader) throws FinderException {
        String evictionStrategyName = dcacheConfigReader.getSpaceProperty(
                Constants.DCache.EVICTION_STRATEGY_PROP, Constants.DCache.EVICTION_STRATEGY_DEFUALT);
        try {
            EvictionStrategy evictionStrategy = (EvictionStrategy) ClassLoaderHelper.loadClass(evictionStrategyName).newInstance();

            if (evictionStrategy instanceof AbstractEvictionStrategy) {
                // eviction quota
                int batchSize = dcacheConfigReader.getIntSpaceProperty(
                        Constants.Engine.ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_PROP,
                        Constants.Engine.ENGINE_MEMORY_USAGE_EVICTION_BATCH_SIZE_DEFAULT);
                ((AbstractEvictionStrategy) evictionStrategy).setBatchSize(batchSize);
            }
            return evictionStrategy;
        } catch (java.lang.ClassNotFoundException e) {
            Logger logger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_SPACEFINDER);
            if (logger.isWarnEnabled())
                logger.warn("Failed to find eviction strategy implementation using NoneEvictionStrategy.", e);

            return new NoneEvictionStrategy();
        } catch (InstantiationException e) {
            throw new FinderException("Failed to create eviction strategy: " + evictionStrategyName, e);
        } catch (IllegalAccessException e) {
            throw new FinderException("Failed to create eviction strategy: " + evictionStrategyName, e);
        }
    }

    private static SpaceConfigReader getDCacheConfigReader(IJSpace space) throws FinderException {
        try {
            return JProperties.loadDCacheConfig((ISpaceProxy) space, space.getFinderURL().getCustomProperties(), Constants.DCache.SPACE_SUFFIX);
        } catch (SpaceInitializationException e) {
            throw new FinderException("Failed to initialize map configuration", e);
        }
    }
}
