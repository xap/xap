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
package com.j_spaces.core;

import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.IndexMetricsContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * @since 15.5.0
 */
public class SpaceMetricsRegistrationUtils {

    //key = typeName, value = map<index name, hits>
    private Map<String, Map<String, LongAdder>> dataTypesIndexesHits = new ConcurrentHashMap<>();

    private final SpaceEngine spaceEngine;
    private final CacheManager cacheManager;

    public SpaceMetricsRegistrationUtils(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        this.spaceEngine = cacheManager.getEngine();
    }

    public void registerSpaceDataTypeMetrics(IServerTypeDesc serverTypeDesc) {

        String typeName = serverTypeDesc.getTypeName();

        if (!typeName.equals(IServerTypeDesc.ROOT_TYPE_NAME)) {

            final MetricRegistrator registrator = spaceEngine.getMetricRegistrator();

            // register read-count
            if( spaceEngine.getMetricManager().getMetricFlagsState().isDataReadCountsMetricEnabled() ) {
                spaceEngine.getDataTypeMetricRegistrar(typeName).register(registrator.toPath("data", "read-count"), new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        SpaceImpl spaceImpl = spaceEngine.getSpaceImpl();
                        SpaceMode spaceMode = spaceImpl.getSpaceMode();
                        if( spaceMode != SpaceMode.PRIMARY ){
                            return null;
                        }

                        LongAdder objectTypeReadCounts = spaceImpl.getObjectTypeReadCounts(typeName);
                        return objectTypeReadCounts == null ? 0 : objectTypeReadCounts.longValue();
                    }
                });
            }

            // register data-types
            if( spaceEngine.getMetricManager().getMetricFlagsState().isDataTypesMetricEnabled() ) {
                spaceEngine.getDataTypeMetricRegistrar(typeName).register(registrator.toPath("data", "data-types"), new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        SpaceMode spaceMode = spaceEngine.getSpaceImpl().getSpaceMode();
                        if( spaceMode != SpaceMode.PRIMARY ){
                            return null;
                        }
                        return cacheManager.getNumberOfEntries(typeName, true);
                    }
                });
            }

            // register index-hits
            if( spaceEngine.getMetricManager().getMetricFlagsState().isDataIndexHitsMetricEnabled() ) {
                ITypeDesc typeDesc = serverTypeDesc.getTypeDesc();
                Map<String, SpaceIndex> indexes = typeDesc.getIndexes();
                if (indexes != null && !indexes.isEmpty()) {
                    //add type to dataTypes indexes hits map
                    dataTypesIndexesHits.put(typeName, new ConcurrentHashMap<>());

                    for (String index : indexes.keySet()) {
                        spaceEngine.getDataTypeMetricRegistrar(typeName, index).register(registrator.toPath("data", "index-hits-total"), new Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                SpaceMode spaceMode = spaceEngine.getSpaceImpl().getSpaceMode();
                                if( spaceMode != SpaceMode.PRIMARY ){
                                    return null;
                                }
                                Map<String, LongAdder> indexesMap = dataTypesIndexesHits.get(typeName);
                                LongAdder indexHits = indexesMap.get(index);
                                return indexHits == null ? 0 : indexHits.longValue();
                            }
                        });
                    }
                }
            }
        }
    }

    public void unregisterSpaceDataTypeMetrics(String typeName) {

        final MetricRegistrator registrator = spaceEngine.getMetricRegistrator();

        //unregister read-count
        if( spaceEngine.getMetricManager().getMetricFlagsState().isDataReadCountsMetricEnabled() ) {
            spaceEngine.getDataTypeMetricRegistrar(typeName).unregisterByPrefix(registrator.toPath("data", "read-count"));
        }
        //unregister data-types
        if( spaceEngine.getMetricManager().getMetricFlagsState().isDataTypesMetricEnabled() ) {
            spaceEngine.getDataTypeMetricRegistrar(typeName).unregisterByPrefix(registrator.toPath("data", "data-types"));
        }
        //unregister index-hits
        if( spaceEngine.getMetricManager().getMetricFlagsState().isDataIndexHitsMetricEnabled() ) {
            IServerTypeDesc serverTypeDesc = spaceEngine.getTypeTableEntry(typeName);
            if (serverTypeDesc != null) {
                ITypeDesc typeDesc = serverTypeDesc.getTypeDesc();
                Map<String, SpaceIndex> indexes = typeDesc.getIndexes();
                if (indexes != null && !indexes.isEmpty()) {
                    for (String index : indexes.keySet()) {
                        spaceEngine.getDataTypeMetricRegistrar(typeName, index).unregisterByPrefix(registrator.toPath("data", "index-hits-total"));
                    }
                }
            }
        }
    }

    public void updateDataTypeIndexUsage(IndexMetricsContext indexMetricsContext) {

        if (indexMetricsContext.isEmpty()) {
            return;
        }

        String typeName = indexMetricsContext.getDataTypeName();
        Map<String, LongAdder> indexesMap = dataTypesIndexesHits.get(typeName);
        if (indexesMap == null) {
            return;
        }

        for (String indexName : indexMetricsContext.getChosenIndexes()) {
            LongAdder currentIndexHitCount = indexesMap.get(indexName);
            if (currentIndexHitCount == null) {
                currentIndexHitCount = new LongAdder();
                LongAdder prev = indexesMap.putIfAbsent(indexName, currentIndexHitCount);
                if (prev != null) {
                    currentIndexHitCount = prev;
                }
            }
            currentIndexHitCount.add(1);
        }
    }
}