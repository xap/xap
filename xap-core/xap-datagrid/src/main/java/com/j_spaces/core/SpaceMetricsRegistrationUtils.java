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
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.MetricManager;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.TypeData;

import java.util.Map;

/**
 * @since 15.5.0
 */
public class SpaceMetricsRegistrationUtils {

    private final SpaceEngine spaceEngine;
    private final CacheManager cacheManager;

    public SpaceMetricsRegistrationUtils(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        this.spaceEngine = cacheManager.getEngine();
    }

    public void registerSpaceDataTypeMetrics(IServerTypeDesc serverTypeDesc, TypeData typeData, MetricManager.MetricFlagsState metricFlagsState) {

        final String typeName = serverTypeDesc.getTypeName();
        final MetricRegistrator registrator = spaceEngine.getMetricRegistrator();

        // register read-count
        if( metricFlagsState.isDataReadCountsMetricEnabled() ) {
            spaceEngine.getDataTypeMetricRegistrar(typeName).register(registrator.toPath("data", "read-count"), serverTypeDesc.getReadCounter());
        }

        // register data-types
        if( metricFlagsState.isDataTypesMetricEnabled() ) {
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
        if( metricFlagsState.isDataIndexHitsMetricEnabled() ) {
            Map<String, SpaceIndex> indexes = serverTypeDesc.getTypeDesc().getIndexes();
            if (indexes != null) {
                indexes.forEach((k, v) -> {
                    if (v.getIndexType().isIndexed())
                        spaceEngine.getDataTypeMetricRegistrar(typeName, k).register(registrator.toPath("data", "index-hits-total"), typeData.getIndex(k).getUsageCounter());
                });
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
                Map<String, SpaceIndex> indexes = serverTypeDesc.getTypeDesc().getIndexes();
                if (indexes != null && !indexes.isEmpty()) {
                    for (String index : indexes.keySet()) {
                        spaceEngine.getDataTypeMetricRegistrar(typeName, index).unregisterByPrefix(registrator.toPath("data", "index-hits-total"));
                    }
                }
            }
        }
    }
}