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

import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricManager;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.QueryExtensionIndexManagerWrapper;
import com.j_spaces.core.cache.TypeData;

import java.util.Map;

/**
 * @since 15.5.0
 */
public class SpaceMetricsRegistrationUtils {

    private final SpaceEngine spaceEngine;
    private final CacheManager cacheManager;
    private final SpaceImpl spaceImpl;

    public SpaceMetricsRegistrationUtils(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        this.spaceEngine = cacheManager.getEngine();
        this.spaceImpl = spaceEngine.getSpaceImpl();
    }

    public void registerSpaceDataTypeMetrics(IServerTypeDesc serverTypeDesc, TypeData typeData, MetricManager.MetricFlagsState metricFlagsState) {

        final String typeName = serverTypeDesc.getTypeName();

        // register read-count
        if( metricFlagsState.isDataReadCountsMetricEnabled() ) {
            spaceEngine.getDataTypeMetricRegistrar(typeName).register("read-count", wrapPrimaryOnly(serverTypeDesc.getReadCounter()));
        }

        if( metricFlagsState.isTieredRamReadCountDataTypesMetricEnabled() ) {
            spaceEngine.getDataTypeMetricRegistrar(typeName).register("read-count-ram", wrapPrimaryOnly(serverTypeDesc.getRAMReadCounter()));
        }

        // register data-types
        if( metricFlagsState.isDataTypesMetricEnabled() ) {
            spaceEngine.getDataTypeMetricRegistrar(typeName).register("data-types", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return cacheManager.getNumberOfEntries(typeName, true) ;
                }
            });
        }

        // register index-hits
        if( metricFlagsState.isDataIndexHitsMetricEnabled() ) {
            registerIndexUsageMetric(typeName, "_gs_uid", typeData.getUidUsageCounter());
            Map<String, SpaceIndex> indexes = serverTypeDesc.getTypeDesc().getIndexes();
            if (indexes != null) {
                indexes.forEach((k, v) -> {
                    if (v.getIndexType().isIndexed())
                        registerIndexUsageMetric(typeName, k, typeData.getIndex(k).getUsageCounter());
                });
            }
            for (QueryExtensionIndexManagerWrapper foreignQueriesHandler : typeData.getForeignQueriesHandlers()) {
                foreignQueriesHandler.getIndexedPathsUsageCounters(typeName).forEach((k, v) ->
                        registerIndexUsageMetric(typeName, k, v));
            }
        }
    }

    public void unregisterSpaceDataTypeMetrics(IServerTypeDesc typeDesc, TypeData typeData) {
        final String typeName = typeDesc.getTypeName();

        MetricManager.MetricFlagsState metricFlagsState = spaceEngine.getMetricManager().getMetricFlagsState();
        //unregister read-count + data-types
        if (metricFlagsState.isDataReadCountsMetricEnabled() || metricFlagsState.isDataTypesMetricEnabled()) {
            spaceEngine.clearDataTypeMetricRegistrarIfExists(typeName);
        }
        //unregister index-hits
        if (metricFlagsState.isDataIndexHitsMetricEnabled()) {
            spaceEngine.clearDataTypeMetricRegistrarIfExists(typeName, "_gs_uid");
            Map<String, SpaceIndex> indexes = typeDesc.getTypeDesc().getIndexes();
            if (indexes != null && !indexes.isEmpty()) {
                for (String index : indexes.keySet()) {
                    spaceEngine.clearDataTypeMetricRegistrarIfExists(typeName, index);
                }
            }
            for (QueryExtensionIndexManagerWrapper foreignQueriesHandler : typeData.getForeignQueriesHandlers()) {
                foreignQueriesHandler.getIndexedPathsUsageCounters(typeName).forEach((k, v) ->
                        spaceEngine.clearDataTypeMetricRegistrarIfExists(typeName, k));
            }
        }
    }

    private void registerIndexUsageMetric(String typeName, String indexName, LongCounter usageCounter) {
        spaceEngine.getDataTypeMetricRegistrar(typeName, indexName).register("index-hits-total", wrapPrimaryOnly(usageCounter));
    }

    /**
     * Wrap the counter to return values only for primary, to reduce resource usage for backups.
     */
    private Gauge<Long> wrapPrimaryOnly(LongCounter counter) {
        return new Gauge<Long>() {
            @Override
            public Long getValue() {
                return spaceImpl.isPrimary() ? counter.getCount() : null;
            }
        };
    }
}
