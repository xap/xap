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
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.metrics.MetricManager;
import com.gigaspaces.metrics.MetricRegistrator;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.QueryExtensionIndexManagerWrapper;
import com.j_spaces.core.cache.TypeData;

import java.util.HashMap;
import java.util.Map;

/**
 * @since 15.5.0
 */
public class SpaceMetricsRegistrationUtils {

    private final CacheManager cacheManager;
    private final SpaceImpl spaceImpl;

    public SpaceMetricsRegistrationUtils(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        this.spaceImpl = cacheManager.getEngine().getSpaceImpl();
    }

    public void registerSpaceDataTypeMetrics(IServerTypeDesc serverTypeDesc, TypeData typeData, MetricManager.MetricFlagsState metricFlagsState) {
        final String typeName = serverTypeDesc.getTypeName();

        // register read-count
        if( metricFlagsState.isDataReadCountsMetricEnabled() ) {
            createRegistrator(typeName).register("read-count", wrapPrimaryOnly(serverTypeDesc.getReadCounter()));
        }

        // register data-types
        if( metricFlagsState.isDataTypesMetricEnabled() ) {
            createRegistrator(typeName).register("data-types", new Gauge<Integer>() {
                @Override
                public Integer getValue() {
                    return spaceImpl.isPrimary() ? cacheManager.getNumberOfEntries(typeName, true) : null;
                }
            });
        }

        // register index-hits
        if( metricFlagsState.isDataIndexHitsMetricEnabled() ) {
            registerIndexHits(typeName, "_gs_uid", typeData.getUidUsageCounter());
            Map<String, SpaceIndex> indexes = serverTypeDesc.getTypeDesc().getIndexes();
            if (indexes != null) {
                indexes.forEach((k, v) -> {
                    if (v.getIndexType().isIndexed())
                        registerIndexHits(typeName, k, typeData.getIndex(k).getUsageCounter());
                });
            }
            for (QueryExtensionIndexManagerWrapper foreignQueriesHandler : typeData.getForeignQueriesHandlers()) {
                foreignQueriesHandler.getIndexedPathsUsageCounters(typeName).forEach((k, v) -> registerIndexHits(typeName, k, v));
            }
        }
    }

    public void unregisterSpaceDataTypeMetrics(IServerTypeDesc typeDesc, TypeData typeData) {
        final String typeName = typeDesc.getTypeName();
        MetricManager.MetricFlagsState metricFlagsState = cacheManager.getEngine().getMetricManager().getMetricFlagsState();
        //unregister read-count
        if (metricFlagsState.isDataReadCountsMetricEnabled()) {
            createRegistrator(typeName).unregisterByPrefix("read-count");
        }
        //unregister data-types
        if (metricFlagsState.isDataTypesMetricEnabled()) {
            createRegistrator(typeName).unregisterByPrefix("data-types");
        }
        //unregister index-hits
        if (metricFlagsState.isDataIndexHitsMetricEnabled()) {
            unregisterIndexHits(typeName, "_gs_uid");
            Map<String, SpaceIndex> indexes = typeDesc.getTypeDesc().getIndexes();
            if (indexes != null && !indexes.isEmpty()) {
                for (String index : indexes.keySet()) {
                    unregisterIndexHits(typeName, index);
                }
            }
            for (QueryExtensionIndexManagerWrapper foreignQueriesHandler : typeData.getForeignQueriesHandlers()) {
                foreignQueriesHandler.getIndexedPathsUsageCounters(typeName).forEach((k, v) -> unregisterIndexHits(typeName, k));
            }
        }
    }

    public MetricRegistrator createRegistrator(String typeName) {
        return createRegistrator(typeName, null);
    }

    private void registerIndexHits(String typeName, String indexName, LongCounter indexUsageCounter) {
        createRegistrator(typeName, indexName).register("index-hits-total", wrapPrimaryOnly(indexUsageCounter));
    }

    private void unregisterIndexHits(String typeName, String indexName) {
        createRegistrator(typeName, indexName).unregisterByPrefix("index-hits-total");
    }

    private Gauge<Long> wrapPrimaryOnly(LongCounter counter) {
        // NOTE: metrics with null are not reported, which optimizes performance.
        return new Gauge<Long>() {
            @Override
            public Long getValue() {
                return spaceImpl.isPrimary() ? counter.getCount() : null;
            }
        };
    }

    public MetricRegistrator createRegistrator(String typeName, String index) {
        Map<String, String> tags = new HashMap<>(index != null ? 2 : 1);
        tags.put("data_type_name", typeName);
        if (index != null)
            tags.put("index", index);
        return cacheManager.getEngine().createSpaceRegistrator(tags).extend("data");
    }
}
