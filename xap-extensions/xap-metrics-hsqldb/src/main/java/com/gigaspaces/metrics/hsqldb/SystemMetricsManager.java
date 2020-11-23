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
package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.api.InternalApi;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @since 15.2
 */
@InternalApi
public class SystemMetricsManager {

    private static Map<String, SystemMetrics> systemMetricTables = new ConcurrentHashMap<>();

    static{
        Arrays.stream( PredefinedSystemMetrics.values() ).forEach(m ->
                systemMetricTables.put( m.getMetricName(), new SystemMetrics( m.getMetricName() ) )  );
    }

    public static Collection<SystemMetrics> getSystemMetricTables() {
        return systemMetricTables.values();
    }

    public static SystemMetrics getSystemMetric( String key ) {
        return systemMetricTables.get( key );
    }

    public static SystemMetrics addDynamicSystemTable(String metricName) {
        SystemMetrics retValue = null;
        if( !systemMetricTables.containsKey( metricName ) ) {
            retValue = new SystemMetrics(metricName);
            systemMetricTables.putIfAbsent(metricName, retValue);
        }
        return retValue;
    }
}