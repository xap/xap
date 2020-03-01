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