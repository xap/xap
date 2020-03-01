package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.api.InternalApi;

import java.util.*;

/**
 * @since 15.2
 */
@InternalApi
public class SystemMetricsManager {

    private static Map<String, SystemMetrics> systemMetricTables = new HashMap<>();
    private static Map<String, SystemMetrics> dynamicSystemMetricTables = new HashMap<>();

    static{
        Arrays.stream( PredefinedSystemMetrics.values() ).forEach(m ->
                systemMetricTables.put( m.getMetricName(), new SystemMetrics( m.getMetricName() ) )  );
    }

    public static Collection<SystemMetrics> getSystemMetricTables() {
        return systemMetricTables.values();
    }

    public static Collection<SystemMetrics> getDynamicSystemMetricTables() {
        return dynamicSystemMetricTables.values();
    }

    public static SystemMetrics getSystemMetric( String key ) {
        return systemMetricTables.get( key );
    }

    public static SystemMetrics addDynamicSystemTable(String metricName) {
        SystemMetrics retValue = null;
        if( !systemMetricTables.containsKey( metricName ) ) {
            retValue = new SystemMetrics(metricName);
            systemMetricTables.put(metricName, retValue);
            dynamicSystemMetricTables.put(metricName, retValue);
        }
        return retValue;
    }
}