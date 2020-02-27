package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.metrics.hsqldb.dynamicTables.DataTypeReadCountMetrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 15.2
 */
@InternalApi
public class SystemMetricsManager {

    private static Map<String, SystemMetrics> systemMetricTables = new HashMap<>();

    static{
        Arrays.stream( PredefinedSystemMetrics.values() ).forEach(m ->
                systemMetricTables.put( m.getMetricName(), new SystemMetrics( m.getMetricName() ) )  );
    }

    public static Map<String, SystemMetrics> getSystemMetricTables() {
        return Collections.unmodifiableMap( systemMetricTables );
    }

    public static SystemMetrics addDynamicSystemTable(String metricName) {
        SystemMetrics retValue = null;
        if (metricName.startsWith(DataTypeReadCountMetrics.METRIC_PREFIX)) {
            retValue = new DataTypeReadCountMetrics(metricName);
        }

        if( retValue == null ){
            return null;
        }

        systemMetricTables.put(metricName, retValue);
        return retValue;
    }
}