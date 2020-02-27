package com.gigaspaces.metrics.hsqldb.dynamicTables;

import com.gigaspaces.metrics.hsqldb.SystemMetrics;
import com.gigaspaces.metrics.hsqldb.PredefinedSystemMetrics;

public class DataTypeReadCountMetrics extends SystemMetrics {

    public static final String METRIC_PREFIX = "space_data_read-count_";
    private static final String TABLE_PREFIX = PredefinedSystemMetrics.toTableName( METRIC_PREFIX );

    public DataTypeReadCountMetrics(String name) {
        super(name);
    }
}