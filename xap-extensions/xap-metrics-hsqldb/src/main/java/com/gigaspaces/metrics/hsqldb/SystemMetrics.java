package com.gigaspaces.metrics.hsqldb;

/**
 * @since 15.2.0
 */
public class SystemMetrics {
    private String metricName;
    private String tableName;

    public SystemMetrics(String name ){

        this.metricName = name;
        this.tableName = PredefinedSystemMetrics.toTableName(name);
    }

    public String getMetricName() {
        return metricName;
    }

    public String getTableName() {
        return tableName;
    }
}