package com.gigaspaces.metrics.hsqldb;

import java.util.Objects;

/**
 * @since 15.2.0
 */
public class SystemMetrics {
    private final String metricName;
    private final String tableName;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SystemMetrics that = (SystemMetrics) o;
        return Objects.equals(metricName, that.metricName) &&
                Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricName, tableName);
    }
}