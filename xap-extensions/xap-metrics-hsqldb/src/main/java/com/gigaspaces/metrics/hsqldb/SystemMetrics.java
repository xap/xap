package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.api.InternalApi;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @since 15.0
 */
@InternalApi
public enum SystemMetrics {
    PROCESS_CPU_USED_PERCENT("process_cpu_used-percent"),
    JVM_MEMORY_HEAP_USED_PERCENT("jvm_memory_heap_used-percent"),
    JVM_MEMORY_HEAP_USED_BYTES("jvm_memory_heap_used-bytes"),
    SPACE_REPLICATION_REDO_LOG_USED_PERCENT("space_replication_redo-log_used-percent"),
    SPACE_REPLICATION_REDO_LOG_SIZE("space_replication_redo-log_size"),
    SPACE_TP_WRITE("space_operations_write-tp"),
    SPACE_TP_READ("space_operations_read-tp"),
    SPACE_TP_READ_MULTIPLE("space_operations_read-multiple-tp"),
    SPACE_TP_TAKE("space_operations_take-tp"),
    SPACE_TP_TAKE_MULTIPLE("space_operations_take-multiple-tp"),
    SPACE_TP_EXECUTE("space_operations_execute-tp"),
    SPACE_BLOBSTORE_OFF_HEAP_USED_BYTES_TOTAL("space_blobstore_off-heap_used-bytes_total"),
    SPACE_BLOBSTORE_OFF_HEAP_USED_PERCENT("space_blobstore_off-heap_used-percent"),
    SPACE_OPERATIONS_READ_TOTAL("space_operations_read-total");

    private final String metricName;
    private final String tableName;
    private static final Map<String, SystemMetrics> valuesMap = Arrays.stream(SystemMetrics.values()).collect(Collectors.toMap(m -> m.getMetricName(), m-> m));

    SystemMetrics(String name) {
        this.metricName = name;
        this.tableName = toTableName(name);
    }

    public String getMetricName() {
        return metricName;
    }

    public String getTableName() {
        return tableName;
    }

    public static Map<String, SystemMetrics> valuesMap() {
        return valuesMap;
    }

    public static String toTableName(String name) {
        return name.toUpperCase()
                .replace( '-', '_' )
                .replace(':', '_')
                .replace('.', '_');
    }
}
