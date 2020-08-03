package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.api.InternalApi;

import java.util.Arrays;
import java.util.List;

/**
 * @since 15.0
 */
@InternalApi
public enum PredefinedSystemMetrics implements TableColumnNames {
    PROCESS_CPU_USED_PERCENT("process_cpu_used-percent"),
    JVM_MEMORY_HEAP_USED_PERCENT("jvm_memory_heap_used-percent"),
    JVM_MEMORY_HEAP_USED_BYTES("jvm_memory_heap_used-bytes"),
    SPACE_REPLICATION_REDO_LOG_USED_PERCENT("space_replication_redo-log_used-percent", Arrays.asList( TIME_COLUMN_NAME, VALUE_COLUMN_NAME, PU_NAME_COLUMN_NAME, PU_INSTANCE_ID_COLUMN_NAME )),
    SPACE_REPLICATION_REDO_LOG_SIZE("space_replication_redo-log_size", Arrays.asList( TIME_COLUMN_NAME, VALUE_COLUMN_NAME, PU_NAME_COLUMN_NAME, PU_INSTANCE_ID_COLUMN_NAME )),
    SPACE_OPERATIONS_WRITE_TP("space_operations_write-tp", Arrays.asList( TIME_COLUMN_NAME, VALUE_COLUMN_NAME, PU_NAME_COLUMN_NAME, SPACE_ACTIVE_COLUMN_NAME ) ),
    SPACE_OPERATIONS_READ_TP("space_operations_read-tp", Arrays.asList( TIME_COLUMN_NAME, VALUE_COLUMN_NAME, PU_NAME_COLUMN_NAME, SPACE_ACTIVE_COLUMN_NAME )),
    SPACE_OPERATIONS_READ_MULTIPLE_TP("space_operations_read-multiple-tp", Arrays.asList( TIME_COLUMN_NAME, VALUE_COLUMN_NAME, PU_NAME_COLUMN_NAME, SPACE_ACTIVE_COLUMN_NAME )),
    SPACE_OPERATIONS_TAKE_TP("space_operations_take-tp", Arrays.asList( TIME_COLUMN_NAME, VALUE_COLUMN_NAME, PU_NAME_COLUMN_NAME, SPACE_ACTIVE_COLUMN_NAME )),
    SPACE_OPERATIONS_TAKE_MULTIPLE_TP("space_operations_take-multiple-tp", Arrays.asList( TIME_COLUMN_NAME, VALUE_COLUMN_NAME, PU_NAME_COLUMN_NAME, SPACE_ACTIVE_COLUMN_NAME )),
    SPACE_OPERATIONS_EXECUTE_TP("space_operations_execute-tp", Arrays.asList( TIME_COLUMN_NAME, VALUE_COLUMN_NAME, PU_NAME_COLUMN_NAME, SPACE_ACTIVE_COLUMN_NAME ) ),
    SPACE_BLOBSTORE_OFF_HEAP_USED_BYTES_TOTAL("space_blobstore_off-heap_used-bytes_total"),
    SPACE_BLOBSTORE_OFF_HEAP_USED_PERCENT("space_blobstore_off-heap_used-percent"),
    SPACE_OPERATIONS_READ_TOTAL("space_operations_read-total"),
    SPACE_OPERATIONS_READ_MULTIPLE_TOTAL("space_operations_read-multiple-total"),
    SPACE_BLOBSTORE_CACHE_HIT_PERCENT("space_blobstore_cache-hit-percent"),
    SPACE_DATA_READ_COUNT( "space_data_read-count", Arrays.asList( TIME_COLUMN_NAME, VALUE_COLUMN_NAME, PU_INSTANCE_ID_COLUMN_NAME, SPACE_NAME_COLUMN_NAME, DATA_TYPE_NAME_COLUMN_NAME ) ),
    SPACE_DATA_INDEX_HITS_TOTAL( "space_data_index-hits-total", Arrays.asList( TIME_COLUMN_NAME, VALUE_COLUMN_NAME, SPACE_NAME_COLUMN_NAME, INDEX_COLUMN_NAME, DATA_TYPE_NAME_COLUMN_NAME ) );

    private final String metricName;
    private final String tableName;
    private final List<String> columns;

    PredefinedSystemMetrics(String name) {
        this( name, null );
    }

    PredefinedSystemMetrics(String name, List<String> columns ) {
        this.metricName = name;
        this.tableName = toTableName(name);
        this.columns = columns;
    }

    public String getMetricName() {
        return metricName;
    }

    public String getTableName() {
        return tableName;
    }

    public static String toTableName(String name) {
        return name.toUpperCase()
                .replace( '-', '_' )
                .replace(':', '_')
                .replace('.', '_');
    }

    public List<String> getColumns() {
        return columns;
    }
}