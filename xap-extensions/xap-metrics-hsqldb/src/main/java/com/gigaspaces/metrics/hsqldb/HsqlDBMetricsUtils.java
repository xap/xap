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

public class HsqlDBMetricsUtils {

  final static String METRIC_NAME_PROCESS_CPU_USED_PERCENT = "process_cpu_used-percent";
  final static String METRIC_NAME_JVM_MEMORY_HEAP_USED_PERCENT = "jvm_memory_heap_used-percent";
  final static String METRIC_NAME_JVM_MEMORY_HEAP_USED_BYTES = "jvm_memory_heap_used-bytes";
  final static String METRIC_NAME_SPACE_REPLICATION_REDO_LOG_USED_PERCENT = "space_replication_redo-log_used-percent";
  final static String METRIC_NAME_SPACE_OPERATIONS_WRITE_TP = "space_operations_write-tp";
  final static String METRIC_NAME_SPACE_OPERATIONS_READ_TP = "space_operations_read-tp";
  final static String METRIC_NAME_SPACE_OPERATIONS_READ_MULTIPLE_TP = "space_operations_read-multiple-tp";
  final static String METRIC_NAME_SPACE_OPERATIONS_TAKE_TP = "space_operations_take-tp";
  final static String METRIC_NAME_SPACE_OPERATIONS_TAKE_MULTIPLE_TP = "space_operations_take-multiple-tp";
  final static String METRIC_NAME_SPACE_OPERATIONS_EXECUTE_TP = "space_operations_execute-tp";

  public static final String TABLE_NAME_PROCESS_CPU_USED_PERCENT = createValidTableName(METRIC_NAME_PROCESS_CPU_USED_PERCENT );// "PROCESS_CPU_USED_PERCENT";
  public static final String TABLE_NAME_JVM_MEMORY_HEAP_USED_PERCENT = createValidTableName(METRIC_NAME_JVM_MEMORY_HEAP_USED_PERCENT );//"JVM_MEMORY_HEAP_USED_PERCENT";

  public static final String TABLE_NAME_JVM_MEMORY_HEAP_USED_BYTES = createValidTableName(METRIC_NAME_JVM_MEMORY_HEAP_USED_BYTES );//"JVM_MEMORY_HEAP_USED_BYTES";
  public static final String TABLE_NAME_SPACE_REPLICATION_REDO_LOG_USED_PERCENT = createValidTableName(METRIC_NAME_SPACE_REPLICATION_REDO_LOG_USED_PERCENT );//"SPACE_REPLICATION_REDO_LOG_USED_PERCENT";

  public static final String TABLE_NAME_SPACE_OPERATIONS_WRITE_TP = createValidTableName(METRIC_NAME_SPACE_OPERATIONS_WRITE_TP );//"SPACE_OPERATIONS_WRITE_TP";
  public static final String TABLE_NAME_SPACE_OPERATIONS_READ_TP = createValidTableName(METRIC_NAME_SPACE_OPERATIONS_READ_TP );//"SPACE_OPERATIONS_READ_TP";
  public static final String TABLE_NAME_SPACE_OPERATIONS_READ_MULTIPLE_TP = createValidTableName(METRIC_NAME_SPACE_OPERATIONS_READ_MULTIPLE_TP );//"SPACE_OPERATIONS_READ_MULTIPLE_TP";
  public static final String TABLE_NAME_SPACE_OPERATIONS_TAKE_TP = createValidTableName(METRIC_NAME_SPACE_OPERATIONS_TAKE_TP );//"SPACE_OPERATIONS_TAKE_TP";
  public static final String TABLE_NAME_SPACE_OPERATIONS_TAKE_MULTIPLE_TP = createValidTableName(METRIC_NAME_SPACE_OPERATIONS_TAKE_MULTIPLE_TP );//"SPACE_OPERATIONS_TAKE_MULTIPLE_TP";
  public static final String TABLE_NAME_SPACE_OPERATIONS_EXECUTE_TP = createValidTableName(METRIC_NAME_SPACE_OPERATIONS_EXECUTE_TP );//"SPACE_OPERATIONS_EXECUTE_TP";

  static String[] METRICS_TABLES = { METRIC_NAME_PROCESS_CPU_USED_PERCENT,
                              METRIC_NAME_JVM_MEMORY_HEAP_USED_PERCENT,
                              METRIC_NAME_JVM_MEMORY_HEAP_USED_BYTES,
                              METRIC_NAME_SPACE_REPLICATION_REDO_LOG_USED_PERCENT,
                              METRIC_NAME_SPACE_OPERATIONS_WRITE_TP,
                              METRIC_NAME_SPACE_OPERATIONS_READ_TP,
                              METRIC_NAME_SPACE_OPERATIONS_READ_MULTIPLE_TP,
                              METRIC_NAME_SPACE_OPERATIONS_TAKE_TP,
                              METRIC_NAME_SPACE_OPERATIONS_TAKE_MULTIPLE_TP,
                              METRIC_NAME_SPACE_OPERATIONS_EXECUTE_TP };

  static String createValidTableName(String dbTableName) {
    return dbTableName.toUpperCase()
        .replace( '-', '_' )
        .replace(':', '_')
        .replace('.', '_');
  }
}