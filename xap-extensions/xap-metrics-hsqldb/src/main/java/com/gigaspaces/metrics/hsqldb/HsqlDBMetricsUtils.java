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

public interface HsqlDBMetricsUtils {

  String[] METRICS_TABLES = { "process_cpu_used-percent",
                              "jvm_memory_heap_used-percent",
                              "jvm_memory_heap_used-bytes",
                              "space_replication_redo-log_used-percent",
                              "space_operations_write-tp",
                              "space_operations_read-tp",
                              "space_operations_read-multiple-tp",
                              "space_operations_take-tp",
                              "space_operations_take-multiple-tp",
                              "space_operations_execute-tp" };

}