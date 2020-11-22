package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.api.InternalApi;

/**
 * @since 15.5
 */
@InternalApi
public interface TableColumnNames {

    String TIME_COLUMN_NAME = "time";
    String VALUE_COLUMN_NAME = "value";
    String PU_INSTANCE_ID_COLUMN_NAME = "pu_instance_id";
    String SPACE_INSTANCE_ID_COLUMN_NAME = "space_instance_id";
    String DATA_TYPE_NAME_COLUMN_NAME = "data_type_name";
    String SPACE_NAME_COLUMN_NAME = "space_name";
    String PU_NAME_COLUMN_NAME = "pu_name";
    String INDEX_COLUMN_NAME = "index";
    String HOST_COLUMN_NAME = "host";
    String PID_COLUMN_NAME = "pid";
    String IP_COLUMN_NAME = "ip";
    String PROCESS_NAME_COLUMN_NAME = "process_name";
}