package com.gigaspaces.metrics.hsqldb;

import com.gigaspaces.api.InternalApi;

/**
 * @since 15.5
 */
@InternalApi
public interface TableColumns {

    String TIME = "time";
    String VALUE = "value";
    String PU_INSTANCE_ID = "pu_instance_id";
    String DATA_TYPE_NAME = "data_type_name";
    String SPACE_NAME = "space_name";
    String PU_NAME = "pu_name";
    String SPACE_ACTIVE = "space_active";
    String INDEX = "index";
}