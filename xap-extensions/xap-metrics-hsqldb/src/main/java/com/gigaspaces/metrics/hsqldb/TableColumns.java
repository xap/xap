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