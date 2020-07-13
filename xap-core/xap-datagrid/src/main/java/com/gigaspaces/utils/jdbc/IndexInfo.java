/*
 * Copyright (c) 2008-2019, GigaSpaces Technologies, Inc. All Rights Reserved.
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

package com.gigaspaces.utils.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates information on a table's index.
 *
 * @author Niv Ingberg
 * @since 15.2.0
 */
public class IndexInfo {
    private final String name;
    private String orderType;
    private final List<String> columns = new ArrayList<>();
    private final boolean nonUnique;

    public IndexInfo(String name, String orderType, boolean nonUnique) {
        this.name = name;
        this.orderType = orderType;
        this.nonUnique = nonUnique;
    }

    public String getName() {
        return name;
    }

    public String getOrderType() {
        return orderType;
    }

    public void setOrderType(String orderType) {
        this.orderType = orderType;
    }

    public List<String> getColumns() {
        return columns;
    }

    public boolean isNonUnique() {
        return nonUnique;
    }
}
