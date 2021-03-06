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

import java.util.List;

/**
 * Encapsulates information on a table.
 *
 * @author Niv Ingberg
 * @since 15.2.0
 */
public class TableInfo {
    private final TableId id;
    private final List<ColumnInfo> columns;
    private final List<String> primaryKey;
    private final List<IndexInfo> indexes;
    private final boolean isUIDColumnExist;

    TableInfo(TableId id, List<ColumnInfo> columns, List<String> primaryKey, List<IndexInfo> indexes, boolean isUIDColumnExist) {
        this.id = id;
        this.columns = columns;
        this.primaryKey = primaryKey;
        this.indexes = indexes;
        this.isUIDColumnExist = isUIDColumnExist;
    }

    public TableId getId() {
        return id;
    }

    public List<ColumnInfo> getColumns() {
        return columns;
    }

    public List<String> getPrimaryKey() {
        return primaryKey;
    }

    public List<IndexInfo> getIndexes() {
        return indexes;
    }

    public boolean isUIDColumnExist() {
        return isUIDColumnExist;
    }
}
