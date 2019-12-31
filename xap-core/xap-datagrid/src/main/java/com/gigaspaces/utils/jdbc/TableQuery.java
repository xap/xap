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

/**
 * Encapsulates details for filtering tables when querying the database.
 *
 * @author Niv Ingberg
 * @since 15.2.0
 */
public class TableQuery {
    private static final String[] DEFAULT_TYPES = new String[] {"TABLE"};

    private String catalog;
    private String schemaPattern;
    private String tableNamePattern = "%";
    private String[] types = DEFAULT_TYPES;

    public String getCatalog() {
        return catalog;
    }
    public TableQuery setCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public String getSchemaPattern() {
        return schemaPattern;
    }
    public TableQuery setSchemaPattern(String schemaPattern) {
        this.schemaPattern = schemaPattern;
        return this;
    }

    public String getTableNamePattern() {
        return tableNamePattern;
    }
    public TableQuery setTableNamePattern(String tableNamePattern) {
        this.tableNamePattern = tableNamePattern;
        return this;
    }

    public String[] getTypes() {
        return types;
    }
    public void setTypes(String[] types) {
        this.types = types;
    }
}
