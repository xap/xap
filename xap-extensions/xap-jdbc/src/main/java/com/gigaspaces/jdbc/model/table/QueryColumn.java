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
package com.gigaspaces.jdbc.model.table;

import java.util.Objects;

public class QueryColumn implements Comparable<QueryColumn>{
    public static final String UUID_COLUMN = "UID";
    protected final TableContainer tableContainer;
    private final String columnName;
    private final String columnAlias;
    private final boolean isVisible;
    private final boolean isUUID;
    private final Class<?> propertyType;
    private final int columnIndex;

    public QueryColumn(String name, Class<?> propertyType, String alias, boolean isVisible, TableContainer tableContainer, int columnIndex) {
        this.columnName = name;
        this.columnAlias = alias;
        this.isVisible = isVisible;
        this.isUUID = name.equalsIgnoreCase(UUID_COLUMN);
        this.tableContainer = tableContainer;
        this.propertyType = propertyType;
        this.columnIndex = columnIndex;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

    public String getName() {
        return columnName;
    }

    public String getAlias() {
        return columnAlias;
    }

    public boolean isVisible() {
        return isVisible;
    }

    public boolean isUUID() {
        return isUUID;
    }

    public TableContainer getTableContainer() {
        return tableContainer;
    }

    public Object getCurrentValue() {
        if (tableContainer.getQueryResult().getCurrent() == null)
            return null;
        return tableContainer.getQueryResult().getCurrent().getPropertyValue(this);
    }

    public Class<?> getPropertyType() {
        return propertyType;
    }

    @Override
    public String toString() {
        return tableContainer.getTableNameOrAlias() + "." + getNameOrAlias();
    }

    public String getNameOrAlias() {
        return columnAlias != null ? columnAlias : columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueryColumn)) return false;
        QueryColumn that = (QueryColumn) o;
        return isVisible() == that.isVisible() && isUUID() == that.isUUID() && Objects.equals(getTableContainer(), that.getTableContainer()) && Objects.equals(getName(), that.getName()) && Objects.equals(getAlias(), that.getAlias());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTableContainer(), getName(), getAlias(), isVisible(), isUUID());
    }

    @Override
    public int compareTo(QueryColumn other) {
        return Integer.compare(this.getColumnIndex(), other.getColumnIndex());
    }
}
