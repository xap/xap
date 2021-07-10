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


import com.gigaspaces.internal.transport.IEntryPacket;

import java.util.Locale;
import java.util.Objects;

public class AggregationColumn implements IQueryColumn {

    private final AggregationFunctionType type;
    private final String functionAlias;
    private final boolean isVisible;
    private final boolean allColumns;
    private final IQueryColumn queryColumn;
    private final int columnOrdinal;

    public AggregationColumn(AggregationFunctionType functionType, String functionAlias, IQueryColumn queryColumn,
                             boolean isVisible, boolean allColumns, int columnOrdinal) {
        this.queryColumn = queryColumn;
        this.type = functionType;
        this.functionAlias = Objects.requireNonNull(functionAlias);
        this.allColumns = allColumns;
        this.isVisible = isVisible;
        this.columnOrdinal = columnOrdinal;
    }

    public AggregationFunctionType getType() {
        return this.type;
    }

    private String getFunctionName() {
        return this.type.name().toLowerCase(Locale.ROOT);
    }

    public String getAlias() {
        return this.functionAlias;
    }

    public TableContainer getTableContainer() {
        return this.queryColumn != null ? this.queryColumn.getTableContainer() : null;
    }

    @Override
    public Object getCurrentValue() {
        if (getTableContainer().getQueryResult().getCurrent() == null) {
            return null;
        }
        return getTableContainer().getQueryResult().getCurrent().getPropertyValue(this);
    }

    @Override
    public Class<?> getReturnType() {
        return this.queryColumn != null ? this.queryColumn.getReturnType() : null;
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        return new AggregationColumn(getType(), columnAlias == null ? columnName : columnAlias, getQueryColumn(),
                isVisible, isAllColumns(), columnOrdinal);
    }

    public String getColumnName() {
        if (this.queryColumn == null) {
            return isAllColumns() ? "*" : null;
        }
        return this.queryColumn.getAlias();  //return either name or alias.
    }

    public boolean isVisible() {
        return this.isVisible;
    }

    @Override
    public boolean isUUID() {
        return false;
    }

    public boolean isAllColumns() {
        return this.allColumns;
    }

    @Override
    public int getColumnOrdinal() {
        return this.columnOrdinal;
    }

    public String getName() {
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    @Override
    public int compareTo(IQueryColumn other) {
        return Integer.compare(this.getColumnOrdinal(), other.getColumnOrdinal());
    }

    public IQueryColumn getQueryColumn() {
        return this.queryColumn;
    }

    @Override
    public String toString() {
        if (getTableContainer() != null) {
            return String.format("%s(%s)", getFunctionName(), getTableContainer().getTableNameOrAlias() + "." + getColumnName());
        }
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggregationColumn)) return false;
        AggregationColumn that = (AggregationColumn) o;
        return isVisible() == that.isVisible()
                && isAllColumns() == that.isAllColumns()
                && getColumnOrdinal() == that.getColumnOrdinal()
                && getType() == that.getType()
                && Objects.equals(getAlias(), that.getAlias())
                && Objects.equals(getQueryColumn(), that.getQueryColumn());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getType(), getAlias(), isVisible(), isAllColumns(), getQueryColumn(), getColumnOrdinal());
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        return queryColumn.getValue(entryPacket);
    }
}
