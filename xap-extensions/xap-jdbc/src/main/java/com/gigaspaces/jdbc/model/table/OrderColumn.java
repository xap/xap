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

public class OrderColumn implements IQueryColumn {

    private final boolean isAsc;
    private final boolean isNullsLast;
    private final IQueryColumn queryColumn;

    public OrderColumn(IQueryColumn queryColumn, boolean isAsc, boolean isNullsLast) {
        this.queryColumn = queryColumn;
        this.isAsc = isAsc;
        this.isNullsLast = isNullsLast;
    }

    public boolean isAsc() {
        return isAsc;
    }

    public boolean isNullsLast() {
        return isNullsLast;
    }

    @Override
    public int getColumnOrdinal() {
        return this.queryColumn.getColumnOrdinal();
    }

    @Override
    public String getName() {
        return this.queryColumn.getName();
    }

    @Override
    public String getAlias() {
        return this.queryColumn.getAlias();
    }

    @Override
    public boolean isVisible() {
        return this.queryColumn.isVisible();
    }

    @Override
    public boolean isUUID() {
        return this.queryColumn.isUUID();
    }

    @Override
    public TableContainer getTableContainer() {
        return this.queryColumn.getTableContainer();
    }

    @Override
    public Object getCurrentValue() {
        if(getTableContainer().getQueryResult().getCurrent() == null) {
            return null;
        }
        return getTableContainer().getQueryResult().getCurrent().getPropertyValue(this); // visit getPropertyValue(OrderColumn)
    }

    @Override
    public Class<?> getReturnType() {
        return this.queryColumn.getReturnType();
    }

    @Override
    public IQueryColumn create(String columnName, String columnAlias, boolean isVisible, int columnOrdinal) {
        return new OrderColumn(new ConcreteColumn(columnName, getReturnType(), columnAlias, isVisible,
                getTableContainer(), columnOrdinal), isAsc(), isNullsLast());
    }

    public IQueryColumn getQueryColumn() {
        return queryColumn;
    }

    @Override
    public String toString() {
        return getAlias() + " " + (isAsc ? "ASC" : "DESC") + " " + (isNullsLast ? "NULLS LAST" : "NULLS FIRST");
    }

    @Override
    public int compareTo(IQueryColumn other) {
        return this.queryColumn.compareTo(other);
    }

    @Override
    public Object getValue(IEntryPacket entryPacket) {
        return queryColumn.getValue(entryPacket);
    }
}
