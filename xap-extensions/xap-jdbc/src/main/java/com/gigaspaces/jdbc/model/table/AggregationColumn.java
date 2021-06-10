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

import java.util.Locale;

//TODO: @sagiv create interface IQueryColumn
public class AggregationColumn extends QueryColumn {

    private final AggregationFunctionType type;
    private final String functionName;
    private final String functionAlias;
    private final boolean allColumns;

    public AggregationColumn(AggregationFunctionType type, String functionName, String alias, String columnName,
                             String columnAlias, TableContainer tableContainer, boolean visible,
                             boolean allColumns, int columnIndex) {
        //TODO: @sagiv propertyType, not needed after the refactor changes.
        super(columnName, null, columnAlias, visible, tableContainer, columnIndex);
        this.type = type;
        this.functionName = functionName;
        this.functionAlias = alias;
        this.allColumns = allColumns;
    }

    public AggregationFunctionType getType() {
        return this.type;
    }

    public String getFunctionName() {
        return this.type.name().toLowerCase(Locale.ROOT);
    }

    public String getAlias() { return this.functionAlias;
    }

    public TableContainer getTableContainer() {
        return super.getTableContainer();
    }

    public String getColumnAlias() {
        return super.getAlias();
    }

    public String getColumnName() {
        return super.getNameOrAlias(); //TODO: @sagiv use getName instead?
    }

    public boolean isVisible() {
        return super.isVisible();
    }

    public boolean isAllColumns() {
        return allColumns;
    }

    public String getName() {
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }

    public String getNameWithLowerCase() {
        return String.format("%s(%s)", getFunctionName().toLowerCase(Locale.ROOT), getColumnName());
    }

    @Override
    public String getNameOrAlias() {
        return getAlias() == null ? getName() : getAlias();
    }

    @Override
    public String toString() {
        if(getTableContainer() != null) {
            return String.format("%s(%s)", getFunctionName(), getTableContainer().getTableNameOrAlias() + "." + getColumnName());
        }
        return String.format("%s(%s)", getFunctionName(), getColumnName());
    }
}
