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

public class OrderColumn extends QueryColumn {

    private boolean isAsc = true;
    private boolean isNullsLast = false;

    public OrderColumn(String name, boolean isVisible, TableContainer tableContainer) {
        super(name, null, isVisible, tableContainer);
    }

    public boolean isAsc() {
        return isAsc;
    }
    public void setAsc(boolean isAsc) {
        this.isAsc = isAsc;
    }
    public OrderColumn withAsc(boolean isAsc) {
        this.isAsc = isAsc;
        return this;
    }

    public boolean isNullsLast() {
        return isNullsLast;
    }
    public void setNullsLast(boolean isNullsLast) {
        this.isNullsLast = isNullsLast;
    }
    public OrderColumn withNullsLast(boolean isNullsLast) {
        this.isNullsLast = isNullsLast;
        return this;
    }

    @Override
    public Object getCurrentValue() {
        if(tableContainer.getQueryResult().getCurrent() == null) {
            return null;
        }
        return tableContainer.getQueryResult().getCurrent().getPropertyValue(this); // visit getPropertyValue(OrderColumn)
    }

    @Override
    public String toString() {
        return getName() + " " + (isAsc ? "ASC" : "DESC") + " " + (isNullsLast ? "NULLS LAST" : "NULLS FIRST");
    }
}
