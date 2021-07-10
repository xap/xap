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
package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.SchemaTableContainer;
import com.gigaspaces.jdbc.model.table.TableContainer;

import java.util.ArrayList;
import java.util.List;

public class SchemaQueryResult extends QueryResult{
    private final SchemaTableContainer schemaTableContainer;
    private List<TableRow> rows = new ArrayList<>();

    public SchemaQueryResult(SchemaTableContainer schemaTableContainer, List<IQueryColumn> selectedColumns) {
        super(selectedColumns);
        this.schemaTableContainer = schemaTableContainer;
    }

    @Override
    public TableContainer getTableContainer() {
        return schemaTableContainer;
    }

    @Override
    public int size() {
        return rows.size();
    }

    @Override
    public void setRows(List<TableRow> rows) {
        this.rows = rows;
    }

    @Override
    public void addRow(TableRow tableRow) {
        rows.add(tableRow);
    }

    @Override
    public List<TableRow> getRows() {
        return rows;
    }
}
