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

import com.gigaspaces.jdbc.model.table.TempTableContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TempQueryResult extends QueryResult{
    private List<TableRow> rows = new ArrayList<>();
    private final TempTableContainer tempTableContainer;

    public TempQueryResult(TempTableContainer tempTableContainer) {
        super(tempTableContainer.getSelectedColumns());
        this.tempTableContainer = tempTableContainer;
        List<TableRow> tableRows = tempTableContainer.getQueryResult().getRows();
        if (tempTableContainer.hasAggregationFunctions() && !tempTableContainer.hasGroupByColumns()) {
            this.rows.add( aggregate(tableRows) );
        } else {
            this.rows = tableRows.stream().map(row -> TableRowFactory.createProjectedTableRow(row, tempTableContainer)).collect(Collectors.toList());
        }
    }

    public TableRow aggregate( List<TableRow> tableRows) {
        return TableRowUtils.aggregate(tableRows, tempTableContainer.getSelectedColumns(),
                            tempTableContainer.getAggregationColumns(), tempTableContainer.getVisibleColumns() );
    }

    @Override
    public int size() {
        return this.rows.size();
    }

    @Override
    public void addRow(TableRow tableRow) {
        this.rows.add(tableRow);
    }

    @Override
    public List<TableRow> getRows() {
        return this.rows;
    }

    @Override
    public void setRows(List<TableRow> rows) {
        this.rows = rows;
    }
}
