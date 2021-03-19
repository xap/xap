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

import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TableRow {
    private QueryColumn[] columns;
    private Object[] values;

    public TableRow(QueryColumn[] columns, Object[] values) {
        this.columns = columns;
        this.values = values;
    }

    public TableRow(IEntryPacket x, List<QueryColumn> queryColumns) {
        this.columns = queryColumns.toArray(new QueryColumn[0]);
        values = new Object[columns.length];
        for (int i = 0; i < queryColumns.size(); i++) {
            values[i] = x.getPropertyValue(queryColumns.get(i).getName());
        }
    }

    public TableRow(List<QueryColumn> queryColumns, TableRow... tablesRow) {
        if (Arrays.stream(tablesRow).allMatch(x -> x instanceof ExplainPlanTableRow)) {
            ExplainPlanTableRow[] explainPlanTableRows = Arrays.stream(tablesRow).map(x -> ((ExplainPlanTableRow) x)).toArray(ExplainPlanTableRow[]::new);
            mergeExplainPlan(explainPlanTableRows);
        } else {
            this.columns = queryColumns.toArray(new QueryColumn[0]);
            values = Arrays.stream(columns)
                    .map(queryColumn ->
                            Arrays.stream(tablesRow)
                                    .filter(tableRow -> tableRow.hasColumn(queryColumn))
                                    .findFirst()
                                    .get()
                                    .getPropertyValue(queryColumn))
                    .toArray();
        }
    }

    private void mergeExplainPlan(ExplainPlanTableRow[] explainPlanTableRows) {
        this.columns = new QueryColumn[]{new ExplainPlanQueryColumn()};
        String explainPlanString = Arrays.stream(explainPlanTableRows).map(x -> x.getPropertyValue(0).toString()).collect(Collectors.joining("\n------\n"));
        this.values = new String[]{explainPlanString};
    }

    public Object getPropertyValue(int index) {
        return values[index];
    }

    public Object getPropertyValue(QueryColumn column) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i].equals(column)) {
                return values[i];
            }
        }
        return null;
    }

    public boolean hasColumn(QueryColumn column) {
        for (QueryColumn queryColumn : columns) {
            if (queryColumn.equals(column)) return true;
        }
        return false;
    }
}
