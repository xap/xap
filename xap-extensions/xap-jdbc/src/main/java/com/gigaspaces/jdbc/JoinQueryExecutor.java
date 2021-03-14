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
package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.core.IJSpace;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class JoinQueryExecutor {
    private final IJSpace space;
    private final List<TableContainer> tables;
    private final List<QueryColumn> queryColumns;
    private final boolean explainPlan;

    public JoinQueryExecutor(List<TableContainer> tables, IJSpace space, List<QueryColumn> queryColumns, boolean explainPlan) {
        this.tables = tables;
        this.space = space;
        this.queryColumns = queryColumns;
        this.explainPlan = explainPlan;
    }

    public QueryResult execute() {
        List<QueryResult> results = new ArrayList<>();
        for (TableContainer table : tables) {
            try {
                results.add(table.executeRead(explainPlan));
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        }
        List<QueryColumn> visibleColumns = explainPlan ? Collections.singletonList(new ExplainPlanQueryColumn()) : this.queryColumns;
        QueryResult res = new QueryResult(visibleColumns);
        Iterator<TableRow> iter1 = results.get(0).iterator();
        while (iter1.hasNext()) {
            TableRow iter1Next = iter1.next();
            Iterator<TableRow> iter2 = results.get(1).iterator();
            while (iter2.hasNext()) {
                TableRow iter2Next = iter2.next();

                res.add(new TableRow(visibleColumns, iter1Next, iter2Next));
            }
        }

        return res;
    }
}
