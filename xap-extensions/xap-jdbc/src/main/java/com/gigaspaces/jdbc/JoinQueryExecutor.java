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

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.ExplainPlanResult;
import com.gigaspaces.jdbc.model.result.JoinTablesIterator;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.core.IJSpace;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class JoinQueryExecutor {
    private final IJSpace space;
    private final List<TableContainer> tables;
    private final List<QueryColumn> queryColumns;
    private final QueryExecutionConfig config;

    public JoinQueryExecutor(List<TableContainer> tables, IJSpace space, List<QueryColumn> queryColumns, QueryExecutionConfig config) {
        this.tables = tables;
        this.space = space;
        this.queryColumns = queryColumns;
        this.config = config;
    }

    public QueryResult execute() {
        for (TableContainer table : tables) {
            try {
                table.executeRead(config);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        }
        if(config.isExplainPlan())
            return explain();
        QueryResult res = new QueryResult(this.queryColumns);
        JoinTablesIterator joinTablesIterator = new JoinTablesIterator(tables);
        while (joinTablesIterator.hasNext()) {
            res.add(new TableRow(this.queryColumns));
        }
        return res;
    }

    private QueryResult explain() {
        TextReportFormatter formatter = new TextReportFormatter();
        formatter.line("Nested Loop Join");
        formatter.line("Select: " + String.join(", ",queryColumns.stream().map(QueryColumn::getName).collect(Collectors.toList())));
        formatter.indent();
        tables.forEach(t -> Arrays.stream(((ExplainPlanResult) t.getQueryResult()).getExplainPlanString().split("\n")).forEach(formatter::line));
        formatter.unindent();
        return new ExplainPlanResult(formatter.toString());
    }
}
