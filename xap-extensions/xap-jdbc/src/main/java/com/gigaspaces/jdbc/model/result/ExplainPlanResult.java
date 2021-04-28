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

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.ResultEntry;

import java.util.Collections;
import java.util.List;

public class ExplainPlanResult extends QueryResult {
    private final JdbcExplainPlan jdbcExplainPlan;
    private final List<QueryColumn> visibleColumns;

    public ExplainPlanResult(List<QueryColumn> visibleColumns, JdbcExplainPlan jdbcExplainPlan, TableContainer tableContainer) {
        super(Collections.singletonList(new ExplainPlanQueryColumn()));
        this.tableContainer = tableContainer;
        this.jdbcExplainPlan = jdbcExplainPlan;
        this.visibleColumns = visibleColumns;
    }

    public JdbcExplainPlan getExplainPlanInfo() {
        return jdbcExplainPlan;
    }

    public List<QueryColumn> getVisibleColumns() {
        return visibleColumns;
    }

    @Override
    public ResultEntry convertEntriesToResultArrays(QueryExecutionConfig config) {
        // Column (field) names and labels (aliases)

        String[] fieldNames = new String[]{config.isExplainPlanVerbose() ? ExplainPlanQueryColumn.EXPLAIN_PLAN_VERBOSE_COL_NAME: ExplainPlanQueryColumn.EXPLAIN_PLAN_COL_NAME};
        String[] columnLabels = new String[]{config.isExplainPlanVerbose() ? ExplainPlanQueryColumn.EXPLAIN_PLAN_VERBOSE_COL_NAME: ExplainPlanQueryColumn.EXPLAIN_PLAN_COL_NAME};


        //the field values for the result
        TextReportFormatter formatter = new TextReportFormatter();
        jdbcExplainPlan.format(formatter, config.isExplainPlanVerbose());

        String[] lines = formatter.toString().split("\n");
        Object[][] fieldValues = new Object[lines.length][1];

        int row = 0;
        for (String line : lines) {
            fieldValues[row++][0] = line;
        }

        return new ResultEntry(
                fieldNames,
                columnLabels,
                null, //TODO
                fieldValues);
    }

}
