package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.j_spaces.jdbc.ResultEntry;

import java.util.Collections;
import java.util.List;

public class ExplainPlanResult extends QueryResult {
    private final JdbcExplainPlan jdbcExplainPlan;
    private final List<QueryColumn> visibleColumns;

    public ExplainPlanResult(List<QueryColumn> visibleColumns, JdbcExplainPlan jdbcExplainPlan) {
        super(Collections.singletonList(new ExplainPlanQueryColumn()));
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

        String[] fieldNames = new String[]{ExplainPlanQueryColumn.EXPLAIN_PLAN_COL_NAME};
        String[] columnLabels = new String[]{ExplainPlanQueryColumn.EXPLAIN_PLAN_COL_NAME};


        //the field values for the result
        TextReportFormatter formatter = new TextReportFormatter();
        jdbcExplainPlan.format(formatter);

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
