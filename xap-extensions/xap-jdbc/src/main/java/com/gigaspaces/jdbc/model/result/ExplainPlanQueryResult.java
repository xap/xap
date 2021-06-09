package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.table.ExplainPlanConcreteColumn;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.jdbc.ResultEntry;

import java.util.Collections;
import java.util.List;

public class ExplainPlanQueryResult extends QueryResult {
    private final JdbcExplainPlan jdbcExplainPlan;
    private final List<IQueryColumn> visibleColumns;
    private final TableContainer tableContainer;

    public ExplainPlanQueryResult(List<IQueryColumn> visibleColumns, JdbcExplainPlan jdbcExplainPlan, TableContainer tableContainer) {
        super(Collections.singletonList(new ExplainPlanConcreteColumn()));
        this.jdbcExplainPlan = jdbcExplainPlan;
        this.visibleColumns = visibleColumns;
        this.tableContainer = tableContainer;
    }

    @Override
    public TableContainer getTableContainer() {
        return this.tableContainer;
    }

    public JdbcExplainPlan getExplainPlanInfo() {
        return jdbcExplainPlan;
    }

    public List<IQueryColumn> getVisibleColumns() {
        return visibleColumns;
    }

    public ResultEntry convertEntriesToResultArrays(QueryExecutionConfig config) {
        // Column (field) names and labels (aliases)
        String[] fieldNames = new String[]{config.isExplainPlanVerbose() ? ExplainPlanConcreteColumn.EXPLAIN_PLAN_VERBOSE_COL_NAME : ExplainPlanConcreteColumn.EXPLAIN_PLAN_COL_NAME};
        String[] columnLabels = new String[]{config.isExplainPlanVerbose() ? ExplainPlanConcreteColumn.EXPLAIN_PLAN_VERBOSE_COL_NAME : ExplainPlanConcreteColumn.EXPLAIN_PLAN_COL_NAME};


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
