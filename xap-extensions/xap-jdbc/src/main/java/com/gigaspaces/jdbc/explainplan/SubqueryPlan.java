package com.gigaspaces.jdbc.explainplan;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;
import com.gigaspaces.jdbc.model.table.QueryColumn;

import java.util.List;
import java.util.stream.Collectors;

public class SubqueryPlan extends JdbcExplainPlan {
    private final List<String> visibleColumns;
    private final JdbcExplainPlan plan;
    private final String tempViewName;

    public SubqueryPlan(List<QueryColumn> visibleColumns, String name, JdbcExplainPlan explainPlanInfo) {
        this.tempViewName = name;
        this.visibleColumns = visibleColumns.stream().map(QueryColumn::getName).collect(Collectors.toList());
        this.plan = explainPlanInfo;
    }

    @Override
    public void format(TextReportFormatter formatter) {
        formatter.line("Subquery scan on " + tempViewName);
        formatter.indent(() -> {
            formatter.line(String.format("Select: %s", String.join(", ", visibleColumns)));
//            formatter.line("Filter: <placeholder>"); //TODO EP
            formatter.indent(() -> {
                formatter.line(String.format("TempView: %s", tempViewName));
                formatter.indent(() -> plan.format(formatter));
            });
        });
    }
}
