package com.gigaspaces.jdbc.explainplan;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;

public class SubqueryPlan extends JdbcExplainPlan {
    private JdbcExplainPlan plan;
    private String tempViewName;

    public SubqueryPlan(String name, JdbcExplainPlan explainPlanInfo) {
        this.tempViewName = name;
        plan = explainPlanInfo;
    }

    @Override
    public void format(TextReportFormatter formatter) {
        formatter.line("TempView: " + tempViewName);
        formatter.indent(() -> plan.format(formatter));
    }
}
