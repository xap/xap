package com.gigaspaces.jdbc.explainplan;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;

import java.util.List;

public class GenericExplainPlan extends JdbcExplainPlan {
    private List<String> selectItems;
    private JdbcExplainPlan child;

    public GenericExplainPlan(List<String> visibleColumns, JdbcExplainPlan jdbcExplainPlan) {
        this.selectItems = visibleColumns;
        this.child = jdbcExplainPlan;
    }

    @Override
    public void format(TextReportFormatter formatter) {
        formatter.line(String.format("Select: %s", String.join(", ", selectItems)));
        formatter.indent(() -> child.format(formatter));
    }

}