package com.gigaspaces.jdbc.explainplan;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;

public class JoinExplainPlan extends JdbcExplainPlan {
    private String joinCondition;
    private String joinType;
    private JdbcExplainPlan left;
    private JdbcExplainPlan right;

    public JoinExplainPlan(JdbcExplainPlan left, JdbcExplainPlan right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void format(TextReportFormatter formatter) {
        formatter.line(String.format("HashJoin (%s)", joinCondition));
        formatter.indent(() -> left.format(formatter));
        formatter.indent(() -> right.format(formatter));
    }
}
