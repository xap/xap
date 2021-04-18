package com.gigaspaces.jdbc.explainplan;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;

import java.util.Collections;
import java.util.List;

public class JoinExplainPlan extends JdbcExplainPlan {
    private String joinCondition;
    private String joinType;
    private String joinAlgorithm;
    private final List<String> selectColumns;
    private JdbcExplainPlan left;
    private JdbcExplainPlan right;

    public JoinExplainPlan(JdbcExplainPlan left, JdbcExplainPlan right) {
        this.left = left;
        this.right = right;
        this.joinCondition = "join_condition"; // TODO EP
        this.joinType = "Inner"; //TODO EP
        this.joinAlgorithm = "Nested"; //Hash/Merge/Nested //TODO EP (
        this.selectColumns = Collections.singletonList("col1"); //TODO EP
    }

    @Override
    public void format(TextReportFormatter formatter) {
        formatter.line(String.format("%s Join (%s)", joinType, joinAlgorithm)); //TODO EP
        formatter.indent(() -> {
//            formatter.line(String.format("Select: %s", String.join(", ", selectColumns))); //TODO EP
//            formatter.line(String.format("Join condition: (%s)", joinCondition)); //TODO EP
            formatter.indent(() -> {
//                formatter.line("BuildPhase"); //TODO EP
//                formatter.indent(() -> {//TODO EP
                        left.format(formatter);
//                });//TODO EP
            });
            formatter.indent(() -> {
//                formatter.line("ProbePhase");//TODO EP
//                formatter.indent(() -> {//TODO EP
                    right.format(formatter);
//                });//TODO EP

            });
        });
    }
}
