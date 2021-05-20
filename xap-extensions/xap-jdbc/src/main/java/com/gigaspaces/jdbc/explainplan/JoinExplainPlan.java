package com.gigaspaces.jdbc.explainplan;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.Cursor;
import com.gigaspaces.jdbc.model.table.OrderColumn;

import java.util.List;
import java.util.stream.Collectors;

public class JoinExplainPlan extends JdbcExplainPlan {
    private final JoinInfo joinInfo;
    private List<String> selectColumns;
    private List<OrderColumn> orderColumns;
    private final JdbcExplainPlan left;
    private final JdbcExplainPlan right;

    public JoinExplainPlan(JoinInfo joinInfo, JdbcExplainPlan left, JdbcExplainPlan right) {
        this.joinInfo = joinInfo;
        this.left = left;
        this.right = right;
    }

    public void setSelectColumns(List<String> selectColumns) {
        this.selectColumns = selectColumns;
    }

    public void setOrderColumns(List<OrderColumn> orderColumns) {
        this.orderColumns = orderColumns;
    }

    @Override
    public void format(TextReportFormatter formatter, boolean verbose) {
        JoinInfo.JoinAlgorithm joinAlgorithm = joinInfo.getRightColumn().getTableContainer().getQueryResult().getCursorType().equals(Cursor.Type.HASH) ? JoinInfo.JoinAlgorithm.Hash : JoinInfo.JoinAlgorithm.Nested;
        boolean hashJoin = joinAlgorithm.equals(JoinInfo.JoinAlgorithm.Hash);
        formatter.line(String.format("%s Join (%s)", joinInfo.getJoinType(), joinAlgorithm));
        formatter.indent(() -> {
            if (selectColumns != null)
                formatter.line(String.format("Select: %s", String.join(", ", selectColumns)));
            if (orderColumns != null && !orderColumns.isEmpty()) {
                formatter.line("OrderBy: " + orderColumns.stream().map(OrderColumn::toString).collect(Collectors.joining(", ")));
            }
            formatter.line(String.format("Join condition: (%s = %s)", joinInfo.getLeftColumn(), joinInfo.getRightColumn()));
            formatter.withPrefix("|", () -> {
                formatter.withFirstLine("->", () ->
                {
                    if (hashJoin) {
                        formatter.line(String.format("BuildPhase - Hash by: %s", joinInfo.getRightColumn()));
                        formatter.indent(() -> left.format(formatter, verbose));
                    } else {
                        left.format(formatter, verbose);
                    }
                });

            });
            formatter.withFirstLine("|->", () -> {
                if (hashJoin) {
                    formatter.line("ProbePhase");
                    formatter.indent(() -> right.format(formatter, verbose));
                } else {
                    right.format(formatter, verbose);
                }
            });

        });
    }


}
