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
package com.gigaspaces.jdbc.explainplan;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;
import com.gigaspaces.jdbc.model.join.JoinInfo;
import com.gigaspaces.jdbc.model.result.Cursor;
import com.gigaspaces.jdbc.model.table.ConcreteColumn;
import com.gigaspaces.jdbc.model.table.ConcreteTableContainer;
import com.gigaspaces.jdbc.model.table.OrderColumn;

import java.util.List;
import java.util.stream.Collectors;

public class JoinExplainPlan extends JdbcExplainPlan {
    private final JoinInfo joinInfo;
    private List<String> selectColumns;
    private List<OrderColumn> orderColumns;
    private List<ConcreteColumn> groupByColumns;
    private boolean distinct;
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

    public void setGroupByColumns(List<ConcreteColumn> groupByColumns) {
        this.groupByColumns = groupByColumns;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    @Override
    public void format(TextReportFormatter formatter, boolean verbose) {
        JoinInfo.JoinAlgorithm joinAlgorithm = joinInfo.getRightColumn().getTableContainer().getQueryResult().getCursorType().equals(Cursor.Type.HASH) ? JoinInfo.JoinAlgorithm.Hash : JoinInfo.JoinAlgorithm.Nested;
        boolean hashJoin = joinAlgorithm.equals(JoinInfo.JoinAlgorithm.Hash);
        formatter.line(String.format("%s Join (%s)", joinInfo.getJoinType(), joinAlgorithm));
        formatter.indent(() -> {
            if (selectColumns != null)
                formatter.line(String.format(distinct ? "Select Distinct: %s" : "Select: %s", String.join(", ", selectColumns)));
            if (orderColumns != null && !orderColumns.isEmpty()) {
                formatter.line("OrderBy: " + orderColumns.stream().map(OrderColumn::toString).collect(Collectors.joining(", ")));
            }
            if (groupByColumns != null && !groupByColumns.isEmpty()) {
                formatter.line("GroupBy: " + groupByColumns.stream().map(ConcreteColumn::toString).collect(Collectors.joining(", ")));
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
