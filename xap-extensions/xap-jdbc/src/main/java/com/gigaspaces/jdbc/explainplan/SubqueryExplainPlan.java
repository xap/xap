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
import com.gigaspaces.jdbc.model.table.ConcreteColumn;
import com.gigaspaces.jdbc.model.table.IQueryColumn;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.List;
import java.util.stream.Collectors;

public class SubqueryExplainPlan extends JdbcExplainPlan {
    private final List<String> visibleColumnNames;
    private final JdbcExplainPlan plan;
    private final String tempViewName;
    private final Expression exprTree;
    private final List<OrderColumn> orderColumns;
    private final List<ConcreteColumn> groupByColumns;
    private final boolean distinct;

    public SubqueryExplainPlan(List<IQueryColumn> visibleColumns, String name, JdbcExplainPlan explainPlanInfo,
                               Expression exprTree, List<OrderColumn> orderColumns, List<ConcreteColumn> groupByColumns,
                               boolean isDistinct) {
        this.tempViewName = name;
        this.visibleColumnNames = visibleColumns.stream().map(IQueryColumn::getName).collect(Collectors.toList());
        this.plan = explainPlanInfo;
        this.exprTree = exprTree;
        this.orderColumns = orderColumns;
        this.groupByColumns = groupByColumns;
        this.distinct = isDistinct;
    }

    @Override
    public void format(TextReportFormatter formatter, boolean verbose) {
        formatter.line("Subquery scan on " + tempViewName); ////
        formatter.indent(() -> {
            formatter.line(String.format(distinct ? "Select Distinct: %s" : "Select: %s", String.join(", ", visibleColumnNames)));
//            formatter.line("Filter: <placeholder>"); //TODO EP
            if (exprTree != null) {
                ExpressionDeParser expressionDeParser = new ExpressionTreeDeParser();
                exprTree.accept(expressionDeParser);
                formatter.line("Filter: " + expressionDeParser.getBuffer().toString());
            }
            if (orderColumns != null && !orderColumns.isEmpty()) {
                formatter.line("OrderBy: " + orderColumns.stream().map(OrderColumn::toString).collect(Collectors.joining(", ")));
            }
            if (groupByColumns != null && !groupByColumns.isEmpty()) {
                formatter.line("GroupBy: " + groupByColumns.stream().map(ConcreteColumn::toString).collect(Collectors.joining(", ")));
            }
            formatter.withFirstLine("->", () -> {
                formatter.line(String.format("TempView: %s", tempViewName));
                formatter.withFirstLine("->", () -> {
                    plan.format(formatter, verbose);
                });
            });
        });
    }
}
