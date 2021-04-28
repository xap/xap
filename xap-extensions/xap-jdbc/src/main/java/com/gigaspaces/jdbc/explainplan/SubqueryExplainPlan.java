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
import com.gigaspaces.jdbc.model.table.QueryColumn;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.List;
import java.util.stream.Collectors;

public class SubqueryExplainPlan extends JdbcExplainPlan {
    private final List<String> visibleColumns;
    private final JdbcExplainPlan plan;
    private final String tempViewName;
    private final Expression exprTree;

    public SubqueryExplainPlan(List<QueryColumn> visibleColumns, String name, JdbcExplainPlan explainPlanInfo, Expression exprTree) {
        this.tempViewName = name;
        this.visibleColumns = visibleColumns.stream().map(QueryColumn::getName).collect(Collectors.toList());
        this.plan = explainPlanInfo;
        this.exprTree = exprTree;
    }

    @Override
    public void format(TextReportFormatter formatter, boolean verbose) {
        formatter.line("Subquery scan on " + tempViewName);
        formatter.indent(() -> {
            formatter.line(String.format("Select: %s", String.join(", ", visibleColumns)));
//            formatter.line("Filter: <placeholder>"); //TODO EP
            if (exprTree != null) {
                ExpressionDeParser expressionDeParser = new ExpressionTreeDeParser();
                exprTree.accept(expressionDeParser);
                formatter.line("Filter: " + expressionDeParser.getBuffer().toString());
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
