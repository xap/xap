package com.gigaspaces.jdbc.explainplan;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;
import com.gigaspaces.internal.query.explainplan.model.JdbcExplainPlan;
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

    public SubqueryExplainPlan(List<IQueryColumn> visibleColumns, String name, JdbcExplainPlan explainPlanInfo,
                               Expression exprTree, List<OrderColumn> orderColumns) {
        this.tempViewName = name;
        this.visibleColumnNames = visibleColumns.stream().map(IQueryColumn::getName).collect(Collectors.toList());
        this.plan = explainPlanInfo;
        this.exprTree = exprTree;
        this.orderColumns = orderColumns;
    }

    @Override
    public void format(TextReportFormatter formatter, boolean verbose) {
        formatter.line("Subquery scan on " + tempViewName); ////
        formatter.indent(() -> {
            formatter.line(String.format("Select: %s", String.join(", ", visibleColumnNames)));
//            formatter.line("Filter: <placeholder>"); //TODO EP
            if (exprTree != null) {
                ExpressionDeParser expressionDeParser = new ExpressionTreeDeParser();
                exprTree.accept(expressionDeParser);
                formatter.line("Filter: " + expressionDeParser.getBuffer().toString());
            }
            if (orderColumns != null && !orderColumns.isEmpty()) {
                formatter.line("OrderBy: " + orderColumns.stream().map(OrderColumn::toString).collect(Collectors.joining(", ")));
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
