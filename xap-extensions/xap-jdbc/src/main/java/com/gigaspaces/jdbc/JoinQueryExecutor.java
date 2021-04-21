package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.explainplan.JoinExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.ExplainPlanResult;
import com.gigaspaces.jdbc.model.result.JoinTablesIterator;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.core.IJSpace;

import java.sql.SQLException;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

public class JoinQueryExecutor {
    private final IJSpace space;
    private final List<TableContainer> tables;
    private final List<QueryColumn> queryColumns;
    private final QueryExecutionConfig config;

    public JoinQueryExecutor(List<TableContainer> tables, IJSpace space, List<QueryColumn> queryColumns, QueryExecutionConfig config) {
        this.tables = tables;
        this.space = space;
        this.queryColumns = queryColumns;
        this.config = config;
    }

    public QueryResult execute() {
        for (TableContainer table : tables) {
            try {
                table.executeRead(config);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        }
        JoinTablesIterator joinTablesIterator = new JoinTablesIterator(tables);
        if(config.isExplainPlan()) {
            return explain(joinTablesIterator);
        }
        QueryResult res = new QueryResult(this.queryColumns);
        while (joinTablesIterator.hasNext()) {
            if(tables.stream().allMatch(TableContainer::checkJoinCondition))
                res.add(new TableRow(this.queryColumns));
        }
        return res;
    }

    private QueryResult explain(JoinTablesIterator joinTablesIterator) {
        Stack<TableContainer> stack = new Stack<>();
        TableContainer current = joinTablesIterator.getStartingPoint();
        stack.push(current);
        while (current.getJoinedTable() != null){
            current = current.getJoinedTable();
            stack.push(current);
        }
        TableContainer first = stack.pop();
        TableContainer second = stack.pop();
        JoinExplainPlan joinExplainPlan = new JoinExplainPlan(first.getJoinInfo(), ((ExplainPlanResult) first.getQueryResult()).getExplainPlanInfo(), ((ExplainPlanResult) second.getQueryResult()).getExplainPlanInfo());
        TableContainer last = second;
        while (!stack.empty()) {
            TableContainer curr = stack.pop();
            joinExplainPlan = new JoinExplainPlan(last.getJoinInfo(), joinExplainPlan, ((ExplainPlanResult) curr.getQueryResult()).getExplainPlanInfo());
            last = curr;
        }
        joinExplainPlan.setSelectColumns(queryColumns.stream().map(QueryColumn::toString).collect(Collectors.toList()));
        return new ExplainPlanResult(queryColumns, joinExplainPlan, null);
    }
}
