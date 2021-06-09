package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.explainplan.JoinExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.ExplainPlanResult;
import com.gigaspaces.jdbc.model.result.JoinTablesIterator;
import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.AggregationColumn;
import com.gigaspaces.jdbc.model.table.OrderColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class JoinQueryExecutor {
    private final List<TableContainer> tables;
    private final Set<QueryColumn> invisibleColumns;
    private final List<QueryColumn> visibleColumns;
    private final QueryExecutionConfig config;
    private final List<AggregationColumn> aggregationColumns;
    private final ArrayList<QueryColumn> allQueryColumn;

    public JoinQueryExecutor(QueryExecutor queryExecutor) {
        this.tables = queryExecutor.getTables();
        this.invisibleColumns = queryExecutor.getInvisibleColumns();
        this.visibleColumns = queryExecutor.getVisibleColumns();
        this.config = queryExecutor.getConfig();
        this.config.setJoinUsed(true);
        //TODO: @sagiv can be obtained from the tables?, just like the OrderColumns at 'execute()'?
        this.aggregationColumns = queryExecutor.getAggregationFunctionColumns();
        this.allQueryColumn = new ArrayList<>(visibleColumns);
        this.allQueryColumn.addAll(invisibleColumns);
    }

    public QueryResult execute() {
        final List<OrderColumn> orderColumns = new ArrayList<>();
        final List<QueryColumn> groupByColumns = new ArrayList<>();
        for (TableContainer table : tables) {
            try {
                table.executeRead(config);
                orderColumns.addAll(table.getOrderColumns());
                groupByColumns.addAll(table.getGroupByColumns());
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        }

        JoinTablesIterator joinTablesIterator = new JoinTablesIterator(tables);
        if(config.isExplainPlan()) {
            return explain(joinTablesIterator, orderColumns);
        }
        QueryResult res = new QueryResult(this.visibleColumns, this.aggregationColumns);
        while (joinTablesIterator.hasNext()) {
            if(tables.stream().allMatch(TableContainer::checkJoinCondition))
                res.add(new TableRow(this.allQueryColumn, orderColumns, groupByColumns));
        }
        if(!this.aggregationColumns.isEmpty()) {
            List<TableRow> aggregateRows = new ArrayList<>();
            aggregateRows.add(TableRow.aggregate(res.getRows(), this.aggregationColumns));
            res.setRows(aggregateRows);
        }
        if(!orderColumns.isEmpty()) {
            res.sort(); //sort the results at the client
        }
        return res;
    }

    private QueryResult explain(JoinTablesIterator joinTablesIterator, List<OrderColumn> orderColumns) {
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
        joinExplainPlan.setSelectColumns(visibleColumns.stream().map(QueryColumn::toString).collect(Collectors.toList()));
        joinExplainPlan.setOrderColumns(orderColumns);
        return new ExplainPlanResult(visibleColumns, joinExplainPlan, null);
    }
}
