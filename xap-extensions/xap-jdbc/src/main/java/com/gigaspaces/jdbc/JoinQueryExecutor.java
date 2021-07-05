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
package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.explainplan.JoinExplainPlan;
import com.gigaspaces.jdbc.model.QueryExecutionConfig;
import com.gigaspaces.jdbc.model.result.*;
import com.gigaspaces.jdbc.model.table.*;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JoinQueryExecutor {
    private final List<TableContainer> tables;
    private final Set<IQueryColumn> invisibleColumns;
    private final List<IQueryColumn> visibleColumns;
    private final QueryExecutionConfig config;
    private final List<AggregationColumn> aggregationColumns;
    private final List<IQueryColumn> allQueryColumns;
    private final List<IQueryColumn> selectedQueryColumns;

    public JoinQueryExecutor(QueryExecutor queryExecutor) {
        this.tables = queryExecutor.getTables();
        this.invisibleColumns = queryExecutor.getInvisibleColumns();
        this.visibleColumns = queryExecutor.getVisibleColumns();
        this.config = queryExecutor.getConfig();
        this.config.setJoinUsed(true);
        this.aggregationColumns = queryExecutor.getAggregationColumns();
        this.allQueryColumns = Stream.concat(visibleColumns.stream(), invisibleColumns.stream()).collect(Collectors.toList());
        this.selectedQueryColumns = Stream.concat(this.visibleColumns.stream(), this.aggregationColumns.stream()).sorted().collect(Collectors.toList());
    }

    public QueryResult execute() {
        final List<OrderColumn> orderColumns = new ArrayList<>();
        final List<ConcreteColumn> groupByColumns = new ArrayList<>();
        boolean isDistinct = false;
        for (TableContainer table : tables) {
            try {
                table.executeRead(config);
                orderColumns.addAll(table.getOrderColumns());
                groupByColumns.addAll(table.getGroupByColumns());
                isDistinct |= table.isDistinct();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        }

        JoinTablesIterator joinTablesIterator = new JoinTablesIterator(tables);
        if(config.isExplainPlan()) {
            return explain(joinTablesIterator, orderColumns, groupByColumns, isDistinct);
        }
        QueryResult res = new JoinQueryResult(this.selectedQueryColumns);
        while (joinTablesIterator.hasNext()) {
            if(tables.stream().allMatch(TableContainer::checkJoinCondition))
                res.addRow(TableRowFactory.createTableRowFromSpecificColumns(this.allQueryColumns, orderColumns,
                        groupByColumns));
        }

        if( groupByColumns.isEmpty()) {
            if( !aggregationColumns.isEmpty() ) {
                List<TableRow> aggregateRows = new ArrayList<>();
                aggregateRows.add(TableRowUtils.aggregate(res.getRows(), selectedQueryColumns, this.aggregationColumns, visibleColumns));
                res.setRows(aggregateRows);
            }
        }
        else {
            res.groupBy(); //group by the results at the client
            if( !aggregationColumns.isEmpty() ) {
                Map<TableRowGroupByKey, List<TableRow>> groupByRowsResult = res.getGroupByRowsResult();
                List<TableRow> totalAggregationsResultRowsList = new ArrayList<>();
                for (List<TableRow> rowsList : groupByRowsResult.values()) {
                    TableRow aggregatedRow = TableRowUtils.aggregate( rowsList, selectedQueryColumns, aggregationColumns, visibleColumns );
                    totalAggregationsResultRowsList.add( aggregatedRow );
                }
                res.setRows( totalAggregationsResultRowsList );
            }
        }
        if(!orderColumns.isEmpty()) {
            res.sort(); //sort the results at the client
        }
        if (isDistinct){
            res.distinct();
        }

        return res;
    }

    private QueryResult explain(JoinTablesIterator joinTablesIterator, List<OrderColumn> orderColumns,
                                List<ConcreteColumn> groupByColumns, boolean isDistinct) {
        Stack<TableContainer> stack = new Stack<>();
        TableContainer current = joinTablesIterator.getStartingPoint();
        stack.push(current);
        while (current.getJoinedTable() != null){
            current = current.getJoinedTable();
            stack.push(current);
        }
        TableContainer first = stack.pop();
        TableContainer second = stack.pop();
        JoinExplainPlan joinExplainPlan = new JoinExplainPlan(first.getJoinInfo(), ((ExplainPlanQueryResult) first.getQueryResult()).getExplainPlanInfo(), ((ExplainPlanQueryResult) second.getQueryResult()).getExplainPlanInfo());
        TableContainer last = second;
        while (!stack.empty()) {
            TableContainer curr = stack.pop();
            joinExplainPlan = new JoinExplainPlan(last.getJoinInfo(), joinExplainPlan, ((ExplainPlanQueryResult) curr.getQueryResult()).getExplainPlanInfo());
            last = curr;
        }
        joinExplainPlan.setSelectColumns(visibleColumns.stream().map(IQueryColumn::toString).collect(Collectors.toList()));
        joinExplainPlan.setOrderColumns(orderColumns);
        joinExplainPlan.setGroupByColumns(groupByColumns);
        joinExplainPlan.setDistinct(isDistinct);
        return new ExplainPlanQueryResult(visibleColumns, joinExplainPlan, null);
    }
}
