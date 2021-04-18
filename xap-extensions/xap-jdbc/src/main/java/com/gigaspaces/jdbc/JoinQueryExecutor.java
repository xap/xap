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
import java.util.Iterator;
import java.util.List;

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
        if(config.isExplainPlan())
            return explain();
        QueryResult res = new QueryResult(this.queryColumns);
        JoinTablesIterator joinTablesIterator = new JoinTablesIterator(tables);
        while (joinTablesIterator.hasNext()) {
            res.add(new TableRow(this.queryColumns));
        }
        return res;
    }

    private QueryResult explain() {
        Iterator<TableContainer> iter = tables.iterator();
        TableContainer first = iter.next();
        TableContainer second = iter.next();
        JoinExplainPlan joinExplainPlan = new JoinExplainPlan(((ExplainPlanResult) first.getQueryResult()).getExplainPlanInfo(), ((ExplainPlanResult) second.getQueryResult()).getExplainPlanInfo());

        while (iter.hasNext()) {
            TableContainer curr = iter.next();
            joinExplainPlan = new JoinExplainPlan(joinExplainPlan, ((ExplainPlanResult) curr.getQueryResult()).getExplainPlanInfo());
        }

        return new ExplainPlanResult(queryColumns, joinExplainPlan);
    }
}
