package com.gigaspaces.jdbc;

import com.gigaspaces.jdbc.model.result.QueryResult;
import com.gigaspaces.jdbc.model.result.TableRow;
import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;
import com.gigaspaces.jdbc.model.table.TableContainer;
import com.j_spaces.core.IJSpace;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class JoinQueryExecutor {
    private final IJSpace space;
    private final List<TableContainer> tables;
    private final List<QueryColumn> queryColumns;
    private final boolean explainPlan;

    public JoinQueryExecutor(List<TableContainer> tables, IJSpace space, List<QueryColumn> queryColumns, boolean explainPlan) {
        this.tables = tables;
        this.space = space;
        this.queryColumns = queryColumns;
        this.explainPlan = explainPlan;
    }

    public QueryResult execute() {
        List<QueryResult> results = new ArrayList<>();
        for (TableContainer table : tables) {
            try {
                results.add(table.executeRead(explainPlan));
            } catch (SQLException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(e);
            }
        }
        List<QueryColumn> visibleColumns = explainPlan ? Collections.singletonList(new ExplainPlanQueryColumn()) : this.queryColumns;
        QueryResult res = new QueryResult(visibleColumns);
        Iterator<TableRow> iter1 = results.get(0).iterator();
        while (iter1.hasNext()) {
            TableRow iter1Next = iter1.next();
            Iterator<TableRow> iter2 = results.get(1).iterator();
            while (iter2.hasNext()) {
                TableRow iter2Next = iter2.next();

                res.add(new TableRow(visibleColumns, iter1Next, iter2Next));
            }
        }

        return res;
    }
}
