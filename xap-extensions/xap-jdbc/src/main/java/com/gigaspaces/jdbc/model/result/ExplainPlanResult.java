package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;

import java.util.Collections;

public class ExplainPlanResult extends QueryResult {
    public ExplainPlanResult(String explainPlanString) {
        super(Collections.singletonList(new ExplainPlanQueryColumn()));
        for (String row : explainPlanString.split("\n")) {
            add(new ExplainPlanTableRow(getQueryColumns().toArray(new QueryColumn[0]), new Object[]{row}));
        }
    }
}
