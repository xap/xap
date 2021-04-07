package com.gigaspaces.jdbc.model.result;

import com.gigaspaces.jdbc.model.table.ExplainPlanQueryColumn;
import com.gigaspaces.jdbc.model.table.QueryColumn;

import java.util.Collections;

public class ExplainPlanResult extends QueryResult {
    private final String explainPlanString;
    public ExplainPlanResult(String explainPlanString) {
        super(Collections.singletonList(new ExplainPlanQueryColumn()));
        this.explainPlanString = explainPlanString;
        for (String row : explainPlanString.split("\n")) {
            add(new ExplainPlanTableRow(getQueryColumns().toArray(new QueryColumn[0]), new Object[]{row}));
        }
    }

    public String getExplainPlanString() {
        return explainPlanString;
    }
}
