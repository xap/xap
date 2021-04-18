package com.gigaspaces.jdbc.model;

import com.gigaspaces.jdbc.model.table.TempTableNameGenerator;

public class QueryExecutionConfig {
    private boolean explainPlan;
    private boolean explainPlanVerbose;
    private final TempTableNameGenerator tempTableNameGenerator = new TempTableNameGenerator();

    public QueryExecutionConfig() {
    }

    public QueryExecutionConfig(boolean explainPlan, boolean explainPlanVerbose) {
        this.explainPlan = explainPlan;
        this.explainPlanVerbose = explainPlanVerbose;
    }


    public boolean isExplainPlan() {
        return explainPlan;
    }

    public boolean isExplainPlanVerbose() {
        return explainPlanVerbose;
    }

    public TempTableNameGenerator getTempTableNameGenerator() {
        return tempTableNameGenerator;
    }
}
