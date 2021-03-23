package com.gigaspaces.jdbc.model;

public class QueryExecutionConfig {
    private boolean explainPlan;
    private boolean explainPlanVerbose;

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
}
