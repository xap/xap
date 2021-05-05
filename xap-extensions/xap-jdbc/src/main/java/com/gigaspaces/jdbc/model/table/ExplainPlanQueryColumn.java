package com.gigaspaces.jdbc.model.table;

public class ExplainPlanQueryColumn extends QueryColumn {
    public static final String EXPLAIN_PLAN_COL_NAME = "Explain Plan";
    public static final String EXPLAIN_PLAN_VERBOSE_COL_NAME = "Explain Plan - verbose";
    public ExplainPlanQueryColumn() {
        super(EXPLAIN_PLAN_COL_NAME, null, null, true, null);
    }
}
