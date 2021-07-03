package com.gigaspaces.jdbc.calcite.experimental.model;

public class ExplainPlanPhysicalColumn extends PhysicalColumn {
    public static final String EXPLAIN_PLAN_COL_NAME = "Explain Plan";
    public static final String EXPLAIN_PLAN_VERBOSE_COL_NAME = "Explain Plan - verbose";
    public ExplainPlanPhysicalColumn() {
        super(EXPLAIN_PLAN_COL_NAME, null, null);
    }
}
