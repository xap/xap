package com.gigaspaces.jdbc.calcite.experimental.model;

public class ExplainPlanConcreteColumn extends ConcreteColumn {
    public static final String EXPLAIN_PLAN_COL_NAME = "Explain Plan";
    public static final String EXPLAIN_PLAN_VERBOSE_COL_NAME = "Explain Plan - verbose";
    public ExplainPlanConcreteColumn() {
        super(EXPLAIN_PLAN_COL_NAME, null, null, null, 0);
    }
}
