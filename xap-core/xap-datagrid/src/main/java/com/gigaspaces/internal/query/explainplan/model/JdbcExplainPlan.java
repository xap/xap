package com.gigaspaces.internal.query.explainplan.model;

import com.gigaspaces.internal.query.explainplan.TextReportFormatter;

public abstract class JdbcExplainPlan {
    public abstract void format(TextReportFormatter formatter, boolean verbose);
}
