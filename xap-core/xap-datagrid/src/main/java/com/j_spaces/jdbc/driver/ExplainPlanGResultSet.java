package com.j_spaces.jdbc.driver;

import com.j_spaces.jdbc.ResultEntry;

/**
 * Wraps GResultSet when explain plan must be returned
 * @since 15.5.0
 * @author Evgeny
 */
@com.gigaspaces.api.InternalApi
public class ExplainPlanGResultSet extends GResultSet{

    private String explainPlan;

    public ExplainPlanGResultSet(GStatement statement, ResultEntry results, String explainPlan) {
        super(statement, results);
        this.explainPlan = explainPlan;
    }

    public String getExplainPlan() {
        return explainPlan;
    }
}