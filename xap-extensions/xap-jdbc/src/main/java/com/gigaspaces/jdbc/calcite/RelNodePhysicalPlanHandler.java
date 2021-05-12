package com.gigaspaces.jdbc.calcite;

import com.gigaspaces.jdbc.PhysicalPlanHandler;
import com.gigaspaces.jdbc.QueryExecutor;
import org.apache.calcite.rel.RelNode;

public class RelNodePhysicalPlanHandler implements PhysicalPlanHandler<RelNode> {
    @Override
    public QueryExecutor prepareForExecution(RelNode relNode) {
        return null;
    }
}
