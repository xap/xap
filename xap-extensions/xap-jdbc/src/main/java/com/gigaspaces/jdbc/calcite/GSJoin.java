package com.gigaspaces.jdbc.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.Set;

public class GSJoin extends Join implements GSRelNode {
    public GSJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode conditionExpr, Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, conditionExpr, variablesSet, joinType);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new GSJoin(getCluster(), traitSet, left, right, conditionExpr, ImmutableSet.of(), joinType);
    }

    public static GSJoin create(RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet,
                                JoinRelType joinType){
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet =
                cluster.traitSetOf(GSConvention.INSTANCE);
        return new GSJoin(cluster, traitSet, left, right, condition,
                variablesSet, joinType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq);
    }
}
