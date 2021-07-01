package com.gigaspaces.jdbc.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class GSAggregate extends Aggregate implements GSRelNode {

    protected GSAggregate(RelOptCluster cluster,
                          RelTraitSet traitSet,
                          RelNode input,
                          ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets,
                          List<AggregateCall> aggCalls) {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
    }

    @Override
    public GSAggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new GSAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls);
    }
}
