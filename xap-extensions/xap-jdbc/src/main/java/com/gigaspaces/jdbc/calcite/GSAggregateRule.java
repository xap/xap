package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;

public class GSAggregateRule extends ConverterRule {
    static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalAggregate.class, Convention.NONE,
                    GSConvention.INSTANCE, "GSAggregateRule")
            .withRuleFactory(GSAggregateRule::new);

    static final GSAggregateRule INSTANCE = new GSAggregateRule(DEFAULT_CONFIG);

    private GSAggregateRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final Aggregate agg = (Aggregate) rel;
        final RelTraitSet traitSet = rel.getCluster()
                .traitSet().replace(GSConvention.INSTANCE);
        return new GSAggregate(
                rel.getCluster(),
                traitSet,
                convert(agg.getInput(), traitSet),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList());
    }
}
