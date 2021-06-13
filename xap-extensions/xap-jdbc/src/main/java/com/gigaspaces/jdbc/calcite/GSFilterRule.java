package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

public class GSFilterRule extends ConverterRule {
    private static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(
            LogicalFilter.class,
            Convention.NONE,
            GSConvention.INSTANCE,
            GSFilterRule.class.getSimpleName())
        .withRuleFactory(GSFilterRule::new);

    public static GSFilterRule INSTANCE = new GSFilterRule(DEFAULT_CONFIG);

    private GSFilterRule(Config config) { super(config); }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalFilter filter = (LogicalFilter) rel;

        return new GSFilter(
            filter.getCluster(),
            filter.getTraitSet().replace(out),
            RelRule.convert(filter.getInput(), filter.getInput().getTraitSet().replace(out)),
            filter.getCondition()
        );
    }
}
