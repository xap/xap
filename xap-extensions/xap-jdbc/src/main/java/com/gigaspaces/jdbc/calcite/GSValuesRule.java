package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalValues;

public class GSValuesRule extends ConverterRule {
    private static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(
            LogicalValues.class,
            Convention.NONE,
            GSConvention.INSTANCE,
            GSValuesRule.class.getSimpleName())
        .withRuleFactory(GSValuesRule::new);

    public static GSValuesRule INSTANCE = new GSValuesRule(DEFAULT_CONFIG);

    private GSValuesRule(Config config) { super(config); }

    @Override public RelNode convert(RelNode rel) {
        final LogicalValues logicalValues = (LogicalValues) rel;
        final GSValues values = GSValues.create(
                logicalValues.getCluster(), logicalValues.getRowType(), logicalValues.getTuples());
        return values.copy(
                logicalValues.getTraitSet().replace(GSConvention.INSTANCE),
                values.getInputs());
    }
}
