package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class GSTableScanRule extends ConverterRule {
    private static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(
            LogicalTableScan.class,
            Convention.NONE,
            GSConvention.INSTANCE,
            GSTableScanRule.class.getSimpleName())
        .withRuleFactory(GSTableScanRule::new);

    public static GSTableScanRule INSTANCE = new GSTableScanRule(DEFAULT_CONFIG);

    private GSTableScanRule(Config config) { super(config); }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalTableScan scan = (LogicalTableScan) rel;

        return new GSTableScan(
            scan.getCluster(),
            scan.getTraitSet().replace(out),
            scan.getHints(),
            scan.getTable()
        );
    }
}
