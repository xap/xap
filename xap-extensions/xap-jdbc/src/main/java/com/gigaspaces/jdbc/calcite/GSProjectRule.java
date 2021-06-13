package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public class GSProjectRule extends ConverterRule {
    private static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(
            LogicalProject.class,
            Convention.NONE,
            GSConvention.INSTANCE,
            GSProjectRule.class.getSimpleName())
        .withRuleFactory(GSProjectRule::new);

    public static GSProjectRule INSTANCE = new GSProjectRule(DEFAULT_CONFIG);

    private GSProjectRule(Config config) { super(config); }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject project = (LogicalProject) rel;

        return new GSProject(
            project.getCluster(),
            project.getTraitSet().replace(out),
            project.getHints(),
            RelRule.convert(project.getInput(), project.getInput().getTraitSet().replace(out)),
            project.getProjects(),
            project.getRowType()
        );
    }
}
