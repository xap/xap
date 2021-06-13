package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexProgram;

import java.util.Collections;

public class GSProjectToCalcRule extends RelRule<GSProjectToCalcRule.Config> {

    public static final GSProjectToCalcRule INSTANCE = Config.DEFAULT.toRule();

    private GSProjectToCalcRule(Config config) { super(config); }

    @Override
    public void onMatch(RelOptRuleCall call) {
        GSProject project = call.rel(0);
        RelNode input = project.getInput();

        RexProgram program = RexProgram.create(
            input.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            project.getCluster().getRexBuilder()
        );

        GSCalc calc = new GSCalc(
            project.getCluster(), project.getTraitSet(), Collections.emptyList(), input, program);

        call.transformTo(calc);
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY
            .withOperandSupplier(
                b -> b.operand(GSProject.class).anyInputs())
            .as(Config.class);

        @Override
        default GSProjectToCalcRule toRule() {
            return new GSProjectToCalcRule(this);
        }
    }
}
