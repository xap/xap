package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;

import java.util.Collections;

public class GSFilterToCalcRule extends RelRule<GSFilterToCalcRule.Config> {

    public static final GSFilterToCalcRule INSTANCE = Config.DEFAULT.toRule();

    private GSFilterToCalcRule(Config config) { super(config); }

    @Override
    public void onMatch(RelOptRuleCall call) {
        GSFilter filter = call.rel(0);
        RelNode input = filter.getInput();

        RexProgramBuilder programBuilder = new RexProgramBuilder(
            input.getRowType(),
            filter.getCluster().getRexBuilder()
        );

        programBuilder.addIdentity();
        programBuilder.addCondition(filter.getCondition());

        RexProgram program = programBuilder.getProgram();

        GSCalc calc = new GSCalc(
            filter.getCluster(), filter.getTraitSet(), Collections.emptyList(), input, program);

        call.transformTo(calc);
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY
            .withOperandSupplier(
                b -> b.operand(GSFilter.class).anyInputs())
            .as(Config.class);

        @Override
        default GSFilterToCalcRule toRule() {
            return new GSFilterToCalcRule(this);
        }
    }
}
