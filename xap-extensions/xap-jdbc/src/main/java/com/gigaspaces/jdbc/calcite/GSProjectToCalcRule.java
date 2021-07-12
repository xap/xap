/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
