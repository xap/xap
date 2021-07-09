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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

public class GSOptimizerProgram {
    private GSOptimizerProgram() {
        // No-op.
    }

    public static Program createProgram() {
        RelMetadataProvider metadataProvider = DefaultRelMetadataProvider.INSTANCE;

        List<Program> programs = new ArrayList<>();

        programs.add(Programs.subQuery(metadataProvider));

        programs.add(new DecorrelateProgram());

        programs.add(new TrimFieldsProgram());

        programs.add(Programs.ofRules(GSOptimizerRules.rules()));

        programs.add(rewriteToCalc(metadataProvider));

        return Programs.sequence(programs.toArray(new Program[0]));
    }

    private static Program rewriteToCalc(RelMetadataProvider metadataProvider) {
        return Programs.hep(GSOptimizerRules.GS_CALC_RULES, true, metadataProvider);
    }

    //Program that trims fields.
    private static class TrimFieldsProgram implements Program {
        public RelNode run(
            RelOptPlanner planner,
            RelNode rel,
            RelTraitSet requiredOutputTraits,
            List<RelOptMaterialization> materializations,
            List<RelOptLattice> lattices
        ) {
            RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);

            return new RelFieldTrimmer(null, relBuilder).trim(rel);
        }
    }

    // Program that de-correlates a query.
    private static class DecorrelateProgram implements Program {
        public RelNode run(RelOptPlanner planner, RelNode rel,
                           RelTraitSet requiredOutputTraits,
                           List<RelOptMaterialization> materializations,
                           List<RelOptLattice> lattices) {
            final CalciteConnectionConfig config = Util.first(
                    planner.getContext().unwrap(CalciteConnectionConfig.class),
                    CalciteConnectionConfig.DEFAULT);
            if (config.forceDecorrelate()) {
                final RelBuilder relBuilder =
                        RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
                return RelDecorrelator.decorrelateQuery(rel, relBuilder);
            }
            return rel;
        }
    }
}
