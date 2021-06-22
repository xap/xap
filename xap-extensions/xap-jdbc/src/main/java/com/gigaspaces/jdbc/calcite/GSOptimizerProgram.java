package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;

public class GSOptimizerProgram {
    private GSOptimizerProgram() {
        // No-op.
    }

    public static Program createProgram() {
        RelMetadataProvider metadataProvider = DefaultRelMetadataProvider.INSTANCE;

        List<Program> programs = new ArrayList<>();

        // TODO: Subquery removal program
        // TODO: Decorrelate program

        programs.add(new TrimFieldsProgram());

        programs.add(Programs.ofRules(GSOptimizerRules.rules()));

        programs.add(rewriteToCalc(metadataProvider));

        return Programs.sequence(programs.toArray(new Program[0]));
    }

    private static Program rewriteToCalc(RelMetadataProvider metadataProvider) {
        return Programs.hep(GSOptimizerRules.GS_CALC_RULES, true, metadataProvider);
    }

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
}
