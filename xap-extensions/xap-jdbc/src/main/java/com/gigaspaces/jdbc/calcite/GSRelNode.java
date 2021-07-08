package com.gigaspaces.jdbc.calcite;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;

import java.util.List;

public interface GSRelNode extends PhysicalNode {
    @Override
    default RelNode passThrough(RelTraitSet required) {
        return null;
    }

    @Override
    default Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(RelTraitSet required) {
        return null;
    }

    @Override
    default Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        return null;
    }
}
