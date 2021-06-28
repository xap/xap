package com.gigaspaces.jdbc.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

public class GSValues extends Values implements GSRelNode {
    private GSValues(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits) {
        super(cluster, rowType, tuples, traits);
    }

    /** Creates an GSValues. */
    public static GSValues create(RelOptCluster cluster,
                                          final RelDataType rowType,
                                          final ImmutableList<ImmutableList<RexLiteral>> tuples) {
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf(GSConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.values(mq, rowType, tuples))
                        .replaceIf(RelDistributionTraitDef.INSTANCE,
                                () -> RelMdDistribution.values(rowType, tuples));
        return new GSValues(cluster, rowType, tuples, traitSet);
    }
}
