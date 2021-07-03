/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Sort}
 *
 * @author Sagiv Michael
 */
public class GSSort extends Sort implements GSRelNode {

    public GSSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset,
                  RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
    }

    public static GSSort create(RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        RelOptCluster cluster = input.getCluster();
        RelTraitSet traitSet = cluster.traitSetOf(GSConvention.INSTANCE).replace(collation);
        return new GSSort(cluster, traitSet, input, collation, offset, fetch);
    }

    @Override
    public Sort copy(RelTraitSet relTraitSet, RelNode relNode, RelCollation relCollation, RexNode offset, RexNode fetch) {
        return new GSSort(getCluster(), relTraitSet, relNode, relCollation, offset, fetch);
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        RelCollation collation = childTraits.getCollation();
        if (collation == null || collation == RelCollations.EMPTY) {
            return null;
        }
        RelTraitSet traits = traitSet.replace(collation);
        return Pair.of(traits, ImmutableList.of(traits));
    }
}
