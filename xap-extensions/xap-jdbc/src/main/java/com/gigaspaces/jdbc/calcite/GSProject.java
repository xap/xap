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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GSProject extends Project implements GSRelNode {
    protected GSProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType
    ) {
        super(cluster, traits, hints, input, projects, rowType);
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new GSProject(getCluster(), traitSet, hints, input, projects, rowType);
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(RelTraitSet childTraits, int childId) {
        final RelDataType inputRowType = input.getRowType();
        final RelDataTypeFactory typeFactory = input.getCluster().getTypeFactory();
        final RelTraitSet currentTraits = traitSet;
        final RelCollation collation = childTraits.getCollation();
        if (collation == null || collation == RelCollations.EMPTY) {
            return null;
        }

        final int maxField = Math.max(exps.size(),
                inputRowType.getFieldCount());
        Mappings.TargetMapping mapping = Mappings
                .create(MappingType.FUNCTION, maxField, maxField);
        for (Ord<RexNode> node : Ord.zip(exps)) {
            if (node.e instanceof RexInputRef) {
                mapping.set(((RexInputRef) node.e).getIndex(), node.i);
            } else if (node.e.isA(SqlKind.CAST)) {
                final RexNode operand = ((RexCall) node.e).getOperands().get(0);
                if (operand instanceof RexInputRef) {
                    mapping.set(((RexInputRef) operand).getIndex(), node.i);
                }
            }
        }

        List<RelFieldCollation> collationFieldsToDerive = new ArrayList<>();
        for (RelFieldCollation rc : collation.getFieldCollations()) {
            if (isCollationOnTrivialExpr(exps, typeFactory, mapping, rc, false)) {
                collationFieldsToDerive.add(rc);
            } else {
                break;
            }
        }

        if (collationFieldsToDerive.size() > 0) {
            final RelCollation newCollation = RelCollations
                    .of(collationFieldsToDerive).apply(mapping);
            return Pair.of(currentTraits.replace(newCollation),
                    ImmutableList.of(currentTraits.replace(collation)));
        } else {
            return null;
        }
    }

    private boolean isCollationOnTrivialExpr(
            List<RexNode> projects, RelDataTypeFactory typeFactory,
            Mappings.TargetMapping map, RelFieldCollation fc, boolean passDown) {
        final int index = fc.getFieldIndex();
        int target = map.getTargetOpt(index);
        if (target < 0) {
            return false;
        }

        final RexNode node = passDown ? projects.get(index) : projects.get(target);
        if (node.isA(SqlKind.CAST)) {
            // Check whether it is a monotonic preserving cast
            final RexCall cast = (RexCall) node;
            RelFieldCollation newFieldCollation = Objects.requireNonNull(RexUtil.apply(map, fc));
            final RexCallBinding binding =
                    RexCallBinding.create(typeFactory, cast,
                            ImmutableList.of(RelCollations.of(newFieldCollation)));
            if (cast.getOperator().getMonotonicity(binding)
                    == SqlMonotonicity.NOT_MONOTONIC) {
                return false;
            }
        }

        return true;
    }
}
