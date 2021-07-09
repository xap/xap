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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;

public class GSSortRule extends ConverterRule {
    private static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(
                    LogicalSort.class,
                    Convention.NONE,
                    GSConvention.INSTANCE,
                    GSSortRule.class.getSimpleName())
            .withRuleFactory(GSSortRule::new);

    public static GSSortRule INSTANCE = new GSSortRule(DEFAULT_CONFIG);

    private GSSortRule(Config config) {
        super(config);
    }

    @Override
    public RelNode convert(RelNode rel) {
        final Sort sort = (Sort) rel;
        if (sort.offset != null || sort.fetch != null) {
            return null;
        }
        final RelNode input = sort.getInput();
        return GSSort.create(
                convert(
                        input,
                        input.getTraitSet().replace(GSConvention.INSTANCE)),
                sort.getCollation(),
                null,
                null);
    }
}
