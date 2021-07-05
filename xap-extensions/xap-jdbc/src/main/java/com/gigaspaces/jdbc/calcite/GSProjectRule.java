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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;

public class GSProjectRule extends ConverterRule {
    private static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(
            LogicalProject.class,
            Convention.NONE,
            GSConvention.INSTANCE,
            GSProjectRule.class.getSimpleName())
        .withRuleFactory(GSProjectRule::new);

    public static GSProjectRule INSTANCE = new GSProjectRule(DEFAULT_CONFIG);

    private GSProjectRule(Config config) { super(config); }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject project = (LogicalProject) rel;

        return new GSProject(
            project.getCluster(),
            project.getTraitSet().replace(out),
            project.getHints(),
            RelRule.convert(project.getInput(), project.getInput().getTraitSet().replace(out)),
            project.getProjects(),
            project.getRowType()
        );
    }
}
