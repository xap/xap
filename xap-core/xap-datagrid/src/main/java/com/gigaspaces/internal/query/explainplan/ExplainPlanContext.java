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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class ExplainPlanContext {

    private SingleExplainPlan singleExplainPlan;
    private IndexChoiceNode fatherNode;
    private IndexChoiceNode match;

    /**
     * Doesn't continue to the phase of processing the entries
     * used in cases of explain plan when we want to enter the space
     * to gather information regarding the execution, i.e. inspected and chosen indexes and their size
     */
    private boolean dryRun;

    public ExplainPlanContext() {
    }

    public SingleExplainPlan getSingleExplainPlan() {
        return singleExplainPlan;
    }

    public void setSingleExplainPlan(SingleExplainPlan singleExplainPlan) {
        this.singleExplainPlan = singleExplainPlan;
    }

    public IndexChoiceNode getFatherNode() {
        return fatherNode;
    }

    public void setFatherNode(IndexChoiceNode fatherNode) {
        this.fatherNode = fatherNode;
    }

    public IndexChoiceNode getMatch() {
        return match;
    }

    public void setMatch(IndexChoiceNode match) {
        this.match = match;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public boolean isDryRun() {
        return dryRun;
    }

}
