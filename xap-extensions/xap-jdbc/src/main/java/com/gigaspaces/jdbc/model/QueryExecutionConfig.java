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
package com.gigaspaces.jdbc.model;

import com.gigaspaces.jdbc.model.table.TempTableNameGenerator;

public class QueryExecutionConfig {
    private boolean explainPlan;
    private boolean explainPlanVerbose;
    private final TempTableNameGenerator tempTableNameGenerator = new TempTableNameGenerator();
    private boolean isJoinUsed = false;

    public QueryExecutionConfig() {
    }

    public QueryExecutionConfig(boolean explainPlan, boolean explainPlanVerbose) {
        this.explainPlan = explainPlan;
        this.explainPlanVerbose = explainPlanVerbose;
    }


    public boolean isExplainPlan() {
        return explainPlan;
    }

    public boolean isExplainPlanVerbose() {
        return explainPlanVerbose;
    }

    public TempTableNameGenerator getTempTableNameGenerator() {
        return tempTableNameGenerator;
    }

    public boolean isJoinUsed() {
        return isJoinUsed;
    }

    public void setJoinUsed(boolean joinUsed) {
        isJoinUsed = joinUsed;
    }


}
