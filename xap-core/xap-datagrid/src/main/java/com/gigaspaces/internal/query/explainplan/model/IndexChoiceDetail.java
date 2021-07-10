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
package com.gigaspaces.internal.query.explainplan.model;

import java.util.List;

/**
 * Part of the index choices of the partition
 *
 * @author Mishel Liberman
 * @since 16.0
 */
public class IndexChoiceDetail {
    private String operator;
    private boolean isUnion;
    private List<IndexInfoDetail> inspectedIndexes;
    private List<IndexInfoDetail> selectedIndexes;


    public IndexChoiceDetail(String operator, boolean isUnion, List<IndexInfoDetail> inspectedIndexes, List<IndexInfoDetail> selectedIndexes) {
        this.operator = operator;
        this.inspectedIndexes = inspectedIndexes;
        this.selectedIndexes = selectedIndexes;
        this.isUnion = isUnion;
    }

    public List<IndexInfoDetail> getInspectedIndexes() {
        return inspectedIndexes;
    }

    public void setInspectedIndexes(List<IndexInfoDetail> inspectedIndexes) {
        this.inspectedIndexes = inspectedIndexes;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public List<IndexInfoDetail> getSelectedIndexes() {
        return selectedIndexes;
    }

    public void setSelectedIndexes(List<IndexInfoDetail> selectedIndexes) {
        this.selectedIndexes = selectedIndexes;
    }

    public boolean isUnion() {
        return isUnion;
    }

    public void setUnion(boolean union) {
        isUnion = union;
    }
}
