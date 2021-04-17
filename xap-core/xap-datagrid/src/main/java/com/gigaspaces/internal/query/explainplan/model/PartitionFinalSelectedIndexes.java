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

import java.util.Collections;
import java.util.List;

/**
 * Represents the final selected indexes of a single partition along with its used tiers
 *
 * @Author: Mishel Liberman
 * @since 16.0
 */
public class PartitionFinalSelectedIndexes {
    private final List<IndexInfoDetail> selectedIndexes;
    private final List<String> usedTiers;

    public PartitionFinalSelectedIndexes() {
        selectedIndexes = Collections.emptyList();
        usedTiers = Collections.emptyList();
    }

    public PartitionFinalSelectedIndexes(List<IndexInfoDetail> selectedIndexes, List<String> usedTiers) {
        this.selectedIndexes = selectedIndexes;
        this.usedTiers = usedTiers;
    }

    public List<IndexInfoDetail> getSelectedIndexes() {
        return selectedIndexes;
    }

    public List<String> getUsedTiers() {
        return usedTiers;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj instanceof PartitionFinalSelectedIndexes) {
            final PartitionFinalSelectedIndexes second = (PartitionFinalSelectedIndexes) obj;
            PartitionFinalSelectedIndexes first = this;
            final List<IndexInfoDetail> firstSelectedIndexes = first.getSelectedIndexes();
            final List<IndexInfoDetail> secondSelectedIndexes = second.getSelectedIndexes();
            final List<String> firstUsedTiers = first.getUsedTiers();
            final List<String> secondUsedTiers = second.getUsedTiers();
            if (firstSelectedIndexes == null || secondSelectedIndexes == null || firstSelectedIndexes.size() != secondSelectedIndexes.size()
                    || firstUsedTiers == null || !firstUsedTiers.equals(secondUsedTiers)) {
                return false;
            }

            for (int i = 0; i < firstSelectedIndexes.size(); i++) {
                final IndexInfoDetail firstDetail = firstSelectedIndexes.get(i);
                final IndexInfoDetail secondDetail = secondSelectedIndexes.get(i);
                if (!firstDetail.getName().equals(secondDetail.getName())
                        || !firstDetail.getValue().equals(secondDetail.getValue())
                        || !firstDetail.getOperator().equals(secondDetail.getOperator())
                        || !firstDetail.getType().equals(secondDetail.getType())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
