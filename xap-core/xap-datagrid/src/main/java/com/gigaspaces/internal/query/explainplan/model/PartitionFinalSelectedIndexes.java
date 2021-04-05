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
}
