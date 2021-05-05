package com.gigaspaces.internal.query.explainplan.model;

import com.gigaspaces.utils.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents the final selected indexes of a single partition along with its used tiers
 *
 * @Author: Mishel Liberman
 * @since 16.0
 */
public class PartitionFinalSelectedIndexes {
    private final List<IndexInfoDetail> selectedIndexes;
    private final List<String> usedTiers;
    private final List<Pair<String, String>> aggregators;

    public PartitionFinalSelectedIndexes() {
        selectedIndexes = Collections.emptyList();
        usedTiers = Collections.emptyList();
        aggregators = Collections.emptyList();
    }

    public PartitionFinalSelectedIndexes(List<IndexInfoDetail> selectedIndexes, List<String> usedTiers,
                                         List<Pair<String, String>> aggregators) {
        this.selectedIndexes = selectedIndexes;
        this.usedTiers = usedTiers;
        this.aggregators = aggregators;
    }

    public List<IndexInfoDetail> getSelectedIndexes() {
        return selectedIndexes;
    }

    public List<String> getUsedTiers() {
        return usedTiers;
    }

    public List<Pair<String, String>> getAggregators() {
        return aggregators;
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
            final List<Pair<String, String>>  firstAggregators = first.getAggregators();
            final List<Pair<String, String>>  secondAggregators = second.getAggregators();
            if (firstSelectedIndexes == null || secondSelectedIndexes == null || firstSelectedIndexes.size() != secondSelectedIndexes.size()
                    || !Objects.equals(firstUsedTiers, secondUsedTiers)
                    || !Objects.equals(firstAggregators, secondAggregators)) {
                return false;
            }

            for (int i = 0; i < firstSelectedIndexes.size(); i++) {
                final IndexInfoDetail firstDetail = firstSelectedIndexes.get(i);
                final IndexInfoDetail secondDetail = secondSelectedIndexes.get(i);
                if (firstDetail.getValue() == null && secondDetail.getValue() == null) {
                    continue;
                } else if (firstDetail.getValue() == null || secondDetail.getValue() == null) {
                    return false;
                }

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

    @Override
    public int hashCode() {
        int hash = 7;
        for (IndexInfoDetail selectedIndex : selectedIndexes) {
            hash = 31 * hash + (selectedIndex.getValue() != null ? selectedIndex.getValue().hashCode() : 0);
            hash = 31 * hash + (selectedIndex.getName() != null ? selectedIndex.getName().hashCode() : 0);
            hash = 31 * hash + (selectedIndex.getType() != null ? selectedIndex.getType().hashCode() : 0);
            hash = 31 * hash + (selectedIndex.getOperator() != null ? selectedIndex.getOperator().hashCode() : 0);
        }

        hash = 31 * hash + (usedTiers != null ? usedTiers.hashCode() : 0);
        hash = 31 * hash + (aggregators != null ? aggregators.hashCode() : 0);
        return hash;
    }
}
