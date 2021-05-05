package com.gigaspaces.internal.query.explainplan.model;

import com.gigaspaces.utils.Pair;

import java.util.List;

/**
 * All the index choices of specific partition
 *
 * @author Mishel Liberman
 * @since 16.0
 */
public class PartitionIndexInspectionDetail {
    private String partition;
    private List<IndexChoiceDetail> indexes;
    private List<String> usedTiers;
    private List<Pair<String, String>> aggregators;

    public PartitionIndexInspectionDetail() {
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public List<IndexChoiceDetail> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexChoiceDetail> indices) {
        this.indexes = indices;
    }

    public List<String> getUsedTiers() {
        return usedTiers;
    }

    public void setUsedTiers(List<String> usedTiers) {
        this.usedTiers = usedTiers;
    }

    public List<Pair<String, String>> getAggregators() {
        return aggregators;
    }

    public void setAggregators(List<Pair<String, String>> aggregators) {
        this.aggregators = aggregators;
    }
}
