package com.gigaspaces.internal.query.explainplan.model;

import java.util.List;

public class PartitionAndSizes {
    private String partitionId;
    private List<Integer> indexSizes;

    public PartitionAndSizes(String partitionId, List<Integer> indexSizes) {
        this.partitionId = partitionId;
        this.indexSizes = indexSizes;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    public List<Integer> getIndexSizes() {
        return indexSizes;
    }

    public void setIndexSizes(List<Integer> indexSizes) {
        this.indexSizes = indexSizes;
    }

}
