package com.gigaspaces.internal.query.explainplan.formatter;

import java.util.ArrayList;
import java.util.List;

public class IndexInspectionFormat {
    private String partition;
    private List<IndexChoiceFormat> indexes = new ArrayList<>();//currently there is no index per type, will be added by need.

    public IndexInspectionFormat() {
    }

    public IndexInspectionFormat(String partitions, List<IndexChoiceFormat> indexes) {
        this.partition = partitions;
        this.indexes = indexes;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public List<IndexChoiceFormat> getIndexes() {
        return indexes;
    }

    public void setIndexes(List<IndexChoiceFormat> indices) {
        this.indexes = indices;
    }

    public boolean addIndex(IndexChoiceFormat indexInfo) {
        return indexes.add(indexInfo);
    }
}
