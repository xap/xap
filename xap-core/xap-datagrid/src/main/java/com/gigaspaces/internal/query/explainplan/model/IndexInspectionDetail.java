package com.gigaspaces.internal.query.explainplan.model;

import java.util.ArrayList;
import java.util.List;

/**
 * All the index choices of specific partition
 * @author Mishel Liberman
 * @since 16.0
 */
public class IndexInspectionDetail {
    private String partition;
    private List<IndexChoiceDetail> indexes = new ArrayList<>();

    public IndexInspectionDetail() {
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

    public boolean addIndex(IndexChoiceDetail indexInfo) {
        return indexes.add(indexInfo);
    }
}
