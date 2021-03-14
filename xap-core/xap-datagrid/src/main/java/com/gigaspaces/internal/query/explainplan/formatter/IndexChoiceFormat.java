package com.gigaspaces.internal.query.explainplan.formatter;

import java.util.List;

public class IndexChoiceFormat {
    private String operator;
    private boolean isUnion;
    private List<IndexInfoFormat> inspectedIndexes;
    private List<IndexInfoFormat> selectedIndexes;


    public IndexChoiceFormat() {
    }

    public IndexChoiceFormat(String operator, boolean isUnion, List<IndexInfoFormat> inspectedIndexes, List<IndexInfoFormat> selectedIndexes) {
        this.operator = operator;
        this.inspectedIndexes = inspectedIndexes;
        this.selectedIndexes = selectedIndexes;
        this.isUnion = isUnion;
    }

    public List<IndexInfoFormat> getInspectedIndexes() {
        return inspectedIndexes;
    }

    public void setInspectedIndexes(List<IndexInfoFormat> inspectedIndexes) {
        this.inspectedIndexes = inspectedIndexes;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public List<IndexInfoFormat> getSelectedIndexes() {
        return selectedIndexes;
    }

    public void setSelectedIndexes(List<IndexInfoFormat> selectedIndexes) {
        this.selectedIndexes = selectedIndexes;
    }

    public boolean isUnion() {
        return isUnion;
    }

    public void setUnion(boolean union) {
        isUnion = union;
    }
}
