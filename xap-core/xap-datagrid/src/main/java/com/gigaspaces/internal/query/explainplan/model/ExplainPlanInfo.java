package com.gigaspaces.internal.query.explainplan.model;

import com.gigaspaces.internal.query.explainplan.ExplainPlanV3;
import com.gigaspaces.internal.query.explainplan.TextReportFormatter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gigaspaces.internal.query.explainplan.ExplainPlanUtil.notEmpty;

public class ExplainPlanInfo {
    private final String spaceName;
    private final String tableName;
    private final String tableAlias;
    private final Map<String, String> visibleColumnsAndAliasMap;
    private List<IndexInspectionDetail> indexInspectionsPerPartition = new ArrayList<>();
    private String criteria;

    private static final String SELECTED_INDEX_STRING = "SelectedIndex:";


    public ExplainPlanInfo(ExplainPlanV3 explainPlan) {
        tableName = explainPlan.getTableName();
        tableAlias = explainPlan.getTableAlias();
        visibleColumnsAndAliasMap = explainPlan.getVisibleColumnsAndAliasMap();
        spaceName = explainPlan.getSpaceName();
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean verbose) {
        TextReportFormatter formatter = new TextReportFormatter();
        String table = notEmpty(tableAlias) ? tableName + " as " + tableAlias : tableName;
        table = notEmpty(spaceName) ? spaceName + "." + table : table;
        if (isNoIndexUsed()) {
            formatter.line("FullScan: " + table);
        } else {
            formatter.line("TableScan: " + table);
        }
        formatter.indent();

        if (visibleColumnsAndAliasMap != null) {
            String columns = visibleColumnsAndAliasMap.entrySet().stream()
                    .map(column -> column.getKey() + (notEmpty(column.getValue()) ? " as " + column.getValue() : ""))
                    .collect(Collectors.joining(", "));
            if (notEmpty(columns)) {
                formatter.line("Select: " + columns);
            }
        }

        if (notEmpty(criteria)) {
            formatter.line("Criteria: " + criteria);
        }

        for (IndexInspectionDetail inspectionFormat : indexInspectionsPerPartition) {
            if (inspectionFormat.getIndexes().isEmpty()) {
                continue;
            }

            formatter.line(String.format("Partition: [%s]", inspectionFormat.getPartition()));
            formatter.indent();
            final IndexChoiceDetail unionIndexChoice = getUnionIndexChoiceIfExists(inspectionFormat.getIndexes());
            if (!verbose && unionIndexChoice != null) {
                formatter.line(SELECTED_INDEX_STRING);
                formatter.indent();
                formatter.line("Union: ");

                formatter.indent();
                unionIndexChoice.getSelectedIndexes().forEach(selected -> formatter.line(selected.toString()));
                formatter.unindent();

                formatter.unindent();
            } else {
                for (int i = inspectionFormat.getIndexes().size() - 1; i >= 0; i--) {
                    IndexChoiceDetail indexChoice = inspectionFormat.getIndexes().get(i);
                    if (verbose) {
                        formatter.line(indexChoice.getOperator());
                        formatter.indent();
                        formatter.line("Inspected: ");
                        formatter.indent();
                        indexChoice.getInspectedIndexes().forEach(inspected -> formatter.line(inspected.toString()));
                        formatter.unindent();
                    }

                    formatter.line(SELECTED_INDEX_STRING);
                    if (indexChoice.isUnion()) {
                        formatter.indent();
                        formatter.line("Union: ");
                    }

                    formatter.indent();
                    indexChoice.getSelectedIndexes().forEach(selected -> formatter.line(selected.toString()));
                    formatter.unindent();

                    if (indexChoice.isUnion()) {
                        formatter.unindent();
                    }
                    if (verbose) {
                        formatter.unindent();
                    }
                }
            }

            formatter.unindent();
        }
        formatter.unindent();
        return formatter.toString();
    }

    private boolean isIndexUsed() {
        return getIndexInspectionsPerPartition().stream().anyMatch(indexInspectionDetail -> !indexInspectionDetail.getIndexes().isEmpty());
    }

    private boolean isNoIndexUsed() {
        return !isIndexUsed();
    }

    private IndexChoiceDetail getUnionIndexChoiceIfExists(List<IndexChoiceDetail> indexes) {
        for (IndexChoiceDetail index : indexes) {
            if (index.isUnion()) {
                return index;
            }
        }
        return null;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public String getTableName() {
        return tableName;
    }

    public String getCriteria() {
        return criteria;
    }

    public void setCriteria(String criteria) {
        this.criteria = criteria;
    }

    public List<IndexInspectionDetail> getIndexInspectionsPerPartition() {
        return indexInspectionsPerPartition;
    }

    public void setIndexInspectionsPerPartition(List<IndexInspectionDetail> indexInspectionsPerPartition) {
        this.indexInspectionsPerPartition = indexInspectionsPerPartition;
    }

    public void addIndexInspection(IndexInspectionDetail indexInspection) {
        indexInspectionsPerPartition.add(indexInspection);
    }

}
