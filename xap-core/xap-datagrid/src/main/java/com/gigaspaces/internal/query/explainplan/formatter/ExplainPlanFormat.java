package com.gigaspaces.internal.query.explainplan.formatter;

import com.gigaspaces.internal.query.explainplan.ExplainPlanUtil;
import com.gigaspaces.internal.query.explainplan.TextReportFormatter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.gigaspaces.internal.query.explainplan.ExplainPlanUtil.notEmpty;

public class ExplainPlanFormat {
    private String spaceName;
    private String tableName;
    private String tableAlias;
    private String criteria;
    private List<IndexInspectionFormat> indexInspectionsPerPartition = new ArrayList<>();
    private Map<String, String> visibleColumnsAndAliasMap;


    public ExplainPlanFormat(String tableName, String tableAlias, Map<String, String> visibleColumnsAndAliasMap, String spaceName) {
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.visibleColumnsAndAliasMap = visibleColumnsAndAliasMap;
        this.spaceName = spaceName;
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean verbose) {
        TextReportFormatter formatter = new TextReportFormatter();
        formatter.line(ExplainPlanUtil.REPORT_START);
        String table = notEmpty(tableAlias) ? tableAlias : tableName;
        table = notEmpty(spaceName) ? spaceName + "." + table : table;
        if (isNoIndexUsed()) {
            formatter.line("FullScan: " + table);
        } else {
            formatter.line("TableScan: " + table);
        }

        if (visibleColumnsAndAliasMap != null) {
            String columns = "";
            for (Map.Entry<String, String> column : visibleColumnsAndAliasMap.entrySet()) {
                columns += column.getKey() + (notEmpty(column.getValue()) ? " as "+column.getValue()+", " : ", ");
            }
            if (columns.length() > 2) {
                columns = columns.substring(0, columns.length() - 2); //trim ', '
            }

            if (notEmpty(columns)) {
                formatter.line("Select: " + columns);
            }
        }


        if (notEmpty(criteria)) {
            formatter.line("Criteria: " + criteria);
        }

        for (IndexInspectionFormat inspectionFormat : indexInspectionsPerPartition) {
            if (inspectionFormat.getIndexes().isEmpty()) {
                continue;
            }

            formatter.line(String.format("Partition: [%s]", inspectionFormat.getPartition()));
            final IndexChoiceFormat unionIndexChoice = getUnionIndexChoiceIfExists(inspectionFormat.getIndexes());
            formatter.indent();
            for (int i = inspectionFormat.getIndexes().size() - 1; i >= 0; i--) {
                IndexChoiceFormat indexChoice = inspectionFormat.getIndexes().get(i);
                if (!verbose && unionIndexChoice != null && !unionIndexChoice.equals(indexChoice)) {
                    continue;//We want to print only the final selected union in this case so skip other inspections.
                }

                if (verbose) {
                    formatter.line(indexChoice.getOperator());
                    formatter.indent();
                    formatter.line("Inspected: ");
                    formatter.indent();
                    for (IndexInfoFormat inspected : indexChoice.getInspectedIndexes()) {
                        formatter.line(inspected.toString());
                    }
                    formatter.unindent();
                }

                formatter.line("SelectedIndex:");
                if (indexChoice.isUnion()) {
                    formatter.indent();
                    formatter.line("Union: ");
                }

                formatter.indent();
                for (IndexInfoFormat selected : indexChoice.getSelectedIndexes()) {
                    formatter.line(selected.toString());
                }
                formatter.unindent();

                if (indexChoice.isUnion()) {
                    formatter.unindent();
                }

                if (verbose) {
                    formatter.unindent();
                }
            }

            formatter.unindent();
        }
        formatter.line(ExplainPlanUtil.REPORT_END);
        return formatter.toString();
    }

    private boolean isIndexUsed() {
        return getIndexInspectionsPerPartition().stream().anyMatch(indexInspectionFormat -> !indexInspectionFormat.getIndexes().isEmpty());
    }

    private boolean isNoIndexUsed() {
        return !isIndexUsed();
    }

    private IndexChoiceFormat getUnionIndexChoiceIfExists(List<IndexChoiceFormat> indexes) {
        for (IndexChoiceFormat index : indexes) {
            if (index.isUnion()) {
                return index;
            }
        }
        return null;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCriteria() {
        return criteria;
    }

    public void setCriteria(String criteria) {
        this.criteria = criteria;
    }

    public List<IndexInspectionFormat> getIndexInspectionsPerPartition() {
        return indexInspectionsPerPartition;
    }

    public void setIndexInspectionsPerPartition(List<IndexInspectionFormat> indexInspectionsPerPartition) {
        this.indexInspectionsPerPartition = indexInspectionsPerPartition;
    }

    public boolean addIndexInspection(IndexInspectionFormat indexInspection) {
        return indexInspectionsPerPartition.add(indexInspection);
    }

}
