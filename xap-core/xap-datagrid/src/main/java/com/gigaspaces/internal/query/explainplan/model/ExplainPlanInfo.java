package com.gigaspaces.internal.query.explainplan.model;

import com.gigaspaces.internal.query.explainplan.ExplainPlanV3;
import com.gigaspaces.internal.query.explainplan.TextReportFormatter;

import java.util.*;
import java.util.stream.Collectors;

import static com.gigaspaces.internal.query.explainplan.ExplainPlanUtil.notEmpty;

/**
 * Base class representing the format of ExplainPlan
 *
 * @author Mishel Liberman
 * @since 16.0
 */
public class ExplainPlanInfo {
    private final String tableName;
    private final String tableAlias;
    private final Map<String, String> visibleColumnsAndAliasMap;
    private List<PartitionIndexInspectionDetail> indexInspectionsPerPartition = new ArrayList<>();
    private String criteria;

    private static final String SELECTED_INDEX_STRING = "Selected Index:";


    public ExplainPlanInfo(ExplainPlanV3 explainPlan) {
        tableName = explainPlan.getTableName();
        tableAlias = explainPlan.getTableAlias();
        visibleColumnsAndAliasMap = explainPlan.getVisibleColumnsAndAliasMap();
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean verbose) {
        TextReportFormatter formatter = new TextReportFormatter();
        String table = notEmpty(tableAlias) ? tableName + " as " + tableAlias : tableName;
        if (isIndexUsed()) {
            formatter.line("IndexScan: " + table);
        } else {
            formatter.line("FullScan: " + table);
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

        if (!verbose) {
            Map<String, List<IndexInfoDetail>> selectedIndexesPerPartition = new HashMap<>();
            for (PartitionIndexInspectionDetail inspectionDetail : indexInspectionsPerPartition) {
                ArrayList<IndexInfoDetail> selectedIndexes = new ArrayList<>();
                IndexChoiceDetail unionIndexChoice = getUnionIndexChoiceIfExists(inspectionDetail.getIndexes());
                if (unionIndexChoice != null) {
                    selectedIndexes.addAll(unionIndexChoice.getSelectedIndexes());
                } else {
                    inspectionDetail.getIndexes().forEach(indexChoiceDetail -> selectedIndexes.addAll(indexChoiceDetail.getSelectedIndexes()));
                }

                selectedIndexesPerPartition.put(inspectionDetail.getPartition(), selectedIndexes);
            }

            Map<String, List<String>> finalResults = new HashMap<>();
            while (!selectedIndexesPerPartition.isEmpty()) {
                // Every iteration we pull group of similar selected indexes and then we make actions on it
                // and remove them from selectedIndexesPerPartition so that the next iteration will be cleaner
                Map<String, List<IndexInfoDetail>> sameSelectedResults = new HashMap<>();
                Iterator<Map.Entry<String, List<IndexInfoDetail>>> iterator = selectedIndexesPerPartition.entrySet().iterator();
                Map.Entry<String, List<IndexInfoDetail>> first = iterator.next();
                sameSelectedResults.put(first.getKey(), first.getValue());
                iterator.remove();
                while (iterator.hasNext()) {
                    Map.Entry<String, List<IndexInfoDetail>> curr = iterator.next();
                    if (isIndexesEqual(first.getValue(), curr.getValue())) {
                        sameSelectedResults.put(curr.getKey(), curr.getValue());
                        iterator.remove();
                    }
                }

                String partitions = String.join(", ", sameSelectedResults.keySet());
                Map<String, List<Integer>> indexNameAndSizes = new HashMap<>();
                for (List<IndexInfoDetail> choiceList : sameSelectedResults.values()) {
                    for (IndexInfoDetail selectedIndex : choiceList) {
                        indexNameAndSizes.computeIfAbsent(selectedIndex.getName(), k -> new ArrayList<>());
                        indexNameAndSizes.get(selectedIndex.getName()).add(selectedIndex.getSize());
                    }
                }

                List<IndexInfoDetail> randomSelectedIndex = sameSelectedResults.values().stream().findAny().orElseGet(Collections::emptyList);
                List<String> selectedToStringWithMinMaxSize = new ArrayList<>();
                for (IndexInfoDetail selectedIndex : randomSelectedIndex) {
                    List<Integer> sizes = indexNameAndSizes.get(selectedIndex.getName());
                    Integer min = sizes.stream().min(Integer::compareTo).orElse(0);
                    Integer max = sizes.stream().max(Integer::compareTo).orElse(0);
                    selectedToStringWithMinMaxSize.add(selectedIndex.toString(min, max));
                }

                finalResults.put(partitions, selectedToStringWithMinMaxSize);
            }

            finalResults.forEach((partitions, selectedIndexesFormatted) -> {
                boolean unionIndexChoice = selectedIndexesFormatted.size() > 1;

                if (partitions.contains(",")) {
                    formatter.line(String.format("Partitions: [%s]", partitions));
                } else {
                    formatter.line(String.format("Partition: [%s]", partitions));
                }
                formatter.indent();

                formatter.line(SELECTED_INDEX_STRING);
                if (unionIndexChoice) {
                    formatter.indent();
                    formatter.line("Union: ");
                }

                formatter.indent();
                selectedIndexesFormatted.forEach(formatter::line);
                formatter.unindent();

                if (unionIndexChoice) {
                    formatter.unindent();
                }
                formatter.unindent();
                System.out.println("Partitions: " + partitions + "\n" + "SelectedIndex: \n" + selectedIndexesFormatted);
            });
        } else {
            for (PartitionIndexInspectionDetail inspectionDetail : indexInspectionsPerPartition) {
                if ((inspectionDetail.getIndexes() == null || inspectionDetail.getIndexes().isEmpty()) &&
                        (inspectionDetail.getUsedTiers() == null || inspectionDetail.getUsedTiers().isEmpty())) {
                    continue;
                }

                formatter.line(String.format("Partition: [%s]", inspectionDetail.getPartition()));
                formatter.indent();
                if (inspectionDetail.getUsedTiers() != null && inspectionDetail.getUsedTiers().size() != 0) {
                    formatter.line(String.format("Tier%s: %s", (inspectionDetail.getUsedTiers().size() > 1 ? "s" : ""), String.join(", ", inspectionDetail.getUsedTiers())));
                }

                if (inspectionDetail.getIndexes() == null || inspectionDetail.getIndexes().isEmpty()) {
                    formatter.unindent();
                    continue;
                }

                for (int i = inspectionDetail.getIndexes().size() - 1; i >= 0; i--) {
                    IndexChoiceDetail indexChoice = inspectionDetail.getIndexes().get(i);
                    formatter.line(indexChoice.getOperator());
                    formatter.indent();
                    formatter.line("Inspected Index: ");
                    formatter.indent();
                    indexChoice.getInspectedIndexes().forEach(inspected -> formatter.line(inspected.toString()));
                    formatter.unindent();

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
                    formatter.unindent();
                }
                formatter.unindent();
            }
        }

        formatter.unindent();
        return formatter.toString();
    }

    private boolean isIndexesEqual(List<IndexInfoDetail> first, List<IndexInfoDetail> second) {
        if (first == null || second == null || first.size() != second.size()) {
            return false;
        }

        for (int i = 0; i < first.size(); i++) {
            final IndexInfoDetail firstDetail = first.get(i);
            final IndexInfoDetail secondDetail = second.get(i);
            if (!firstDetail.getName().equals(secondDetail.getName())
                    || !firstDetail.getValue().equals(secondDetail.getValue())
                    || !firstDetail.getOperator().equals(secondDetail.getOperator())
                    || !firstDetail.getType().equals(secondDetail.getType())) {
                return false;
            }
        }
        return true;
    }

    private boolean isIndexUsed() {
        return getIndexInspectionsPerPartition().stream().anyMatch(indexInspectionDetail -> indexInspectionDetail.getIndexes() != null && !indexInspectionDetail.getIndexes().isEmpty());
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

    public List<PartitionIndexInspectionDetail> getIndexInspectionsPerPartition() {
        return indexInspectionsPerPartition;
    }

    public void setIndexInspectionsPerPartition(List<PartitionIndexInspectionDetail> indexInspectionsPerPartition) {
        this.indexInspectionsPerPartition = indexInspectionsPerPartition;
    }

    public void addIndexInspection(PartitionIndexInspectionDetail indexInspection) {
        indexInspectionsPerPartition.add(indexInspection);
    }

}
