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
            // Gather the selected indexes per partition along with their usedTier
            Map<String, PartitionFinalSelectedIndexes> selectedIndexesPerPartition = new HashMap<>();
            for (PartitionIndexInspectionDetail inspectionDetail : indexInspectionsPerPartition) {
                ArrayList<IndexInfoDetail> selectedIndexes = new ArrayList<>();
                List<IndexChoiceDetail> indexChoices = inspectionDetail.getIndexes();
                IndexChoiceDetail unionIndexChoice = getUnionIndexChoiceIfExists(indexChoices);
                if (unionIndexChoice != null) {
                    selectedIndexes.addAll(unionIndexChoice.getSelectedIndexes());
                } else {
                    if (indexChoices != null) {
                        indexChoices.forEach(indexChoiceDetail -> selectedIndexes.addAll(indexChoiceDetail.getSelectedIndexes()));
                    }
                }

                selectedIndexesPerPartition.put(inspectionDetail.getPartition(), new PartitionFinalSelectedIndexes(selectedIndexes, inspectionDetail.getUsedTiers()));
            }

            Map<String, List<String>> finalResults = new LinkedHashMap<>();
            Map<String, List<String>> partitionsAndUsedTiers = new HashMap<>();
            // Every iteration we pull group of similar selected indexes and then we make actions on it
            // and remove them from selectedIndexesPerPartition so that the next iteration will be cleaner
            while (!selectedIndexesPerPartition.isEmpty()) {
                Map<String, PartitionFinalSelectedIndexes> sameSelectedResults = new HashMap<>();
                Iterator<Map.Entry<String, PartitionFinalSelectedIndexes>> iterator = selectedIndexesPerPartition.entrySet().iterator();
                Map.Entry<String, PartitionFinalSelectedIndexes> first = iterator.next();
                sameSelectedResults.put(first.getKey(), first.getValue());
                iterator.remove();
                while (iterator.hasNext()) {
                    Map.Entry<String, PartitionFinalSelectedIndexes> curr = iterator.next();
                    if (isIndexesEqual(first.getValue(), curr.getValue())) {
                        sameSelectedResults.put(curr.getKey(), curr.getValue());
                        iterator.remove();
                    }
                }

                String partitions = String.join(", ", sameSelectedResults.keySet());
                Map<String, List<Integer>> indexNameAndSizes = new HashMap<>();
                for (PartitionFinalSelectedIndexes partitionSelectedIndexes : sameSelectedResults.values()) {
                    List<IndexInfoDetail> choiceList = partitionSelectedIndexes.getSelectedIndexes();
                    for (IndexInfoDetail selectedIndex : choiceList) {
                        indexNameAndSizes.computeIfAbsent(selectedIndex.getName(), k -> new ArrayList<>());
                        indexNameAndSizes.get(selectedIndex.getName()).add(selectedIndex.getSize());
                    }
                }

                final PartitionFinalSelectedIndexes partitionSelectedIndexes = sameSelectedResults.values().stream().findFirst().orElseGet(PartitionFinalSelectedIndexes::new);
                List<IndexInfoDetail> randomSelectedIndex = partitionSelectedIndexes.getSelectedIndexes();
                List<String> selectedToStringWithMinMaxSize = new ArrayList<>();
                for (IndexInfoDetail selectedIndex : randomSelectedIndex) {
                    List<Integer> sizes = indexNameAndSizes.get(selectedIndex.getName());
                    Integer min = sizes.stream().min(Integer::compareTo).orElse(0);
                    Integer max = sizes.stream().max(Integer::compareTo).orElse(0);
                    selectedToStringWithMinMaxSize.add(selectedIndex.toString(min, max));
                }

                finalResults.put(partitions, selectedToStringWithMinMaxSize);
                partitionsAndUsedTiers.put(partitions, partitionSelectedIndexes.getUsedTiers());
            }

            finalResults.forEach((partitions, selectedIndexesFormatted) -> {
                List<String> usedTiers = partitionsAndUsedTiers.get(partitions);
                boolean usedTieredStorage = usedTiers != null && usedTiers.size() != 0;
                boolean unionIndexChoice = selectedIndexesFormatted.size() > 1;
                if (selectedIndexesFormatted.isEmpty() && !usedTieredStorage) {
                    return; //skip this iteration
                }

                if (partitions.contains(",")) {
                    formatter.line(String.format("Partitions: [%s]", partitions));
                } else {
                    formatter.line(String.format("Partition: [%s]", partitions));
                }
                formatter.indent();

                if (usedTieredStorage) {
                    formatter.line(getTiersFormatted(usedTiers));
                }

                if (!selectedIndexesFormatted.isEmpty()) {
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
                }
                formatter.unindent();
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
                    formatter.line(getTiersFormatted(inspectionDetail.getUsedTiers()));
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

    private String getTiersFormatted(List<String> usedTiers) {
        return String.format("Tier%s: %s", (usedTiers.size() > 1 ? "s" : ""), String.join(", ", usedTiers));
    }

    private boolean isIndexesEqual(PartitionFinalSelectedIndexes first, PartitionFinalSelectedIndexes second) {
        final List<IndexInfoDetail> firstSelectedIndexes = first.getSelectedIndexes();
        final List<IndexInfoDetail> secondSelectedIndexes = second.getSelectedIndexes();
        final List<String> firstUsedTiers = first.getUsedTiers();
        final List<String> secondUsedTiers = second.getUsedTiers();
        if (firstSelectedIndexes == null || secondSelectedIndexes == null || firstSelectedIndexes.size() != secondSelectedIndexes.size()
                || firstUsedTiers == null || !firstUsedTiers.equals(secondUsedTiers)) {
            return false;
        }

        for (int i = 0; i < firstSelectedIndexes.size(); i++) {
            final IndexInfoDetail firstDetail = firstSelectedIndexes.get(i);
            final IndexInfoDetail secondDetail = secondSelectedIndexes.get(i);
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
        if (indexes != null) {
            for (IndexChoiceDetail index : indexes) {
                if (index.isUnion()) {
                    return index;
                }
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
