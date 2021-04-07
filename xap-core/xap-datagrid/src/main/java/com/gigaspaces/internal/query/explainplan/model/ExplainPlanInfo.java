/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gigaspaces.internal.query.explainplan.model;

import com.gigaspaces.internal.query.explainplan.ExplainPlanV3;
import com.gigaspaces.internal.query.explainplan.TextReportFormatter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gigaspaces.internal.query.explainplan.ExplainPlanUtil.notEmpty;

/**
 * Base class representing the format of ExplainPlan
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
            final IndexChoiceDetail unionIndexChoice = getUnionIndexChoiceIfExists(inspectionDetail.getIndexes());
            if (!verbose && unionIndexChoice != null) {
                formatter.line(SELECTED_INDEX_STRING);
                formatter.indent();
                formatter.line("Union: ");

                formatter.indent();
                unionIndexChoice.getSelectedIndexes().forEach(selected -> formatter.line(selected.toString()));
                formatter.unindent();

                formatter.unindent();
            } else {
                for (int i = inspectionDetail.getIndexes().size() - 1; i >= 0; i--) {
                    IndexChoiceDetail indexChoice = inspectionDetail.getIndexes().get(i);
                    if (verbose) {
                        formatter.line(indexChoice.getOperator());
                        formatter.indent();
                        formatter.line("Inspected Index: ");
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
