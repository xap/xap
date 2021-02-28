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
package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.api.ExperimentalApi;
import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.IntegerObjectMap;
import com.gigaspaces.query.explainplan.ExplainPlan;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.util.*;

/**
 * @author yael nahon
 * @since 12.0.1
 */
@ExperimentalApi
public class ExplainPlanImpl implements ExplainPlan {

    private final SQLQuery<?> query;
    private final Map<String,SingleExplainPlan> plans = new HashMap<String, SingleExplainPlan>();
    private final IntegerObjectMap<String> indexInfoDescCache = CollectionsFactory.getInstance().createIntegerObjectMap();

    /**
     * @param query can be null
     */
    public ExplainPlanImpl(SQLQuery query) {
        this.query = query;
    }

    public static ExplainPlanImpl fromQueryPacket(Object query) {
        ExplainPlanImpl result = null;
        if (query instanceof QueryTemplatePacket) {
            result = (ExplainPlanImpl) ((QueryTemplatePacket)query).getExplainPlan();
        }
        if (result != null) {
            result.reset();
        }

        return result;
    }

    public SingleExplainPlan getPlan(String partitionId) {
        return plans.get(partitionId);
    }

    public Map<String, SingleExplainPlan> getAllPlans() {
        return plans;
    }

    public void reset() {
        plans.clear();
        indexInfoDescCache.clear();
    }

    public void aggregate(SingleExplainPlan plan) {
        plans.put(plan.getPartitionId(), plan);
    }

    @Override
    public String toString() {
        TextReportFormatter report = new TextReportFormatter();
        report.line(ExplainPlanUtil.REPORT_START);
        final Map<String, Object> plan = CreateV2Plan(report);
//        append(report);
        report.line(ExplainPlanUtil.REPORT_END);
        return report.toString();
    }

    public String toStringV2() {
        TextReportFormatter report = new TextReportFormatter();
        report.line(ExplainPlanUtil.REPORT_START);
        final Map<String, Object> plan = CreateV2Plan(report);
        report.line(ExplainPlanUtil.REPORT_END);
//        return PlanFormatter.format(plan);
        return report.toString();
    }

    protected void append(TextReportFormatter report) {
        report.line("Query: " + query );
        if (plans.isEmpty()) {
            report.line("Not executed yet");
        } else {
            appendSummary(report);
            appendDetailed(report);
        }
    }

    protected Map<String, Object> CreateV2Plan(TextReportFormatter report) {
        String queryString;
        String filters = "";
        if (query == null) {
            queryString = "";//TODO get the query
//            filters = getFiltersV2();
        } else {
            queryString = this.query.toString();
            filters = this.query.getQuery();
            report.line("TableScan: " + query.getTypeName());
        }

        if (plans.isEmpty()) {
            report.line("Not executed yet");
            return Collections.singletonMap(queryString, "Not executed yet");
        }
        return Collections.singletonMap(queryString, appendDetailedV2(report, filters));
    }

//    private String getFiltersV2() {
//        getAllPlans()
//
//    }

    protected void appendSummary(TextReportFormatter report) {
        report.line("Execution Information Summary:");
        report.indent();
        report.line("Total scanned partitions: " + plans.size());
        int totalScanned = 0;
        int totalMatched = 0;
        for (SingleExplainPlan plan : plans.values()) {
            for (ScanningInfo scanningInfo : plan.getScanningInfo().values()) {
                totalScanned += scanningInfo.getScanned();
                totalMatched += scanningInfo.getMatched();
            }
        }

        report.line("Total scanned entries: " + totalScanned);
        report.line("Total matched entries: " + totalMatched);
        report.unindent();
    }

    protected void appendDetailed(TextReportFormatter report) {
        indexInfoDescCache.clear();
        report.line("Detailed Execution Information:");
        report.indent();
        report.line("Query Tree:");
        report.indent();
        append(report, plans.values().iterator().next().getRoot());
        report.unindent();

        for (Map.Entry<String, SingleExplainPlan> entry : plans.entrySet()) {
            append(report, entry.getKey(), entry.getValue());
        }
        report.unindent();
    }

    protected List<Map> appendDetailedV2(TextReportFormatter report, String filters) {
        indexInfoDescCache.clear();
        final List<Map> partitionsPlan = new ArrayList<>();
        if (!filters.isEmpty()) {
            final String filterKey = "Filter: ";
            report.line(filterKey + filters);
            partitionsPlan.add(Collections.singletonMap(filterKey, filters));
        } else {
            report.inline("Query Filter Tree:");
            appendV2(report, plans.values().iterator().next().getRoot());
            report.newLine();
        }

        for (Map.Entry<String, SingleExplainPlan> entry : plans.entrySet()) {
            partitionsPlan.add(appendPartitionPlanV2(report, entry.getKey(), entry.getValue()));
        }
        report.unindent();
        return partitionsPlan;
    }

    protected void append(TextReportFormatter report, QueryOperationNode node) {
        report.line(node.toString());
        report.indent();
        for (QueryOperationNode subNode : node.getChildren()) {
            append(report, subNode);
        }
        report.unindent();
    }

    protected void appendV2(TextReportFormatter report, QueryOperationNode node) {

        if (node instanceof QueryJunctionNode) {
            report.indent();
            report.newLine();
            report.inline(node.toStringV2());
        } else {
            report.inline(node.toStringV2()+" ");
        }
        for (QueryOperationNode subNode : node.getChildren()) {
            appendV2(report, subNode);
        }

        if (node instanceof QueryJunctionNode) {
            report.unindent();
        }
    }

    protected void append(TextReportFormatter report, String partitionId, SingleExplainPlan singleExplainPlan) {
        report.line("Partition Id: " + partitionId);
        final Map<String, List<IndexChoiceNode>> indexesInfo = singleExplainPlan.getIndexesInfo();
        final Map<String, ScanningInfo> scanningInfo = singleExplainPlan.getScanningInfo();
        if (indexesInfo.isEmpty()) {
            report.line("Index Information: NO INDEX USED");
            report.indent();
            for (Map.Entry<String, ScanningInfo> entry : scanningInfo.entrySet()) {
                report.line(entry.getKey() + ":");
                append(report, entry.getValue());
            }
            report.unindent();
        } else if (indexesInfo.size() == 1) {
            Map.Entry<String, List<IndexChoiceNode>> entry = indexesInfo.entrySet().iterator().next();
            ScanningInfo scanningInfoEntry = scanningInfo != null ? scanningInfo.get(entry.getKey()) : null;
            report.indent();
            append(report, null, entry.getValue(), scanningInfoEntry);
            report.unindent();
        } else {
            report.indent();
            for (Map.Entry<String, List<IndexChoiceNode>> entry : indexesInfo.entrySet()) {
                ScanningInfo scanningInfoEntry = scanningInfo != null ? scanningInfo.get(entry.getKey()) : null;
                append(report, entry.getKey(), entry.getValue(), scanningInfoEntry);
            }
            report.unindent();
        }
    }

    protected Map<String, Object> appendPartitionPlanV2(TextReportFormatter report, String partitionId, SingleExplainPlan singleExplainPlan) {
        final String partitionIdString = "Partition Id: " + partitionId;
        report.line(partitionIdString);
        final Map<String, List<IndexChoiceNode>> indexesInfo = singleExplainPlan.getIndexesInfo();
        final Map<String, Object> partitionPlan = new HashMap<>();
        if (indexesInfo.isEmpty()) {
            report.line("Index Information: NO INDEX USED");
            partitionPlan.put(partitionIdString, "Index Information: NO INDEX USED");
        } else if (indexesInfo.size() == 1) {
            Map.Entry<String, List<IndexChoiceNode>> entry = indexesInfo.entrySet().iterator().next();
            report.indent();
            final Map<String, Object> indexInspections = appendIndexInspectionV2(report, null, entry.getValue());
            report.unindent();
            partitionPlan.put(partitionIdString, indexInspections);
        } else {
            report.indent();
            for (Map.Entry<String, List<IndexChoiceNode>> entry : indexesInfo.entrySet()) {
                final Map<String, Object> indexInspections = appendIndexInspectionV2(report, entry.getKey(), entry.getValue());
                //TODO handle this case
            }
            report.unindent();
        }
        return partitionPlan;
    }

    protected void append(TextReportFormatter report, String typeName, List<IndexChoiceNode> list, ScanningInfo scanningInfo) {
        if (typeName != null) {
            report.line("Type name: " + typeName);
            report.indent();
        }
        append(report, scanningInfo);
        report.line("Index scan report:");
        report.indent();
        for (int i = list.size()-1 ; i >= 0 ; i--) {
            IndexChoiceNode node = list.get(i);
            report.line(node.getName());
            report.indent();
            report.line("Inspected: ");
            report.indent();
            for (IndexInfo option : node.getOptions()) {
                report.line("[" + getOptionDesc(option) + "] " + option.toString());
            }
            report.unindent();
            report.line("Selected: " + "[" + getOptionDesc(node.getChosen()) + "] " + getSelectedDesc(node.getChosen()));
            report.unindent();
        }
        report.unindent();
        if (typeName != null)
            report.unindent();
    }

    protected Map<String, Object> appendIndexInspectionV2(TextReportFormatter report, String typeName, List<IndexChoiceNode> list) {
        Map<String, Object> operatorAndPartitionIndexInspected = new HashMap<>();

        int i = list.size()-1;
        if (list.get(0).getChosen() instanceof UnionIndexInfo) {
            i = 0;
        }

        for (; i >= 0 ; i--) {
            IndexChoiceNode node = list.get(i);
            final List<String> selectedDescV2 = getSelectedDescV2(node.getChosen());
            report.line(selectedDescV2);
            List<String> selected = new ArrayList<>(selectedDescV2);
            final String selectedKey = "Selected: ";
            Map<String, Object> operatorAndIndexInspection = (Map<String, Object>) operatorAndPartitionIndexInspected.get(node.getName());
            if (operatorAndIndexInspection == null) {
                operatorAndIndexInspection = new HashMap<>();
                operatorAndIndexInspection.put(selectedKey, new ArrayList<>());
            }

            final List<String> selectedTotal = (List<String>) operatorAndIndexInspection.get(selectedKey);
            if (selectedTotal == null) {
                operatorAndIndexInspection.put(selectedKey, selected);
            } else {
                selectedTotal.addAll(selected);
            }

            operatorAndPartitionIndexInspected.put(node.getName(), operatorAndIndexInspection);
        }
        report.unindent();
        return operatorAndPartitionIndexInspected;
    }

    private String getSelectedDesc(IndexInfo indexInfo) {
        if (indexInfo == null) return "N/A";
        if (indexInfo instanceof UnionIndexInfo) {
            final List<IndexInfo> options = ((UnionIndexInfo) indexInfo).getOptions();
            if (options.size() == 0)
                return "Union []";
            StringBuilder sb = new StringBuilder();
            for (IndexInfo option : options) {
                sb.append(sb.length() == 0 ? "Union [" : ", ");
                sb.append(getOptionDesc(option));
            }
            sb.append("]");
            return sb.toString();
        }
        return indexInfo.toString();
    }


    private List<String> getSelectedDescV2(IndexInfo indexInfo) {
        final List<String> unitedIndexes = new ArrayList<>();
        if (indexInfo == null) return Collections.singletonList("N/A");
        if (indexInfo instanceof UnionIndexInfo) {
            final List<IndexInfo> options = ((UnionIndexInfo) indexInfo).getOptions();
            if (options.size() == 0)
                return Collections.singletonList("Union []");

            unitedIndexes.add("Selected Union: ");
            for (IndexInfo option : options) {
                unitedIndexes.add("[" + getOptionDesc(option) + "] " + option.toStringV2());
            }
            return unitedIndexes;
        }
        return Collections.singletonList("Selected: " + indexInfo.toStringV2());
    }

    protected void append(TextReportFormatter report, ScanningInfo scanningInfo) {
        Integer scanned = scanningInfo != null ? scanningInfo.getScanned() : 0;
        Integer matched = scanningInfo != null ? scanningInfo.getMatched() : 0;
        report.line("Scanned entries: " + scanned);
        report.line("Matched entries: " + matched);
    }

    private String getOptionDesc(IndexInfo indexInfo) {
        final int id = System.identityHashCode(indexInfo);
        String desc = indexInfoDescCache.get(id);
        if (desc == null) {
            desc = "@" + (indexInfoDescCache.size() + 1);
            indexInfoDescCache.put(id, desc);
        }
        return desc;
    }
}
