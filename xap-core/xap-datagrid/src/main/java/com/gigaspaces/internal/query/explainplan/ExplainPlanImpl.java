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
import com.gigaspaces.internal.utils.Constants;
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
        Map<String, Object> plan = Collections.emptyMap();
        TextReportFormatter report = new TextReportFormatter();
        report.line(ExplainPlanUtil.REPORT_START);
        if (Boolean.parseBoolean(System.getProperty(Constants.USE_OLD_EXPLAIN_PLAN_PROPERTY))) {
            append(report);
        } else {
            plan = createPlan(report);
        }
        System.out.println(plan.size());
        report.line(ExplainPlanUtil.REPORT_END);
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

    /**
     * Creates the new explain plan in case GS_OLD_EXPLAIN_PLAN wasn't set to true
     * @return JSON structured plan
     * @since GS-14433, 16.0
     */
    protected Map<String, Object> createPlan(TextReportFormatter report) {
        String queryString;
        String typeName = "";
        final Map<String, Object> explainPlan = new HashMap<>();
        if (query == null) {
            typeName = plans.values().iterator().next().getIndexesInfo().keySet().stream().findFirst().orElse("");
            queryString = typeName;
        } else {
            queryString = this.query.toString();
            typeName = query.getTypeName();
        }
        report.line("TableScan: " + typeName);
        explainPlan.put("TableScan: ", typeName);

        if (plans.isEmpty()) {
            final String notExecutedYet = Constants.NOT_EXECUTED_YET_ERROR;
            report.line(notExecutedYet);
            return Collections.singletonMap(queryString, notExecutedYet);
        }

        explainPlan.put(queryString, appendScanDetails(report));
        return explainPlan;
    }

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

    /**
     * @since GS-14433, 16.0
     */
    protected List<Map> appendScanDetails(TextReportFormatter report) {
        indexInfoDescCache.clear();
        final List<Map> partitionsPlan = new ArrayList<>();
        final String filterKey = Constants.QUERY_FILTER_DESCRIPTION;
        report.line(filterKey);
        String queryFilterTree = appendQueryFilterTree(report, plans.values().iterator().next().getRoot());
        partitionsPlan.add(Collections.singletonMap(filterKey, queryFilterTree));

        for (Map.Entry<String, SingleExplainPlan> entry : plans.entrySet()) {
            partitionsPlan.add(appendPartitionPlan(report, entry.getKey(), entry.getValue()));
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

    /**
     * @since GS-14433, 16.0
     */
    protected String appendQueryFilterTree(TextReportFormatter report, QueryOperationNode node) {
        final String nodeTreeOutput = node.printTree();
        if (node instanceof QueryJunctionNode) {
            report.indent();
            report.line(nodeTreeOutput);
            report.unindent();
        }
        return nodeTreeOutput;
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

    /**
     * @since GS-14433, 16.0
     */
    protected Map<String, Object> appendPartitionPlan(TextReportFormatter report, String partitionId, SingleExplainPlan singleExplainPlan) {
        final String partitionIdString = "Partition Id: " + partitionId;
        report.line(partitionIdString);
        
        final Map<String, List<IndexChoiceNode>> indexesInfo = singleExplainPlan.getIndexesInfo();
        final Map<String, Object> partitionPlan = new HashMap<>();
        final Map<String, Object> tablePlan = new HashMap<>();
        if (indexesInfo.isEmpty()) {
            report.line(Constants.NO_INDEX_USED_MESSAGE);
            partitionPlan.put(partitionIdString, Constants.NO_INDEX_USED_MESSAGE);
            return partitionPlan;
        }

        report.indent();
        for (Map.Entry<String, List<IndexChoiceNode>> entry : indexesInfo.entrySet()) {
            List<IndexChoiceNode> indexChoices = entry.getValue();
            String dataTypeName = entry.getKey();
            final List<Map> indexInspections = appendIndexInspectionPerTable(report, indexChoices);
            tablePlan.put(dataTypeName, indexInspections);
        }
        report.unindent();

        partitionPlan.put(partitionIdString, tablePlan);
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

    /**
     * @since GS-14433, 16.0
     */
    protected List<Map> appendIndexInspectionPerTable(TextReportFormatter report, List<IndexChoiceNode> indexChoices) {
        List<Map> operatorsInspectionList = new ArrayList<>();

        int i = indexChoices.size()-1;
        if (indexChoices.get(0).getChosen() instanceof UnionIndexInfo) {
            i = 0;
        }

        for (; i >= 0 ; i--) {
            IndexChoiceNode node = indexChoices.get(i);
            final List<String> selected = getSelectedIndexesDescription(node.getChosen());
            report.line(selected);
            operatorsInspectionList.add(Collections.singletonMap(node.getName(), selected));
        }
        report.unindent();
        return operatorsInspectionList;
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

    /**
     * @since GS-14433, 16.0
     */
    private List<String> getSelectedIndexesDescription(IndexInfo indexInfo) {
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
