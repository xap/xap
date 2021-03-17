package com.gigaspaces.internal.query.explainplan;

import com.gigaspaces.internal.query.explainplan.model.ExplainPlanInfo;
import com.gigaspaces.internal.query.explainplan.model.IndexChoiceDetail;
import com.gigaspaces.internal.query.explainplan.model.IndexInfoDetail;
import com.gigaspaces.internal.query.explainplan.model.IndexInspectionDetail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * ExplainPlan for the JDBC Driver V3
 * @author Mishel Liberman
 * @since 16.0
 */
public class ExplainPlanV3 extends ExplainPlanImpl {

    private final String spaceName;
    private final String tableName;
    private final String tableAlias;
    private final Map<String, String> visibleColumnsAndAliasMap;

    public ExplainPlanV3(String tableName, String tableAlias, Map<String, String> visibleColumnsAndAliasMap, String spaceName) {
        super(null);
        this.spaceName = spaceName;
        this.tableName = tableName;
        this.tableAlias = tableAlias;
        this.visibleColumnsAndAliasMap = visibleColumnsAndAliasMap;
    }

    @Override
    public String toString() {
        return createPlan().toString();
    }

    public String getSpaceName() {
        return spaceName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public Map<String, String> getVisibleColumnsAndAliasMap() {
        return visibleColumnsAndAliasMap;
    }

    /**
     * @return JSON structured plan
     */
    protected ExplainPlanInfo createPlan() {
        ExplainPlanInfo planFormat = new ExplainPlanInfo(this);
        if (!plans.isEmpty()) {
            appendScanDetails(planFormat);
        }
        return planFormat;
    }

    protected void appendScanDetails(ExplainPlanInfo planFormat) {
        indexInfoDescCache.clear();
        String queryFilterTree = getQueryFilterTree(plans.values().iterator().next().getRoot());
        planFormat.setCriteria(queryFilterTree);
        for (Map.Entry<String, SingleExplainPlan> entry : plans.entrySet()) {
            planFormat.addIndexInspection(getPartitionPlan(entry.getKey(), entry.getValue()));
        }
    }

    protected String getQueryFilterTree(QueryOperationNode node) {
        return node == null ? null : node.getPrettifiedString();
    }

    protected IndexInspectionDetail getPartitionPlan(String partitionId, SingleExplainPlan singleExplainPlan) {
        final IndexInspectionDetail indexInspection = new IndexInspectionDetail();
        final Map<String, List<IndexChoiceNode>> indexesInfo = singleExplainPlan.getIndexesInfo();
        if (indexesInfo.size() == 1) {
            Map.Entry<String, List<IndexChoiceNode>> entry = indexesInfo.entrySet().iterator().next();
            List<IndexChoiceNode> indexChoices = entry.getValue();
            List<IndexChoiceDetail> indexInspections = getIndexInspectionPerTableType(indexChoices);
            indexInspection.setIndexes(indexInspections);
            indexInspection.setPartition(partitionId);
        } else if (indexesInfo.size() != 0) {
            throw new UnsupportedOperationException("Not supported with more than one type");
        }
        return indexInspection;
    }

    protected List<IndexChoiceDetail> getIndexInspectionPerTableType(List<IndexChoiceNode> indexChoices) {
        List<IndexChoiceDetail> indexChoiceDetailList = new ArrayList<>();
        for (IndexChoiceNode node : indexChoices) {
            final List<IndexInfoDetail> selected = getSelectedIndexesDescription(node.getChosen());
            final List<IndexInfoDetail> inspected = getInspectedIndexesDescription(node.getOptions());
            boolean isUnion = node.getChosen() instanceof UnionIndexInfo;
            final IndexChoiceDetail indexChoiceDetail = new IndexChoiceDetail(node.getName(), isUnion, inspected, selected);
            indexChoiceDetailList.add(indexChoiceDetail);
        }
        return indexChoiceDetailList;
    }

    private List<IndexInfoDetail> getInspectedIndexesDescription(List<IndexInfo> options) {
        final List<IndexInfoDetail> indexInfoDetails = new ArrayList<>();
        for (IndexInfo option : options) {
            final IndexInfoDetail infoFormat = new IndexInfoDetail(getOptionDesc(option), option);
            indexInfoDetails.add(infoFormat);
        }
        return indexInfoDetails;
    }

    private List<IndexInfoDetail> getSelectedIndexesDescription(IndexInfo indexInfo) {
        final List<IndexInfoDetail> indexInfoDetails = new ArrayList<>();
        if (indexInfo == null) return indexInfoDetails;
        if (indexInfo instanceof UnionIndexInfo) {
            final List<IndexInfo> options = ((UnionIndexInfo) indexInfo).getOptions();
            if (options.size() == 0)
                return null;

            for (IndexInfo option : options) {
                final IndexInfoDetail infoFormat = new IndexInfoDetail(getOptionDesc(option), option);
                indexInfoDetails.add(infoFormat);
            }
            return indexInfoDetails;
        }
        final IndexInfoDetail infoFormat = new IndexInfoDetail(getOptionDesc(indexInfo), indexInfo);
        return Collections.singletonList(infoFormat);
    }
}
