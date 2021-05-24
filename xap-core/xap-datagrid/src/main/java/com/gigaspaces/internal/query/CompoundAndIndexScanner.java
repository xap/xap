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

package com.gigaspaces.internal.query;

import com.gigaspaces.internal.query.explainplan.IndexChoiceNode;
import com.gigaspaces.internal.query.explainplan.IndexInfo;
import com.gigaspaces.internal.query.explainplan.UnionIndexInfo;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.IObjectsList;
import com.j_spaces.kernel.list.IScanListIterator;
import com.j_spaces.kernel.list.MultiIntersectedStoredList;
import com.j_spaces.kernel.list.ScanUidsIterator;

import java.util.List;

/**
 * Scans the indexes and gets index with the least number of entries. This will be used as the
 * potential matching list by the CacheManager.
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class CompoundAndIndexScanner extends AbstractCompoundIndexScanner {
    private static final long serialVersionUID = 1L;

    public CompoundAndIndexScanner() {
        super();

    }

    public CompoundAndIndexScanner(List<IQueryIndexScanner> indexScanners) {
        super(indexScanners);
    }

    public String getIndexName() {
        return null;
    }

    public Object getIndexValue() {
        return null;
    }

    public boolean requiresOrderedIndex() {
        return false;
    }

    public boolean supportsFifoOrder() {
        return false;
    }

    public boolean supportsTemplateIndex() {
        return false;
    }

    @Override
    public IObjectsList getIndexedEntriesByType(Context context, TypeData typeData,
                                                ITemplateHolder template, int latestIndexToConsider) {

        IStoredList<IEntryCacheInfo> shortestPotentialMatchList = null;
        IScanListIterator<IEntryCacheInfo> shortestExtendedIndexMatch = null;
        MultiIntersectedStoredList<IEntryCacheInfo> intersectedList = null;   //if index intersection desired
        ScanUidsIterator uidsIter = null;
        int uidsSize = Integer.MAX_VALUE;


        IndexChoiceNode fatherNode = null;
        IndexChoiceNode choiceNode = null;
        IQueryIndexScanner shortestIndex = null;
        final boolean isExplainPlan = context.getExplainPlanContext() != null;
        final boolean trackIndexHits = context.getIndexMetricsContext() != null;
        if(isExplainPlan){
            fatherNode = context.getExplainPlanContext().getFatherNode();
            choiceNode = new IndexChoiceNode("AND");
            context.getExplainPlanContext().getSingleExplainPlan().addScanIndexChoiceNode(typeData.getClassName(), choiceNode);
        }

        // Iterate over custom indexes to find shortest potential match list:
        for (IQueryIndexScanner queryIndex : indexScanners) {
            // Get entries in space that match the indexed value in the query (a.k.a potential match list):
            IObjectsList result;

            if (trackIndexHits && queryIndex.isExtendsAbstractQueryIndex()) {
                context.getIndexMetricsContext().setIgnoreUpdates(true);
            }

            if (isExplainPlan){
                IndexChoiceNode prevFather = context.getExplainPlanContext().getFatherNode();
                context.getExplainPlanContext().setFatherNode(choiceNode);
                result = queryIndex.getIndexedEntriesByType(context, typeData, template, latestIndexToConsider);
                context.getExplainPlanContext().setFatherNode(prevFather);
            }else {
                result = queryIndex.getIndexedEntriesByType(context, typeData, template, latestIndexToConsider);
            }

            if (trackIndexHits && queryIndex.isExtendsAbstractQueryIndex()) {
                context.getIndexMetricsContext().setIgnoreUpdates(false);
            }

            if (result == IQueryIndexScanner.RESULT_IGNORE_INDEX) {
                context.setBlobStoreUsePureIndexesAccess(false);
                continue;
            }

            if (result == IQueryIndexScanner.RESULT_NO_MATCH)
                return null;

            //check the return type - can be iterator
            if (result != null && result.isIterator()) {
                boolean wasUids = uidsIter != null;
                if (wasUids && !context.isIndicesIntersectionEnabled())
                    continue; //uids iter wins over iters
                if (!wasUids && queryIndex.isUidsScanner())
                {
                    uidsIter = (ScanUidsIterator)result;
                    uidsSize = uidsIter.size();
                }
                if (context.isIndicesIntersectionEnabled())
                    intersectedList = addToIntersectedList(context, intersectedList, result, template.isFifoTemplate(), false/*shortest*/, typeData);

                if (!wasUids) {
                    shortestExtendedIndexMatch = (IScanListIterator<IEntryCacheInfo>) result;
                    if (isExplainPlan || trackIndexHits) {
                        shortestIndex = queryIndex;
                    }
                }
                continue;
            }

            final IStoredList<IEntryCacheInfo> potentialMatchList = (IStoredList<IEntryCacheInfo>) result;
            final int potentialMatchListSize = potentialMatchList == null ? 0 : potentialMatchList.size();
            // If the potential match list is empty, there's no need to continue:
            if (potentialMatchListSize == 0){
                if (isExplainPlan){
                    fatherNode.addOption(choiceNode.getChosen());
                }
                return IQueryIndexScanner.RESULT_NO_MATCH;
            }
            if (context.isIndicesIntersectionEnabled())
                intersectedList = addToIntersectedList(context, intersectedList, potentialMatchList, template.isFifoTemplate(), false/*shortest*/, typeData);

            // If the potential match list is shorter than the shortest match list so far, keep it:
            if (shortestPotentialMatchList == null || potentialMatchListSize <= shortestPotentialMatchList.size()){
                shortestPotentialMatchList = potentialMatchList;
                if(isExplainPlan || trackIndexHits){
                    shortestIndex = queryIndex;
                }
            }
            if (!shortestPotentialMatchList.isMultiObjectCollection() && !context.isIndicesIntersectionEnabled())
                break;

        }

        if (shortestPotentialMatchList != null && (uidsSize == Integer.MAX_VALUE || shortestPotentialMatchList.size() <= uidsSize)) {
            if (context.isIndicesIntersectionEnabled()) {
                intersectedList = addToIntersectedList(context, intersectedList, shortestPotentialMatchList, template.isFifoTemplate(), true/*shortest*/, typeData);
                if (shortestExtendedIndexMatch != null)
                    intersectedList = addToIntersectedList(context, intersectedList, shortestExtendedIndexMatch, template.isFifoTemplate(), false/*shortest*/, typeData);
                return intersectedList;
            }

            if (isExplainPlan){
                addChosenIndex(context, typeData, fatherNode, choiceNode, shortestIndex != null ? shortestIndex.getIndexName() : "");
            }

            if (trackIndexHits && shortestIndex != null) {
                context.getIndexMetricsContext().addChosenIndex(shortestIndex);
            }

            return shortestPotentialMatchList;
        }

        if (shortestExtendedIndexMatch != null) {
            if (context.isIndicesIntersectionEnabled()) {
                intersectedList = addToIntersectedList(context, intersectedList, shortestExtendedIndexMatch, template.isFifoTemplate(), true/*shortest*/, typeData);
                return intersectedList;
            }

            if (isExplainPlan){
                addChosenIndex(context, typeData, fatherNode, choiceNode, shortestIndex != null ? shortestIndex.getIndexName() : null);
            }

            if (trackIndexHits && shortestIndex != null) {
                context.getIndexMetricsContext().addChosenIndex(shortestIndex);
            }

            return shortestExtendedIndexMatch;
        }
        if(isExplainPlan && !choiceNode.getOptions().isEmpty()){
            IndexInfo firstOption = choiceNode.getOptions().get(0);
            fatherNode.addOption(firstOption);
            if (firstOption.isUsable()) {
                choiceNode.setChosen(firstOption);
                fatherNode.setChosen(new UnionIndexInfo(fatherNode.getOptions()));
            }

        }
        return IQueryIndexScanner.RESULT_IGNORE_INDEX;
    }

    private void addChosenIndex(Context context, TypeData typeData, IndexChoiceNode fatherNode, IndexChoiceNode choiceNode, String shortestIndexName) {
        IndexInfo chosen = context.getExplainPlanContext().getSingleExplainPlan().getLatestIndexChoiceNode(typeData.getClassName()).getOptionByName(shortestIndexName);
        choiceNode.setChosen(chosen);
        fatherNode.addOption(chosen);
    }

    private MultiIntersectedStoredList<IEntryCacheInfo> addToIntersectedList(Context context, MultiIntersectedStoredList<IEntryCacheInfo> intersectedList, IObjectsList list, boolean fifoScan, boolean shortest, TypeData typeData) {
        if (intersectedList == null)
            intersectedList = new MultiIntersectedStoredList<>(context, list, fifoScan, typeData.getEntries(), !context.isBlobStoreUsePureIndexesAccess() /*false positive only*/);
        else
            intersectedList.add(list, shortest);
        return intersectedList;
    }

    public Object getEntriesByIndex(TypeDataIndex<Object> index) {
        throw new UnsupportedOperationException();
    }
}
