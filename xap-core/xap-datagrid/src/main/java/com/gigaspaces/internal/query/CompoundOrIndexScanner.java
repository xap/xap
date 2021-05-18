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

import com.gigaspaces.internal.query.explainplan.ExplainPlanContext;
import com.gigaspaces.internal.query.explainplan.IndexChoiceNode;
import com.gigaspaces.internal.query.explainplan.UnionIndexInfo;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.kernel.list.IObjectsList;
import com.j_spaces.kernel.list.MultiStoredList;

/**
 * Scans the indexes and gets the union of all indexed entries. This will be used as the potential
 * matching list by the CacheManager. Note: Currently the unified list will contain duplicates in
 * case of overlapping ranges. This should be optimized in the future.
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class CompoundOrIndexScanner extends AbstractCompoundIndexScanner

{
    private static final long serialVersionUID = 1L;

    public CompoundOrIndexScanner() {
        super();

    }

    public String getIndexName() {
        return null;
    }

    public Object getIndexValue() {
        return null;
    }

    public boolean requiresOrderedIndex() {
        return true;
    }

    public boolean supportsFifoOrder() {
        return false;
    }

    public boolean supportsTemplateIndex() {
        return false;
    }


    public IObjectsList getIndexedEntriesByType(Context context, TypeData typeData,
                                                ITemplateHolder template, int latestIndexToConsider) {
        // ignore indexes that don't support fifo order scanning - otherwise the
        // results won't preserve the fifo order
        if (template.isFifoTemplate() && !supportsFifoOrder())
            return IQueryIndexScanner.RESULT_IGNORE_INDEX;

        MultiStoredList<IEntryCacheInfo> unionList = new MultiStoredList<>();
        if (template.isFifoGroupPoll())
            context.setFifoGroupQueryContainsOrCondition(true);

        IndexChoiceNode choiceNode = null;
        if(context.getExplainPlanContext() != null){
            choiceNode = new IndexChoiceNode("OR");
            ExplainPlanContext explainPlanContext = context.getExplainPlanContext();
            explainPlanContext.getSingleExplainPlan().addScanIndexChoiceNode(typeData.getClassName(), choiceNode);
            explainPlanContext.setFatherNode(choiceNode);
        }

        for (IQueryIndexScanner indexScanner : indexScanners) {
            IObjectsList indexResult = indexScanner.getIndexedEntriesByType(context, typeData, template, latestIndexToConsider);

            if (indexResult == IQueryIndexScanner.RESULT_IGNORE_INDEX) {
                context.setBlobStoreUsePureIndexesAccess(false);
                return indexResult;
            }

            if (indexResult == IQueryIndexScanner.RESULT_NO_MATCH)
                continue;

            if (indexResult == null)
                continue;

            unionList.add(indexResult);
        }
        if(context.getExplainPlanContext() != null){
            choiceNode.setChosen(new UnionIndexInfo(choiceNode.getOptions()));
        }
        return unionList;
    }

    public void add(IQueryIndexScanner customIndex) {
        indexScanners.add(customIndex);
    }

    public Object getEntriesByIndex(TypeDataIndex<Object> index) {
        throw new UnsupportedOperationException();
    }

}
