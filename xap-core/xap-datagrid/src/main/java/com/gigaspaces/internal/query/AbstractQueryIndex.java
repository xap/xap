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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.explainplan.ExplainPlanUtil;
import com.gigaspaces.internal.query.explainplan.IndexChoiceNode;
import com.gigaspaces.internal.query.explainplan.IndexInfo;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.IObjectsList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author anna
 * @since 7.1
 */
public abstract class AbstractQueryIndex implements IQueryIndexScanner {
    private static final long serialVersionUID = -4783385338736231377L;

    private String _indexName;

    public AbstractQueryIndex() {
        super();
    }

    public AbstractQueryIndex(String indexName) {
        super();

        if (indexName == null)
            throw new IllegalArgumentException("Index name can not be null");
        _indexName = indexName;
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.query_poc.server.ICustomIndexQuery#getIndexName()
     */
    public String getIndexName() {
        return _indexName;
    }


    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _indexName = IOUtils.readString(in);
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeString(out, _indexName);
    }


    /**
     * by default fifo order is not preserved
     */
    public boolean supportsFifoOrder() {
        return false;
    }


    public IObjectsList getIndexedEntriesByType(Context context, TypeData typeData,
                                                ITemplateHolder template, int latestIndexToConsider) {
        ResultIndicator resultIndicator = null;
        // Find type index by current query index:
        final TypeDataIndex index = typeData.getIndex(getIndexName());
        if (index == null) {
            // maybe belongs to another class in the type hierarchy - so ignore
             resultIndicator = IQueryIndexScanner.RESULT_IGNORE_INDEX;
        }
        else if (latestIndexToConsider < index.getIndexCreationNumber()){
            resultIndicator =  IQueryIndexScanner.RESULT_IGNORE_INDEX; // uncompleted index
        }
        // ignore indexes that don't support fifo order scanning - otherwise the
        // results won't preserve the fifo order
        else if (template.isFifoTemplate() && !supportsFifoOrder()){
            resultIndicator =  IQueryIndexScanner.RESULT_IGNORE_INDEX;
        }
        // check the cases when ordered index can not be used:
        // ordered index is not defined
        else if (requiresOrderedIndex() && index.getExtendedIndexForScanning() == null){
            resultIndicator =  IQueryIndexScanner.RESULT_IGNORE_INDEX;
        }

        // Get index value in query. If null, skip to next index unless its an isNull:
        else if (requiresValueForIndexSearch() && !hasIndexValue()){
            resultIndicator =  IQueryIndexScanner.RESULT_IGNORE_INDEX;
        }
        else if (template.isFifoGroupPoll() && !index.isFifoGroupsMainIndex() && (context.isFifoGroupQueryContainsOrCondition() || requiresOrderedIndex())){
            resultIndicator =  IQueryIndexScanner.RESULT_IGNORE_INDEX; ////query of "OR" by non f-g index results can be non-fifo within the f-g
        }
        if (resultIndicator != null  ){
            if(context.getExplainPlanContext() != null ){
                IndexChoiceNode choiceNode = context.getExplainPlanContext().getFatherNode();
                //check in order to prevent throwing NPE
                if( choiceNode != null ) {
                    IndexInfo indexInfo = ExplainPlanUtil.createIndexInfo(this, index, typeData, -1, false);
                    choiceNode.addOption(indexInfo);
                }
            }
            return resultIndicator;
        }

        // Get entries in space that match the indexed value in the query (a.k.a
        // potential match list):
        IObjectsList entriesByIndex = getEntriesByIndex(context, typeData, index, template.isFifoGroupPoll() /*fifoGroupsScan*/);

        if(context.getExplainPlanContext() != null && entriesByIndex != IQueryIndexScanner.RESULT_IGNORE_INDEX ){
            IndexChoiceNode choiceNode = context.getExplainPlanContext().getFatherNode();
            if(choiceNode ==  null){
                choiceNode = new IndexChoiceNode(inferIndexScannerName());
                context.getExplainPlanContext().getSingleExplainPlan().addScanIndexChoiceNode(typeData.getClassName(), choiceNode);
                context.getExplainPlanContext().setFatherNode(choiceNode);
            }
            int size;
            if (entriesByIndex == null){
                size = 0;
            } else if (entriesByIndex instanceof IStoredList){
                size = ((IStoredList) entriesByIndex).size();
            } else{
                size = -1;
            }
            IndexInfo info = ExplainPlanUtil.createIndexInfo(this, index, typeData, size, true);
            choiceNode.addOption(info);
            choiceNode.setChosen(info);
        }

        if( context.getIndexMetricsContext() != null ) {
            context.getIndexMetricsContext().addChosenIndex(this);
        }

        return entriesByIndex;
    }

    private String inferIndexScannerName() {
        String simpleName = this.getClass().getSimpleName();
        return simpleName.substring(0, simpleName.indexOf("Index"));
    }

    public boolean requiresValueForIndexSearch() {
        return true;
    }

    protected abstract boolean hasIndexValue();


    protected abstract IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan);

    @Override
    public boolean  isUidsScanner() {return false;}

    @Override
    public boolean isExtendsAbstractQueryIndex() {
        return true;
    }
}
