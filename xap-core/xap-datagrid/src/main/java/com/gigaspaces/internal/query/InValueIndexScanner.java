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
import com.gigaspaces.internal.query.explainplan.IndexChoiceNode;
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.ClientUIDHandler;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.IObjectsList;
import com.j_spaces.kernel.list.MultiStoredList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Scans the index with the defined index name only for entries that match one of the values in the
 * index value set.
 *
 * @author anna
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class InValueIndexScanner extends AbstractQueryIndex {
    private static final long serialVersionUID = 1L;

    private Set<Object> _indexInValueSet;
    private transient ConvertedObjectWrapper _convertedValueWrapper;

    public InValueIndexScanner() {
        super();
    }

    public InValueIndexScanner(String indexName, Set<Object> indexInValueSet) {
        super(indexName);
        _indexInValueSet = indexInValueSet;
    }

    @Override
    public IObjectsList getIndexedEntriesByType(Context context, TypeData typeData,
                                                ITemplateHolder template, int latestIndexToConsider) {
        if (template.isFifoGroupPoll()) {
            TypeDataIndex index = typeData.getIndex(getIndexName());
            if (index == null || !index.isFifoGroupsMainIndex())
                return IQueryIndexScanner.RESULT_IGNORE_INDEX; ////query of "OR" by non f-g index results can be non-fifo within the f-g
        }

        IndexChoiceNode fatherNode = null;
        IndexChoiceNode choiceNode = null;
        if (context.getExplainPlanContext() != null && context.getExplainPlanContext().getSingleExplainPlan() != null){
            fatherNode = context.getExplainPlanContext().getFatherNode();
            choiceNode = new IndexChoiceNode("IN");
            context.getExplainPlanContext().getSingleExplainPlan().addScanIndexChoiceNode(typeData.getClassName(), choiceNode);
            context.getExplainPlanContext().setFatherNode(choiceNode);
        }
        IObjectsList indexedEntriesByType = super.getIndexedEntriesByType(context, typeData, template, latestIndexToConsider);
        if (context.getExplainPlanContext() != null && context.getExplainPlanContext().getSingleExplainPlan() != null){
            if (choiceNode.getOptions().size() !=0){
                choiceNode.setChosen(choiceNode.getOptions().get(0));
                context.getExplainPlanContext().setFatherNode(fatherNode);
            }
        }
        return indexedEntriesByType;

    }


    @Override
    protected IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan) {
        MultiStoredList<IEntryCacheInfo> inMatchResult = new MultiStoredList<IEntryCacheInfo>();
        for (Object indexValue : _indexInValueSet) {
            if (!typeData.disableIdIndexForEntries(index) || indexValue == null) {
                _convertedValueWrapper = ConvertedObjectWrapper.create(indexValue,
                        index.getValueType());
                // If conversion could not be performed, return null
                if (_convertedValueWrapper == null)
                    return null;
            }
            IStoredList<IEntryCacheInfo> matchingEntries = null;
            if (typeData.disableIdIndexForEntries(index) && indexValue != null)
                matchingEntries = typeData.getCacheManager().getPEntryByUid(typeData.generateUid(indexValue));
            else
                matchingEntries = index.getIndexEntries(_convertedValueWrapper.getValue());

            if (matchingEntries == null)
                continue; // no matching values
            inMatchResult.add(matchingEntries);

            if (fifoGroupsScan)
                context.setFifoGroupIndexUsedInFifoGroupScan(matchingEntries, index);

        }

        return inMatchResult;
    }

    public boolean requiresOrderedIndex() {
        return false;
    }

    @Override
    protected boolean hasIndexValue() {
        return !_indexInValueSet.isEmpty();
    }

    public Object getIndexValue() {
        return null;
    }

    public Set<Object> get_indexInValueSet() {
        return _indexInValueSet;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

        _indexInValueSet = IOUtils.readObject(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, _indexInValueSet);
    }

    public boolean supportsTemplateIndex() {
        return false;
    }
}
