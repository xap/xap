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
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.ClientUIDHandler;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.IObjectsList;
import com.j_spaces.kernel.list.MultiStoredList;
import com.j_spaces.kernel.list.ScanUidsIterator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Set;

/**
 * Scans the entries according to the supplied uids
 *
 * @author Yechiel
 * @since 14.3
 */
@com.gigaspaces.api.InternalApi
public class UidsIndexScanner  extends AbstractQueryIndex {
    private static final long serialVersionUID = 1L;

    private Set<String> _uids;

    public UidsIndexScanner() {
        super();
    }

    public UidsIndexScanner(String indexName, Set<String> indexInValueSet) {
        super(indexName);
        _uids = indexInValueSet;
    }

    @Override
    public IObjectsList getIndexedEntriesByType(Context context, TypeData typeData,
                                                ITemplateHolder template, int latestIndexToConsider) {
        if (template.isFifoGroupPoll()) {
                return IQueryIndexScanner.RESULT_IGNORE_INDEX;
        }

        IndexChoiceNode fatherNode = null;
        IndexChoiceNode choiceNode = null;
        if (context.getExplainPlanContext() != null && context.getExplainPlanContext().getSingleExplainPlan() != null){
            fatherNode = context.getExplainPlanContext().getFatherNode();
            choiceNode = new IndexChoiceNode("UIDS");
            context.getExplainPlanContext().getSingleExplainPlan().addScanIndexChoiceNode(typeData.getClassName(), choiceNode);
            context.getExplainPlanContext().setFatherNode(choiceNode);
        }
        ScanUidsIterator iter = new ScanUidsIterator( typeData.getCacheManager(), _uids);
        if (context.getExplainPlanContext() != null && context.getExplainPlanContext().getSingleExplainPlan() != null){
            if (choiceNode.getOptions().size() !=0){
                choiceNode.setChosen(choiceNode.getOptions().get(0));
                context.getExplainPlanContext().setFatherNode(fatherNode);
            }
        }
        return iter;

    }

    @Override
    protected IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan) {
        throw new UnsupportedOperationException();
    }

    public boolean requiresOrderedIndex() {
        return false;
    }

    @Override
    protected boolean hasIndexValue() {
        return !_uids.isEmpty();
    }

    public Object getIndexValue() {
        return null;
    }

    public Set<Object> get_indexInValueSet() {
        /*return (Set<Object>)_uids;*/
        throw new UnsupportedOperationException();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        _uids = new HashSet<String>();
        int size = in.readInt();
        for (int i = 0; i < size; i++)
        {
            _uids.add(in.readUTF());
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(_uids.size());
        if (_uids.size() > 0)
        {
            for (String s : _uids)
            {
                out.writeUTF(s);
            }
        }
    }

    public boolean supportsTemplateIndex() {
        return false;
    }

    @Override
    public boolean  isUidsScanner() {return true;}
}
