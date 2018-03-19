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
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.ClientUIDHandler;
import com.j_spaces.kernel.list.IObjectsList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Scans the index with the defined index name only for entries that match the exact index value
 *
 * @author anna
 * @since 7.1
 */
@com.gigaspaces.api.InternalApi
public class ExactValueIndexScanner extends AbstractQueryIndex {
    private static final long serialVersionUID = -2127730673864716510L;

    private Object _indexValue;
    private transient ConvertedObjectWrapper _convertedValueWrapper;

    public ExactValueIndexScanner() {
        super();
    }

    public ExactValueIndexScanner(String indexName, Object indexValue) {
        super(indexName);
        _indexValue = indexValue;
    }

    @Override
    protected IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan) {
        if (!typeData.disableIdIndexForEntries(index) || _indexValue == null) {
            // If converted value wrapper is not initialized, try to convert:
            if (_convertedValueWrapper == null)
                _convertedValueWrapper = ConvertedObjectWrapper.create(_indexValue, index.getValueType());
            // If conversion could not be performed, return null
            if (_convertedValueWrapper == null)
                return null;
        }
        IObjectsList res = null;
        if (typeData.disableIdIndexForEntries(index) && _indexValue != null)
            res = typeData.getCacheManager().getPEntryByUid(typeData.generateUid(_indexValue));
        else
            res = index.getIndexEntries(_convertedValueWrapper.getValue());
        if (fifoGroupsScan && res != null)
            context.setFifoGroupIndexUsedInFifoGroupScan(res, index);
        if (context.getExplainPlanContext() != null && context.getExplainPlanContext().getSingleExplainPlan() != null){
            IndexChoiceNode fatherNode = context.getExplainPlanContext().getFatherNode();
            if (fatherNode == null) {
                IndexChoiceNode choiceNode = new IndexChoiceNode("AND");
                context.getExplainPlanContext().getSingleExplainPlan().addScanIndexChoiceNode(typeData.getClassName(), choiceNode);
                context.getExplainPlanContext().setFatherNode(choiceNode);
            }

        }
        return res;
    }

    public boolean requiresOrderedIndex() {
        return false;
    }

    @Override
    protected boolean hasIndexValue() {
        return _indexValue != null;
    }

    public Object getIndexValue() {
        return _indexValue;
    }

    protected void setIndexValue(Object indexValue) {
        _indexValue = indexValue;
    }

    /**
     * equality matching preserves fifo order
     */
    @Override
    public boolean supportsFifoOrder() {
        return true;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

        _indexValue = in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(_indexValue);
    }

    public boolean supportsTemplateIndex() {
        return true;
    }
}
