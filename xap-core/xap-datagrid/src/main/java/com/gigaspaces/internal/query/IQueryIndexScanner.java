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

import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.gigaspaces.metrics.LongCounter;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.kernel.list.IObjectsList;

import java.io.Externalizable;

/**
 * Represents an index that can be provided in a {@link ICustomQuery} and optimize the query run
 * time.
 *
 * @author anna
 * @since 7.1
 */
public interface IQueryIndexScanner extends SmartExternalizable

{
    public static final class ResultIndicator implements IObjectsList {
        public boolean isIterator() {
            return false;
        }
    }

    public static final ResultIndicator RESULT_NO_MATCH = new ResultIndicator();
    public static final ResultIndicator RESULT_IGNORE_INDEX = new ResultIndicator();


    /**
     * The name of the index - index is stored in the space by this name
     */
    String getIndexName();

    /**
     * The value of the index - objects are indexed and stored according to specific index value.
     * This value will be used to search the index structure for entries that match this value .
     */
    Object getIndexValue();

    /**
     * Returns true if this index scanner requires an ordered index
     */
    boolean requiresOrderedIndex();

    /**
     * returns true if this index scanner preserves the fifo order
     */
    boolean supportsFifoOrder();

    /**
     * Returns true if this index scanner supports template indexing
     */
    boolean supportsTemplateIndex();

    boolean isUidsScanner();

    IObjectsList getIndexedEntriesByType(Context context, TypeData typeData, ITemplateHolder template, int latestIndexToConsider);

    boolean isExtendsAbstractQueryIndex();

    default LongCounter getIndexUsageCounter(TypeData typeData) {
        String indexName = getIndexName();
        if (indexName != null) {
            TypeDataIndex<?> typeDataIndex = typeData.getIndex(indexName);
            if (typeDataIndex != null) {
                return typeDataIndex.getUsageCounter();
            }
        }
        return null;
    }
}
