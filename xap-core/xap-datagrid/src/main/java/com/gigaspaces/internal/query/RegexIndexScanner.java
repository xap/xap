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
import com.gigaspaces.internal.server.storage.ITemplateHolder;
import com.j_spaces.core.cache.IEntryCacheInfo;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cache.TypeDataIndex;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.kernel.IStoredList;
import com.j_spaces.kernel.list.IObjectsList;
import com.j_spaces.kernel.list.MultiStoredList;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Scans the index with the defined index name only for entries that match the given regular
 * expression.
 *
 * @author anna
 * @since 10.0.0
 */
@com.gigaspaces.api.InternalApi
public class RegexIndexScanner extends AbstractQueryIndex {
    private static final long serialVersionUID = 1L;

    private String _regex;

    public RegexIndexScanner() {
        super();
    }

    public RegexIndexScanner(String indexName, String regex) {
        super(indexName);
        _regex = regex;
    }

    @Override
    public IObjectsList getIndexedEntriesByType(Context context, TypeData typeData,
                                                ITemplateHolder template, int latestIndexToConsider) {
        if (template.isFifoGroupPoll()) {
            TypeDataIndex index = typeData.getIndex(getIndexName());
            if (index == null || !index.isFifoGroupsMainIndex())
                return IQueryIndexScanner.RESULT_IGNORE_INDEX; ////query of "OR" by non f-g index results can be non-fifo within the f-g
        }
        return super.getIndexedEntriesByType(context, typeData, template, latestIndexToConsider);
    }


    @Override
    protected IObjectsList getEntriesByIndex(Context context, TypeData typeData, TypeDataIndex<Object> index, boolean fifoGroupsScan) {

        // the optimization is relevant only for non unique indexes
        if (index.isUniqueIndex()) {
            return IQueryIndexScanner.RESULT_IGNORE_INDEX;
        }

        // optimization is relevant only when there is a high ratio of non-unique values
        // can happen when super class is abstract
        if (index.getNonUniqueEntriesStore().size() == 0 || typeData.getEntries().size() / index.getNonUniqueEntriesStore().size() < 2)
            return IQueryIndexScanner.RESULT_IGNORE_INDEX;

        MultiStoredList<IEntryCacheInfo> matchResult = new MultiStoredList<>();

        Pattern pattern = index.getCacheManager().getEngine().getTemplateScanner().getRegexCache().getPattern(_regex);

        final Set<Object> indexes = index.getNonUniqueEntriesStore().keySet();
        for (Object indexValue : indexes) {

            IStoredList<IEntryCacheInfo> matchingEntries = null;
            Matcher m = pattern.matcher((String) indexValue);
            if (!m.matches())
                continue;
            matchingEntries = index.getIndexEntries(indexValue);
            if (matchingEntries == null) {
                continue; // no matching values
            }
            matchResult.add(matchingEntries);

            if (fifoGroupsScan)
                context.setFifoGroupIndexUsedInFifoGroupScan(matchingEntries, index);

        }

        return matchResult;
    }

    public boolean requiresOrderedIndex() {
        return false;
    }

    @Override
    protected boolean hasIndexValue() {
        return true;
    }

    public Object getIndexValue() {
        return null;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);

        _regex = IOUtils.readString(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        IOUtils.writeString(out, _regex);
    }

    public boolean supportsTemplateIndex() {
        return false;
    }

    public String getRegex() {
        return _regex;
    }
}
