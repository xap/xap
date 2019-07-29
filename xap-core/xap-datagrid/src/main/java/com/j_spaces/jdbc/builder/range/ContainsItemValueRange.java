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

package com.j_spaces.jdbc.builder.range;

import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.IContainsItemsCustomQuery;
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.query.predicate.comparison.ContainsItemPredicate;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;
import com.j_spaces.jdbc.parser.ColumnNode;
import com.j_spaces.jdbc.query.QueryColumnData;
import com.j_spaces.jdbc.query.QueryTableData;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * a range for c collection imposede on single item rendered.
 *
 * @author Yechiel Fefer
 * @since 9.6
 */

@com.gigaspaces.api.InternalApi
public class ContainsItemValueRange extends ContainsValueRange implements
        IContainsItemsCustomQuery {

    private static final long serialVersionUID = 2596910972611179585L;

    private transient IContainsItemsCustomQuery _root;
    private transient Range _actualRangeForIndexing;


    public ContainsItemValueRange() {
        super();
    }

    public ContainsItemValueRange(String relativePath, String fullPath, FunctionCallDescription functionCallDescription, Object value, short templateMatchCode) {
        super(fullPath, functionCallDescription, value, templateMatchCode, new ContainsItemPredicate(value, relativePath, functionCallDescription, templateMatchCode));
    }

    public ContainsItemValueRange(String relativePath, String fullPath, FunctionCallDescription functionCallDescription, Object value, short templateMatchCode, String relation, String typeName) {
        super(fullPath, functionCallDescription, value, templateMatchCode, new ContainsItemPredicate(value, relativePath, functionCallDescription, templateMatchCode, relation,typeName ,fullPath));
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, Object collectionItem) {
        // TODO Auto-generated method stub
        getPredicate().setCacheManagerForExecution(cacheManager);
        return getPredicate().execute(collectionItem);
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath) {
        throw new RuntimeException("top level entry match not supported fullpath=" + getPath() + " actual=" + ((ContainsItemPredicate) getPredicate()).getFullPath());
    }


    @Override
    public boolean isRootHandler() {
        return false;
    }

    @Override
    public boolean isInternalRange() {
        return true;
    }

    public IContainsItemsCustomQuery getRoot() {
        return _root;
    }

    public void setRoot(IContainsItemsCustomQuery root) {
        _root = root;
    }

    Range getActualRangeForIndexing() {
        return _actualRangeForIndexing;
    }

    void initActualRangeForIndexingIfNeeded() {
        if (_actualRangeForIndexing == null)
            _actualRangeForIndexing = QueryTemplateBuilder.toRange(getPath(), getFunctionCallDescription(), getValue(), _templateMatchCode);
    }

    //currently only for segment ranges- other are meaningless
    public boolean supportsIntersection() {
        return (_templateMatchCode == TemplateMatchCodes.GT || _templateMatchCode == TemplateMatchCodes.LT || _templateMatchCode == TemplateMatchCodes.GE || _templateMatchCode == TemplateMatchCodes.LE);

    }

    public boolean intersectIfPossible(ContainsItemValueRange other) {
        if (_templateMatchCode != TemplateMatchCodes.GT && _templateMatchCode != TemplateMatchCodes.LT && _templateMatchCode != TemplateMatchCodes.GE && _templateMatchCode != TemplateMatchCodes.LE)
            return false;
        if (getValue().getClass() != other.getValue().getClass())
            return false;
        initActualRangeForIndexingIfNeeded();
        other.initActualRangeForIndexingIfNeeded();
        Range r = _actualRangeForIndexing.intersection(other.getActualRangeForIndexing());
        if (r.isEmptyRange())
            return false;

        _actualRangeForIndexing = r;
        return true;
    }

    @Override
    public IQueryIndexScanner getIndexScanner() {
        if (_actualRangeForIndexing == null)
            _actualRangeForIndexing = QueryTemplateBuilder.toRange(getPath(), getFunctionCallDescription(), getValue(), _templateMatchCode);

        return _actualRangeForIndexing.getIndexScanner();
    }


    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }


}
