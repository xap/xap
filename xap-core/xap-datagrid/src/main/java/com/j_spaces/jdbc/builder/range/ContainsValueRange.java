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
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.query.predicate.comparison.ContainsPredicate;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.QueryTemplateBuilder;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Represents a value range used by ContainsNode.
 *
 * @author idan
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ContainsValueRange extends SingleValueRange {
    // serialVersionUID should never be changed. 
    private static final long serialVersionUID = 1L;
    protected short _templateMatchCode;

    public ContainsValueRange() {
        super();
    }

    public ContainsValueRange(String colName, FunctionCallDescription functionCallDescription, Object value, short templateMatchCode) {
        this(colName, functionCallDescription, value, templateMatchCode, new ContainsPredicate(value, functionCallDescription, colName, templateMatchCode));
    }

    public ContainsValueRange(String colName, FunctionCallDescription functionCallDescription, Object value, short templateMatchCode, ContainsPredicate predicate) {
        super(colName, functionCallDescription, value, predicate);
        this._templateMatchCode = templateMatchCode;
    }

    public ContainsValueRange(String colName, FunctionCallDescription functionCallDescription, Object value, short templateMatchCode, String relation, String typeName) {
        this(colName, functionCallDescription, value, templateMatchCode, new ContainsPredicate(value, functionCallDescription, colName, templateMatchCode, relation,typeName));
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#toExternalEntry(com.j_spaces.core.client.ExternalEntry, int)
     */
    @Override
    public void toEntryPacket(QueryTemplatePacket e, int index) {

    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.QueryTemplatePacket.AbstractRange#intersection(com.j_spaces.jdbc.builder.QueryTemplatePacket.AbstractRange)
     */
    @Override
    public Range intersection(Range range) {
        return new ContainsCompositeRange(this, range);
    }

    @Override
    public Range intersection(EqualValueRange range) {
        throw new IllegalStateException("Range intersection paths are different: '" + this.getPath() + "' != '" + range.getPath() + "'");
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.SegmentRange)
     */
    @Override
    public Range intersection(SegmentRange range) {
        throw new IllegalStateException("Range intersection paths are different: '" + this.getPath() + "' != '" + range.getPath() + "'");
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotEqualValueRange)
     */
    @Override
    public Range intersection(NotEqualValueRange range) {
        throw new IllegalStateException("Range intersection paths are different: '" + this.getPath() + "' != '" + range.getPath() + "'");
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.IsNullRange)
     */
    @Override
    public Range intersection(IsNullRange range) {
        throw new IllegalStateException("Range intersection paths are different: '" + this.getPath() + "' != '" + range.getPath() + "'");
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    @Override
    public Range intersection(NotNullRange range) {
        throw new IllegalStateException("Range intersection paths are different: '" + this.getPath() + "' != '" + range.getPath() + "'");
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.RegexRange)
     */
    @Override
    public Range intersection(RegexRange range) {
        throw new IllegalStateException("Range intersection paths are different: '" + this.getPath() + "' != '" + range.getPath() + "'");
    }

    @Override
    public Range intersection(NotRegexRange range) {
        throw new IllegalStateException("Range intersection paths are different: '" + this.getPath() + "' != '" + range.getPath() + "'");
    }

    @Override
    public Range intersection(InRange range) {
        throw new IllegalStateException("Range intersection paths are different: '" + this.getPath() + "' != '" + range.getPath() + "'");
    }

    @Override
    public Range intersection(RelationRange range) {
        throw new IllegalStateException("Range intersection paths are different: '" + this.getPath() + "' != '" + range.getPath() + "'");
    }

    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        // Empty query since EDS can't handle this operation and therefore should return all entries.
        return new SQLQuery(typeDesc.getTypeName(), "");
    }

    @Override
    public boolean isComplex() {
        return true;
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath) {
        getPredicate().setCacheManagerForExecution(cacheManager);
        return getPredicate().execute(entry);
    }

    @Override
    public IQueryIndexScanner getIndexScanner() {
        return QueryTemplateBuilder.toRange(getPath(), getFunctionCallDescription(), getValue(), _templateMatchCode).getIndexScanner();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _templateMatchCode = in.readShort();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeShort(_templateMatchCode);
    }

    public short getTemplateMatchCode() {
        return _templateMatchCode;
    }
}