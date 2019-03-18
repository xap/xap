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
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.query.InValueIndexScanner;
import com.gigaspaces.internal.query.UidsIndexScanner;
import com.gigaspaces.internal.query.predicate.comparison.InSpacePredicate;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.context.Context;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.SQLFunctions;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.BIND_PARAMETER;

/**
 * Represents a set of uids to match against the candidates
 *
 * @author Yechiel
 * @since 14.3
 */
@com.gigaspaces.api.InternalApi
public class UidsRange
        extends Range {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;
    private transient Set<String>  _uids;

    /**
     * @return the value
     */
    public Set getInValues() {
        final InSpacePredicate inSpacePredicate = (InSpacePredicate) getPredicate();
        return inSpacePredicate.getInValues();
    }

    public UidsRange() {
        super();
    }

    public UidsRange(String colName,Set inValues) {
        this(colName,null, inValues);
    }

    public UidsRange(String colName,FunctionCallDescription functionCallDescription, Set inValues) {
        super(colName, functionCallDescription, new InSpacePredicate(inValues));
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.Range)
     */
    public Range intersection(Range range) {
        if (range.isUidsRange())
            return intersectionUids((UidsRange)range);
        return range.intersection(this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.SegmentRange)
     */
    public Range intersection(SegmentRange range) {
        return  new CompositeRange(range, this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.EqualValueRange)
     */
    public Range intersection(EqualValueRange range) {
        return new CompositeRange(this, range);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotEqualValueRange)
     */
    public Range intersection(NotEqualValueRange range) {
        return new CompositeRange(this, range);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.IsNullRange)
     */
    public Range intersection(IsNullRange range) {
            return Range.EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    public Range intersection(NotNullRange range) {
        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.CompositeRange)
     */
    public Range intersection(InRange range) {
        return new CompositeRange(this, range);
    }

    private Range intersectionUids(UidsRange range) {
        Set<Object> matchObjects = new HashSet<Object>();
        Set myInValues = this.getInValues();
        Set otherInValues = range.getInValues();
        for (Object myInValue : myInValues) {
            if (otherInValues.contains(myInValue)) {
                matchObjects.add(myInValue);
            }
        }
        return chooseUidsRange(matchObjects);
    }
    private Range chooseUidsRange(Set<Object> matchObjects) {
        if (matchObjects.size() >= 1) {
            // path is the colName name
            return new UidsRange(getPath(), null, matchObjects);
        }else
            return EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#toExternalEntry(com.j_spaces.core.client.ExternalEntry, int)
     */
    public void toEntryPacket(QueryTemplatePacket e, int index) {

    }

    public Range intersection(NotRegexRange range) {
        return new CompositeRange(range, this);
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#getSQLString()
     */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        StringBuilder sqlQuerybuilder = new StringBuilder(getPath());
        sqlQuerybuilder.append(" in (");

        for (Iterator iterator = getInValues().iterator(); iterator.hasNext(); ) {
            iterator.next();
            sqlQuerybuilder.append(BIND_PARAMETER);
            if (iterator.hasNext())
                sqlQuerybuilder.append(",");

        }
        sqlQuerybuilder.append(")");
        SQLQuery query = new SQLQuery(typeDesc.getTypeName(), sqlQuerybuilder.toString());

        int index = 0;
        for (Object inValue : getInValues()) {
            query.setParameter(++index, inValue);

        }
        return query;
    }


    @Override
    public Range intersection(RegexRange range) {
        return new CompositeRange(range, this);
    }

    @Override
    public Range intersection(RelationRange range) {
        return new CompositeRange(range, this);
    }

    @Override
    public boolean isComplex() {
        return true;
    }

    @Override
    public IQueryIndexScanner getIndexScanner() {
        //noinspection unchecked
        return new UidsIndexScanner(getPath(), getInValues());
    }

    @Override
    public boolean isIndexed(ITypeDesc typeDesc) {
        return true;
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath) {
        Context context = cacheManager.viewCacheContext();
        if (context == null)
            throw new RuntimeException("UidsRange and no cache context!!!!!!!!!!");
        if (_uids ==null)
            _uids = getInValues();

        String uid = context.getOnMatchUid();
        return _uids.contains(uid);
    }

    @Override
    public boolean isUidsRange() {return true;}

    @Override
    public boolean isRelevantForAllIndexValuesOptimization() {return true;}

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

}
