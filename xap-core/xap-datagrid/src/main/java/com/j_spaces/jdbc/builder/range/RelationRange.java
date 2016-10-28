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
import com.gigaspaces.internal.query.RelationIndexScanner;
import com.gigaspaces.internal.query.predicate.comparison.RelationPredicate;
import com.j_spaces.core.client.SQLQuery;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Barak Bar Orion 8/24/15.
 */
@com.gigaspaces.api.InternalApi
public class RelationRange extends SingleValueRange {

    private static final long serialVersionUID = 1L;
    private static final Logger _logger = Logger.getLogger(RelationRange.class.getName());

    private String typeName;
    private String namespace;
    private String relation;

    public RelationRange() {
    }

    public RelationRange(String typeName, String columnPath, Object subject, String namespace, String relation) {
        super(columnPath, null, subject, new RelationPredicate(namespace, typeName, columnPath, relation, subject));
        this.typeName = typeName;
        this.namespace = namespace;
        this.relation = relation;
    }

    public String getRelation() {
        return relation;
    }

    @Override
    public Range intersection(Range range) {
        return range.intersection(this);
    }

    @Override
    public Range intersection(IsNullRange range) {
        return EMPTY_RANGE;
    }

    @Override
    public Range intersection(NotNullRange range) {
        return this;
    }

    @Override
    public Range intersection(EqualValueRange range) {
        return new CompositeRange(range, this);
    }

    @Override
    public Range intersection(NotEqualValueRange range) {
        return new CompositeRange(this, range);
    }

    @Override
    public Range intersection(RegexRange range) {
        return new CompositeRange(this, range);
    }

    @Override
    public Range intersection(NotRegexRange range) {
        return new CompositeRange(this, range);
    }

    @Override
    public Range intersection(SegmentRange range) {
        return new CompositeRange(range, this);
    }

    @Override
    public Range intersection(InRange range) {
        return new CompositeRange(this, range);
    }

    @Override
    public Range intersection(RelationRange range) {
        if (this.equals(range) && range.getValue().equals(getValue())) {
            return this;
        }
        return new CompositeRange(this, range);
    }

    @Override
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        return null;
    }

    @Override
    public IQueryIndexScanner getIndexScanner() {
        return new RelationIndexScanner(typeName, getPath(), namespace, relation, getValue());
    }

    @Override
    public boolean isComplex() {
        return true;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeString(out, typeName);
        IOUtils.writeString(out, namespace);
        IOUtils.writeString(out, relation);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        typeName = IOUtils.readString(in);
        namespace = IOUtils.readString(in);
        relation = IOUtils.readString(in);
    }

    @Override
    public boolean isIndexed(ITypeDesc typeDesc) {
        boolean result = typeDesc.getQueryExtensions() != null && typeDesc.getQueryExtensions().isIndexed(namespace, getPath());
        if (_logger.isLoggable(Level.FINE))
            _logger.log(Level.FINE, "isIndexed(" + typeName + "." + getPath() + ") - " + result);
        return result;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationRange that = (RelationRange) o;
        return Objects.equals(typeName, that.typeName) &&
                Objects.equals(namespace, that.namespace) &&
                Objects.equals(relation, that.relation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, namespace, relation);
    }
}
