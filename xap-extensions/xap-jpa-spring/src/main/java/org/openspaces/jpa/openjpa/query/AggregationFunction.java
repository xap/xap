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

package org.openspaces.jpa.openjpa.query;

import org.apache.openjpa.kernel.exps.ExpressionVisitor;
import org.apache.openjpa.kernel.exps.Path;
import org.apache.openjpa.kernel.exps.Value;
import org.apache.openjpa.meta.ClassMetaData;

/**
 * An aggregation function representation. Supports the following aggregations: Sum, Minimum,
 * Maximum, Count & Average.
 *
 * @author idan
 * @since 8.0
 */
public class AggregationFunction implements Value, ExpressionNode {
    //
    private static final long serialVersionUID = 1L;

    /**
     * Aggregation function type
     */
    public enum AggregationType {
        COUNT, AVERAGE, MINIMUM, MAXIMUM, SUM
    }

    private FieldPathNode _path;
    private AggregationType _aggregationType;

    public AggregationFunction(Value path, AggregationType type) {
        _path = (FieldPathNode) path;
        _aggregationType = type;
    }

    /**
     * Return the expected type for this value, or <code>Object</code> if the type is unknown.
     */
    @SuppressWarnings("rawtypes")
    public Class getType() {
        return long.class;
    }

    /**
     * Set the implicit type of the value, based on how it is used in the filter. This method is
     * only called on values who return <code>Object</code> from {@link #getType}.
     */
    @SuppressWarnings("rawtypes")
    public void setImplicitType(Class type) {
    }

    /**
     * Return true if this value is a variable.
     */
    public boolean isVariable() {
        return false;
    }

    /**
     * Return true if this value is an aggregate.
     */
    public boolean isAggregate() {
        return true;
    }

    /**
     * Return true if this value is an XML Path.
     */
    public boolean isXPath() {
        return false;
    }

    /**
     * Return any associated persistent type.
     */
    public ClassMetaData getMetaData() {
        return null;
    }

    /**
     * Associate a persistent type with this value.
     */
    public void setMetaData(ClassMetaData meta) {
    }

    /**
     * Accept a visit from a tree visitor.
     */
    public void acceptVisit(ExpressionVisitor visitor) {
        visitor.enter(this);
    }

    /**
     * Return select item alias
     */
    public String getAlias() {
        return null;
    }

    /**
     * Set select item alias
     */
    public void setAlias(String alias) {
    }

    /**
     * Return 'this' concrete class if alias is set, otherwise null
     */
    public Value getSelectAs() {
        return null;
    }

    public Path getPath() {
        return _path;
    }

    public String getName() {
        switch (_aggregationType) {
            case COUNT:
                return "COUNT";
            case AVERAGE:
                return "AVG";
            case SUM:
                return "SUM";
            case MINIMUM:
                return "MIN";
            case MAXIMUM:
                return "MAX";
        }
        return "";
    }

    @Override
    public String toString() {
        return getName();
    }

    public AggregationType getAggregationType() {
        return _aggregationType;
    }

    public void appendSql(StringBuilder sql) {
        sql.append(getName());
        sql.append("(");
        String path = getPath().getName();
        if (path.length() == 0)
            path = "*";
        sql.append(path);
        sql.append(")");
    }

    public NodeType getNodeType() {
        return NodeType.AGGREGATION_FUNCTION;
    }

}
