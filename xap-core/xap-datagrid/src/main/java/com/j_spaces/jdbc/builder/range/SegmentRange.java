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
import com.gigaspaces.internal.query.QueryIndexes;
import com.gigaspaces.internal.query.predicate.ISpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.BetweenSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.GreaterEqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.GreaterSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.LessEqualsSpacePredicate;
import com.gigaspaces.internal.query.predicate.comparison.LessSpacePredicate;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.SQLFunctions;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.j_spaces.core.client.TemplateMatchCodes.GE;
import static com.j_spaces.core.client.TemplateMatchCodes.GT;
import static com.j_spaces.core.client.TemplateMatchCodes.LE;
import static com.j_spaces.core.client.TemplateMatchCodes.LT;
import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.AND;
import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.BIND_PARAMETER;
import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.mapCodeToSign;

/**
 * Defines a range of objects. The range is defined by two objects - _min and _max. And represents a
 * range in the following way: (_min,_max)  ==> _min < x < _max (null,_max) ==> x < _max (_min,null)
 * ==> x > _min
 *
 * @author anna
 */
@com.gigaspaces.api.InternalApi
public class SegmentRange extends Range {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private Comparable _min;
    private boolean _includeMin;
    private Comparable _max;
    private boolean _includeMax;

    public SegmentRange() {
        super();
    }

    public SegmentRange(String colName, Comparable<?> value1, boolean includeMin,
                        Comparable<?> value2, boolean includeMax) {
        this(colName, null, value1, includeMin, value2, includeMax);
    }

    public SegmentRange(String colName, FunctionCallDescription functionCallDescription, Comparable<?> value1, boolean includeMin,
                        Comparable<?> value2, boolean includeMax) {
        super(colName, functionCallDescription, createSpacePredicate(value1, includeMin, value2, includeMax));
        this._min = value1;
        this._max = value2;

        //noinspection unchecked
        if (_min != null && _max != null && _min.compareTo(_max) > 0)
            throw new IllegalArgumentException("Invalid range (" + value1 + ","
                    + value2 + ")");

        _includeMin = includeMin;
        _includeMax = includeMax;
    }

    private static ISpacePredicate createSpacePredicate(Comparable<?> value1, boolean includeMin,
                                                        Comparable<?> value2, boolean includeMax) {
        if (value1 != null && value2 != null)
            return new BetweenSpacePredicate(value1, value2, includeMin, includeMax);

        if (value1 != null)
            return includeMin ? new GreaterEqualsSpacePredicate(value1) : new GreaterSpacePredicate(value1);

        if (value2 != null)
            return includeMax ? new LessEqualsSpacePredicate(value2) : new LessSpacePredicate(value2);

        throw new IllegalArgumentException("Both values cannot be null.");
    }

    /**
     * @return the _min
     */
    public Comparable getMin() {
        return _min;
    }

    /**
     * @return the _max
     */
    public Comparable getMax() {
        return _max;
    }

    /**
     * @return the _includeMin
     */
    public boolean isIncludeMin() {
        return _includeMin;
    }

    /**
     * @param includeMin the includeMin to set
     */
    @SuppressWarnings("unused")
    public void setIncludeMin(boolean includeMin) {
        _includeMin = includeMin;
    }

    /**
     * @param includeMax the includeMax to set
     */
    public void setIncludeMax(boolean includeMax) {
        _includeMax = includeMax;
    }

    /**
     * @return the _includeMax
     */
    public boolean isIncludeMax() {
        return _includeMax;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#toExternalEntry(com.j_spaces.core.client.ExternalEntry, int)
     */
    public void toEntryPacket(QueryTemplatePacket e, int index) {
        if (getMin() == null) {
            e.setFieldValue(index, getMax());
            e.setExtendedMatchCode(index, _includeMax ? LE : LT);
        } else if (getMax() == null) {
            e.setFieldValue(index, getMin());
            e.setExtendedMatchCode(index, _includeMin ? GE : GT);
        } else {
            e.setFieldValue(index, getMin());
            e.setExtendedMatchCode(index, _includeMin ? GE : GT);
            e.setRangeValue(index, getMax());
            e.setRangeValueInclusion(index, _includeMax);
        }
    }

    /**
     * @return true if this range contains given value
     */
    @SuppressWarnings("JavaDoc")
    public boolean contains(Comparable value) {
        // Value is larger than _min or equals min in case of inclusion
        if (_min != null) {
            //noinspection unchecked
            int compareTo = _min.compareTo(value);

            if (compareTo > 0 || (compareTo == 0 && !_includeMin))
                return false;
        }

        // Value is smaller  than _max
        if (_max != null) {
            //noinspection unchecked
            int compareTo = _max.compareTo(value);

            if (compareTo < 0 || (compareTo == 0 && !_includeMax))
                return false;
        }

        return true;
    }

    /**
     * Calculates the intersection between this range and the given range
     */
    public Range intersection(SegmentRange range) {
        if (!hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // both ranges do not have function name
            return handleSegmentWithSegment(range);
        } else if (!twoBuiltInFunctions(this, range)) { // can't optimize not build in functions
            return new CompositeRange(this, range);
        } else if (hasFunctionOnlyOnOneSide(this, range)) {
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // both ranges has functions
            if (hasSameFunction(range)) { // both ranges has same functions
                return handleSegmentWithSegment(range);
            } else { // both ranges has functions but different functions
                return new CompositeRange(this, range);
            }
        }
        // none of the options fits - return composite
        return new CompositeRange(this, range);
    }

    private Range handleSegmentWithSegment(SegmentRange range) {
        Comparable min;
        Comparable max;
        boolean includeMin;
        boolean includeMax;

        // Get the largest _min value
        if (getMin() == null) {
            min = range.getMin();
            includeMin = range.isIncludeMin();
        } else if (range.getMin() == null) {
            min = getMin();
            includeMin = isIncludeMin();
        } else {
            //noinspection unchecked
            if (getMin().compareTo(range.getMin()) == 0) {
                min = getMin();
                includeMin = isIncludeMin() && range.isIncludeMin();
            } else //noinspection unchecked
                if (getMin().compareTo(range.getMin()) > 0) {
                    min = getMin();
                    includeMin = isIncludeMin();
                } else {
                    min = range.getMin();
                    includeMin = range.isIncludeMin();
                }

        }

        // Get the smallest _max value
        if (getMax() == null) {
            max = range.getMax();
            includeMax = range.isIncludeMax();
        } else if (range.getMax() == null) {
            max = getMax();
            includeMax = isIncludeMax();
        } else {
            //noinspection unchecked
            if (getMax().compareTo(range.getMax()) == 0) {
                max = getMax();
                includeMax = isIncludeMax() && range.isIncludeMax();
            } else //noinspection unchecked
                if (getMax().compareTo(range.getMax()) < 0) {
                    max = getMax();
                    includeMax = isIncludeMax();
                } else {
                    max = range.getMax();
                    includeMax = range.isIncludeMax();
                }

        }

        // Check for empty intersection
        if (min == null && max == null)
            return EMPTY_RANGE;

        // Check for extreme cases
        if (min != null && max != null) {
            // If both edges are the same - check that both are inclusive
            if (min.equals(max)) {
                if (includeMin && includeMax)
                    return new EqualValueRange(getPath(), getFunctionCallDescription(), min);
                else
                    return EMPTY_RANGE;
            }

            // If max is smaller than min - empty range
            //noinspection unchecked
            if (min.compareTo(max) > 0) {
                return EMPTY_RANGE;
            }
        }

        return new SegmentRange(getPath(), getFunctionCallDescription(), min, includeMin, max, includeMax);
    }


    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.QueryTemplatePacket.AbstractRange#intersection(com.j_spaces.jdbc.builder.QueryTemplatePacket.AbstractRange)
     */
    public Range intersection(Range range) {
        return range.intersection(this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.EqualValueRange)
     */
    public Range intersection(EqualValueRange range) {
        if (!hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // both ranges do no have function name
            if (this.contains((Comparable) range.getValue())) {
                return range;
            } else {
                return EMPTY_RANGE;
            }
        } else if (!twoBuiltInFunctions(this, range)) { // can't optimize not build in functions
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // in values has function, equalRange not
            Object applyOrNull = SQLFunctions.apply(getFunction(), getFunctionCallDescription().setColumnValue(range.getValue()));

            if (applyOrNull != null) {
                if (this.contains(((Comparable) applyOrNull))) {
                    return range;
                } else {
                    return EMPTY_RANGE;
                }
            } else {
                return new CompositeRange(this, range);
            }
        } else if (!hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // in values NOT have function, but equalRange has
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // both ranges has functions
            if (hasSameFunction(range)) { // both ranges has same functions
                if (this.contains((Comparable) range.getValue())) {
                    return range;
                } else {
                    return EMPTY_RANGE;
                }
            } else { // both ranges has functions but not same functions
                return new CompositeRange(this, range);
            }
        }
        // none of the options fits - return composite
        return new CompositeRange(this, range);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotEqualValueRange)
     */
    public Range intersection(NotEqualValueRange range) {
        if (!hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // both ranges do not have function name
            return checkEdges(range);
        } else if (!twoBuiltInFunctions(this, range)) { // can't optimize not build in functions
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // in values has function, NotEqualRange not
            return new CompositeRange(this, range);
        } else if (!hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // in values NOT have function, but NotEqualRange has
            Object applyOnMin = SQLFunctions.apply(range.getFunction(), range.getFunctionCallDescription().setColumnValue(getMin()));
            Object applyOnMax = SQLFunctions.apply(range.getFunction(), range.getFunctionCallDescription().setColumnValue(getMax()));
            Object notEqualValue = range.getValue();

            if (applyOnMin != null && applyOnMax != null) {
                boolean isFuncOnMinNotEqual = applyOnMin.equals(notEqualValue); // func on min
                boolean isFuncOnMaxNotEqual = applyOnMax.equals(notEqualValue); // func on max

                if (!isFuncOnMinNotEqual && !isFuncOnMaxNotEqual) {
                    return new CompositeRange(this, range);
                } else {
                    // includes are in NOT because we search for not equal
                    return new SegmentRange(this.getPath(), getFunctionCallDescription(), this.getMin(), !isFuncOnMinNotEqual, this.getMax(), !isFuncOnMaxNotEqual);
                }
            } else {
                return new CompositeRange(this, range);
            }
        } else if (hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // both ranges has functions
            if (hasSameFunction(range)) { // both ranges has same functions
                return checkEdges(range);
            } else { // both ranges has functions but not same functions
                return new CompositeRange(this, range);
            }
        }
        // none of the options fits - return composite
        return new CompositeRange(this, range);
    }

    private Range checkEdges(NotEqualValueRange range) {
        // Check if value equals min or max
        if (getMin() != null && getMin().equals(range.getValue())) {
            return new SegmentRange(getPath(), getFunctionCallDescription(), getMin(), false, getMax(), isIncludeMax());
        } else if (getMax() != null && getMax().equals(range.getValue())) {
            return new SegmentRange(getPath(), getFunctionCallDescription(), getMin(), isIncludeMin(), getMax(), false);
        }

        // This range is composite - can't be expressed as a single range
        if (contains((Comparable) range.getValue())) {
            return new CompositeRange(this, range);
        }

        // no overlap - no change in the segment
        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.IsNullRange)
     */
    public Range intersection(IsNullRange range) {

        return EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    public Range intersection(NotNullRange range) {

        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.RegexRange)
     */
    public Range intersection(RegexRange range) {

        return new CompositeRange(this, range);
    }


    public Range intersection(NotRegexRange range) {
        return new CompositeRange(this, range);
    }

    public Range intersection(InRange range) {
        if (!hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // both ranges do no have function name
            Set<Object> matchObjects = new HashSet<Object>();
            for (Object inRangeValue : range.getInValues()) {
                if (this.contains((Comparable) inRangeValue)) {
                    matchObjects.add(inRangeValue);
                }
            }
            return chooseRange(matchObjects);
        } else if (!twoBuiltInFunctions(this, range)) { // can't optimize not build in functions
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // in values has function, equalRange not
            Set<Object> matchObjects = new HashSet<Object>();
            Set inRangeValues = range.getInValues();
            for (Object inRangeValue : inRangeValues) {
                Object apply = SQLFunctions.apply(getFunction(), getFunctionCallDescription().setColumnValue(inRangeValue));
                if (apply != null && this.contains((Comparable) apply)) {
                    matchObjects.add(inRangeValue);
                }
            }
            return chooseRange(matchObjects);
        } else if (!hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // in values NOT have function, but equalRange has
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // both ranges has functions
            return new CompositeRange(this, range);
        }
        // none of the options fits - return composite
        return new CompositeRange(this, range);
    }

    @Override
    public Range intersection(RelationRange range) {
        return new CompositeRange(this, range);
    }

    @Override
    public boolean suitableAsCompoundIndexSegment() {
        return false;  //currently we don't support compounds
    }

    @Override
    public boolean isSegmentRange() {
        return true;
    }

    @Override
    public boolean isRelevantForAllIndexValuesOptimization() {
        return true;
    }


    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        _min = IOUtils.readObject(in);
        _includeMin = in.readBoolean();
        _max = IOUtils.readObject(in);
        _includeMax = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        IOUtils.writeObject(out, _min);
        out.writeBoolean(_includeMin);
        IOUtils.writeObject(out, _max);
        out.writeBoolean(_includeMax);
    }

    @Override
    public IQueryIndexScanner getIndexScanner() {
        return QueryIndexes.newRangeIndexScanner(getPath(), _min, _includeMin, _max, _includeMax);
    }

    /* (non-Javadoc)
     * @see com.gigaspaces.internal.query_poc.server.ICustomQuery#getSQLString()
     */
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        List<Comparable> parameters = new LinkedList<Comparable>();
        StringBuilder b = new StringBuilder();
        if (_min != null) {
            b.append(getPath()).append(mapCodeToSign(_includeMin ? GE : GT)).append(BIND_PARAMETER);
            parameters.add(_min);
        }

        if (_max != null) {
            if (b.length() > 0)
                b.append(AND);

            b.append(getPath()).append(mapCodeToSign(_includeMax ? LE : LT)).append(BIND_PARAMETER);
            parameters.add(_max);
        }

        return new SQLQuery(typeDesc.getTypeName(),
                b.toString(),
                parameters.toArray());
    }

    @Override
    public String toString() {
        return "SegmentRange{" +
                "_min=" + _min +
                ", _includeMin=" + _includeMin +
                ", _max=" + _max +
                ", _includeMax=" + _includeMax +
                '}';
    }
}