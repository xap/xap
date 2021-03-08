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
import com.gigaspaces.internal.query.QueryIndexes;
import com.gigaspaces.internal.query.predicate.comparison.EqualsSpacePredicate;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.SQLFunctions;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.BIND_PARAMETER;
import static com.j_spaces.sadapter.datasource.DefaultSQLQueryBuilder.mapCodeToSign;

@com.gigaspaces.api.InternalApi
public class EqualValueRange extends SingleValueRange {
    // serialVersionUID should never be changed. 
    private static final long serialVersionUID = 1L;

    public EqualValueRange() {
        super();
    }

    /**
     *
     * @param colName
     * @param value
     */
    @SuppressWarnings("JavaDoc")
    public EqualValueRange(String colName, FunctionCallDescription functionCallDescription, Object value) {
        super(colName, functionCallDescription, value, new EqualsSpacePredicate(value, functionCallDescription));
    }

    public EqualValueRange(String colName, Object value) {
        this(colName, null, value);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#toExternalEntry(com.j_spaces.core.client.ExternalEntry, int)
	 */
    @Override
    public void toEntryPacket(QueryTemplatePacket e, int index) {
        // check for UID
        ITypeDesc typeDesc = e.getTypeDescriptor();
        String idPropertyName = typeDesc.getIdPropertyName();
        e.setFieldValue(index, getValue());
        //noinspection deprecation
        e.setExtendedMatchCode(index, TemplateMatchCodes.EQ);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.QueryTemplatePacket.AbstractRange#intersection(com.j_spaces.jdbc.builder.QueryTemplatePacket.AbstractRange)
     */
    @Override
    public Range intersection(Range range) {
        return range.intersection(this);
    }

    @Override
    public Range intersection(EqualValueRange range) {
        if (!hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // both ranges do not have function name
            if (this.getValue().equals(range.getValue())) {
                return this;
            } else {
                return EMPTY_RANGE;
            }
        } else if (!twoBuiltInFunctions(this, range)) { // can't optimize not build in functions
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // left side has function
            Object applyOrNull = SQLFunctions.apply(getFunction(), getFunctionCallDescription().setColumnValue(range.getValue()));
            if (applyOrNull != null && applyOrNull.equals(getValue())) {
                return range;
            } else {
                return new EmptyRange();
            }

        } else if (!hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // right side has function
            FunctionCallDescription functionCallDescription = range.getFunctionCallDescription();
            Object valueToSearch = range.getValue();
            Object applyOrNull = SQLFunctions.apply(range.getFunction(), range.getFunctionCallDescription().setColumnValue(getValue()));
            String colName = range.getPath();
            return equalValueWithOneFunction(range, functionCallDescription, valueToSearch, applyOrNull, colName);
        } else if (hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // both ranges has functions
            if (hasSameFunction(range)) { // both ranges has same functions
                if (this.getValue().equals(range.getValue())) {
                    return this;
                } else {
                    return EMPTY_RANGE;
                }
            } else { // both ranges has functions but different functions
                return new CompositeRange(this, range);
            }
        }
        // none of the options fits - return composite
        return new CompositeRange(this, range);
    }

    private Range equalValueWithOneFunction(EqualValueRange range, FunctionCallDescription functionCallDescription, Object valueToSearch, Object applyOrNull, String colName) {
        if (applyOrNull == null) {
            return new CompositeRange(this, range);
        } else if (!applyOrNull.equals(valueToSearch)) {
            return EMPTY_RANGE;
        } else {
            return new EqualValueRange(colName, functionCallDescription, valueToSearch);
        }
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.SegmentRange)
     */
    @Override
    public Range intersection(SegmentRange range) {
        return range.intersection(this);
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotEqualValueRange)
     */
    @Override
    public Range intersection(NotEqualValueRange range) {

        if (!hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // both ranges do not have function name
            return isEqualValues(range);
        } else if (!twoBuiltInFunctions(this, range)) { // can't optimize not build in functions
            return new CompositeRange(this, range);
        } else if (hasFunctionCallDescription(this) && !hasFunctionCallDescription(range)) { // left side has function
            return new CompositeRange(this, range);
        } else if (!hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // right side has function
            Object notEqualValue = range.getValue();
            Object applyOrNull = SQLFunctions.apply(range.getFunction(), range.getFunctionCallDescription().setColumnValue(getValue()));
            if (applyOrNull != null) {
                if (applyOrNull.equals(notEqualValue)) {
                    return EMPTY_RANGE;
                } else { // result of function and notEqualValue are NOT equal
                    return this;
                }
            } else {
                return new CompositeRange(this, range);
            }
        } else if (hasFunctionCallDescription(this) && hasFunctionCallDescription(range)) { // both ranges has functions
            if (hasSameFunction(range)) { // both ranges has same functions
                return isEqualValues(range);
            } else { // both ranges has functions but different functions
                return new CompositeRange(this, range);
            }
        }
        // none of the options fits - return composite
        return new CompositeRange(this, range);
    }

    private Range isEqualValues(NotEqualValueRange range) {
        if (this.getValue().equals(range.getValue())) {
            return EMPTY_RANGE;
        } else {
            return this;
        }
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.IsNullRange)
     */
    @Override
    public Range intersection(IsNullRange range) {
        return EMPTY_RANGE;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.NotNullRange)
     */
    @Override
    public Range intersection(NotNullRange range) {
        return this;
    }

    /* (non-Javadoc)
     * @see com.j_spaces.jdbc.builder.range.Range#intersection(com.j_spaces.jdbc.builder.range.RegexRange)
     */
    @Override
    public Range intersection(RegexRange range) {
        return new CompositeRange(range, this);
    }

    @Override
    public Range intersection(NotRegexRange range) {
        return new CompositeRange(range, this);
    }


    @Override
    public IQueryIndexScanner getIndexScanner() {
        return QueryIndexes.newIndexScanner(getPath(), getValue());
    }

    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        //noinspection deprecation
        return new SQLQuery(typeDesc.getTypeName(), getPath() + mapCodeToSign(TemplateMatchCodes.EQ) + BIND_PARAMETER, getValue());
    }

    @Override
    public Range intersection(InRange range) {
        return range.intersection(this);
    }

    @Override
    public Range intersection(RelationRange range) {
        return new CompositeRange(range, this);
    }

    @Override
    public boolean suitableAsCompoundIndexSegment() {
        return true;
    }

    @Override
    public boolean isEqualValueRange() {
        return true;
    }

    @Override
    public String toString() {
        return "EqualValueRange{"+getPath()+" = "+getValue()+"}";
    }
}