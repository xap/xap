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
import com.gigaspaces.internal.query.AbstractCustomQuery;
import com.gigaspaces.internal.query.IQueryIndexScanner;
import com.gigaspaces.internal.query.predicate.ISpacePredicate;
import com.gigaspaces.internal.query.valuegetter.ISpaceValueGetter;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPathGetter;
import com.gigaspaces.internal.query.valuegetter.SpaceEntryPropertyGetter;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.jdbc.SQLFunctions;
import com.j_spaces.jdbc.builder.QueryTemplatePacket;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.Set;

public abstract class Range extends AbstractCustomQuery {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private transient String _path;
    private FunctionCallDescription functionCallDescription;
    private transient boolean _isNested;

    private ISpaceValueGetter<ServerEntry> _valueGetter;
    private ISpacePredicate _predicate;

    private SqlFunction function;

    /**
     * Default constructor for Externalizable.
     */
    public Range() {
    }

    protected Range(String path, FunctionCallDescription functionCallDescription, ISpacePredicate predicate) {
        this._path = path;
        this._isNested = isNested(path);
        this._valueGetter = _isNested ? new SpaceEntryPathGetter(path) : new SpaceEntryPropertyGetter(path);
        this._predicate = predicate;
        this.setFunctionCallDescription(functionCallDescription);
    }

    public boolean isIndexed(ITypeDesc typeDesc) {
        return functionCallDescription == null && typeDesc.getIndexes().containsKey(_path);
    }

    public ISpacePredicate getPredicate() {
        return _predicate;
    }

    public String getPath() {
        return _path;
    }

    /**
     * Returns true if the query is nested ex: person.address.street = 'Wallstreet'
     */
    public boolean isNestedQuery() {
        return _isNested;
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath) {
        if (skipAlreadyMatchedIndexPath != null && skipAlreadyMatchedIndexPath.equals(getPath()))
            return true;
        Object value = _valueGetter.getValue(entry);
        if (this instanceof CompositeRange) {
            return handleCompositeRange((CompositeRange) this, value, cacheManager);
        }
        return matchRange(this, value, cacheManager);
    }

    private boolean handleCompositeRange(CompositeRange compositeRange, Object value, CacheManager cacheManager) {
        LinkedList<Range> ranges = compositeRange.get_ranges();
        int size = ranges.size();
        boolean[] booleans = new boolean[size];
        for (int i = 0; i < size; i++) {
            Range range = ranges.get(i);
            boolean matchRange = matchRange(range, value, cacheManager);
            booleans[i] = matchRange;
        }
        return checkIfAllTrue(booleans);
    }

    private boolean checkIfAllTrue(boolean[] booleans) {
        for (boolean aBoolean : booleans) {
            if (!aBoolean) {
                return false;
            }
        }
        return true;
    }

    private boolean matchRange(Range range, Object value, CacheManager cacheManager) {

        if (range.getFunctionCallDescription() != null && range.getFunction() == null) { // set function if it not exists
            SqlFunction sqlFunction = SQLFunctions.getFunction(range.getFunctionCallDescription().getName());
            if (sqlFunction != null) {
                range.setFunction(sqlFunction);
            } else {
                throw new RuntimeException("can't find SQL function: " + range.getFunctionCallDescription().getName());
            }
        }
        if (range.getFunction() != null) {
            try {
                value = SQLFunctions.apply(range.getFunction(), range.getFunctionCallDescription().setColumnValue(value));
            } catch (Exception e) {
                return false;
            }
        }
        if (range.getPredicate().requiresCacheManagerForExecution()) {
            range.getPredicate().setCacheManagerForExecution(cacheManager);
        }
        return range.getPredicate().execute(value);
    }

    private static boolean isNested(String path) {
        return path.indexOf('.') != -1;
    }

    /**
     * Empty range indicates an intersection between two non-overlapping ranges
     */
    public static final EmptyRange EMPTY_RANGE = new EmptyRange();

    /**
     * Convert the range to a single external entry expression
     */
    public void toEntryPacket(QueryTemplatePacket e, int index) {
    }

    public boolean isComplex() {
        return functionCallDescription != null;
    }

    public IQueryIndexScanner getIndexScanner() {
        return null;
    }

    public abstract Range intersection(Range range);

    public Range intersection(EmptyRange range) {
        return range;
    }

    public Range intersection(CompositeRange range) {
        return range.intersection(this);
    }

    public boolean suitableAsCompoundIndexSegment() {
        return false;
    }

    public boolean isEqualValueRange() {
        return false;
    }

    public boolean isSegmentRange() {
        return false;
    }

    public boolean isInternalRange() {
        return false;
    }

    public boolean isEmptyRange() {
        return false;
    }

    public abstract Range intersection(IsNullRange range);

    public abstract Range intersection(NotNullRange range);

    public abstract Range intersection(EqualValueRange range);

    public abstract Range intersection(NotEqualValueRange range);

    public abstract Range intersection(RegexRange range);

    public abstract Range intersection(NotRegexRange range);

    public abstract Range intersection(SegmentRange range);

    public abstract Range intersection(InRange range);

    public abstract Range intersection(RelationRange range);

    //optimization used in blob store (currently in count())
    public boolean isRelevantForAllIndexValuesOptimization() {
        return false;
    }

    public FunctionCallDescription getFunctionCallDescription() {
        return functionCallDescription;
    }

    public void setFunctionCallDescription(FunctionCallDescription functionCallDescription) {
        this.functionCallDescription = functionCallDescription;
        if (functionCallDescription != null) {
            String functionName = functionCallDescription.getName();
            if (SQLFunctions.isBuiltIn(functionName)) {
                this.function = SQLFunctions.getFunction(functionName);
            }
        }
    }

    protected boolean hasSameFunction(Range range) {
        return (range.functionCallDescription == null && functionCallDescription == null) ||
                (range.functionCallDescription != null && range.functionCallDescription.equals(functionCallDescription));
    }

    protected boolean hasFunctionCallDescription(Range range) {
        return (range.getFunctionCallDescription() != null && range.getFunctionCallDescription().getName() != null && !range.getFunctionCallDescription().getName().isEmpty());
    }

    protected boolean twoBuiltInFunctions(Range range1, Range range2) {
        String range1FunctionName;
        String range2FunctionName;
        boolean isBuiltinFunctionRange1 = true; // init as true if there is no function --> consider as buildIn function
        boolean isBuiltinFunctionRange2 = true; // init as true if there is no function --> consider as buildIn function

        if (range1.getFunctionCallDescription() != null) {
            range1FunctionName = range1.getFunctionCallDescription().getName();
            isBuiltinFunctionRange1 = SQLFunctions.isBuiltIn(range1FunctionName);
        }
        if (range2.getFunctionCallDescription() != null) {
            range2FunctionName = range2.getFunctionCallDescription().getName();
            isBuiltinFunctionRange2 = SQLFunctions.isBuiltIn(range2FunctionName);
        }
        return isBuiltinFunctionRange1 && isBuiltinFunctionRange2;
    }

    public SqlFunction getFunction() {
        return function;
    }

    public void setFunction(SqlFunction function) {
        this.function = function;
    }

    public boolean isUidsRange() {return false;}

    @Override
    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);

        //noinspection unchecked
        _valueGetter = (ISpaceValueGetter<ServerEntry>) in.readObject();
        _predicate = (ISpacePredicate) in.readObject();
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0)) {
            setFunctionCallDescription((FunctionCallDescription) in.readObject());
        }
        // Restore transient members:
        if (_valueGetter instanceof SpaceEntryPathGetter)
            _path = ((SpaceEntryPathGetter) _valueGetter).getPath();
        else if (_valueGetter instanceof SpaceEntryPropertyGetter)
            _path = ((SpaceEntryPropertyGetter) _valueGetter).getPropertyName();
        else
            _path = "";
        this._isNested = isNested(_path);
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);

        out.writeObject(_valueGetter);
        out.writeObject(_predicate);
        out.writeObject(functionCallDescription);
    }

    protected Range chooseRange(Set<Object> matchObjects) {
        if (matchObjects.size() >= 2) {
            // path is the colName name
            return new InRange(getPath(), null, matchObjects);
        } else if (matchObjects.size() == 1) {
            return new EqualValueRange(getPath(), null, matchObjects.iterator().next());
        } else {
            return EMPTY_RANGE;
        }
    }

    protected boolean hasFunctionOnlyOnOneSide(Range range, Range range1) {
        return hasFunctionCallDescription(range) && !hasFunctionCallDescription(range1) ||
                !hasFunctionCallDescription(range) && hasFunctionCallDescription(range1);
    }

}