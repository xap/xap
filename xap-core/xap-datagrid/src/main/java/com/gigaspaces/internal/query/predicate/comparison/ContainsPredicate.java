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

package com.gigaspaces.internal.query.predicate.comparison;

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ShortList;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.AbstractTypeIntrospector;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.query.ConvertedObjectWrapper;
import com.gigaspaces.internal.query.predicate.ISpacePredicate;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.client.TemplateMatchCodes;
import com.j_spaces.jdbc.builder.range.FunctionCallDescription;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

/**
 * Represents a contains predicate. This predicate returns true if and only if the predicate's
 * argument is a collection and contains the expected value.
 *
 * @author Anna Pavtulov
 * @author Idan Moyal
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ContainsPredicate extends ScalarSpacePredicate {
    private static final long serialVersionUID = 1L;
    protected String[] _tokens = null;
    protected SpacePropertyInfo[] _propertyInfo = null;
    protected ISpacePredicate _spacePredicate = null;
    protected String _fieldPath = null;
    protected short[] _containsIndexes;
    protected short _templateMatchCode;
    private String relation = null;
    private String typeName;
    private transient CacheManager cacheManager;


    /**
     * Default constructor for Externalizable.
     */
    public ContainsPredicate() {
    }

    public ContainsPredicate(Object expectedValue, FunctionCallDescription functionCallDescription, String fieldPath, short templateMatchCode) {
        super(expectedValue, functionCallDescription);
        this._fieldPath = fieldPath;
        this._templateMatchCode = templateMatchCode;
    }

    public ContainsPredicate(Object expectedValue, FunctionCallDescription functionCallDescription, String fieldPath, short templateMatchCode, String relation, String typeName) {
        this(expectedValue,functionCallDescription,fieldPath,templateMatchCode);
        this.relation = relation;
        this.typeName = typeName;
    }

    @Override
    protected boolean match(Object actual, Object expected) {
        if (actual == null)
            return false;

        // For "=" and "<>" use Collection.contains() method
        if (_templateMatchCode == TemplateMatchCodes.EQ)
            return ((Collection<?>) actual).contains(getPreparedExpectedValue(actual));
        if (_templateMatchCode == TemplateMatchCodes.NE)
            return !((Collection<?>) actual).contains(getPreparedExpectedValue(actual));

        // Otherwise iterate through collection items and execute predicate
        for (Object item : (Collection<?>) actual) {
            if (item == null)
                continue;
            if (executePredicate(item))
                return true;
        }
        // no match
        return false;
    }

    @Override
    public void setCacheManagerForExecution(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    protected String getOperatorName() {
        return "contains";
    }

    /**
     * Since we don't know the generic type of the collection we try to get it from the first
     * matching attempt target.
     */
    @Override
    protected Object getPreparedExpectedValue(Object target) {

        // If converted value wrapper is not initialized, try to convert:
        if (_convertedValueWrapper == null) {
            // If target is null we cannot prepare - return unprepared value without caching result:
            Collection<?> targetCollection = (Collection<?>) target;
            if (targetCollection == null)
                return _expectedValue;

            Object collectionItem = null;
            for (Object item : targetCollection) {
                if (item != null) {
                    collectionItem = item;
                    break;
                }
            }
            if (collectionItem == null)
                return _expectedValue;

            _convertedValueWrapper = ConvertedObjectWrapper.create(_expectedValue, collectionItem.getClass());
        }
        // If conversion could not be performed, return null
        if (_convertedValueWrapper == null)
            return _expectedValue;
        return _convertedValueWrapper.getValue();


    }

    /**
     * Iterate through the the field's path tokens and when reached to a collection, iterate through
     * its items for property matching (collection[*].property = ?) or call match for regular
     * contains (collection[*] = ?). The target object should be of ServerEntry type which is passed
     * from the relevant Range object.
     */
    @Override
    public boolean execute(Object target) {
        // Initialize tokens & property info array.
        // Verify all fields are initialized because several threads can call the
        // initialize method and we want to verify that no thread will pass this check
        // before the predicate is initialized.
        if (_tokens == null || _containsIndexes == null || _propertyInfo == null)
            initialize();

        Object value = ((ServerEntry) target).getPropertyValue(_tokens[0]);
        return (value == null) ? false : performMatching(value, 1, 0);
    }

    protected void initialize() {
        String[] temp = _fieldPath.split("\\.|\\[\\*\\]", -1);
        ArrayList<String> tokens = new ArrayList<String>();
        ShortList containsIndexes = CollectionsFactory.getInstance().createShortList();
        short tokenIndex = 0;
        for (String token : temp) {
            if (token.length() == 0) {
                containsIndexes.add(tokenIndex);
            } else {
                tokens.add(token);
                tokenIndex++;
            }
        }
        this._tokens = tokens.toArray(new String[tokens.size()]);
        this._containsIndexes = containsIndexes.toNativeArray();
        this._propertyInfo = new SpacePropertyInfo[_tokens.length];
    }

    protected boolean performMatching(Object value, int tokenIndex, int currentContainsIndex) {
        // Get next collection
        while (tokenIndex != _containsIndexes[currentContainsIndex]) {
            value = AbstractTypeIntrospector.getNestedValue(value, tokenIndex++, _tokens, _propertyInfo, _fieldPath);
            // NPE check
            if (value == null)
                return false;
        }

        // Collection validation
        if (value instanceof Object[]) {
            value = Arrays.asList((Object[]) value);
        } else if (!(value instanceof Collection<?>))
            throw new IllegalArgumentException(
                    "[*] can only follow a Collection or Object Array. '" + _tokens[tokenIndex - 1] +
                            "' is not a collection or array in '" + _fieldPath + "'");

        // If this is the last collection perform matching
        if (_containsIndexes.length == ++currentContainsIndex)
            return matchValue((Collection<?>) value, tokenIndex);

        // Otherwise, attempt to perform matching for each of the collection items
        for (Object item : (Collection<?>) value) {
            if (item == null)
                continue;
            if (performMatching(item, tokenIndex, currentContainsIndex))
                return true;
        }

        // No match
        return false;
    }

    /**
     * Perform matching on the provided collection or collection items nested properties.
     */
    private boolean matchValue(Collection<?> collection, int tokenIndex) {
        // contains is last - a.b.c[*] = ?
        if (tokenIndex == _tokens.length)
            return match(collection, getExpectedValue());

        // contains is not last - a.b[*].c.d = ?
        return matchCollectionItemNestedProperty(collection, tokenIndex);
    }

    /**
     * Handles the following query: "...collection[*].property = ?"
     */
    private boolean matchCollectionItemNestedProperty(Collection<?> collection, int tokenIndex) {
        for (Object item : collection) {
            for (int i = tokenIndex; i < _tokens.length && item != null; i++)
                item = AbstractTypeIntrospector.getNestedValue(item, i, _tokens, _propertyInfo, _fieldPath);

            //Handle contains [*].property with is null or is not null
            switch (_templateMatchCode) {
                case TemplateMatchCodes.IS_NULL:
                    if (item == null) return true;
                    else continue;
                case TemplateMatchCodes.NOT_NULL:
                    if (item != null) return true;
                    else continue;
            }

            if (item != null && executePredicate(item))
                return true;
        }
        return false;
    }

    /**
     * Performs a single value matching. Initializes the appropriate space predicate for matching if
     * necessary.
     */
    protected boolean executePredicate(Object valueToMatch) {
        if (_spacePredicate == null) {
            Object convertedTemplateValue = ConvertedObjectWrapper.create(_expectedValue, valueToMatch.getClass()).getValue();
            _spacePredicate = createSpacePredicate(convertedTemplateValue);
            if(_spacePredicate.requiresCacheManagerForExecution())
                _spacePredicate.setCacheManagerForExecution(cacheManager);
        }
        return _spacePredicate.execute(valueToMatch);
    }

    /**
     * Creates a space predicate according to the templateMatchCode member. The templateMatchCode
     * member indicates the used contains operator in the executed query.
     */
    private ISpacePredicate createSpacePredicate(Object expectedValue) {
        switch (_templateMatchCode) {
            case TemplateMatchCodes.EQ:
                return new EqualsSpacePredicate(expectedValue);
            case TemplateMatchCodes.NE:
                return new NotEqualsSpacePredicate(expectedValue);
            case TemplateMatchCodes.LT:
                return new LessSpacePredicate(castToComparable(expectedValue));
            case TemplateMatchCodes.LE:
                return new LessEqualsSpacePredicate(castToComparable(expectedValue));
            case TemplateMatchCodes.GT:
                return new GreaterSpacePredicate(castToComparable(expectedValue));
            case TemplateMatchCodes.GE:
                return new GreaterEqualsSpacePredicate(castToComparable(expectedValue));
            case TemplateMatchCodes.REGEX:
                return new RegexSpacePredicate(((String) expectedValue).replaceAll("%", ".*").replaceAll("_", "."));
            case TemplateMatchCodes.NOT_REGEX:
                return new NotRegexSpacePredicate(((String) expectedValue).replaceAll("%", ".*").replaceAll("_", "."));
            case TemplateMatchCodes.IN:
                return new InSpacePredicate((Set) expectedValue);
            case TemplateMatchCodes.RELATION: {
                String[] relations = relation.split(":");
                String namespace = relations[0];
                String op = relations[1];
                String path = getContainsPath();
                return new RelationPredicate(namespace,typeName,path,op,expectedValue);
            }
            default:
                throw new IllegalArgumentException("Unsupported contains operator.");
        }
    }

    /**
     * Cast the provided object to Comparable. Relevant for ">", "<", "<=" & ">=" operators.
     */
    private Comparable<?> castToComparable(Object value) {
        try {
            return (Comparable<?>) value;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Contains operator requires a Comparable class type.", e);
        }
    }

    protected String getFieldPath() {
        return _fieldPath;
    }

    protected String[] getTokens() {
        return _tokens;
    }

    protected short[] getContainsIndexes() {
        return _containsIndexes;
    }

    protected SpacePropertyInfo[] getPropertyInfo() {
        return _propertyInfo;
    }

    protected String getContainsPath(){
        return _fieldPath;
    }

    @Override
    protected void readExternalImpl(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternalImpl(in);
        _fieldPath = IOUtils.readString(in);
        _templateMatchCode = in.readShort();
        if(_templateMatchCode == TemplateMatchCodes.RELATION) {
            relation = IOUtils.readString(in);
            typeName = IOUtils.readString(in);
        }
    }

    @Override
    protected void writeExternalImpl(ObjectOutput out)
            throws IOException {
        super.writeExternalImpl(out);
        IOUtils.writeString(out, _fieldPath);
        out.writeShort(_templateMatchCode);
        if(_templateMatchCode == TemplateMatchCodes.RELATION) {
            IOUtils.writeString(out, relation);
            IOUtils.writeString(out, typeName);
        }
    }

}
