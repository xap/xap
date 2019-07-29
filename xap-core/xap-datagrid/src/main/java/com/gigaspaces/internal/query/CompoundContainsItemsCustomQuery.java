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

import com.gigaspaces.internal.collections.CollectionsFactory;
import com.gigaspaces.internal.collections.ShortList;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.AbstractTypeIntrospector;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.metadata.SpacePropertyInfo;
import com.gigaspaces.internal.metadata.TypeDesc;
import com.gigaspaces.serialization.IllegalSerializationVersionException;
import com.gigaspaces.server.ServerEntry;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.client.SQLQuery;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * using a common root query for item in collection-contains.
 *
 * @author Yechiel Fefer
 * @since 9.6
 */

@com.gigaspaces.api.InternalApi
public class CompoundContainsItemsCustomQuery extends AbstractCustomQuery
        implements IContainsItemsCustomQuery {

    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;
    // If serialization changes, increment GigaspacesVersionID and modify read/writeExternal appropiately.
    private static final byte GigaspacesVersionID = 1;
    private List<IContainsItemsCustomQuery> _subQueries;
    String _rootPath;
    boolean _isTop;    //true if its the to root
    protected transient String[] _tokens = null;
    protected transient SpacePropertyInfo[] _propertyInfo = null;
    protected transient short[] _containsIndexes;


    public CompoundContainsItemsCustomQuery(String rootPath, String fullPath, List<IContainsItemsCustomQuery> subQueries) {
        super();
        _rootPath = rootPath;
        _isTop = rootPath.equals(fullPath);
        if (subQueries.isEmpty())
            throw new RuntimeException("no sub-queries for contains root " + fullPath);
        _subQueries = subQueries;
    }

    public CompoundContainsItemsCustomQuery() {
        super();
    }

    public List<IContainsItemsCustomQuery> getSubQueries() {
        return _subQueries;
    }

    @Override
    public SQLQuery toSQLQuery(ITypeDesc typeDesc) {
        // TODO Auto-generated method stub
        // Empty query since EDS can't handle this operation and therefore should return all entries.
        return new SQLQuery(typeDesc.getTypeName(), "");
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, Object collectionItem) {
        // TODO Auto-generated method stub
        if (_isTop)
            throw new IllegalArgumentException("cannot call matches on top root with collection item");
        //traverse the root until we reach the item bottom level collection

        if (_tokens == null || _containsIndexes == null || _propertyInfo == null)
            // Initialize tokens & property info array.
            // Verify all fields are initialized because several threads can call the
            // initialize method and we want to verify that no thread will pass this check
            // before the predicate is initialized.
            initialize();

        return performMatching(cacheManager, collectionItem, 0, 0, entry);
    }

    @Override
    public boolean matches(CacheManager cacheManager, ServerEntry entry, String skipAlreadyMatchedIndexPath) {
        // TODO Auto-generated method stub
        if (!_isTop)
            throw new IllegalArgumentException("cannot call matches on top root with collection item");

        //traverse the root until we reach the item bottom level collection

        if (_tokens == null || _containsIndexes == null || _propertyInfo == null)
            // Initialize tokens & property info array.
            // Verify all fields are initialized because several threads can call the
            // initialize method and we want to verify that no thread will pass this check
            // before the predicate is initialized.
            initialize();

        Object value = entry.getPropertyValue(_tokens[0]);
        return (value == null) ? false : performMatching(cacheManager, value, 1, 0, entry);
    }

    private boolean performMatching(CacheManager cacheManager, Object value, int tokenIndex, int currentContainsIndex, ServerEntry entry) {//
        // Get next collection
        while (tokenIndex != _containsIndexes[currentContainsIndex]) {
            value = AbstractTypeIntrospector.getNestedValue(value, tokenIndex++, _tokens, _propertyInfo, _rootPath);
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
                            "' is not a collection or array in '" + _rootPath + "'");

        // If this is the last collection perform matching
        if (_containsIndexes.length == ++currentContainsIndex)
            return matchValue( cacheManager, (Collection<?>) value, tokenIndex, entry);

        // Otherwise, attempt to perform matching for each of the collection items
        for (Object item : (Collection<?>) value) {
            if (item == null)
                continue;
            if (performMatching(cacheManager, item, tokenIndex, currentContainsIndex, entry))
                return true;
        }

        // No match
        return false;
    }

    /**
     * Perform matching on the provided collection or collection items nested properties.
     */
    private boolean matchValue(CacheManager cacheManager, Collection<?> collection, int tokenIndex, ServerEntry entry) {
        // contains is last - a.b.c[*] = ?
        if (tokenIndex == _tokens.length)
            return matchOnRoot(cacheManager, collection, entry);
        else
            throw new RuntimeException("invalid root path: " + _rootPath + " - should end with a collection");
    }

    private boolean matchOnRoot(CacheManager cacheManager, Collection<?> collection, ServerEntry entry) {
        for (Object item : collection) {
            if (item == null)
                continue;
            //traverse the subqueries/ranges using this item
            boolean res = true;
            for (IContainsItemsCustomQuery q : _subQueries) {
                if (!q.matches(cacheManager, entry, item)) {
                    res = false;
                    break;
                }
            }
            if (res)
                return true;
        }
        return false;

    }


    private void initialize() {
        String[] temp = _rootPath.split("\\.|\\[\\*\\]", -1);
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

    @Override
    public boolean isRootHandler() {
        return true;
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        super.readExternal(in);
        byte version = in.readByte();

        if (version == GigaspacesVersionID)
            readExternalV1(in);
        else {
            switch (version) {
                default:
                    throw new IllegalSerializationVersionException(TypeDesc.class, version);
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out)
            throws IOException {
        super.writeExternal(out);
        out.writeByte(GigaspacesVersionID);
        writeExternalV1(out);
    }

    private void readExternalV1(ObjectInput in)
            throws IOException, ClassNotFoundException {
        if (in.readBoolean())
            _rootPath = IOUtils.readString(in);

        _isTop = in.readBoolean();
        _subQueries = (List<IContainsItemsCustomQuery>) in.readObject();
    }

    private void writeExternalV1(ObjectOutput out)
            throws IOException {
        if (_rootPath != null) {
            out.writeBoolean(true);
            IOUtils.writeString(out, _rootPath);
        } else
            out.writeBoolean(false);
        out.writeBoolean(_isTop);
        out.writeObject(_subQueries);
    }


}
