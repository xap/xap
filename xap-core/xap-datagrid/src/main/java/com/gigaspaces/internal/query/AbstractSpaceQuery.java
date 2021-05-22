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

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.query.predicate.ISpacePredicate;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

/**
 * Common base class for space query implementations.
 *
 * @author Niv Ingberg
 * @since 7.1
 */
public abstract class AbstractSpaceQuery<T> implements ISpaceQuery<T>, ICustomQuery, SmartExternalizable {
    private static final long serialVersionUID = 537740993667475544L;

    private String _entryTypeName;
    private ISpacePredicate _predicate;

    // list of custom defined indexes
    private List<IQueryIndexScanner> _customIndexes = new LinkedList<IQueryIndexScanner>();

    /**
     * Default constructor for Externalizable.
     */
    protected AbstractSpaceQuery() {
    }

    /**
     * Creates a space query using the specified type name and predicate.
     *
     * @param entryTypeName Entry type name.
     * @param predicate     Predicate.
     */
    protected AbstractSpaceQuery(String entryTypeName, ISpacePredicate predicate) {
        this._entryTypeName = entryTypeName;
        this._predicate = predicate;
    }

    /**
     * Creates a space query using the specified class and predicate.
     *
     * @param entryClass Entry class.
     * @param predicate  Predicate.
     */
    protected AbstractSpaceQuery(Class<?> entryClass, ISpacePredicate predicate) {
        this._entryTypeName = entryClass.getName();
        this._predicate = predicate;
    }

    /**
     * Gets the query entry type name.
     */
    public String getEntryTypeName() {
        return _entryTypeName;
    }

    /**
     * Gets the query predicate.
     */
    public ISpacePredicate getPredicate() {
        return _predicate;
    }

    /**
     * @param customIndex
     */
    public void addCustomIndex(IQueryIndexScanner customIndex) {
        _customIndexes.add(customIndex);
    }

    public List<IQueryIndexScanner> getCustomIndexes() {
        return _customIndexes;
    }

    public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
        this._entryTypeName = IOUtils.readString(in);
        this._predicate = (ISpacePredicate) in.readObject();

        boolean hasCustomIndex = in.readBoolean();

        if (hasCustomIndex)
            _customIndexes = (List<IQueryIndexScanner>) in.readObject();
    }

    public void writeExternal(ObjectOutput out)
            throws IOException {
        IOUtils.writeString(out, this._entryTypeName);
        out.writeObject(this._predicate);

        if (_customIndexes != null) {
            out.writeBoolean(true);
            out.writeObject(_customIndexes);
        } else {
            out.writeBoolean(false);
        }
    }
}
