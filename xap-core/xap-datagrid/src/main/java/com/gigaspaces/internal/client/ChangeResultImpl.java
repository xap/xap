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

package com.gigaspaces.internal.client;

import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangedEntryDetails;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class ChangeResultImpl<T>
        implements ChangeResult<T>, SmartExternalizable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("rawtypes")
    public static final ChangeResultImpl<?> SINGLE = new ChangeResultImpl(1);
    public static final ChangeResultImpl<?> EMPTY = new ChangeResultImpl(0);

    private int _numOfChangesEntries;

    public ChangeResultImpl() {
    }

    public ChangeResultImpl(int numOfChangesEntries) {
        _numOfChangesEntries = numOfChangesEntries;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(_numOfChangesEntries);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _numOfChangesEntries = in.readInt();
    }

    @Override
    public Collection<ChangedEntryDetails<T>> getResults() {
        throw new UnsupportedOperationException("In order to get detailed result for change operation, the 'ChangeModifiers.RETURN_DETAILED_RESULTS' should be used");
    }

    @Override
    public int getNumberOfChangedEntries() {
        return _numOfChangesEntries;
    }

}
