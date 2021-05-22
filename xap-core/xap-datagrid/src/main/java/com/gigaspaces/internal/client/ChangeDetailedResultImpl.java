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
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class ChangeDetailedResultImpl<T>
        implements ChangeResult<T>, SmartExternalizable {

    private static final long serialVersionUID = 1L;

    @SuppressWarnings("rawtypes")
    public static final ChangeDetailedResultImpl<?> EMPTY = new ChangeDetailedResultImpl();

    private ChangedEntryDetails<T> _singleChangeEntryResult;

    private Collection<ChangedEntryDetails<T>> _multipleBatchResults;

    public ChangeDetailedResultImpl() {
    }

    public ChangeDetailedResultImpl(ChangedEntryDetails<T> singleChangeEntryResult) {
        _singleChangeEntryResult = singleChangeEntryResult;
    }

    public ChangeDetailedResultImpl(Collection<ChangedEntryDetails<T>> multipleBatchResults) {
        _multipleBatchResults = multipleBatchResults;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<ChangedEntryDetails<T>> getResults() {
        if (_multipleBatchResults != null)
            return _multipleBatchResults;
        if (_singleChangeEntryResult == null)
            return Collections.EMPTY_LIST;
        return Collections.singletonList(_singleChangeEntryResult);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(_multipleBatchResults != null);
        if (_multipleBatchResults != null)
            IOUtils.writeObject(out, _multipleBatchResults);
        else
            IOUtils.writeObject(out, _singleChangeEntryResult);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        if (in.readBoolean())
            _multipleBatchResults = IOUtils.readObject(in);
        else
            _singleChangeEntryResult = IOUtils.readObject(in);
    }

    @Override
    public int getNumberOfChangedEntries() {
        return _multipleBatchResults != null ? _multipleBatchResults.size() : (_singleChangeEntryResult == null ? 0 : 1);
    }

}
