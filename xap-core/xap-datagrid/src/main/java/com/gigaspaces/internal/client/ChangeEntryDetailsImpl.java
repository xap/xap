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

import com.gigaspaces.client.ChangeOperationResult;
import com.gigaspaces.client.ChangedEntryDetails;
import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.utils.Textualizable;
import com.gigaspaces.internal.utils.Textualizer;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author eitany
 * @since 9.1
 */
@com.gigaspaces.api.InternalApi
public class ChangeEntryDetailsImpl<T>
        implements ChangedEntryDetails<T>, SmartExternalizable, Textualizable {
    private static final long serialVersionUID = 1L;

    private String _typeName;
    private Object _id;
    private int _version;
    private List<Serializable> _results;

    private transient Collection<SpaceEntryMutator> _mutators;


    public ChangeEntryDetailsImpl() {
    }

    public ChangeEntryDetailsImpl(String typeName, Object id, int version, List<Serializable> results) {
        _typeName = typeName;
        _id = id;
        _version = version;
        _results = results;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeRepetitiveString(out, _typeName);
        IOUtils.writeObject(out, _id);
        out.writeInt(_version);
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_7_0))
            IOUtils.writeObject(out, _results);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _typeName = IOUtils.readRepetitiveString(in);
        _id = IOUtils.readObject(in);
        _version = in.readInt();
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v9_7_0))
            _results = IOUtils.readObject(in);
    }

    @Override
    public String getTypeName() {
        return _typeName;
    }

    @Override
    public Object getId() {
        return _id;
    }

    @Override
    public int getVersion() {
        return _version;
    }

    @Override
    public void toText(Textualizer textualizer) {
        textualizer.append("typeName", _typeName);
        textualizer.append("id", _id);
        textualizer.append("version", _version);
    }

    @Override
    public String toString() {
        return Textualizer.toString(this);
    }

    public void setMutators(Collection<SpaceEntryMutator> mutators) {
        _mutators = mutators;
    }

    @Override
    public List<ChangeOperationResult> getChangeOperationsResults() {
        if (_results == null) {
            List<ChangeOperationResult> changeOperationResults = new ArrayList<ChangeOperationResult>(_mutators.size());
            for (SpaceEntryMutator mutator : _mutators)
                changeOperationResults.add(mutator.getChangeOperationResult(null));
            return changeOperationResults;
        }

        if (_mutators == null || _mutators.size() != _results.size())
            throw new IllegalStateException("Mutators size does not match change operation results size [mutators=" + _mutators + ", results=" + _results + "]");

        List<ChangeOperationResult> changeOperationResults = new ArrayList<ChangeOperationResult>(_results.size());
        Iterator<Serializable> iterator = _results.iterator();
        for (SpaceEntryMutator mutator : _mutators)
            changeOperationResults.add(mutator.getChangeOperationResult(iterator.next()));

        return changeOperationResults;
    }

}
