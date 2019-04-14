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

package com.gigaspaces.internal.cluster.node.impl.replica.data;

import com.gigaspaces.internal.cluster.node.impl.IIncomingReplicationFacade;
import com.gigaspaces.internal.cluster.node.impl.replica.IExecutableSpaceReplicaData;
import com.gigaspaces.internal.cluster.node.impl.replica.ISpaceReplicaConsumeFacade;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceCopyIntermediateResult;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.j_spaces.core.cluster.IReplicationFilterEntry;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


@com.gigaspaces.api.InternalApi
public class SpaceTypeReplicaData
        implements IExecutableSpaceReplicaData {
    private static final long serialVersionUID = 1L;

    private ITypeDesc _typeDescriptor;

    public SpaceTypeReplicaData() {
    }

    public SpaceTypeReplicaData(ITypeDesc typeDescriptor) {
        super();
        _typeDescriptor = typeDescriptor;
    }

    public ITypeDesc getTypeDescriptor() {
        return _typeDescriptor;
    }

    public String getUid() {
        return _typeDescriptor.getTypeName();
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, _typeDescriptor);
    }

    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _typeDescriptor = IOUtils.readObject(in);
    }

    public void execute(ISpaceReplicaConsumeFacade consumeFacade,
                        SpaceCopyIntermediateResult intermediateResult, IIncomingReplicationFacade incomingReplicationFacade) throws Exception {
        if (!_typeDescriptor.isInactive()) {
            consumeFacade.addTypeDesc(_typeDescriptor);
        }
    }

    public boolean supportsReplicationFilter() {
        return false;
    }

    @Override
    public boolean isEntryReplicaData() {
        return false;
    }

    public IReplicationFilterEntry toFilterEntry(SpaceTypeManager typeManager) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "SpaceTypeReplicaData " + _typeDescriptor;
    }
}
