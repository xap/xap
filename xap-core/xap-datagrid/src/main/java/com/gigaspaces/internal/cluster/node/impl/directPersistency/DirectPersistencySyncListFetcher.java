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

package com.gigaspaces.internal.cluster.node.impl.directPersistency;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;

/**
 * @author Boris
 * @since 10.2.0
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencySyncListFetcher implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private IRemoteSpace spaceStub;

    public DirectPersistencySyncListFetcher() {
    }

    public DirectPersistencySyncListFetcher(IRemoteSpace spaceStub) {
        this.spaceStub = spaceStub;
    }

    public DirectPersistencySyncListBatch fetchBatch() throws RemoteException {
        return spaceStub.getSynchronizationListBatch();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, spaceStub);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        spaceStub = IOUtils.readObject(in);
    }

}
