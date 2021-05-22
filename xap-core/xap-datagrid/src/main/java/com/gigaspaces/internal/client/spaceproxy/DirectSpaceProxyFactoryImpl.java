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

package com.gigaspaces.internal.client.spaceproxy;

import com.gigaspaces.client.DirectSpaceProxyFactory;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.client.ProxySettings;
import com.j_spaces.core.client.SpaceSettings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 11.0
 */
@com.gigaspaces.api.InternalApi
public class DirectSpaceProxyFactoryImpl implements DirectSpaceProxyFactory, SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private IRemoteSpace remoteSpace;
    private SpaceSettings spaceSettings;
    private boolean clustered;

    /**
     * Required for Externalizable
     */
    public DirectSpaceProxyFactoryImpl() {
    }

    public DirectSpaceProxyFactoryImpl(IRemoteSpace remoteSpace, SpaceSettings spaceSettings, boolean clustered) {
        this.remoteSpace = remoteSpace;
        this.spaceSettings = spaceSettings;
        this.clustered = clustered;
    }

    public DirectSpaceProxyFactoryImpl createCopy(boolean clustered) {
        return new DirectSpaceProxyFactoryImpl(remoteSpace, spaceSettings, clustered);
    }

    public DirectSpaceProxyFactoryImpl createCopyWithoutClusterPolicyIfNeeded() {
        if (spaceSettings.getSpaceConfig().getClusterPolicy() == null)
            return this;
        SpaceSettings spaceSettings = this.spaceSettings.clone();
        spaceSettings.getSpaceConfig().setClusterPolicy(null);
        return new DirectSpaceProxyFactoryImpl(remoteSpace, spaceSettings, clustered);
    }


    public boolean isClustered() {
        return clustered;
    }

    @Override
    public SpaceProxyImpl createSpaceProxy() {
        return new SpaceProxyImpl(this, new ProxySettings(remoteSpace, spaceSettings));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, remoteSpace);
        IOUtils.writeObject(out, spaceSettings);
        out.writeBoolean(clustered);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        remoteSpace = IOUtils.readObject(in);
        spaceSettings = IOUtils.readObject(in);
        clustered = in.readBoolean();
    }
}
