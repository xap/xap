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

package com.gigaspaces.internal.client.spaceproxy.operations;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.serialization.SmartExternalizable;
import com.j_spaces.core.client.SpaceSettings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Niv Ingberg
 * @since 10.0.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceConnectResult implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private SpaceSettings spaceSettings;
    private boolean clustered;

    /**
     * Required for Externalizable
     */
    public SpaceConnectResult() {
    }

    public SpaceSettings getSpaceSettings() {
        return spaceSettings;
    }

    public void setSpaceSettings(SpaceSettings spaceSettings) {
        this.spaceSettings = spaceSettings;
    }

    public void setClustered(boolean clustered) {
        this.clustered = clustered;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        IOUtils.writeObject(out, spaceSettings);
        out.writeBoolean(clustered);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.spaceSettings = IOUtils.readObject(in);
        this.clustered = in.readBoolean();
    }
}
