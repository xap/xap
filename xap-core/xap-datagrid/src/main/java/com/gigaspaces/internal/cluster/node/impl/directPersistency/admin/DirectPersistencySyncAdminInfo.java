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

package com.gigaspaces.internal.cluster.node.impl.directPersistency.admin;

import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

/**
 * The synchronizing direct-persistency handler
 *
 * @author yechielf
 * @since 10.2
 */
@com.gigaspaces.api.InternalApi
public class DirectPersistencySyncAdminInfo implements SmartExternalizable {

    private static final long serialVersionUID = 1L;

    private Map<Long, PlatformLogicalVersion> _versions;

    public DirectPersistencySyncAdminInfo(Map<Long, PlatformLogicalVersion> versions) {
        _versions = new HashMap<Long, PlatformLogicalVersion>(versions);
    }

    public DirectPersistencySyncAdminInfo() {
    }

    public Map<Long, PlatformLogicalVersion> getVersions() {
        return _versions;
    }

    public static String getStorageKey() {
        return "SYNC_ADMIN";
    }


    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        _versions = new HashMap<Long, PlatformLogicalVersion>();
        PlatformLogicalVersion myversion = (PlatformLogicalVersion) in.readObject();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long generation = in.readLong();
            PlatformLogicalVersion l = (PlatformLogicalVersion) in.readObject();
            _versions.put(generation, l);
        }

    }

    public void writeExternal(ObjectOutput out) throws IOException {
        PlatformLogicalVersion curversion = PlatformLogicalVersion.getLogicalVersion();
        out.writeObject(curversion);
        out.writeInt(_versions.size());
        for (Map.Entry<Long, PlatformLogicalVersion> e : _versions.entrySet()) {
            out.writeLong(e.getKey());
            out.writeObject(e.getValue());
        }
    }

}
