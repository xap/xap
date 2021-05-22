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

package com.gigaspaces.query.extension.metadata.impl;

import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.query.extension.metadata.QueryExtensionPathInfo;
import com.gigaspaces.query.extension.metadata.TypeQueryExtension;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@com.gigaspaces.api.InternalApi
public class TypeQueryExtensionImpl implements TypeQueryExtension, SmartExternalizable {
    // serialVersionUID should never be changed.
    private static final long serialVersionUID = 1L;

    private final Map<String, List<QueryExtensionPathInfo>> propertiesInfo = new HashMap<String, List<QueryExtensionPathInfo>>();

    /**
     * Required for Externalizable
     */
    public TypeQueryExtensionImpl() {
    }

    public void add(String path, QueryExtensionPathInfo queryExtensionPathInfo) {
        if(!propertiesInfo.containsKey(path)) {
            propertiesInfo.put(path, new ArrayList<QueryExtensionPathInfo>());
        }
        this.propertiesInfo.get(path).add(queryExtensionPathInfo);
    }

    @Override
    public List<QueryExtensionPathInfo> get(String path) {
        return this.propertiesInfo.get(path);
    }

    @Override
    public Set<String> getPaths() {
        return propertiesInfo.keySet();
    }

    @Override
    public boolean isIndexed(String path) {
        List<QueryExtensionPathInfo> pathInfos = propertiesInfo.get(path);
        if(pathInfos == null) {
            return false;
        }
        for (QueryExtensionPathInfo pathInfo: pathInfos) {
            if(pathInfo.isIndexed()) {
                return true;
            }
        }
        return false;
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v12_1_0)) {
            writeExternal_v12_1(out);
        } else {
            writeExternal_v12_0(out);
        }
    }

    private void writeExternal_v12_1(ObjectOutput out) throws IOException {
        out.writeInt(propertiesInfo.size());
        for (Map.Entry<String, List<QueryExtensionPathInfo>> entry : propertiesInfo.entrySet()) {
            IOUtils.writeString(out, entry.getKey());
            IOUtils.writeList(out, entry.getValue());
        }
    }

    private void writeExternal_v12_0(ObjectOutput out) throws IOException {
        out.writeInt(propertiesInfo.size());
        for (Map.Entry<String, List<QueryExtensionPathInfo>> entry : propertiesInfo.entrySet()) {
            IOUtils.writeString(out, entry.getKey());
            IOUtils.writeObject(out, entry.getValue().get(0));
        }
    }


    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v12_1_0)) {
            readExternal_v12_1(in);
        } else {
            readExternal_v12_0(in);
        }
    }

    private void readExternal_v12_1(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = IOUtils.readString(in);
            List<QueryExtensionPathInfo> value = IOUtils.readList(in);
            propertiesInfo.put(key, value);
        }
    }

    private void readExternal_v12_0(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String key = IOUtils.readString(in);
            QueryExtensionPathInfo value = IOUtils.readObject(in);
            propertiesInfo.put(key, Collections.singletonList(value));
        }
    }
}
