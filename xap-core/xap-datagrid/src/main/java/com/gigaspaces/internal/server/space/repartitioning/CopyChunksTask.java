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
package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.client.spaceproxy.executors.SystemDistributedTask;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;

public class CopyChunksTask extends SystemDistributedTask<SpaceResponseInfo> implements SmartExternalizable {
    private static final long serialVersionUID = -6534857288071741108L;
    private CopyChunksRequestInfo requestInfo;

    public CopyChunksTask() {
    }

    public CopyChunksTask(ClusterTopology newMap, String spaceName, Map<Integer, String> instanceIds, QuiesceToken token, ScaleType scaleType) {
        this.requestInfo = new CopyChunksRequestInfo(newMap, spaceName, instanceIds, token, scaleType);
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return this.requestInfo;
    }

    @Override
    public SpaceResponseInfo reduce(List<AsyncResult<SpaceResponseInfo>> asyncResults) throws Exception {
        CompoundChunksResponse compoundResponse = new CompoundChunksResponse();
        for (AsyncResult<SpaceResponseInfo> asyncResult : asyncResults) {
            if (asyncResult.getException() != null) {
                throw asyncResult.getException();
            }
            CopyChunksResponseInfo result = (CopyChunksResponseInfo) asyncResult.getResult();
            if (result.getException() != null) {
                throw result.getException();
            }

            compoundResponse.addResponse(result);

        }
        return compoundResponse;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, requestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.requestInfo = IOUtils.readObject(in);
    }
}
