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
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterExecutionType;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherOperationFutureListener;
import com.gigaspaces.internal.remoting.routing.partitioned.ScatterGatherRemoteOperationRequest;
import com.gigaspaces.internal.server.space.operations.SpaceOperationsCodes;
import com.gigaspaces.internal.utils.Textualizer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.UUID;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class CloseIteratorSpaceOperationRequest extends SpaceOperationRequest<CloseIteratorSpaceOperationResult> {
    private static final long serialVersionUID = 1L;

    private UUID _uuid;
    private transient CloseIteratorSpaceOperationResult _finalResult;

    /**
     * Required for Externalizable
     */
    public CloseIteratorSpaceOperationRequest() {
    }

    public CloseIteratorSpaceOperationRequest(UUID uuid) {
        _uuid = uuid;
    }

    @Override
    public int getOperationCode() {
        return SpaceOperationsCodes.CLOSE_SERVER_ITERATOR;
    }

    @Override
    public CloseIteratorSpaceOperationResult createRemoteOperationResult() {
        return new CloseIteratorSpaceOperationResult();
    }

    @Override
    public PartitionedClusterExecutionType getPartitionedClusterExecutionType() {
        return PartitionedClusterExecutionType.BROADCAST_CONCURRENT;
    }

    @Override
    public Object getPartitionedClusterRoutingValue(PartitionedClusterRemoteOperationRouter router) {
        return null;
    }

    @Override
    public String getLRMIMethodTrackingId() {
        return "closeSpaceIterator";
    }

    public UUID getUuid() {
        return _uuid;
    }

    @Override
    public boolean processPartitionResult(CloseIteratorSpaceOperationResult remoteOperationResult, List<CloseIteratorSpaceOperationResult> previousResults, int numOfPartitions) {
        if (remoteOperationResult.getExecutionException() != null) {
            _finalResult = remoteOperationResult;
            return false;
        }
        if (_finalResult == null)
            _finalResult = new CloseIteratorSpaceOperationResult();
        return true;
    }

    public CloseIteratorSpaceOperationResult getFinalResult() {
        return _finalResult == null ? getRemoteOperationResult() : _finalResult;
    }

    @Override
    public void toText(Textualizer textualizer) {
        super.toText(textualizer);
        textualizer.append("uuid", _uuid);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeUUID(out, _uuid);
    }


    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        _uuid = IOUtils.readUUID(in);
    }
}
