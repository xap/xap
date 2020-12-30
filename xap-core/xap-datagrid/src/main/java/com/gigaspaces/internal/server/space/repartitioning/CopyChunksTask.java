package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.client.spaceproxy.executors.SystemDistributedTask;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;

public class CopyChunksTask extends SystemDistributedTask<SpaceResponseInfo> implements Externalizable {

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
