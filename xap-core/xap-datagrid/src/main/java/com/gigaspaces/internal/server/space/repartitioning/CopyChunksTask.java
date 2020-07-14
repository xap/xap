package com.gigaspaces.internal.server.space.repartitioning;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.client.spaceproxy.executors.SystemDistributedTask;
import com.gigaspaces.internal.cluster.PartitionToChunksMap;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;

public class CopyChunksTask extends SystemDistributedTask<CopyChunksResponseInfo> implements Externalizable {

    private CopyChunksRequestInfo requestInfo;

    public CopyChunksTask() {
    }

    public CopyChunksTask(PartitionToChunksMap newMap, String spaceName, Map<Integer, String> instanceIds, QuiesceToken token) {
        this.requestInfo = new CopyChunksRequestInfo(newMap, spaceName, instanceIds, token);
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return this.requestInfo;
    }

    @Override
    public CopyChunksResponseInfo reduce(List<AsyncResult<CopyChunksResponseInfo>> asyncResults) throws Exception {
        CompoundCopyChunksResponse compoundResponse = new CompoundCopyChunksResponse();
        for (AsyncResult<CopyChunksResponseInfo> asyncResult : asyncResults) {
            if (asyncResult.getException() != null) {
                throw asyncResult.getException();
            }
            if (asyncResult.getResult().getException() != null) {
                throw asyncResult.getResult().getException();
            }

            compoundResponse.addResponse(asyncResult.getResult());

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
