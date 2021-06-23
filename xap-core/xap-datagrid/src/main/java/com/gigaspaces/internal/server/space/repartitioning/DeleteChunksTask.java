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

public class DeleteChunksTask extends SystemDistributedTask<SpaceResponseInfo> implements SmartExternalizable {
    private static final long serialVersionUID = -1251693505937070466L;
    private DeleteChunksRequestInfo requestInfo;

    public DeleteChunksTask() {
    }

    public DeleteChunksTask(ClusterTopology newMap, QuiesceToken token) {
        this.requestInfo = new DeleteChunksRequestInfo(newMap, token);
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
            DeleteChunksResponseInfo result = (DeleteChunksResponseInfo) asyncResult.getResult();
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
