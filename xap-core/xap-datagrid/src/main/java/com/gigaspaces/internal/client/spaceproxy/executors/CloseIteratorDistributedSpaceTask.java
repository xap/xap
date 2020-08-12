package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.client.CloseIteratorSpaceResponseInfo;
import com.gigaspaces.internal.space.requests.CloseIteratorSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;

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
public class CloseIteratorDistributedSpaceTask extends SystemDistributedTask<CloseIteratorSpaceResponseInfo> {
    private static final long serialVersionUID = 1L;

    private CloseIteratorSpaceRequestInfo _closeIteratorSpaceRequestInfo;

    public CloseIteratorDistributedSpaceTask() {
    }

    public CloseIteratorDistributedSpaceTask(UUID uuid) {
        this._closeIteratorSpaceRequestInfo = new CloseIteratorSpaceRequestInfo(uuid);
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _closeIteratorSpaceRequestInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(_closeIteratorSpaceRequestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this._closeIteratorSpaceRequestInfo = (CloseIteratorSpaceRequestInfo) in.readObject();
    }

    @Override
    public CloseIteratorSpaceResponseInfo reduce(List<AsyncResult<CloseIteratorSpaceResponseInfo>> asyncResults) throws Exception {
        return null;
    }
}
