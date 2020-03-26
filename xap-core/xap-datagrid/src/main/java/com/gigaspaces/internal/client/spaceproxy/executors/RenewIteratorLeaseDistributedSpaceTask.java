package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.client.RenewIteratorLeaseSpaceResponseInfo;
import com.gigaspaces.internal.space.requests.RenewIteratorLeaseSpaceRequestInfo;
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
public class RenewIteratorLeaseDistributedSpaceTask extends SystemDistributedTask<RenewIteratorLeaseSpaceResponseInfo> {
    private static final long serialVersionUID = 1L;

    private RenewIteratorLeaseSpaceRequestInfo _renewIteratorLeaseSpaceRequestInfo;

    public RenewIteratorLeaseDistributedSpaceTask() {
    }

    public RenewIteratorLeaseDistributedSpaceTask(UUID uuid) {
        this._renewIteratorLeaseSpaceRequestInfo = new RenewIteratorLeaseSpaceRequestInfo(uuid);
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _renewIteratorLeaseSpaceRequestInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(_renewIteratorLeaseSpaceRequestInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this._renewIteratorLeaseSpaceRequestInfo = (RenewIteratorLeaseSpaceRequestInfo) in.readObject();
    }

    @Override
    public RenewIteratorLeaseSpaceResponseInfo reduce(List<AsyncResult<RenewIteratorLeaseSpaceResponseInfo>> asyncResults) throws Exception {
        return null;
    }
}
