package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.space.requests.*;
import com.gigaspaces.internal.space.responses.BroadcastTableSpaceResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import static com.gigaspaces.internal.space.requests.BroadcastTableSpaceRequestInfo.Action.*;

/**
 * @author alon shoham
 * @since 15.8.0
 */
@com.gigaspaces.api.InternalApi
public class BroadcastTableSpaceTask extends SystemDistributedTask<BroadcastTableSpaceResponseInfo> {
    private static final long serialVersionUID = 1L;

    private BroadcastTableSpaceRequestInfo _broadcastTableSpaceRequestInfo;

    public BroadcastTableSpaceTask() {
    }

    public BroadcastTableSpaceTask(BroadcastTableSpaceRequestInfo _broadcastTableSpaceRequestInfo) {
        this._broadcastTableSpaceRequestInfo = _broadcastTableSpaceRequestInfo;
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return _broadcastTableSpaceRequestInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeByte(_broadcastTableSpaceRequestInfo.getAction().value);
        _broadcastTableSpaceRequestInfo.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this._broadcastTableSpaceRequestInfo = generateRequest(in.readByte());
        this._broadcastTableSpaceRequestInfo.readExternal(in);
    }

    @Override
    public BroadcastTableSpaceResponseInfo reduce(List<AsyncResult<BroadcastTableSpaceResponseInfo>> asyncResults) throws Exception {
        return _broadcastTableSpaceRequestInfo.reduce(asyncResults);
    }

    private BroadcastTableSpaceRequestInfo generateRequest(byte action){
        if(action == PUSH_ENTRY.value)
            return new PushBroadcastTableEntrySpaceRequestInfo();
        if(action == PUSH_ENTRIES.value)
            return new PushBroadcastTableEntriesSpaceRequestInfo();
        if(action == PULL_ENTRIES.value)
            return new PullBroadcastTableEntriesSpaceRequestInfo();
        throw new IllegalArgumentException("Unknown broadcast table action " + action);
    }
}
