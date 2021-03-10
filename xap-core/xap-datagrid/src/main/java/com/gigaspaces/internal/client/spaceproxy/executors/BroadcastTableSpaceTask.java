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
package com.gigaspaces.internal.client.spaceproxy.executors;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.internal.space.requests.*;
import com.gigaspaces.internal.space.responses.BroadcastTableSpaceResponseInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.NoSuchElementException;

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
        switch (BroadcastTableSpaceRequestInfo.Action.valueOf(action)){
            case PUSH_ENTRY:
                return new PushBroadcastTableEntrySpaceRequestInfo();
            case PUSH_ENTRIES:
                return new PushBroadcastTableEntriesSpaceRequestInfo();
            case PULL_ENTRIES:
                return new PullBroadcastTableEntriesSpaceRequestInfo();
            case CLEAR_ENTRIES:
                return new ClearBroadcastTableEntriesSpaceRequestInfo();
            default:
                throw new NoSuchElementException("Couldn't match broadcast table action to value " + action);
        }
    }
}
