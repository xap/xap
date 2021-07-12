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
package com.gigaspaces.internal.extension;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.async.AsyncResultFilter;
import com.gigaspaces.async.AsyncResultFilterEvent;
import com.gigaspaces.internal.client.spaceproxy.executors.SystemDistributedTask;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.internal.server.space.quiesce.WaitForDrainCompoundResponse;
import com.gigaspaces.internal.server.space.quiesce.WaitForDrainPartitionResponse;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.serialization.SmartExternalizable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

public class WaitForDataDrainTask extends SystemDistributedTask<SpaceResponseInfo> implements SmartExternalizable, AsyncResultFilter<SpaceResponseInfo> {

    private WaitForDataDrainRequest request;
    private transient WaitForDrainCompoundResponse response;


    public WaitForDataDrainTask() {
    }

    public WaitForDataDrainTask(long timeout, long minTimeToWait, boolean backupOnly) {
        this.request = new WaitForDataDrainRequest(timeout, minTimeToWait, backupOnly);
        this.response = new WaitForDrainCompoundResponse();
    }

    @Override
    public SpaceRequestInfo getSpaceRequestInfo() {
        return request;
    }

    public WaitForDataDrainTask setRequest(WaitForDataDrainRequest request) {
        this.request = request;
        return this;
    }

    public WaitForDrainCompoundResponse getResponse() {
        return response;
    }

    @Override
    public SpaceResponseInfo reduce(List<AsyncResult<SpaceResponseInfo>> asyncResults) throws Exception {
        return response;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        IOUtils.writeObject(out, request);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.request = IOUtils.readObject(in);
    }

    @Override
    public Decision onResult(AsyncResultFilterEvent<SpaceResponseInfo> event) {
        if(event.getCurrentResult().getException() != null){
            response.addException(-1, event.getCurrentResult().getException());
            return Decision.CONTINUE;
        }
        final WaitForDrainPartitionResponse partitionResponse = (WaitForDrainPartitionResponse) event.getCurrentResult().getResult();
        if(partitionResponse != null){
            if(partitionResponse.getException() != null){
                response.addException(partitionResponse.getPartitionId(), partitionResponse.getException());
            } else if(partitionResponse.isSuccessful()){
                response.addSuccessfulPartition(partitionResponse.getPartitionId());
            }
        }
        return Decision.CONTINUE;
    }
}
