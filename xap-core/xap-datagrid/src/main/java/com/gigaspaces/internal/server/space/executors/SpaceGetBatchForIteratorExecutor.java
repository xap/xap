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
package com.gigaspaces.internal.server.space.executors;

import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.iterator.ServerIteratorRequestInfo;
import com.gigaspaces.internal.space.requests.GetBatchForIteratorSpaceRequestInfo;
import com.gigaspaces.internal.space.requests.SpaceRequestInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.security.authorities.SpaceAuthority;
import com.j_spaces.core.GetBatchForIteratorException;
import com.j_spaces.core.ServerIteratorAnswerHolder;

import java.util.UUID;

public class SpaceGetBatchForIteratorExecutor extends SpaceActionExecutor{
    @Override
    public SpaceResponseInfo execute(SpaceImpl space, SpaceRequestInfo spaceRequestInfo) {
        GetBatchForIteratorSpaceRequestInfo requestInfo = (GetBatchForIteratorSpaceRequestInfo) spaceRequestInfo;
        ServerIteratorAnswerHolder serverIteratorAnswerHolder = null;
        GetBatchForIteratorException exception = null;
        try {
            serverIteratorAnswerHolder = space.getNextBatchFromServerIterator(requestInfo.getTemplatePacket(), requestInfo.getSpaceContext(),requestInfo.getModifiers(), new ServerIteratorRequestInfo(requestInfo.getIteratorId(), requestInfo.getBatchSize(), requestInfo.getBatchNumber(), requestInfo.getMaxInactiveDuration()));
        } catch (Exception e) {
            exception = e instanceof GetBatchForIteratorException ? (GetBatchForIteratorException) e : new GetBatchForIteratorException(e) ;
        }
        int partitionId = space.getPartitionId();
        UUID uuid = requestInfo.getIteratorId();
        if(exception != null) {
            exception.setPartitionId(partitionId).setBatchNumber(requestInfo.getBatchNumber());
            return new SpaceIteratorBatchResult(exception, uuid);
        }
        return new SpaceIteratorBatchResult(serverIteratorAnswerHolder.getEntryPackets(), partitionId, serverIteratorAnswerHolder.getBatchNumber(), uuid);
    }

    @Override
    public SpaceAuthority.SpacePrivilege getPrivilege() {
        return SpaceAuthority.SpacePrivilege.READ;
    }
}
