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

package com.gigaspaces.internal.server.space.operations;

import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntrySpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntrySpaceOperationResult;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.lrmi.nio.IResponseContext;
import com.gigaspaces.lrmi.nio.ResponseContext;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.client.Modifiers;

/**
 * Executed a single read or take operation on the space
 *
 * @author anna
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class ReadTakeEntrySpaceOperation extends AbstractSpaceOperation<ReadTakeEntrySpaceOperationResult, ReadTakeEntrySpaceOperationRequest> {
    @Override
    public void execute(ReadTakeEntrySpaceOperationRequest request, ReadTakeEntrySpaceOperationResult result, SpaceImpl space, boolean oneway)
            throws Exception {
        IResponseContext respContext = ResponseContext.getResponseContext();
        if (respContext != null)
            respContext.setInvokedFromNewRouter(true);
        long startTime = System.currentTimeMillis();
        AnswerHolder answerHolder = space.readNew(request.getTemplatePacket(),
                request.getTransaction(),
                request.getTimeout(),
                request.isIfExists(),
                request.isTake(),
                null /*IJSpaceProxyListener*/,
                request.getSpaceContext(),
                request.isReturnOnlyUid(),
                request.getModifiers());


        long endTime = System.currentTimeMillis();
        space.getOperationLogger().info( ">> space.readNew took " + ( endTime - startTime ) + " msec., startTime=" + startTime + ", endTime=" + endTime);

        if (answerHolder != null && answerHolder.getAnswerPacket() != null) {
            if (answerHolder.getExplainPlan() != null) {
                result.setExplainPlan(answerHolder.getExplainPlan());
            }
            result.setEntryPacket(answerHolder.getAnswerPacket().m_EntryPacket);
            result.setSyncReplicationLevel(answerHolder.getSyncRelplicationLevel());
            if (Modifiers.contains(request.getModifiers(), Modifiers.LOG_SCANNED_ENTRIES_COUNT))
                result.setNumOfEntriesMatched(answerHolder.getNumOfEntriesMatched());
        }
    }

    @Override
    public String getLogName(ReadTakeEntrySpaceOperationRequest request,
                             ReadTakeEntrySpaceOperationResult result) {
        return request.isTake() ? "take" : "read";
    }
}
