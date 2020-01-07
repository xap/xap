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

package com.gigaspaces.internal.client.spaceproxy.actions;

import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.GetBatchForIteratorProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.operations.GetBatchForIteratorSpaceOperationRequest;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

/**
 * @author Alon Shoham
 * @since 15.2.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplGetBatchForIteratorAction extends GetBatchForIteratorProxyAction<SpaceProxyImpl> {
    public SpaceIteratorBatchResult getNextBatch(SpaceProxyImpl spaceProxy, GetBatchForIteratorProxyActionInfo actionInfo)
            throws RemoteException, TransactionException, UnusableEntryException {
        SpaceIteratorBatchResult results = null;
        try {
            final GetBatchForIteratorSpaceOperationRequest request = new GetBatchForIteratorSpaceOperationRequest(actionInfo.queryPacket, actionInfo.modifiers, actionInfo.batchSize, actionInfo.getQuery(), actionInfo.uuid, actionInfo.firstTime);
            spaceProxy.getProxyRouter().execute(request);
            results = request.getFinalResult();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return results;
    }
}
