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

import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.WriteMultipleProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.WriteProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.operations.WriteEntriesSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.WriteEntrySpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.WriteEntrySpaceOperationResult;
import com.gigaspaces.internal.server.space.operations.WriteEntryResult;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.client.Modifiers;

import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplWriteAction extends WriteProxyAction<SpaceProxyImpl> {
    @Override
    public LeaseContext<?> write(SpaceProxyImpl spaceProxy, WriteProxyActionInfo actionInfo)
            throws RemoteException, TransactionException, InterruptedException {
        spaceProxy.beforeSpaceAction(actionInfo);

        WriteEntrySpaceOperationRequest request = new WriteEntrySpaceOperationRequest(actionInfo.entryPacket,
                actionInfo.txn, actionInfo.lease, actionInfo.timeout, actionInfo.modifiers, actionInfo.isUpdate());
        if (Modifiers.contains(actionInfo.modifiers, Modifiers.ONE_WAY)) {
            spaceProxy.getProxyRouter().executeOneway(request);
            return null;
        }
        spaceProxy.getProxyRouter().execute(request);
        WriteEntrySpaceOperationResult result = request.getRemoteOperationResult();
        result.processExecutionException();
        WriteEntryResult writeResult = result.getResult();
        return spaceProxy.getTypeManager().processWriteResult(writeResult, actionInfo.entry, actionInfo.entryPacket);
    }

    @Override
    public LeaseContext<?>[] writeMultiple(SpaceProxyImpl spaceProxy, WriteMultipleProxyActionInfo actionInfo)
            throws RemoteException, TransactionException, InterruptedException {
        spaceProxy.beforeSpaceAction(actionInfo);
        WriteEntriesSpaceOperationRequest request = new WriteEntriesSpaceOperationRequest(spaceProxy.getTypeManager(),
                actionInfo.entries, actionInfo.entryPackets, actionInfo.txn,
                actionInfo.lease, actionInfo.leases, actionInfo.timeout, actionInfo.modifiers, actionInfo.isBroadcast);

        if (Modifiers.contains(actionInfo.modifiers, Modifiers.ONE_WAY)) {
            spaceProxy.getProxyRouter().executeOneway(request);
            return null;
        }
        spaceProxy.getProxyRouter().execute(request);
        return request.getFinalResult();
    }
}
