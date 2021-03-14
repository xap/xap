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

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.operations.ExecuteTaskSpaceOperationRequest;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.j_spaces.core.DropClassException;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceHealthStatus;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;

/**
 * @author kimchy
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImplAdminAction extends AdminProxyAction<SpaceProxyImpl> {
    @Override
    public void dropClass(SpaceProxyImpl spaceProxy, String className)
            throws RemoteException, DropClassException {
        //get admin will check permissions too
        IRemoteJSpaceAdmin admin = (IRemoteJSpaceAdmin) spaceProxy.getAdmin();
        SpaceContext sc = spaceProxy.getSecurityManager().acquireContext(spaceProxy.getRemoteJSpace());

        //call the space server
        admin.dropClass(className, sc);
        //drop internal class definitions
        spaceProxy.directDropClass(className);
    }

    @Override
    public void ping(SpaceProxyImpl spaceProxy) throws RemoteException {
        IRemoteSpace rj = spaceProxy.getRemoteJSpace();
        if (rj == null)
            throw new RemoteException("Ping space failed. [" + spaceProxy.getName() + "] is unavailable.");

        rj.ping();
    }

    @Override
    public SpaceHealthStatus getSpaceHealthStatus(SpaceProxyImpl spaceProxy) throws RemoteException {
        return spaceProxy.getRemoteJSpace().getSpaceHealthStatus();
    }

    @Override
    public AsyncFuture execute(SpaceProxyImpl spaceProxy, SpaceTask task, Object routing, Transaction txn, AsyncFutureListener listener)
            throws RemoteException, TransactionException {
        txn = spaceProxy.beforeSpaceAction(txn);
        ExecuteTaskSpaceOperationRequest request = new ExecuteTaskSpaceOperationRequest(task, txn, routing);
        return spaceProxy.getProxyRouter().executeAsync(request, listener);
    }
}
