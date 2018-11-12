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

package com.gigaspaces.internal.client.spaceproxy.router;

import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.operations.SpaceOperationRequest;
import com.gigaspaces.internal.quiesce.QuiesceTokenProvider;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorProxy;
import com.gigaspaces.internal.remoting.routing.embedded.EmbeddedRemoteOperationRouter;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.lrmi.LRMIRuntime;

import java.rmi.RemoteException;

/**
 * Embedded space remote operations router.
 *
 * @author idan
 * @since 9.0.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceEmbeddedRemoteOperationRouter extends EmbeddedRemoteOperationRouter implements SpaceProxyRemoteOperationRouter {
    private final SpaceProxyImpl _spaceProxy;

    public SpaceEmbeddedRemoteOperationRouter(SpaceProxyImpl spaceProxy, int partitionId, QuiesceTokenProvider quiesceTokenProvider) {
        super(new RemoteOperationsExecutorProxy(spaceProxy.getRemoteMemberName(), spaceProxy.getRemoteJSpace(), quiesceTokenProvider),
                partitionId, spaceProxy.getName(), LRMIRuntime.getRuntime().getThreadPool());
        _spaceProxy = spaceProxy;
    }

    @Override
    protected boolean beforeOperationExecution(RemoteOperationRequest<?> request) {
        if (!super.beforeOperationExecution(request))
            return false;

        try {
            return _spaceProxy.beforeExecute((SpaceOperationRequest<?>) request, (IRemoteSpace) _executor, getPartitionId(), getName(), true);
        } catch (RemoteException e) {
            request.setRemoteOperationExecutionError(e);
            return false;
        }
    }

    @Override
    protected void afterOperationExecution(RemoteOperationRequest<?> request) {
        _spaceProxy.afterExecute((SpaceOperationRequest<?>) request, (IRemoteSpace) _executor, getPartitionId(), false /*isRejected*/);
        super.afterOperationExecution(request);
    }

    @Override
    public String getActiveMemberName(int partitionId) {
        return _spaceProxy.getRemoteMemberName();
    }
}
