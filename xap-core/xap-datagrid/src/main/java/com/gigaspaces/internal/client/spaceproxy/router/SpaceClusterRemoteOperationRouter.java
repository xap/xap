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
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.routing.clustered.ClusterRemoteOperationRouter;
import com.gigaspaces.internal.remoting.routing.clustered.PostponedAsyncOperationsQueue;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorProxy;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorsCluster;
import com.gigaspaces.internal.server.space.IRemoteSpace;

import java.rmi.RemoteException;

/**
 * @author idan
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceClusterRemoteOperationRouter extends ClusterRemoteOperationRouter implements SpaceProxyRemoteOperationRouter {
    private final SpaceProxyImpl _spaceProxy;
    private final int _failOverPartitionId;

    public SpaceClusterRemoteOperationRouter(RemoteOperationsExecutorsCluster cluster,
                                             PostponedAsyncOperationsQueue postponedAsyncOperationsQueue, SpaceProxyImpl spaceProxy) {
        super(cluster, postponedAsyncOperationsQueue);
        _spaceProxy = spaceProxy;
        // failOverPartitionId != -1 indicates fail over support for transactions (relevant on partitioned topology with backups)
        _failOverPartitionId = cluster.getPartitionId();
    }

    @Override
    protected boolean beforeOperationExecution(RemoteOperationRequest<?> request, RemoteOperationsExecutorProxy proxy)
            throws RemoteException {
        if (!super.beforeOperationExecution(request, proxy))
            return false;

        return _spaceProxy.beforeExecute((SpaceOperationRequest<?>) request, (IRemoteSpace) proxy.getExecutor(), _failOverPartitionId, _cluster.getName(), false);
    }

    @Override
    protected void afterOperationExecution(RemoteOperationRequest<?> request, RemoteOperationsExecutorProxy proxy,
                                           ExecutionStatus status) {
        _spaceProxy.afterExecute((SpaceOperationRequest<?>) request, (IRemoteSpace) proxy.getExecutor(), _cluster.getPartitionId(), status != ExecutionStatus.COMPLETED);
        super.afterOperationExecution(request, proxy, status);
    }

    @Override
    public String getActiveMemberName(int partitionId) {
        RemoteOperationsExecutorProxy activeMember = getAnyActiveMember();
        return activeMember != null ? activeMember.getName() : null;
    }

    public SpaceProxyImpl getSpaceProxy(){
        return _spaceProxy;
    }
}
