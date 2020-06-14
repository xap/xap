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
package com.gigaspaces.internal.remoting.routing.clustered;

import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.router.SpacePartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.cluster.PartitionToChunksMap;

public class CheckRoutingGenerationTask implements com.gigaspaces.internal.utils.concurrent.CompetitiveTask {

    private final SpaceProxyImpl spaceProxy;
    private final int clientGeneration;
    private boolean isLatestGeneration;
    private PartitionToChunksMap newMap;


    CheckRoutingGenerationTask(SpaceProxyImpl executorProxy, int clientCurrentGeneration) {
        this.spaceProxy = executorProxy;
        this.clientGeneration = clientCurrentGeneration;
    }


    public boolean isLatest() {
        return isLatestGeneration;
    }

    public PartitionToChunksMap getNewMap() {
        return newMap;
    }

    @Override
    public boolean execute(boolean isLastIteration) {
        RemoteOperationsExecutorProxy executorProxy = ((SpacePartitionedClusterRemoteOperationRouter) this.spaceProxy.getProxyRouter().getOperationRouter()).getAnyAvailableCachedMember();
        if (executorProxy == null) {
            return false;
        }
        PartitionToChunksMap chunksMap = executorProxy.getExecutor().checkChunkMapGeneration(clientGeneration);
        if (chunksMap == null) {
            this.isLatestGeneration = true;
            return false;
        } else
            this.newMap = chunksMap;
        return true;
    }
}
