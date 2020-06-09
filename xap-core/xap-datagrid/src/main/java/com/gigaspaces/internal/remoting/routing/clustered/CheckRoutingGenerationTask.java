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
