package com.gigaspaces.internal.remoting.routing.clustered;

import com.gigaspaces.internal.client.spaceproxy.SpaceProxyImpl;
import com.gigaspaces.internal.client.spaceproxy.router.SpacePartitionedClusterRemoteOperationRouter;
import com.gigaspaces.internal.cluster.ClusterTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckRoutingGenerationTask implements com.gigaspaces.internal.utils.concurrent.CompetitiveTask {

    private static Logger logger = LoggerFactory.getLogger("ROUTING_TASK");
    private final SpaceProxyImpl spaceProxy;
    private final int clientGeneration;
    private boolean isLatestGeneration;
    private ClusterTopology newMap;


    CheckRoutingGenerationTask(SpaceProxyImpl executorProxy, int clientCurrentGeneration) {
        this.spaceProxy = executorProxy;
        this.clientGeneration = clientCurrentGeneration;
    }


    public boolean isLatest() {
        return isLatestGeneration;
    }

    public ClusterTopology getNewMap() {
        return newMap;
    }

    @Override
    public boolean execute(boolean isLastIteration) {
        try {
            RemoteOperationsExecutorProxy executorProxy = ((SpacePartitionedClusterRemoteOperationRouter) this.spaceProxy.getProxyRouter().getOperationRouter()).getAnyAvailableCachedMember();
            if (executorProxy == null) {
                logger.debug("couldn't find any cached member");
                return false;
            }
            ClusterTopology chunksMap = executorProxy.getExecutor().checkChunkMapGeneration(clientGeneration);
            if (chunksMap == null) {
                logger.debug("generation is ok");
                this.isLatestGeneration = true;
                return false;
            } else {
                logger.debug("generation changed");
                this.newMap = chunksMap;
                return true;
            }
        } catch (Exception e){
            logger.error("exception thrown in CheckRoutingGenerationTask", e);
            return false;
        }
    }
}
