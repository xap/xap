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
import com.gigaspaces.internal.client.spaceproxy.operations.SpacePreciseDistributionGroupingCodes;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.remoting.routing.clustered.RemoteOperationsExecutorsCluster;
import com.gigaspaces.internal.remoting.routing.partitioned.CoordinatorFactory;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterRemoteOperationRouter;

/**
 * @author Niv Ingberg
 * @since 9.0.1
 */
@com.gigaspaces.api.InternalApi
public class SpacePartitionedClusterRemoteOperationRouter extends PartitionedClusterRemoteOperationRouter implements SpaceProxyRemoteOperationRouter {

    private final SpaceProxyImpl spaceProxy;

    public SpacePartitionedClusterRemoteOperationRouter(String name,
                                                        SpaceProxyRemoteOperationRouter[] partitions,
                                                        boolean broadcastDisabled,
                                                        RemoteOperationsExecutorsCluster partitionedCluster, SpaceClusterInfo clusterInfo, SpaceProxyImpl spaceProxy) {
        super(name,
                partitions,
                new CoordinatorFactory(),
                broadcastDisabled,
                SpacePreciseDistributionGroupingCodes.NUMBER_OF_GROUPS,
                partitionedCluster, clusterInfo);
        this.spaceProxy = spaceProxy;
    }

    @Override
    public SpaceProxyRemoteOperationRouter getPartitionRouter(int partitionId) {
        return (SpaceProxyRemoteOperationRouter) super.getPartitionRouter(partitionId);
    }

    @Override
    public String getActiveMemberName(int partitionId) {
        return getPartitionRouter(partitionId).getActiveMemberName(partitionId);
    }

    public SpaceProxyImpl getSpaceProxy() {
        return spaceProxy;
    }
}
