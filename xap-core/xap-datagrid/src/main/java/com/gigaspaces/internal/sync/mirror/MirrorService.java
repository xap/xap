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

package com.gigaspaces.internal.sync.mirror;

import com.gigaspaces.cluster.replication.async.mirror.MirrorStatistics;
import com.gigaspaces.cluster.replication.async.mirror.MirrorStatisticsImpl;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInTransactionHandler;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNodeBuilder;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfig;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfigBuilder;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.ReliableAsyncAdaptiveProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.router.DirectOnlyReplicationRouter;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.sync.SpaceSynchronizationEndpoint;

import net.jini.id.Uuid;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Niv Ingberg
 * @since 10.0
 */
@com.gigaspaces.api.InternalApi
public class MirrorService {

    private static final Logger _mirrorLogger = LoggerFactory.getLogger(com.gigaspaces.logger.Constants.LOGGER_MIRROR_REPLICATION);

    private final MirrorStatisticsImpl _mirrorStatistics;
    private final ReplicationNode _replicationNode;


    public MirrorService(String name, Uuid uuid,
                         MirrorConfig mirrorConfig,
                         SpaceSynchronizationEndpoint syncEndpoint,
                         Class<?> dataClass,
                         SpaceTypeManager typeManager, MetricRegistrator metricRegister) {
        this._mirrorStatistics = new MirrorStatisticsImpl();
        this._mirrorStatistics.setMetricRegistrator(metricRegister);
        this._replicationNode = createReplicationNode(name, uuid, mirrorConfig, syncEndpoint, dataClass, typeManager, _mirrorStatistics, metricRegister);
    }

    private static ReplicationNode createReplicationNode(String name,
                                                         Uuid uuid,
                                                         MirrorConfig mirrorConfig,
                                                         SpaceSynchronizationEndpoint syncEndpoint,
                                                         Class<?> dataClass,
                                                         SpaceTypeManager typeManager,
                                                         MirrorStatisticsImpl mirrorStatistics,
                                                         MetricRegistrator metricRegister) {
        // Create the node builder with the proper building blocks
        ReplicationNodeBuilder nodeBuilder = new ReplicationNodeBuilder();
        nodeBuilder.setReplicationRouterBuilder(new DirectOnlyReplicationRouter.Builder(name, uuid));

        MirrorFixFacade fixFacade = new MirrorFixFacade(syncEndpoint, typeManager);
        ReplicationPacketDataConsumer dataConsumer = new ReplicationPacketDataConsumer(typeManager, fixFacade, null/*The mediator in the mirror is not really used*/);
        nodeBuilder.setReplicationProcessLogBuilder(new ReliableAsyncAdaptiveProcessLogBuilder(dataConsumer, mirrorConfig.getDistributedTransactionProcessingParameters()));

        ReplicationNodeConfig replicationNodeConfig = ReplicationNodeConfigBuilder.getInstance().createMirrorConfig(mirrorConfig, nodeBuilder);

        ReplicationNode replicationNode = new ReplicationNode(replicationNodeConfig, nodeBuilder, name, metricRegister);

        MirrorBulkExecutor bulkExecutor = new MirrorBulkExecutor(syncEndpoint, typeManager, dataClass);
        replicationNode.setInEntryHandler(new MirrorReplicationInEntryHandler(bulkExecutor, mirrorStatistics));

        MirrorReplicationMetadataEventHandler metadataHandler = new MirrorReplicationMetadataEventHandler(bulkExecutor, mirrorStatistics);
        replicationNode.setInDataTypeCreatedHandler(metadataHandler);
        replicationNode.setInDataTypeIndexAddedHandler(metadataHandler);

        replicationNode.setInBatchConsumptionHandler(new MirrorReplicationInBatchConsumptionHandler(bulkExecutor, mirrorStatistics));

        MirrorConfig.BulkOperationGrouping transactionGrouping = mirrorConfig.getOperationGrouping();
        IReplicationInTransactionHandler txnHandler;
        switch (transactionGrouping) {
            case GROUP_BY_REPLICATION_BULK:
                txnHandler = new MirrorReplicationInAggregatedTransactionHandler(bulkExecutor, mirrorStatistics);
                break;
            case GROUP_BY_SPACE_TRANSACTION:
                txnHandler = new MirrorReplicationInTransactionHandler(bulkExecutor, mirrorStatistics);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported transaction grouping " + transactionGrouping);
        }
        replicationNode.setInTransactionHandler(txnHandler);

        if (_mirrorLogger.isDebugEnabled())
            _mirrorLogger.debug("No source cluster provided for mirror, mirror will attach to the first encountered cluster");

        replicationNode.getAdmin().setNodeStateListener(new MirrorNodeStateListener(name, mirrorConfig));
        replicationNode.getAdmin().getRouterAdmin().enableIncomingCommunication();

        return replicationNode;
    }

    public ReplicationNode getReplicationNode() {
        return _replicationNode;
    }

    public MirrorStatistics getMirrorStatistics() {
        return _mirrorStatistics;
    }
}
