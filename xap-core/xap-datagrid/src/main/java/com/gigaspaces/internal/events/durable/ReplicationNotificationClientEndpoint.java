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

package com.gigaspaces.internal.events.durable;

import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.events.batching.BatchRemoteEventListener;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.executors.RegisterReplicationNotificationTask;
import com.gigaspaces.internal.client.spaceproxy.executors.UnregisterReplicationNotificationTask;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInBatchConsumptionHandler;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.ReplicationNodeBuilder;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfig;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfigBuilder;
import com.gigaspaces.internal.cluster.node.impl.notification.NotificationReplicationDataConsumeFixFacade;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IDataConsumeFixFacade;
import com.gigaspaces.internal.cluster.node.impl.packets.data.ReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.ReliableAsyncAdaptiveProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile.DistributedTransactionProcessingConfiguration;
import com.gigaspaces.internal.cluster.node.impl.router.DirectOnlyReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationRouterBuilder;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils;
import com.gigaspaces.internal.server.space.SpaceConfigReader;
import com.gigaspaces.internal.server.space.metadata.SpaceTypeManager;
import com.gigaspaces.internal.space.requests.RegisterReplicationNotificationRequestInfo;
import com.gigaspaces.internal.space.requests.UnregisterReplicationNotificationRequestInfo;
import com.gigaspaces.internal.space.responses.RegisterReplicationNotificationResponseInfo;
import com.gigaspaces.internal.space.responses.SpaceResponseInfo;
import com.gigaspaces.internal.sync.mirror.MirrorConfig;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.SharedHandlerProviderCache;
import com.gigaspaces.metrics.DummyMetricRegistrator;
import com.gigaspaces.security.AccessDeniedException;
import com.gigaspaces.start.SystemInfo;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;

import net.jini.id.Uuid;
import net.jini.id.UuidFactory;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationNotificationClientEndpoint {
    private final Logger _logger;

    public static final String LOOKUP_NAME_PREFIX = "replication:NotifyDur:";

    private static final AtomicLong _eventIdGenerator = new AtomicLong(0);

    private final ReplicationNode _notificationReplicationNode;
    private final IDirectSpaceProxy _remoteSpace;
    private final Object _template;
    private final ITemplatePacket _templatePacket;
    private final int _partitionId;
    private final int _numOfPartitions;
    private final NotifyInfo _notifyInfo;
    private final long _eventID;
    private final Uuid _spaceUID;
    private final IAsyncHandlerProvider _asyncProvider;
    private final DurableNotificationLease _lease;
    private final DurableNotificationReplicationNodeStateListener _stateListener;

    private final Object _lock = new Object();

    private boolean _closed;

    public ReplicationNotificationClientEndpoint(
            IDirectSpaceProxy remoteSpace,
            Object template,
            NotifyInfo info,
            EventSessionConfig sessionConfig,
            long lease) throws RemoteException {
        _logger = remoteSpace.getDataEventsManager().getLogger();
        _eventID = _eventIdGenerator.getAndIncrement();
        _remoteSpace = remoteSpace;
        _template = template;
        _notifyInfo = info;
        _notifyInfo.getOrInitTemplateUID();

        //((SpaceProxyImpl)_remoteSpace).checkProxyInit();
        SpaceClusterInfo clusterInfo = _remoteSpace.getSpaceClusterInfo();
        _numOfPartitions = clusterInfo.isPartitioned() ? clusterInfo.getNumberOfPartitions() : 1;
        _templatePacket = prepareTemplatePacket();
        _partitionId = getPartitionId();

        // running validation after partial configuration (which is needed for validation)
        // but before any allocation that may need to be closed
        validateConfiguration(remoteSpace, info);

        _asyncProvider = SharedHandlerProviderCache.getSharedThreadPoolProviderCache().getProvider();

        _lease = new DurableNotificationLease(this, lease, sessionConfig, _asyncProvider);
        _stateListener = new DurableNotificationReplicationNodeStateListener(sessionConfig,
                _lease,
                _asyncProvider,
                _numOfPartitions,
                _partitionId);
        _notificationReplicationNode = createReplicationNode();
        _spaceUID = registerNotificationReplicationNode();

        if (sessionConfig.isAutoRenew() && sessionConfig.getLeaseListener() != null)
            _stateListener.startDisconnectionMonitoring();

        _lease.startLeaseReaperIfNecessary();

    }

    public Logger getLogger() {
        return _logger;
    }

    private void validateConfiguration(IDirectSpaceProxy remoteSpaceProxy, NotifyInfo info) {
        if (info.getFilter() != null)
            throw new UnsupportedOperationException("IDelegatorFilter is not supported using durable notifications. Use space filters instead.");

        if (!remoteSpaceProxy.getSpaceClusterInfo().isReplicated() || remoteSpaceProxy.getSpaceClusterInfo().isActiveActive())
            throw new UnsupportedOperationException("Durable notifications requires a partitioned primary backup topology.");

        if (!_templatePacket.getTypeDescriptor().isReplicable())
            throw new UnsupportedOperationException("Durable notifications on a non replicable template is currently not supported.");
    }

    private ITemplatePacket prepareTemplatePacket() throws RemoteException {
        ObjectType templateObjectType = ObjectType.fromObject(_template);
        ITemplatePacket templatePacket = _remoteSpace.getDirectProxy().getTypeManager().getTemplatePacketFromObject(_template, templateObjectType);
        if (preProcessQuery(_template)) {
            templatePacket = _remoteSpace.getQueryManager().getSQLTemplate((SQLQueryTemplatePacket) templatePacket, null);
        }

        // GS-10990
        templatePacket = templatePacket.clone();
        templatePacket.setSerializeTypeDesc(true);

        return templatePacket;

    }

    private boolean preProcessQuery(Object query) {
        if (query == null)
            return false;

        if (query instanceof SQLQuery<?>) {
            SQLQuery<?> sqlQuery = (SQLQuery<?>) query;

            // Use the query is is was defined or no template was provided
            if (sqlQuery.isNullExpression() && sqlQuery.getObject() != null) {
                // Handle SQLQuery with null expression - use the entry instead
                query = sqlQuery.getObject();
                return false;
            }
            return true;
        }
        if (query instanceof SQLQueryTemplatePacket)
            return true;

        return false;
    }

    private int getPartitionId() {
        return PartitionedClusterUtils.getPartitionId((_templatePacket).getRoutingFieldValue(),
                _numOfPartitions);
    }

    public void close() {
        synchronized (_lock) {
            if (_closed)
                return;

            if (_logger.isLoggable(Level.FINE))
                _logger.log(Level.FINE, "Closing replication client endpoint");

            if (getNotificationReplicationNode() != null) {
                _stateListener.close();
                unregisterNotificationReplicationNode();
                getNotificationReplicationNode().close();
                _closed = true;
            }

        }
    }

    private ReplicationNode createReplicationNode() {

        Uuid uuid = UuidFactory.generate();
        final String hostname = SystemInfo.singleton().network().getHost().getHostName();
        final String processId = "[" + SystemInfo.singleton().os().processId() + "]";

        String lookupName = LOOKUP_NAME_PREFIX + hostname + processId + "_" + uuid;

        ReplicationNodeBuilder replicationNodeBuilder = createReplicationNodeBuilder(uuid, lookupName);
        ReplicationNodeConfig replicationNodeConfig = ReplicationNodeConfigBuilder.getInstance().createNotificationConfig(
                _remoteSpace.getName(), _remoteSpace.getSpaceClusterInfo(), replicationNodeBuilder, _partitionId, _numOfPartitions);
        ReplicationNode replicationNode = new ReplicationNode(replicationNodeConfig,
                replicationNodeBuilder, lookupName, DummyMetricRegistrator.get());

        final boolean isBatchListener = _notifyInfo.getListener() instanceof BatchRemoteEventListener;

        AbstractReplicationNotificationInEntryHandler entryHandler = isBatchListener ? new ReplicationNotificationBatchInEntryHandler(_notifyInfo,
                _remoteSpace,
                _eventID,
                _templatePacket) :
                new ReplicationNotificationInEntryHandler(_notifyInfo,
                        _remoteSpace,
                        _eventID,
                        _templatePacket,
                        this);


        ReplicationNotificationInTransactionEntryHandler txnEntryHandler = new ReplicationNotificationInTransactionEntryHandler(entryHandler, isBatchListener);

        if (isBatchListener) {
            IReplicationInBatchConsumptionHandler batchHanlder = new ReplicationNotificationInBatchConsumptionHandler(_notifyInfo,
                    _templatePacket,
                    this);
            replicationNode.setInBatchConsumptionHandler(batchHanlder);
        }

        replicationNode.setTag(_notifyInfo.getTag());
        replicationNode.setInEntryHandler(entryHandler);
        replicationNode.setInEntryLeaseExpiredHandler(entryHandler);

        replicationNode.setInTransactionHandler(txnEntryHandler);

        replicationNode.getAdmin().setNodeStateListener(_stateListener);

        replicationNode.getAdmin().setActive();

        replicationNode.getAdmin().getRouterAdmin().enableIncomingCommunication();

        return replicationNode;
    }

    private ReplicationNodeBuilder createReplicationNodeBuilder(Uuid uuid, String lookupName) {
        ReplicationNodeBuilder nodeBuilder = new ReplicationNodeBuilder();

        ReplicationRouterBuilder routerBuilder = new DirectOnlyReplicationRouter.Builder(lookupName, uuid);
        nodeBuilder.setReplicationRouterBuilder(routerBuilder);

        SpaceTypeManager typeManager = new SpaceTypeManager(new SpaceConfigReader(lookupName));

        IDataConsumeFixFacade fixFacade = new NotificationReplicationDataConsumeFixFacade(_remoteSpace);

        ReplicationPacketDataConsumer dataConsumer = new ReplicationPacketDataConsumer(
                typeManager, fixFacade, null/*The mediator in the notification node is not really used*/);

        DistributedTransactionProcessingConfiguration distributedTransactionProcessingConfiguration = new DistributedTransactionProcessingConfiguration(
                MirrorConfig.DIST_TX_WAIT_TIMEOUT, MirrorConfig.DIST_TX_WAIT_FOR_OPERATIONS);
        nodeBuilder.setReplicationProcessLogBuilder(new ReliableAsyncAdaptiveProcessLogBuilder(dataConsumer, distributedTransactionProcessingConfiguration));

        nodeBuilder.setAsyncHandlerProvider(_asyncProvider);

        return nodeBuilder;
    }

    private Uuid registerNotificationReplicationNode()
            throws DurableNotificationRegistrationException {
        Uuid result = null;

        try {
            List<Future<RegisterReplicationNotificationResponseInfo>> futureList = new ArrayList<Future<RegisterReplicationNotificationResponseInfo>>();

            // Register replication node with template for each partition asynchronously:

            int startIndex = _partitionId == PartitionedClusterUtils.NO_PARTITION ? 0 : _partitionId;
            int endIndex = _partitionId == PartitionedClusterUtils.NO_PARTITION ? (_numOfPartitions - 1) : _partitionId;

            for (int i = startIndex; i <= endIndex; i++) {
                RegisterReplicationNotificationRequestInfo request = new RegisterReplicationNotificationRequestInfo();
                request.template = _templatePacket;
                request.viewStub = getNotificationReplicationNode().getAdmin().getRouterAdmin().getMyRouterStubHolder();
                request.notifyInfo = new NotifyInfo(null, _notifyInfo);
                request.eventId = _eventID;

                RegisterReplicationNotificationTask task = new RegisterReplicationNotificationTask(request);

                try {
                    Future<RegisterReplicationNotificationResponseInfo> future = _remoteSpace.execute(task, i, null /*transaction*/, null /*listener*/);
                    futureList.add(future);
                } catch (Exception e) {
                    throw new DurableNotificationRegistrationException(e.getMessage(), e);
                }
            }

            // Wait for all registrations to finish:
            try {
                for (Future<RegisterReplicationNotificationResponseInfo> future : futureList) {
                    RegisterReplicationNotificationResponseInfo response = future.get();
                    if (response.exception != null)
                        throw new DurableNotificationRegistrationException(response.exception.getMessage(), response.exception);

                    if (result == null)
                        result = response.spaceUID;

                }
            } catch (ExecutionException e) {
                throw new DurableNotificationRegistrationException(e.getMessage(), e.getCause());
            } catch (InterruptedException e) {
                throw new DurableNotificationRegistrationException(e.getMessage(), e);
            }
        } catch (DurableNotificationRegistrationException e) {
            if (_logger.isLoggable(Level.WARNING))
                _logger.log(Level.WARNING, "Notification replication node registration failed", e);

            close();

            Throwable cause = e.getCause();

            if (cause instanceof AccessDeniedException)
                throw (AccessDeniedException) cause;

            throw e;
        }

        return result;
    }

    private void unregisterNotificationReplicationNode() {
        UnregisterReplicationNotificationRequestInfo request = new UnregisterReplicationNotificationRequestInfo();
        request.viewStubHolderName = getNotificationReplicationNode().getAdmin().getRouterAdmin().getMyRouterStubHolder().getMyEndpointDetails().getLookupName();

        int startIndex = _partitionId == PartitionedClusterUtils.NO_PARTITION ? 0 : _partitionId;
        int endIndex = _partitionId == PartitionedClusterUtils.NO_PARTITION ? (_numOfPartitions - 1) : _partitionId;

        List<Future<SpaceResponseInfo>> futureList = new ArrayList<Future<SpaceResponseInfo>>();
        for (int i = startIndex; i <= endIndex; i++) {
            if (_logger.isLoggable(Level.FINER))
                _logger.log(Level.FINER, "sending replication node unregistration request to node on partition " + i);

            UnregisterReplicationNotificationTask task = new UnregisterReplicationNotificationTask(request);
            try {
                Future<SpaceResponseInfo> future = _remoteSpace.execute(task, i, null /* transaction */, null /* listener */);
                futureList.add(future);
            } catch (Exception e) {
                if (_logger.isLoggable(Level.FINER))
                    _logger.log(Level.FINER, "Notification replication node unregistration failed", e);
            }
        }

        for (Future<SpaceResponseInfo> future : futureList) {
            try {
                future.get();
            } catch (InterruptedException e) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.log(Level.FINE, "Notification replication node unregistration interrupted", e);
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                if (_logger.isLoggable(Level.FINE))
                    _logger.log(Level.FINE, "Notification replication node unregistration failed", e.getCause());
            }
        }
    }

    public long getEventID() {
        return _eventID;
    }

    public Uuid getSpaceUID() {
        return _spaceUID;
    }

    public DurableNotificationLease getLease() {
        return _lease;
    }

    public ReplicationNode getNotificationReplicationNode() {
        return _notificationReplicationNode;
    }

}
