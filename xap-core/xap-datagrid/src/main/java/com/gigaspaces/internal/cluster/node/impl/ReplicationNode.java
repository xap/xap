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

package com.gigaspaces.internal.cluster.node.impl;

import com.gigaspaces.cluster.replication.IRedoLogStatistics;
import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.cluster.replication.RedoLogStatistics;
import com.gigaspaces.internal.cluster.node.IReplicationNode;
import com.gigaspaces.internal.cluster.node.IReplicationNodeAdmin;
import com.gigaspaces.internal.cluster.node.IReplicationNodeStateListener;
import com.gigaspaces.internal.cluster.node.IReplicationOutContext;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInBatchConsumptionHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInDataTypeCreatedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInDataTypeIndexAddedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryLeaseCancelledHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryLeaseExpiredHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEntryLeaseExtendedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEvictEntryHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotificationSentHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateCreatedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateLeaseExpiredHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateLeaseExtendedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInNotifyTemplateRemovedHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInTransactionHandler;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.OperationWeightInfo;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeConfig;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeMode;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencyBackupSyncIteratorHandler;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.IDirectPersistencySyncHandler;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.BrokenReplicationTopologyException;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationDynamicTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationStaticTargetGroupBuilder;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationTargetGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationUnreliableOperation;
import com.gigaspaces.internal.cluster.node.impl.groups.ISpaceItemGroupsExtractor;
import com.gigaspaces.internal.cluster.node.impl.groups.NoSuchReplicationGroupExistException;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicationNodeGroupsHolder;
import com.gigaspaces.internal.cluster.node.impl.packets.ReplicaRequestPacket;
import com.gigaspaces.internal.cluster.node.impl.processlog.DefaultProcessLogExceptionHandlerBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandlerBuilder;
import com.gigaspaces.internal.cluster.node.impl.replica.CurrentStageInfo;
import com.gigaspaces.internal.cluster.node.impl.replica.ISpaceReplicaData;
import com.gigaspaces.internal.cluster.node.impl.replica.ISpaceReplicaDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.replica.ReplicationNodeReplicaHandler;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceCopyIntermediateResult;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceCopyReplicaRequestContext;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceCopyReplicaRunnable;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceReplicaState;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceSynchronizeReplicaRequestContext;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.router.ConnectionState;
import com.gigaspaces.internal.cluster.node.impl.router.IIncomingReplicationHandler;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouterAdmin;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationRouterBuilder;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyReplicaParameters;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyReplicaRequestContext;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyReplicaState;
import com.gigaspaces.internal.cluster.node.replica.ISpaceCopyResult;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaRequestContext;
import com.gigaspaces.internal.cluster.node.replica.ISpaceSynchronizeReplicaState;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.storage.IEntryHolder;
import com.gigaspaces.internal.server.storage.NotifyTemplateHolder;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.concurrent.CyclicAtomicInteger;
import com.gigaspaces.internal.utils.concurrent.GSThreadFactory;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.threadlocal.PoolFactory;
import com.gigaspaces.internal.utils.threadlocal.ThreadLocalPool;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.metrics.MetricConstants;
import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.cache.blobStore.BlobStoreReplicaConsumeHelper;
import com.j_spaces.core.cache.blobStore.BlobStoreReplicationBulkConsumeHelper;
import com.j_spaces.core.exception.ClosedResourceException;

import net.jini.core.event.EventRegistration;
import net.jini.core.transaction.server.ServerTransaction;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The implementation of the {@link IReplicationNode} interface
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationNode
        implements IReplicationNode, IIncomingReplicationHandler,
        IIncomingReplicationFacade, IReplicationTargetGroupStateListener,
        IReplicationSourceGroupStateListener {
    private static final Logger _logger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_NODE);
    private static final Logger _loggerReplica = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_REPLICA);

    private final ReplicationNodeConfig _replicationNodeConfig;
    private final IReplicationNodeBuilder _nodeBuilder;

    private final IReplicationNodeAdmin _replicationNodeAdmin;

    private final Map<String, GroupMapping> _groupsMapping;

    private final IReplicationRouter _replicationRouter;
    private final ISpaceItemGroupsExtractor _spaceItemGroupsExtractor;
    private final IAsyncHandlerProvider _asyncHandlerProvider;
    private final ISpaceReplicaDataConsumer _replicaDataConsumer;
    private final IReplicationOutFilter _replicationOutFilter;
    private final IReplicationInFilter _replicationInFilter;

    private final ReplicationNodeReplicaHandler _replicaHandler;
    private final ReplicationNodeGroupsHolder _groupsHolder;
    private final ReplicationNodeInFacade _inFacade;
    private final IReplicationNodePluginFacade _pluginFacade;

    private final IReplicationProcessLogExceptionHandlerBuilder _exceptionHandlerBuilder;

    private final ScheduledExecutorService _statisticsMonitor;

    private final String _name;
    private final MetricRegistrator metricRegister;
    private final ThreadLocalPool<ReplicationOutContext> _contextPool = new ThreadLocalPool<ReplicationOutContext>(new PoolFactory<ReplicationOutContext>() {
        public ReplicationOutContext create() {
            return new ReplicationOutContext();
        }
    });
    private volatile IReplicationNodeStateListener _stateListener;
    private volatile boolean _closed;
    private volatile ReplicationNodeMode _nodeMode;

    private String tag; //optional (may be null) - name/id/tag of endpoint remote event listener

    //blob store stuff
    private BlobStoreReplicaConsumeHelper _blobStoreReplicaConsumeHelper;
    private volatile IDirectPersistencySyncHandler _directPesistencySyncHandler;
    private BlobStoreReplicationBulkConsumeHelper _blobStoreReplicationBulkConsumeHelper;

    public ReplicationNode(ReplicationNodeConfig replicationNodeConfig,
                           IReplicationNodeBuilder nodeBuilder, String name, MetricRegistrator metricRegister) {
        _replicationNodeConfig = replicationNodeConfig;
        _nodeBuilder = nodeBuilder;
        _name = name;
        this.metricRegister = metricRegister.extend(MetricConstants.REPLICATION_METRIC_NAME);

        if (_logger.isDebugEnabled())
            _logger.debug(getLogPrefix()
                    + "creating replication node with config:"
                    + StringUtils.NEW_LINE + replicationNodeConfig
                    + StringUtils.NEW_LINE + "builder:" + StringUtils.NEW_LINE
                    + _nodeBuilder);

        _replicationNodeAdmin = new ReplicationNodeAdmin(this);
        _replicationRouter = _nodeBuilder.createReplicationRouter(this);
        _spaceItemGroupsExtractor = _nodeBuilder.createSpaceItemGroupsExtractor();
        _asyncHandlerProvider = _nodeBuilder.getAsyncHandlerProvider();
        _replicaDataConsumer = _nodeBuilder.createReplicaDataConsumer();
        _directPesistencySyncHandler = _nodeBuilder.createDirectPersistencySyncHandler();

        _replicationOutFilter = _replicationNodeConfig.getReplicationOutFilter();
        _replicationInFilter = _replicationNodeConfig.getReplicationInFilter();
        _groupsMapping = _replicationNodeConfig.getSourceGroupsMapping();
        _pluginFacade = _replicationNodeConfig.getPluginFacade();

        _replicaHandler = new ReplicationNodeReplicaHandler(this,
                _nodeBuilder.createReplicaDataGenerator(),
                _replicationNodeConfig.getSpaceCopyReplicaOutFilter());
        _groupsHolder = new ReplicationNodeGroupsHolder();
        _inFacade = new ReplicationNodeInFacade(_groupsHolder);
        _exceptionHandlerBuilder = new DefaultProcessLogExceptionHandlerBuilder();
        _statisticsMonitor = new ScheduledThreadPoolExecutor(1,
                new GSThreadFactory("StatisticsMonitor",
                        true));

        createSourceGroups(ReplicationNodeMode.ALWAYS);
        createTargetGroups(ReplicationNodeMode.ALWAYS);

        _statisticsMonitor.scheduleAtFixedRate(new StatisticsMonitor(),
                3,
                3,
                TimeUnit.SECONDS);
    }

    public String getName() {
        return _name;
    }

    public Object getUniqueId() {
        return _replicationRouter.getMyUniqueId();
    }

    public IReplicationOutContext createContext() {
        return _contextPool.get();
    }

    public int execute(IReplicationOutContext context) {
        validateNotClose();

        ReplicationOutContext replicationContext = (ReplicationOutContext) context;

        if (_logger.isTraceEnabled())
            _logger.trace(getLogPrefix() + "executing replication context "
                    + context);
        if (replicationContext.isEmpty())
            return 0;
        // Dispatch execute to all replication groups that attached themselves
        // to the context
        if (replicationContext.isSingleGroupParticipant()) {
            IReplicationGroupOutContext groupOutContext = replicationContext.getSingleGroupParticipantContext();
            // GS-12752 groupOutContext can be null if blobstore bulk was discarded entirely
            if (context.isBlobstorePendingReplicationBulk() && groupOutContext == null) {
                return 0;
            }
            return _groupsHolder.getSourceGroup(groupOutContext.getName())
                    .execute(groupOutContext);
        } else {
            int res = 0;
            for (IReplicationGroupOutContext groupOutContext : replicationContext) {
                // GS-12752 groupOutContext can be null if blobstore bulk was discarded entirely
                if (context.isBlobstorePendingReplicationBulk() && groupOutContext == null) {
                    continue;
                }
                res += _groupsHolder.getSourceGroup(groupOutContext.getName()).execute(groupOutContext);
            }
            return res;
        }
    }

    public ReplicationRouterBuilder getReplicationRouterBuilder() {
        return _nodeBuilder.getReplicationRouterBuilder();
    }

    public IReplicationRouter getReplicationRouter() {
        return _replicationRouter;
    }

    public IReplicationNodeAdmin getAdmin() {
        return _replicationNodeAdmin;
    }

    public void outCancelEntryLease(IReplicationOutContext context,
                                    IEntryHolder entryHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.CANCEL_LEASE;
        routeBeforeReplicate(context, entryHolder, operationType);
    }

    public void outEntryLeaseExpired(IReplicationOutContext context,
                                     IEntryHolder entryHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.ENTRY_LEASE_EXPIRED;
        routeBeforeReplicate(context, entryHolder, operationType);
    }

    public void outNotifyTemplateLeaseExpired(IReplicationOutContext context,
                                              NotifyTemplateHolder templateHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.NOTIFY_TEMPLATE_LEASE_EXPIRED;
        routeBeforeReplicate(context, templateHolder, operationType);
    }


    public void outEvictEntry(IReplicationOutContext context,
                              IEntryHolder entryHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.EVICT;
        routeBeforeReplicate(context, entryHolder, operationType);
    }

    public void outExtendEntryLeasePeriod(IReplicationOutContext context,
                                          IEntryHolder entryHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.EXTEND_ENTRY_LEASE;
        routeBeforeReplicate(context, entryHolder, operationType);
    }

    public void outExtendNotifyTemplateLeasePeriod(
            IReplicationOutContext context, NotifyTemplateHolder entryHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.EXTEND_NOTIFY_TEMPLATE_LEASE;
        routeBeforeReplicate(context, entryHolder, operationType);
    }

    public void outInsertNotifyTemplate(IReplicationOutContext context,
                                        NotifyTemplateHolder templateHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.INSERT_NOTIFY_TEMPLATE;
        routeBeforeReplicate(context, templateHolder, operationType);
    }

    public void outNotificationSentAndExecute(OperationID operationId) {
        validateNotClose();

        IReplicationUnreliableOperation operation = new NotificationSentSyncOperation(operationId);
        // TODO currently route to all
        for (IReplicationSourceGroup sourceGroup : _groupsHolder.getSourceGroups())
            sourceGroup.execute(operation);
    }

    public void outRemoveEntry(IReplicationOutContext context,
                               IEntryHolder entryHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.REMOVE_ENTRY;
        routeBeforeReplicate(context, entryHolder, operationType);
    }

    public void outRemoveNotifyTemplate(IReplicationOutContext context,
                                        NotifyTemplateHolder templateHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.REMOVE_NOTIFY_TEMPLATE;
        routeBeforeReplicate(context, templateHolder, operationType);
    }

    public void outTransaction(IReplicationOutContext context,
                               ServerTransaction transaction,
                               ArrayList<IEntryHolder> lockedEntries) {
        ReplicationMultipleOperationType operationType = ReplicationMultipleOperationType.TRANSACTION_ONE_PHASE;
        routeBeforeTransactionExecute(context,
                transaction,
                lockedEntries,
                operationType);
    }

    @Override
    public void outTransactionCommit(IReplicationOutContext context,
                                     ServerTransaction transaction) {
        ReplicationMultipleOperationType operationType = ReplicationMultipleOperationType.TRANSACTION_TWO_PHASE_COMMIT;
        routeBeforeTransactionExecute(context,
                transaction,
                null,
                operationType);
    }

    @Override
    public void outTransactionAbort(IReplicationOutContext context,
                                    ServerTransaction transaction) {
        ReplicationMultipleOperationType operationType = ReplicationMultipleOperationType.TRANSACTION_TWO_PHASE_ABORT;
        routeBeforeTransactionExecute(context,
                transaction,
                null,
                operationType);
    }

    @Override
    public void outTransactionPrepare(IReplicationOutContext context,
                                      ServerTransaction transaction, ArrayList<IEntryHolder> lockedEntries) {
        ReplicationMultipleOperationType operationType = ReplicationMultipleOperationType.TRANSACTION_TWO_PHASE_PREPARE;
        routeBeforeTransactionExecute(context,
                transaction,
                lockedEntries,
                operationType);
    }

    private void routeBeforeTransactionExecute(IReplicationOutContext context,
                                               ServerTransaction transaction,
                                               ArrayList<IEntryHolder> lockedEntries,
                                               ReplicationMultipleOperationType operationType) {
        ReplicationOutContext replicationContext = (ReplicationOutContext) context;

        GroupMapping groupMapping = GroupMapping.getAllMapping();
        replicationContext.setGroupMapping(groupMapping);

        if (_logger.isTraceEnabled())
            _logger.trace(getLogPrefix() + "before replicate of operation "
                    + operationType + " and entries " + lockedEntries + " routed to " + groupMapping + " groups");

        for (IReplicationSourceGroup sourceGroup : _groupsHolder.getSourceGroups()) {
            sourceGroup.beforeTransactionExecute(replicationContext,
                    transaction,
                    lockedEntries,
                    operationType);
        }
    }

    public void outUpdateEntry(IReplicationOutContext context,
                               IEntryHolder entryHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.UPDATE;
        routeBeforeReplicate(context, entryHolder, operationType);
    }

    @Override
    public void outChangeEntry(IReplicationOutContext context,
                               IEntryHolder entryHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.CHANGE;
        routeBeforeReplicate(context, entryHolder, operationType);
    }

    public void outWriteEntry(IReplicationOutContext context,
                              IEntryHolder entryHolder) {
        ReplicationSingleOperationType operationType = ReplicationSingleOperationType.WRITE;
        routeBeforeReplicate(context, entryHolder, operationType);
    }

    public void outDataTypeIntroduce(IReplicationOutContext context,
                                     ITypeDesc typeDescriptor) {

        ReplicationOutContext replicationContext = (ReplicationOutContext) context;

        replicationContext.setGroupMapping(GroupMapping.getAllMapping());

        for (IReplicationSourceGroup sourceGroup : _groupsHolder.getSourceGroups())
            sourceGroup.beforeExecuteGeneric(replicationContext,
                    typeDescriptor,
                    ReplicationSingleOperationType.DATA_TYPE_INTRODUCE);

    }

    public void outDataTypeAddIndex(IReplicationOutContext context,
                                    AddTypeIndexesRequestInfo addIndexesRequest) {

        ReplicationOutContext replicationContext = (ReplicationOutContext) context;

        replicationContext.setGroupMapping(GroupMapping.getAllMapping());

        for (IReplicationSourceGroup sourceGroup : _groupsHolder.getSourceGroups())
            sourceGroup.beforeExecuteGeneric(replicationContext,
                    addIndexesRequest,
                    ReplicationSingleOperationType.DATA_TYPE_ADD_INDEX);

    }

    public void setInEvictEntryHandler(IReplicationInEvictEntryHandler handler) {
        _inFacade.setInEvictEntryHandler(handler);
    }


    public void setInNotificationSentHandler(
            IReplicationInNotificationSentHandler handler) {
        _inFacade.setInNotificationSentHandler(handler);
    }

    public void setInTransactionHandler(IReplicationInTransactionHandler handler) {
        _inFacade.setInTransactionHandler(handler);
    }

    public void setInEntryHandler(IReplicationInEntryHandler handler) {
        _inFacade.setInEntryHandler(handler);
    }

    @Override
    public void setInEntryLeaseCancelledHandler(IReplicationInEntryLeaseCancelledHandler handler) {
        _inFacade.setInEntryLeaseCancelledHandler(handler);
    }

    @Override
    public void setInEntryLeaseExtendedHandler(IReplicationInEntryLeaseExtendedHandler handler) {
        _inFacade.setInEntryLeaseExtendedHandler(handler);
    }

    @Override
    public void setInEntryLeaseExpiredHandler(IReplicationInEntryLeaseExpiredHandler handler) {
        _inFacade.setInEntryLeaseExpiredHandler(handler);
    }

    @Override
    public void setInNotifyTemplateCreatedHandler(IReplicationInNotifyTemplateCreatedHandler handler) {
        _inFacade.setInNotifyTemplateCreatedHandler(handler);
    }

    @Override
    public void setInNotifyTemplateRemovedHandler(IReplicationInNotifyTemplateRemovedHandler handler) {
        _inFacade.setInNotifyTemplateRemovedHandler(handler);
    }

    @Override
    public void setInNotifyTemplateLeaseExtendedHandler(IReplicationInNotifyTemplateLeaseExtendedHandler handler) {
        _inFacade.setInNotifyTemplateLeaseExtendedHandler(handler);
    }

    @Override
    public void setInNotifyTemplateLeaseExpiredHandler(IReplicationInNotifyTemplateLeaseExpiredHandler handler) {
        _inFacade.setInNotifyTemplateLeaseExpiredHandler(handler);
    }

    @Override
    public void setInDataTypeCreatedHandler(IReplicationInDataTypeCreatedHandler handler) {
        _inFacade.setInDataTypeCreatedHandler(handler);
    }

    @Override
    public void setInDataTypeIndexAddedHandler(IReplicationInDataTypeIndexAddedHandler handler) {
        _inFacade.setInDataTypeIndexAddedHandler(handler);
    }

    @Override
    public void setInBatchConsumptionHandler(
            IReplicationInBatchConsumptionHandler handler) {
        _inFacade.setInBatchConsumptionHandler(handler);
    }

    public ISpaceSynchronizeReplicaState spaceSynchronizeReplicaRequest(
            ISpaceSynchronizeReplicaRequestContext context) {
        SpaceSynchronizeReplicaRequestContext replicaContext = (SpaceSynchronizeReplicaRequestContext) context;
        _replicaHandler.initFifoBatchesHandler();
        if (_loggerReplica.isDebugEnabled())
            _loggerReplica.debug(getLogPrefix()
                    + "starting space synchronization replica process using Url "
                    + replicaContext.getOriginUrl());
        return doSpaceReplica(replicaContext.getOriginUrl(),
                replicaContext.getConcurrentConsumers(),
                replicaContext.getFetchBatchSize(),
                replicaContext.getSynchronizeGroupName(),
                replicaContext.getParameters(),
                replicaContext.getProgressTimeout());

    }

    public ISpaceCopyReplicaState spaceCopyReplicaRequest(
            ISpaceCopyReplicaRequestContext context) {
        SpaceCopyReplicaRequestContext replicaContext = (SpaceCopyReplicaRequestContext) context;
        if (_loggerReplica.isDebugEnabled())
            _loggerReplica.debug(getLogPrefix()
                    + "starting space copy replica process using Url "
                    + replicaContext.getOriginUrl());
        return doSpaceReplica(replicaContext.getOriginUrl(),
                replicaContext.getConcurrentConsumers(),
                replicaContext.getFetchBatchSize(),
                null /* sync group name */,
                replicaContext.getParameters(),
                replicaContext.getProgressTimeout());
    }

    public void rollbackCopyReplica(ISpaceCopyResult replicaResult) {
        if (replicaResult == null
                || !(replicaResult instanceof SpaceCopyIntermediateResult))
            return;
        SpaceCopyIntermediateResult copyResult = (SpaceCopyIntermediateResult) replicaResult;
        // Handle templates recovery failure - cancel already recovered
        // templates

        if (copyResult.getNotifyRegistrations() != null) {
            for (EventRegistration eventReg : copyResult.getNotifyRegistrations()) {
                try {
                    eventReg.getLease().cancel();
                } catch (Exception e) {
                }
            }
        }
    }

    public void close() {
        if (_closed)
            return;
        if (_logger.isDebugEnabled())
            _logger.debug(getLogPrefix() + " closing replication node");

        _replicaHandler.close();
        _statisticsMonitor.shutdownNow();
        _groupsHolder.close();
        _replicationRouter.close();
        if (_asyncHandlerProvider != null)
            _asyncHandlerProvider.close();
        _closed = true;
        if (_logger.isDebugEnabled())
            _logger.debug(getLogPrefix() + " replication node closed");
    }

    @Override
    public void setBlobStoreReplicationBulkConsumeHelper(BlobStoreReplicationBulkConsumeHelper blobStoreReplicationBulkConsumeHelper) {
        _blobStoreReplicationBulkConsumeHelper = blobStoreReplicationBulkConsumeHelper;
    }

    @Override
    public BlobStoreReplicationBulkConsumeHelper getBlobStoreReplicationBulkConsumeHelper() {
        return _blobStoreReplicationBulkConsumeHelper;
    }

    private ISpaceSynchronizeReplicaState doSpaceReplica(Object originUrl,
                                                         int concurrentConsumers, int fetchBatchSize, String syncGroupName,
                                                         ISpaceCopyReplicaParameters parameters, long progressTimeout) {
        IReplicationMonitoredConnection originConnection = _replicationRouter.getUrlConnection(originUrl);
        // Fast fail if no connection to target available
        if (originConnection.getState() == ConnectionState.DISCONNECTED) {
            Exception reason = originConnection.getLastDisconnectionReason();
            originConnection.close();
            return FailedSyncSpaceReplicateState.createFailedSyncState(reason);
        }

        ReplicaRequestPacket requestPacket = new ReplicaRequestPacket(syncGroupName,
                parameters);
        Object replicaRemoteContext = null;
        try {
            replicaRemoteContext = originConnection.dispatch(requestPacket);
        } catch (Exception e) {
            originConnection.close();
            return FailedSyncSpaceReplicateState.createFailedSyncState(e);
        }
        boolean isSynchronize = syncGroupName != null;

        IReplicationTargetGroup targetGroup = null;
        // This is a synchronize request not just a copy
        if (isSynchronize)
            targetGroup = getReplicationTargetGroup(syncGroupName);

        SpaceReplicaState result = new SpaceReplicaState(originConnection,
                isSynchronize,
                progressTimeout,
                targetGroup);

        CyclicAtomicInteger orderProvider = new CyclicAtomicInteger(concurrentConsumers - 1);
        // Start copy
        for (int i = 0; i < concurrentConsumers; ++i) {
            SpaceCopyReplicaRunnable consumer = new SpaceCopyReplicaRunnable(this,
                    originConnection,
                    _replicaDataConsumer,
                    _replicationNodeConfig.getSpaceCopyReplicaInFilter(),
                    replicaRemoteContext,
                    fetchBatchSize,
                    result,
                    orderProvider);
            result.addReplicateConsumer(consumer);
            _asyncHandlerProvider.start(consumer,
                    1,
                    "CopyReplicaConsumer-"
                            + i
                            + "-"
                            + getName()
                            + "."
                            + originConnection.getFinalEndpointLookupName(),
                    false);
        }
        return result;
    }

    public <T> T onReplication(AbstractReplicationPacket<T> packet)
            throws RemoteException {
        try {
            validateNotClose();

            return packet.accept(this);
        } catch (ClosedResourceException e) {
            throw new RemoteException(e.getMessage(), e);
        }
    }

    private void routeBeforeReplicate(IReplicationOutContext context,
                                      IEntryHolder entryHolder,
                                      ReplicationSingleOperationType operationType) {
        ReplicationOutContext replicationContext = (ReplicationOutContext) context;
        // Extract the group mapping this operation should be replicated to
        String routing = _spaceItemGroupsExtractor.extractGroupMapping(entryHolder);
        // Get all the groups from the mapping
        GroupMapping mapping = _groupsMapping.get(routing);
        if (_logger.isTraceEnabled())
            _logger.trace(getLogPrefix() + "before replicate of operation "
                    + operationType + " and entry " + entryHolder + " routed to " + mapping + " groups");
        replicationContext.setGroupMapping(mapping);
        // Dispatch to specific mapping
        if (mapping.hasSpecificGroups()) {
            String[] routedGroups = mapping.getSpecificGroups();
            // Dispatch beforeExecute operation to all relevant replication
            // groups
            for (String routedGroup : routedGroups)
                _groupsHolder.getSourceGroup(routedGroup)
                        .beforeExecute(replicationContext,
                                entryHolder,
                                operationType);
        }
        // Dispatch to all groups
        else {
            for (IReplicationSourceGroup sourceGroup : _groupsHolder.getSourceGroups())
                sourceGroup.beforeExecute(replicationContext,
                        entryHolder,
                        operationType);
        }
    }

    // Package visible used from admin
    void createSourceGroups(ReplicationNodeMode nodeMode) {
        for (IReplicationSourceGroupBuilder builder : _replicationNodeConfig.getSourceGroupBuilders(nodeMode)) {
            IReplicationSourceGroup group = builder.createGroup(_replicationRouter,
                    _replicationOutFilter,
                    this);
            if (_logger.isDebugEnabled())
                _logger.debug(getLogPrefix() + "created source group "
                        + group.getGroupName());
            _groupsHolder.addSourceGroup(group, nodeMode);
        }
        registerMetrics(nodeMode);
    }

    private void registerMetrics(ReplicationNodeMode nodeMode) {
        metricRegister.clear(); // just in case embedded splitbrain.
        if (ReplicationNodeMode.PASSIVE != nodeMode) {
            boolean first = true;
            for (IReplicationSourceGroup replicationSourceGroup : _groupsHolder.getSourceGroups()) {
                if (first) {
                    replicationSourceGroup.getGroupBacklog().registerWith(metricRegister.extend(MetricConstants.REDO_LOG_METRIC_NAME));
                    first = false;
                }
                replicationSourceGroup.registerWith(metricRegister);
            }
        }
    }

    // Package visible used from admin
    void createTargetGroups(ReplicationNodeMode nodeMode) {
        for (IReplicationStaticTargetGroupBuilder builder : _replicationNodeConfig.getTargetGroupBuilders(nodeMode)) {
            IReplicationTargetGroup group = builder.createStaticGroup(_replicationRouter,
                    _inFacade,
                    _exceptionHandlerBuilder,
                    _replicationInFilter,
                    this);
            if (_logger.isDebugEnabled())
                _logger.debug(getLogPrefix() + "created target group "
                        + group.getGroupName());
            _groupsHolder.addTargetGroup(group, nodeMode);
        }
    }

    // Package visible used from admin
    void closeSourceGroups(ReplicationNodeMode nodeMode) {
        _groupsHolder.closeSourceGroups(nodeMode);
    }

    // Package visible used from admin
    void closeTargetGroups(ReplicationNodeMode nodeMode) {
        _groupsHolder.closeTargetGroups(nodeMode);
    }

    // Package visible used from admin
    ReplicationNodeMode getNodeMode() {
        return _nodeMode;
    }

    // Package visible used from admin
    void setNodeMode(ReplicationNodeMode newMode) {
        _nodeMode = newMode;
        IReplicationNodeStateListener stateListener = _stateListener;
        if (stateListener != null)
            stateListener.onReplicationNodeModeChange(newMode);
    }

    public IReplicationTargetGroup getReplicationTargetGroup(String groupName)
            throws NoSuchReplicationGroupExistException {
        try {
            return _groupsHolder.getTargetGroup(groupName);
        } catch (NoSuchReplicationGroupExistException e) {
            // TODO WAN: concurrency problem since admin can change states
            // concurrently,
            // we need to refactor this to have a proper place that creates new
            // groups.
            IReplicationDynamicTargetGroupBuilder matchingGroupBuilder = _replicationNodeConfig.getMatchingTargetGroupBuilder(groupName,
                    getNodeMode());
            if (matchingGroupBuilder == null){
                matchingGroupBuilder = _replicationNodeConfig.createMatchingTargetGroupBuilder(groupName,
                        getNodeMode(), this._nodeBuilder);
                if(matchingGroupBuilder == null){
                    throw e;
                }
            }

            IReplicationTargetGroup dynamicGroup = matchingGroupBuilder.createDynamicGroup(groupName,
                    _replicationRouter,
                    _inFacade,
                    _exceptionHandlerBuilder,
                    _replicationInFilter,
                    _stateListener);
            IReplicationTargetGroup finalGroup = _groupsHolder.addIfAbsentTargetGroup(dynamicGroup,
                    getNodeMode());
            if (dynamicGroup == finalGroup) {
                if (_logger.isDebugEnabled())
                    _logger.debug(getLogPrefix()
                            + "created dynamic target group "
                            + dynamicGroup.getGroupName());
                switch (getNodeMode()) {
                    case ACTIVE:
                        dynamicGroup.setActive();
                        break;
                    case PASSIVE:
                        dynamicGroup.setPassive();
                        break;
                }
            }
            return finalGroup;
        }
    }

    public IReplicationSourceGroup getReplicationSourceGroup(String groupName)
            throws NoSuchReplicationGroupExistException {
        return _groupsHolder.getSourceGroup(groupName);
    }

    // exposed for DirectPersistencyEntryReplicaProducer in order to access the backlog using the requestContext
    public IReplicationGroupBacklog getGroupBacklogByRequestContext(Object requestContext) {
        return _replicaHandler.getGroupBacklogByRequestContext(requestContext);
    }

    @Override
    public RouterStubHolder getRemoteRouterStub(String routerLookupName) {
        return getRouterAdmin().getRemoteRouterStub(routerLookupName);
    }

    @Override
    public ReplicationEndpointDetails getEndpointDetails() {
        return getRouterAdmin().getMyEndpointDetails();
    }

    public <T> T accept(AbstractReplicationPacket<T> packet) {
        if (_pluginFacade != null)
            return _pluginFacade.accept(packet);

        throw new UnsupportedOperationException("No plugin present");
    }

    IReplicationSourceGroup[] getReplicationSourceGroups() {
        return _groupsHolder.getSourceGroups();
    }

    IReplicationTargetGroup[] getReplicationTargetGroups() {
        return _groupsHolder.getTargetGroups();
    }

    public Object newReplicaRequest(String requesterLookupName,
                                    ReplicaRequestPacket replicaRequestPacket) {
        return _replicaHandler.newReplicaRequest(requesterLookupName,
                replicaRequestPacket);
    }

    public Collection<ISpaceReplicaData> getNextReplicaBatch(Object context,
                                                             int fetchBatchSize) {
        return _replicaHandler.getNextReplicaBatch(context, fetchBatchSize);
    }

    public void clearStaleReplicas(long expirationTime) {
        _replicaHandler.clearStaleReplicas(expirationTime);
    }

    public CurrentStageInfo nextReplicaStage(Object replicaRemoteContext) {
        return _replicaHandler.nextReplicaState(replicaRemoteContext);
    }

    public String getLogPrefix() {
        String prefix = "Replication [" + getName() + "] ";
        if (tag != null) {
            prefix += "tag ["+tag+"] ";
        }
        return prefix;
    }

    @Override
    public String toString() {
        return getLogPrefix();
    }

    public IRedoLogStatistics getBackLogStatistics() {
        // get shared backlog statistics - use the first backlog
        for (IReplicationSourceGroup group : getReplicationSourceGroups()) {
            IReplicationGroupBacklog groupBacklog = group.getGroupBacklog();
            return groupBacklog.getStatistics();
        }
        return RedoLogStatistics.EMPTY_STATISTICS;
    }

    public void monitorState(OperationWeightInfo info) {
        for (IReplicationSourceGroup sourceGroup : _groupsHolder.getSourceGroups()) {
            sourceGroup.getGroupBacklog().monitor(info);
            sourceGroup.monitorConsistencyLevel();
        }

    }

    public void setNodeStateListener(IReplicationNodeStateListener listener) {
        _stateListener = listener;
    }

    public boolean onTargetChannelOutOfSync(String groupName,
                                            String channelSourceLookupName,
                                            IncomingReplicationOutOfSyncException outOfSyncReason) {
        IReplicationNodeStateListener stateListener = _stateListener;
        if (stateListener != null)
            return stateListener.onTargetChannelOutOfSync(groupName,
                    channelSourceLookupName,
                    outOfSyncReason);
        return false;
    }

    public void onTargetChannelBacklogDropped(String groupName,
                                              String channelSourceLooString, IBacklogMemberState memberState) {
        IReplicationNodeStateListener stateListener = _stateListener;
        if (stateListener != null)
            stateListener.onTargetChannelBacklogDropped(groupName,
                    channelSourceLooString,
                    memberState);
    }

    public void onTargetChannelCreationValidation(String groupName,
                                                  String sourceMemberName, Object sourceUniqueId) {
        IReplicationNodeStateListener stateListener = _stateListener;
        if (stateListener != null)
            stateListener.onTargetChannelCreationValidation(groupName,
                    sourceMemberName,
                    sourceUniqueId);

    }

    @Override
    public void onTargetChannelSourceDisconnected(String groupName,
                                                  String sourceMemberName, Object sourceUniqueId) {
        IReplicationNodeStateListener stateListener = _stateListener;
        if (stateListener != null)
            stateListener.onTargetChannelSourceDisconnected(groupName,
                    sourceMemberName,
                    sourceUniqueId);
    }

    @Override
    public void onTargetChannelConnected(String groupName,
                                         String sourceMemberName, Object sourceUniqueId) {
        IReplicationNodeStateListener stateListener = _stateListener;
        if (stateListener != null)
            stateListener.onTargetChannelConnected(groupName,
                    sourceMemberName,
                    sourceUniqueId);
    }

    public void onSourceBrokenReplicationTopology(String groupName,
                                                  String channelMemberName, BrokenReplicationTopologyException error) {
        IReplicationNodeStateListener stateListener = _stateListener;
        if (stateListener != null)
            stateListener.onSourceBrokenReplicationTopology(groupName,
                    channelMemberName,
                    error);
    }

    public void onSourceChannelActivated(String groupName, String memberName) {
        IReplicationNodeStateListener stateListener = _stateListener;
        if (stateListener != null)
            stateListener.onSourceChannelActivated(groupName, memberName);
    }

    public void onNewReplicaRequest(String groupName, String channelName, boolean isSynchronizeRequest) {
        IReplicationNodeStateListener stateListener = _stateListener;
        if (stateListener != null)
            stateListener.onNewReplicaRequest(groupName, channelName, isSynchronizeRequest);
    }

    public boolean flushPendingReplication(long timeout, TimeUnit units) {
        if (_logger.isDebugEnabled())
            _logger.debug(getLogPrefix()
                    + "initiating flush of pending replication");
        long remainingTime = units.toMillis(timeout);
        for (IReplicationSourceGroup sourceGroup : _groupsHolder.getSourceGroups()) {
            long iterationStartTime = SystemTime.timeMillis();
            boolean flushPendingReplication = sourceGroup.flushPendingReplication(remainingTime,
                    TimeUnit.MILLISECONDS);
            if (!flushPendingReplication)
                return false;
            remainingTime = remainingTime
                    - (SystemTime.timeMillis() - iterationStartTime);
            if (remainingTime <= 0)
                return false;
        }
        if (_logger.isInfoEnabled())
            _logger.info(getLogPrefix() + "completed replication.");
        return true;
    }

    public String dumpState() {
        return "---- Replication Groups ----" + StringUtils.NEW_LINE
                + _groupsHolder.dumpState() + StringUtils.NEW_LINE
                + "---- Replication Router ----" + StringUtils.NEW_LINE
                + _replicationRouter.dumpState() + StringUtils.NEW_LINE
                + "---- Replica Handler ----" + StringUtils.NEW_LINE
                + _replicaHandler.dumpState();
    }

    private void validateNotClose() {
        if (_closed)
            throw new ClosedResourceException("Replication module is closed");
    }


    public IReplicationRouterAdmin getRouterAdmin() {
        return _replicationRouter.getAdmin();
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public class StatisticsMonitor
            implements Runnable {

        public void run() {
            for (IReplicationSourceGroup sourceGroup : getReplicationSourceGroups()) {
                sourceGroup.sampleStatistics();
            }
        }

    }

    /*
      blobStoreReplicaHelper provides bluking help in case of replication recovery
      */
    @Override
    public void setBlobStoreReplicaConsumeHelper(BlobStoreReplicaConsumeHelper blobStoreReplicaConsumeHelper) {
        _blobStoreReplicaConsumeHelper = blobStoreReplicaConsumeHelper;
    }

    /*
      blobStoreReplicaHelper provides bluking help in case of replication recovery
      */
    @Override
    public BlobStoreReplicaConsumeHelper getBlobStoreReplicaConsumeHelper() {
        return _blobStoreReplicaConsumeHelper;
    }

    /*
      handles direct persistency consistency
      */
    @Override
    public IDirectPersistencySyncHandler getDirectPesistencySyncHandler() {
        return _directPesistencySyncHandler;
    }


    @Override
    public DirectPersistencyBackupSyncIteratorHandler getDirectPersistencyBackupSyncIteratorHandler() {
        return _replicaHandler.getDirectPersistencyBackupSyncIteratorHandler();
    }

    @Override
    public void setDirectPersistencyBackupSyncIteratorHandler(DirectPersistencyBackupSyncIteratorHandler directPersistencyBackupSyncIteratorHandler) {
        _replicaHandler.setDirectPersistencyBackupSyncIteratorHandler(directPersistencyBackupSyncIteratorHandler);
    }

    public MetricRegistrator getMetricRegister() {
        return metricRegister;
    }

    public ReplicationNodeReplicaHandler getReplicaHandler() {
        return _replicaHandler;
    }
}
