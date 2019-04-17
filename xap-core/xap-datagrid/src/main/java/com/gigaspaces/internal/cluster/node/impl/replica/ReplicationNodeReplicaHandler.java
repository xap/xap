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

package com.gigaspaces.internal.cluster.node.impl.replica;

import com.gigaspaces.internal.cluster.node.impl.ReplicationNode;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencyBackupSyncIteratorHandler;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncListBatch;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncListFetcher;
import com.gigaspaces.internal.cluster.node.impl.filters.ISpaceCopyReplicaOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.packets.ReplicaRequestPacket;
import com.gigaspaces.internal.cluster.node.impl.replica.data.AbstractEntryReplicaData;
import com.gigaspaces.internal.cluster.node.replica.SpaceCopyReplicaParameters;
import com.gigaspaces.internal.extension.XapExtensions;
import com.gigaspaces.internal.server.metadata.IServerTypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cache.CacheManager;
import com.j_spaces.core.cache.TypeData;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.kernel.SystemProperties;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Inner component of the {@link ReplicationNode} that is in charge of handling incoming replica
 * requests
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ReplicationNodeReplicaHandler {
    protected final static Logger _logger = Logger.getLogger(Constants.LOGGER_REPLICATION_REPLICA);

    private final ReplicationNode _replicationNode;
    private final ISpaceReplicaDataProducerBuilder<? extends ISpaceReplicaData> _replicaDataProducerBuilder;
    private final Map<Object, ReplicaRequestData> _activeReplicaProcesses = new CopyOnUpdateMap<Object, ReplicaRequestData>();
    private final ISpaceCopyReplicaOutFilter _outFilter;
    private final boolean _isFiltered;
    private int _lastContextId;
    private volatile DirectPersistencyBackupSyncIteratorHandler _directPersistencyBackupSyncIteratorHandler;
    private final ReentrantLock _lock;
    private final SpaceEngine _spaceEngine;
    private volatile SpaceReplicaFifoBatchesHandler _fifoBatchesHandler;


    public ReplicationNodeReplicaHandler(
            ReplicationNode replicationNode,
            ISpaceReplicaDataProducerBuilder<? extends ISpaceReplicaData> replicaDataProducerBuilder,
            ISpaceCopyReplicaOutFilter outFilter) {
        _replicationNode = replicationNode;
        _replicaDataProducerBuilder = replicaDataProducerBuilder;
        _outFilter = outFilter;
        _isFiltered = (_outFilter != null);
        _lock = new ReentrantLock();
        if (_replicaDataProducerBuilder != null && (_replicaDataProducerBuilder instanceof SpaceReplicaDataProducerBuilder))
            _spaceEngine = ((SpaceReplicaDataProducerBuilder)(_replicaDataProducerBuilder)).getSpaceEngine();
        else
            _spaceEngine = null;

    }

    public int newReplicaRequest(String requesterLookupName,
                                              ReplicaRequestPacket replicaRequestPacket) {

        _lock.lock();
        try {
            String channelName = XapExtensions.getInstance().getReplicationUtils().toChannelName(requesterLookupName);

            if (_logger.isLoggable(Level.FINE))
                _logger.fine(_replicationNode.getLogPrefix()
                        + "new incoming replica request from "
                        + channelName + ", request details "
                        + replicaRequestPacket);

            String groupName = replicaRequestPacket.getGroupName();

            _replicationNode.onNewReplicaRequest(groupName, channelName, replicaRequestPacket.isSynchronizeRequest());

            // Handle synchronize request (recovery)
            if (replicaRequestPacket.isSynchronizeRequest()) {
                IReplicationSourceGroup sourceGroup = _replicationNode.getReplicationSourceGroup(groupName);
                // Check for already existing replica request from this source name,
                // if so remove it.
                for (Iterator<Entry<Object, ReplicaRequestData>> iterator = _activeReplicaProcesses.entrySet()
                        .iterator(); iterator.hasNext(); ) {
                    Entry<Object, ReplicaRequestData> next = iterator.next();
                    ReplicaRequestData requestData = next.getValue();
                    if (requestData.isSynchronizeReplica()
                            && requestData.getGroupName().equals(groupName)
                            && requestData.getOriginLookupName()
                            .equals(channelName)) {
                        sourceGroup.stopSynchronization(channelName);
                        iterator.remove();
                    }
                }

                boolean syncListRecovery = isDirectPersistencySyncReplicaRequest(replicaRequestPacket);

                sourceGroup.beginSynchronizing(channelName,
                        replicaRequestPacket.getSourceUniqueId(),
                        syncListRecovery);

                // handle receiving sync list from backup by chunks
                if (syncListRecovery)
                    handleSyncList(replicaRequestPacket);
            }


            int contextId = _lastContextId++;
            _activeReplicaProcesses.put(contextId,
                    new ReplicaRequestData(groupName,
                            channelName,
                            _replicaDataProducerBuilder.createProducer(replicaRequestPacket.getParameters(),
                                    contextId),
                            replicaRequestPacket.isSynchronizeRequest()));
            // We use integer as ID, all external components are ambivalent to that
            // and use object
            if (_logger.isLoggable(Level.FINER))
                _logger.finer(_replicationNode.getLogPrefix()
                        + "new replica request from " + channelName
                        + ", received context id " + contextId);
            return contextId;
        }
        finally
        {
            _lock.unlock();
        }
    }

    public IReplicationGroupBacklog getGroupBacklogByRequestContext(Object requestContext) {
        _lock.lock();
        try {
            ReplicaRequestData replicaRequestData = _activeReplicaProcesses.get(requestContext);
            IReplicationSourceGroup replicationSourceGroup = _replicationNode.getReplicationSourceGroup(replicaRequestData.getGroupName());
            return replicationSourceGroup.getGroupBacklog();
        }
        finally
        {
            _lock.unlock();
        }

    }

    private void handleSyncList(ReplicaRequestPacket replicaRequestPacket) {
        SpaceCopyReplicaParameters replicaParams = (SpaceCopyReplicaParameters) replicaRequestPacket.getParameters();
        List<String> syncList = null;
        try {
            syncList = fetchSynchronizationList(replicaParams);
            replicaParams.setSyncList(syncList);
        } catch (RemoteException e) {
            throw new ReplicaFailFetchingSynchronizationListException(e);
        }
    }

    private boolean isDirectPersistencySyncReplicaRequest(ReplicaRequestPacket replicaRequestPacket) {
        return replicaRequestPacket.getParameters() instanceof SpaceCopyReplicaParameters
                && ((SpaceCopyReplicaParameters) replicaRequestPacket.getParameters()).getSynchronizationListFetcher() != null;
    }

    private List<String> fetchSynchronizationList(SpaceCopyReplicaParameters params) throws RemoteException {
        List<String> syncList = new LinkedList<String>();
        int batchSize = Integer.getInteger(SystemProperties.REPLICATION_BLOBSTORE_SYNC_LIST_BATCH_SIZE,
                SystemProperties.REPLICATION_BLOBSTORE_SYNC_LIST_BATCH_SIZE_DEFAULT);
        DirectPersistencySyncListFetcher fetcher = params.getSynchronizationListFetcher();
        if (_logger.isLoggable(Level.FINEST)) {
            _logger.finest(_replicationNode.getLogPrefix() + "fetching sync list from backup, batch size is " + batchSize);
        }
        DirectPersistencySyncListBatch batch;
        do {
            batch = fetcher.fetchBatch();
            syncList.addAll(batch.getBatch());
        }
        while (batch.size() == batchSize);

        return syncList;
    }

    public Collection<ISpaceReplicaData> getNextReplicaBatch(
            final Object context, int batchSize) {
        _lock.lock();
        try {
            if (_logger.isLoggable(Level.FINEST))
                _logger.finest(_replicationNode.getLogPrefix() + "context ["
                        + context + "] get next replica batch request");
            final ReplicaRequestData replicaData = getReplicaContext(context);
            final ISynchronizationCallback syncCallback = new ISynchronizationCallback() {
                public boolean synchronizationDataGenerated(ISpaceReplicaData data) {
                    // If this is a synchronize replica, we need to notify the
                    // corresponding
                    // replication source group of the creation of this replica data
                    if (replicaData.isSynchronizeReplica()) {
                        IReplicationSourceGroup sourceGroup = _replicationNode.getReplicationSourceGroup(replicaData.getGroupName());
                        boolean duplicateUid = sourceGroup.synchronizationDataGenerated(replicaData.getOriginLookupName(),
                                data.getUid());

                        // handles objects that were already recovered
                        // this can happen because the data set is not fully locked
                        // while recovering
                        if (duplicateUid) {
                            if (_logger.isLoggable(Level.FINEST))
                                _logger.finest(_replicationNode.getLogPrefix()
                                        + "context [" + context
                                        + "] filtered replica data [" + data
                                        + "] due to duplicate uid ["
                                        + data.getUid() + "]");
                        }
                        return duplicateUid;

                    }

                    return false;
                }
            };

            SpaceReplicaBatch result = new SpaceReplicaBatch(batchSize);
            boolean isFifoBatch = false;
            while (result.size() < batchSize) {
                ISpaceReplicaData data = replicaData.getProducer().produceNextData(syncCallback);
                if (data == null)
                    break;
                if (_isFiltered && data.supportsReplicationFilter()) {
                    IReplicationFilterEntry filterEntry = replicaData.getProducer()
                            .toFilterEntry(data);
                    _outFilter.filterOut(filterEntry,
                            replicaData.getOriginLookupName());
                    // If filtered, we continue without adding this data
                    if (filterEntry.isDiscarded())
                        continue;
                }
                // We add a packet to the batch
                result.add(data);
                if(!isFifoBatch){
                    isFifoBatch = isFifoType(data);
                }

            }


            if(isFifoBatch){
                result.setFifoId(replicaData.nextFifoBatchId());
            }

            if (!result.isEmpty() && _logger.isLoggable(Level.FINEST))
                _logger.finest(_replicationNode.getLogPrefix()
                        + "context [" + context
                        + "] returning batch " + result);

            return result;
        }
        finally
        {
            _lock.unlock();
        }
    }

    private boolean isFifoType(ISpaceReplicaData data) {
        if(data.isEntryReplicaData()) {
            CacheManager cacheManager = _spaceEngine.getCacheManager();

            IServerTypeDesc serverTypeDesc = cacheManager.getTypeManager()
                    .getServerTypeDesc(((AbstractEntryReplicaData) data).getEntryPacket().getTypeName());

            TypeData typeData = cacheManager.getTypeData(serverTypeDesc);
            return typeData.isFifoSupport() || (typeData.getFifoGroupingIndex() != null);
        }
        return false;
    }

    private ReplicaRequestData getReplicaContext(Object context) {
        ReplicaRequestData replicaData = _activeReplicaProcesses.get(context);
        if (replicaData == null)
            throw new IllegalArgumentException("Context [" + context
                    + "] is not valid");
        replicaData.touch();
        return replicaData;
    }

    public CurrentStageInfo nextReplicaState(Object context) {
        ReplicaRequestData replicaData = getReplicaContext(context);

        CurrentStageInfo currentStage = replicaData.getProducer().nextReplicaStage();
        if (currentStage.isLastStage()) {
            if (_logger.isLoggable(Level.FINER))
                _logger.finer(_replicationNode.getLogPrefix() + "context ["
                        + context + "] all stages completed, closing request");
            replicaData.getProducer().close(false /*forced*/);

            //Clear sync list in direct persistency when recovery is over
            if (_replicationNode.getDirectPesistencySyncHandler() != null) {
                if (_logger.isLoggable(Level.FINER))
                    _logger.finer(_replicationNode.getLogPrefix() + "clearing direct persistency sync list");
                _replicationNode.getDirectPesistencySyncHandler().afterRecovery();
            }

            //Notify backlog synchronization is over
            if (replicaData.isSynchronizeReplica())
                _replicationNode.getReplicationSourceGroup(replicaData.getGroupName()).synchronizationCopyStageDone(replicaData.getOriginLookupName());
            _activeReplicaProcesses.remove(context);
        } else if (_logger.isLoggable(Level.FINER))
            _logger.finer(_replicationNode.getLogPrefix() + "context ["
                    + context + "] completed stage [" + currentStage.getStageName() + "] moving to next stage [" + currentStage.getNextStageName() + "]");
        return currentStage;
    }


    public void clearStaleReplicas(long expirationTime) {

        if (_spaceEngine != null && _spaceEngine.getLeaseManager().isCurrentLeaseReaperThread())
        {
            if (!_lock.tryLock()) {
                if (_logger.isLoggable(Level.FINER))
                    _logger.finer("LeaseManager thread-->clearStaleReplicas: No need in  clearing stale replicas done by shutdowm thread");
                return;
            }
        }
        else
        {
            _lock.lock();
        }
        try {
            if (_logger.isLoggable(Level.FINER))
                _logger.finer("clearing stale replicas, last touched before "
                        + expirationTime);

            clearReplicas(expirationTime);
        }
        finally
        {
            _lock.unlock();
        }
    }


    private void clearReplicas(long forcedExpirationTime) {
        _lock.lock();
        try {
            for (Iterator<Entry<Object, ReplicaRequestData>> iterator = _activeReplicaProcesses.entrySet()
                    .iterator(); iterator.hasNext(); ) {
                Entry<Object, ReplicaRequestData> entry = iterator.next();
                ReplicaRequestData replicaRequestData = entry.getValue();
                if (forcedExpirationTime == 0L || replicaRequestData.getLastTouched() <= forcedExpirationTime) {
                    if (forcedExpirationTime != 0L && _logger.isLoggable(Level.FINER))
                        _logger.finer("clearing stale replica [" + entry.getKey()
                                + "]" + replicaRequestData);
                    if (forcedExpirationTime == 0L)
                        _logger.warning("forced clearing of replica [" + entry.getKey()
                                + "]" + replicaRequestData);

                    try {
                        ISingleStageReplicaDataProducer.CloseStatus closeStatus = replicaRequestData.getProducer().close(forcedExpirationTime == 0L);
                        // if closeStatus is CLOSING, another thread is trying to close the producer, skipping to prevent deadlock @GS-11850
                        if (closeStatus == ISingleStageReplicaDataProducer.CloseStatus.CLOSING) {
                            continue;
                        }
                        iterator.remove();
                        // If this is a synchronize replica, we signal the source
                        // group that it should no longer
                        // keep synchronization state for this member
                        if (replicaRequestData.isSynchronizeReplica()) {
                            IReplicationSourceGroup sourceGroup = _replicationNode.getReplicationSourceGroup(replicaRequestData.getGroupName());
                            sourceGroup.stopSynchronization(replicaRequestData.getOriginLookupName());
                        }
                    } catch (Exception e) {
                        if (_logger.isLoggable(Level.WARNING))
                            _logger.log(Level.WARNING,
                                    "error while clearing stale/forced replica ["
                                            + entry.getKey() + "]"
                                            + replicaRequestData,
                                    e);
                    }
                }
            }
        }
        finally
        {
            _lock.unlock();
        }
    }


    public void close() {
        clearReplicas(0L);  //forced clearing any ongoing replica
    }


    public String dumpState() {
        StringBuilder dump = new StringBuilder("Active outgoing replicas: ");
        Set<Entry<Object, ReplicaRequestData>> entrySet = _activeReplicaProcesses.entrySet();
        if (entrySet.isEmpty()) {
            dump.append(StringUtils.NEW_LINE);
            dump.append("NONE");
        }
        for (Entry<Object, ReplicaRequestData> entry : entrySet) {
            dump.append(StringUtils.NEW_LINE);
            dump.append("Context id [" + entry.getKey() + "]: "
                    + entry.getValue());
        }
        return dump.toString();
    }

    public DirectPersistencyBackupSyncIteratorHandler getDirectPersistencyBackupSyncIteratorHandler() {
        return _directPersistencyBackupSyncIteratorHandler;
    }

    public void setDirectPersistencyBackupSyncIteratorHandler(DirectPersistencyBackupSyncIteratorHandler _directPersistencyBackupSyncIteratorHandler) {
        this._directPersistencyBackupSyncIteratorHandler = _directPersistencyBackupSyncIteratorHandler;
    }

    public SpaceReplicaFifoBatchesHandler getFifoBatchesHandler() {
        return _fifoBatchesHandler;
    }

    public void initFifoBatchesHandler() {
        this._fifoBatchesHandler = new SpaceReplicaFifoBatchesHandler(_replicationNode);
        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine(_replicationNode.getLogPrefix() + "created fifoBatchesHandler");
        }
    }

    public void clearFifoBatchesHandler() {
        this._fifoBatchesHandler = null;
        if (_logger.isLoggable(Level.FINE)) {
            _logger.fine(_replicationNode.getLogPrefix() + "cleared fifoBatchesHandler");
        }
    }

    private static class ReplicaRequestData {
        private final String _groupName;
        private final String _originLookupName;
        private final ISpaceReplicaDataProducer<? extends ISpaceReplicaData> _producer;
        private final boolean _synchronizeReplica;
        private volatile long _lastTouched;
        private int _fifoIdGenerator = 0;

        public ReplicaRequestData(
                String groupName,
                String originLookupName,
                ISpaceReplicaDataProducer<? extends ISpaceReplicaData> producer,
                boolean synchronizeReplica) {
            _groupName = groupName;
            _originLookupName = originLookupName;
            _producer = producer;
            _synchronizeReplica = synchronizeReplica;
            _lastTouched = SystemTime.timeMillis();
        }

        public void touch() {
            _lastTouched = SystemTime.timeMillis();
        }

        public long getLastTouched() {
            return _lastTouched;
        }

        public String getOriginLookupName() {
            return _originLookupName;
        }

        public String getGroupName() {
            return _groupName;
        }

        public ISpaceReplicaDataProducer getProducer() {
            return _producer;
        }

        public boolean isSynchronizeReplica() {
            return _synchronizeReplica;
        }

        public int nextFifoBatchId() {
            return ++_fifoIdGenerator;
        }

        @Override
        public String toString() {
            return "Origin [" + getOriginLookupName() + "] isSynchronize ["
                    + isSynchronizeReplica() + "] Synchronize group name ["
                    + getGroupName() + "] Last touched time ["
                    + getLastTouched() + "]" + StringUtils.NEW_LINE
                    + "\tProducer state: " + getProducer().dumpState();
        }
    }

}
