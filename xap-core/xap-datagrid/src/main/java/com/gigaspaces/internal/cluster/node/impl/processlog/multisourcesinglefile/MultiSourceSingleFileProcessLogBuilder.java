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

package com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile;

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.IReplicationReliableAsyncMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataBatchConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationBatchConsumeAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderReliableAsyncTargetKeeperProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncTargetProcessLog;
import com.j_spaces.core.ProcessMemoryManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author idan
 * @since 8.0.4
 */
@com.gigaspaces.api.InternalApi
public class MultiSourceSingleFileProcessLogBuilder
        implements IReplicationProcessLogBuilder {

    private final IReplicationPacketDataBatchConsumer<?> _dataConsumer;
    private final IReplicationParticipantsMediator _participantsMediator = new ReplicationParticipantsMediator();

    public MultiSourceSingleFileProcessLogBuilder(
            IReplicationPacketDataBatchConsumer<?> dataConsumer) {
        _dataConsumer = dataConsumer;
    }

    public GlobalOrderTargetProcessLog createSyncProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory) {
        throw new UnsupportedOperationException("Use global order replication processing instead");
    }

    public GlobalOrderTargetProcessLog createSyncOneWayProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory) {
        throw new UnsupportedOperationException("Use global order replication processing instead");
    }

    public GlobalOrderTargetProcessLog createAsyncProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory) {
        throw new UnsupportedOperationException("Use global order replication processing instead");
    }

    @Override
    public GlobalOrderReliableAsyncTargetKeeperProcessLog createReliableAsyncKeeperProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory,
            IReplicationReliableAsyncMediator mediator, IReplicationSyncTargetProcessLog synchronizingProcessLog) {
        if (synchronizingProcessLog == null)
            return new MultiSourceSingleFileReliableAsyncTargetKeeperProcessLog((MultiSourceSingleFileProcessLogConfig) groupConfig.getProcessLogConfig(),
                    _dataConsumer,
                    exceptionHandler,
                    replicationInFacade,
                    myLookupName,
                    groupName,
                    sourceLookupName,
                    false,
                    groupHistory,
                    mediator);

        MultiSourceSingleFileReliableAsyncTargetKeeperProcessLog typedSynchronizingProcessLog = (MultiSourceSingleFileReliableAsyncTargetKeeperProcessLog) synchronizingProcessLog;
        return new MultiSourceSingleFileReliableAsyncTargetKeeperProcessLog((MultiSourceSingleFileProcessLogConfig) groupConfig.getProcessLogConfig(),
                _dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName,
                typedSynchronizingProcessLog.getLastProcessedKey(),
                typedSynchronizingProcessLog.isFirstHandshakeForTarget(),
                false,
                groupHistory,
                mediator);
    }

    @Override
    public MultiSourceSingleFileReliableAsyncTargetProcessLog createReliableAsyncProcessLog(
            TargetGroupConfig groupConfig, IBacklogHandshakeRequest handshakeRequest,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationReliableAsyncTargetProcessLog synchronizingProcessLog,
            IReplicationGroupHistory groupHistory, Logger specificLogger) {
        // Check if we are creating a new process log that replace an existing
        // one or the first creation
        if (synchronizingProcessLog == null)
            return new MultiSourceSingleFileReliableAsyncTargetProcessLog((MultiSourceSingleFileProcessLogConfig) groupConfig.getProcessLogConfig(),
                    _dataConsumer,
                    exceptionHandler,
                    replicationInFacade,
                    myLookupName,
                    groupName,
                    sourceLookupName,
                    true,
                    groupHistory,
                    getParticipantsMediator(),
                    ProcessMemoryManager.INSTANCE);

        // TODO replace with copy constructor
        MultiSourceSingleFileReliableAsyncTargetProcessLog sharedReliableAsyncProcessLog = (MultiSourceSingleFileReliableAsyncTargetProcessLog) synchronizingProcessLog;
        return new MultiSourceSingleFileReliableAsyncTargetProcessLog((MultiSourceSingleFileProcessLogConfig) groupConfig.getProcessLogConfig(),
                _dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName,
                sharedReliableAsyncProcessLog.isFirstHandshakeForTarget(),
                groupHistory,
                getParticipantsMediator(),
                sharedReliableAsyncProcessLog.getLastProcessedKey(),
                sharedReliableAsyncProcessLog.getFirstUnprocessedKey(),
                sharedReliableAsyncProcessLog.getPendingPacketsQueue(),
                sharedReliableAsyncProcessLog.getPacketsByUidHelperMap(),
                ProcessMemoryManager.INSTANCE);
    }

    @Override
    public IReplicationBatchConsumeAsyncTargetProcessLog createBatchConsumeAsyncProcessLog(
            TargetGroupConfig groupConfig, IBacklogHandshakeRequest handshakeRequest,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory) {
        throw new UnsupportedOperationException("Use global order replication processing instead");
    }

    private IReplicationParticipantsMediator getParticipantsMediator() {
        return _participantsMediator;
    }

    @Override
    public String toString() {
        return "MultiSourceSingleFileProcessLogBuilder [_dataConsumer="
                + _dataConsumer + "]";
    }

}
