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

package com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile;

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.IReplicationReliableAsyncMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataBatchConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationBatchConsumeAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncTargetProcessLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileProcessLogBuilder
        implements IReplicationProcessLogBuilder {

    private final IReplicationPacketDataBatchConsumer<?> _dataConsumer;

    public MultiBucketSingleFileProcessLogBuilder(
            IReplicationPacketDataBatchConsumer<?> dataConsumer) {
        _dataConsumer = dataConsumer;
    }

    public IReplicationSyncTargetProcessLog createSyncProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory) {
        return new MultiBucketSingleFileSyncTargetProcessLog(groupConfig.getProcessLogConfig(),
                _dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName,
                groupHistory);
    }

    public IReplicationAsyncTargetProcessLog createAsyncProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory) {
        // TODO unsupported
        throw new UnsupportedOperationException("Use global order replication processing instead");
    }

    public IReplicationSyncTargetProcessLog createReliableAsyncKeeperProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory,
            IReplicationReliableAsyncMediator mediator, IReplicationSyncTargetProcessLog synchronizingProcessLog) {
        if (synchronizingProcessLog == null)
            return new MultiBucketSingleFileReliableAsyncTargetKeeperProcessLog(groupConfig.getProcessLogConfig(),
                    _dataConsumer,
                    exceptionHandler,
                    replicationInFacade,
                    myLookupName,
                    groupName,
                    sourceLookupName,
                    groupHistory,
                    mediator);

        MultiBucketSingleFileReliableAsyncTargetKeeperProcessLog typedSynchronizingProcessLog = (MultiBucketSingleFileReliableAsyncTargetKeeperProcessLog) synchronizingProcessLog;

        return new MultiBucketSingleFileReliableAsyncTargetKeeperProcessLog(groupConfig.getProcessLogConfig(),
                _dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName,
                typedSynchronizingProcessLog.getLastProcessedKeys(),
                typedSynchronizingProcessLog.getLastGlobalProcessedKeys(),
                typedSynchronizingProcessLog.isFirstHandshakeForTarget(),
                groupHistory,
                mediator);
    }

    public IReplicationReliableAsyncTargetProcessLog createReliableAsyncProcessLog(
            TargetGroupConfig groupConfig, IBacklogHandshakeRequest handshakeRequest,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationReliableAsyncTargetProcessLog synchronizingProcessLog,
            IReplicationGroupHistory groupHistory, Logger specificLogger) {
        // Check if we are creating a new process log that replace an existing
        // one or the first creation
        if (synchronizingProcessLog == null)
            return new MultiBucketSingleFileReliableAsyncTargetProcessLog(groupConfig.getProcessLogConfig(),
                    _dataConsumer,
                    exceptionHandler,
                    replicationInFacade,
                    myLookupName,
                    groupName,
                    sourceLookupName,
                    groupHistory);

        // TODO replace with copy constructor
        MultiBucketSingleFileReliableAsyncTargetProcessLog typedProcessLog = (MultiBucketSingleFileReliableAsyncTargetProcessLog) synchronizingProcessLog;
        return new MultiBucketSingleFileReliableAsyncTargetProcessLog(groupConfig.getProcessLogConfig(),
                _dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName,
                typedProcessLog.getLastGlobalProcessedKey(),
                typedProcessLog.getLastProcessedKeys(),
                typedProcessLog.getLastGlobalProcessedKeys(),
                typedProcessLog.isFirstHandshakeForTarget(),
                groupHistory);
    }

    public IReplicationBatchConsumeAsyncTargetProcessLog createBatchConsumeAsyncProcessLog(
            TargetGroupConfig groupConfig, IBacklogHandshakeRequest handshakeRequest,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory) {
        return new MultiBucketSingleFileBatchConsumeTargetProcessLog(groupConfig.getProcessLogConfig(),
                _dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName,
                groupHistory);
    }

    @Override
    public String toString() {
        return "MultiBucketSingleFileProcessLogBuilder [_dataConsumer="
                + _dataConsumer + "]";
    }

}
