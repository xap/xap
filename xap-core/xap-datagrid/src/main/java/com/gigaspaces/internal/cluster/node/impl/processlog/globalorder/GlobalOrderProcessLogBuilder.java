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

package com.gigaspaces.internal.cluster.node.impl.processlog.globalorder;

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.IReplicationReliableAsyncMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataBatchConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationBatchConsumeAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncTargetProcessLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@com.gigaspaces.api.InternalApi
public class GlobalOrderProcessLogBuilder
        implements IReplicationProcessLogBuilder {

    protected final IReplicationPacketDataBatchConsumer<?> _dataConsumer;

    public GlobalOrderProcessLogBuilder(
            IReplicationPacketDataBatchConsumer<?> dataConsumer) {
        _dataConsumer = dataConsumer;
    }

    public GlobalOrderTargetProcessLog createSyncProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName, IReplicationGroupHistory groupHistory) {
        return new GlobalOrderTargetProcessLog((GlobalOrderProcessLogConfig) groupConfig.getProcessLogConfig(),
                _dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName, false, groupHistory);
    }

    public GlobalOrderTargetProcessLog createAsyncProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory) {
        return new GlobalOrderTargetProcessLog((GlobalOrderProcessLogConfig) groupConfig.getProcessLogConfig(),
                _dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName, false, groupHistory);
    }

    public GlobalOrderReliableAsyncTargetKeeperProcessLog createReliableAsyncKeeperProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory,
            IReplicationReliableAsyncMediator mediator, IReplicationSyncTargetProcessLog synchronizingProcessLog) {
        if (synchronizingProcessLog == null)
            return new GlobalOrderReliableAsyncTargetKeeperProcessLog((GlobalOrderProcessLogConfig) groupConfig.getProcessLogConfig(),
                    _dataConsumer,
                    exceptionHandler,
                    replicationInFacade,
                    myLookupName,
                    groupName,
                    sourceLookupName,
                    false,
                    groupHistory,
                    mediator);

        GlobalOrderReliableAsyncTargetKeeperProcessLog typedProcessLog = (GlobalOrderReliableAsyncTargetKeeperProcessLog) synchronizingProcessLog;
        return new GlobalOrderReliableAsyncTargetKeeperProcessLog((GlobalOrderProcessLogConfig) groupConfig.getProcessLogConfig(),
                _dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName,
                typedProcessLog.getLastProcessedKey(),
                typedProcessLog.isFirstHandshakeForTarget(),
                false,
                groupHistory,
                mediator);
    }

    public GlobalOrderReliableAsyncTargetProcessLog createReliableAsyncProcessLog(
            TargetGroupConfig groupConfig,
            IBacklogHandshakeRequest handshakeRequest,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationReliableAsyncTargetProcessLog synchronizingProcessLog,
            IReplicationGroupHistory groupHistory, Logger specificLogger) {
        // Check if we are creating a new process log that replace an existing
        // one or the first creation
        if (synchronizingProcessLog == null)
            return new GlobalOrderReliableAsyncTargetProcessLog(_dataConsumer,
                    exceptionHandler,
                    replicationInFacade,
                    myLookupName,
                    groupName,
                    sourceLookupName,
                    groupHistory);

        // TODO replace with copy constructor
        GlobalOrderReliableAsyncTargetProcessLog sharedReliableAsyncProcessLog = (GlobalOrderReliableAsyncTargetProcessLog) synchronizingProcessLog;
        return new GlobalOrderReliableAsyncTargetProcessLog(_dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName,
                sharedReliableAsyncProcessLog.getLastProcessedKey(),
                sharedReliableAsyncProcessLog.isFirstHandshakeForTarget(),
                groupHistory);
    }

    public IReplicationBatchConsumeAsyncTargetProcessLog createBatchConsumeAsyncProcessLog(
            TargetGroupConfig groupConfig, IBacklogHandshakeRequest handshakeRequest,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory) {
        return new GlobalOrderBatchConsumeTargetProcessLog(_dataConsumer,
                exceptionHandler,
                replicationInFacade,
                myLookupName,
                groupName,
                sourceLookupName,
                groupHistory);
    }

    @Override
    public String toString() {
        return "GlobalOrderProcessLogBuilder [_dataConsumer=" + _dataConsumer
                + "]";
    }

}
