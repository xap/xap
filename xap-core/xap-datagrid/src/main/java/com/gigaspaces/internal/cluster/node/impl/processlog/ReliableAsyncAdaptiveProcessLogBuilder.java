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

package com.gigaspaces.internal.cluster.node.impl.processlog;

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.MultiBucketSingleFileHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.multisourcesinglefile.MultiSourceSingleFileBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.IReplicationReliableAsyncMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataBatchConsumer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.async.IReplicationBatchConsumeAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessLogConfig;
import com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile.DistributedTransactionProcessingConfiguration;
import com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile.MultiSourceSingleFileProcessLogBuilder;
import com.gigaspaces.internal.cluster.node.impl.processlog.multisourcesinglefile.MultiSourceSingleFileProcessLogConfig;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.processlog.sync.IReplicationSyncTargetProcessLog;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A process log builder that creates a processing log according to the given target group config
 * process log configuration
 *
 * @author eitany
 * @since 8.0
 */
@com.gigaspaces.api.InternalApi
public class ReliableAsyncAdaptiveProcessLogBuilder
        implements IReplicationProcessLogBuilder {

    private static final String MULTI_SOURCE_MODE = "multi-source";

    private final GlobalOrderProcessLogBuilder _globalOrderBuilder;
    private final MultiBucketSingleFileProcessLogBuilder _multiBucketSingleFileBuilder;
    private final IReplicationPacketDataConsumer<?> _dataConsumer;
    private final MultiSourceSingleFileProcessLogBuilder _multiSourceSingleFileBuilder;
    private final DistributedTransactionProcessingConfiguration _distributedTransactionProcessingConfiguration;


    public ReliableAsyncAdaptiveProcessLogBuilder(
            IReplicationPacketDataBatchConsumer<?> dataConsumer,
            DistributedTransactionProcessingConfiguration distributedTransactionProcessingConfiguration) {
        _dataConsumer = dataConsumer;
        _globalOrderBuilder = new GlobalOrderProcessLogBuilder(dataConsumer);
        _multiBucketSingleFileBuilder = new MultiBucketSingleFileProcessLogBuilder(dataConsumer);
        _multiSourceSingleFileBuilder = new MultiSourceSingleFileProcessLogBuilder(dataConsumer);
        _distributedTransactionProcessingConfiguration = distributedTransactionProcessingConfiguration;
    }

    public IReplicationSyncTargetProcessLog createSyncProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName, IReplicationGroupHistory groupHistory) {
        throw new UnsupportedOperationException();
    }

    public IReplicationAsyncTargetProcessLog createAsyncProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName, IReplicationGroupHistory groupHistory) {
        throw new UnsupportedOperationException();
    }

    public IReplicationBatchConsumeAsyncTargetProcessLog createBatchConsumeAsyncProcessLog(
            TargetGroupConfig groupConfig, IBacklogHandshakeRequest handshakeRequest,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName, IReplicationGroupHistory groupHistory) {
        if (handshakeRequest instanceof MultiBucketSingleFileHandshakeRequest) {
            MultiBucketSingleFileProcessLogConfig processLogConfig = new MultiBucketSingleFileProcessLogConfig();
            processLogConfig.setBucketsCount((short) ((MultiBucketSingleFileHandshakeRequest) handshakeRequest).getBucketsConfirmedKeys().length);
            TargetGroupConfig newGroupConfig = new TargetGroupConfig(groupConfig.getName(),
                    processLogConfig,
                    groupConfig.getGroupChannelType(),
                    groupConfig.getMembersLookupNames());
            newGroupConfig.setChannelCloseTimeout(groupConfig.getChannelCloseTimeout());
            newGroupConfig.setUnbounded(groupConfig.isUnbounded());
            return _multiBucketSingleFileBuilder.createBatchConsumeAsyncProcessLog(newGroupConfig,
                    handshakeRequest,
                    replicationInFacade,
                    exceptionHandler,
                    myLookupName,
                    groupName,
                    sourceLookupName,
                    groupHistory);
        }

        return _globalOrderBuilder.createBatchConsumeAsyncProcessLog(groupConfig,
                handshakeRequest,
                replicationInFacade,
                exceptionHandler,
                myLookupName,
                groupName,
                sourceLookupName,
                groupHistory);
    }

    public IReplicationSyncTargetProcessLog createReliableAsyncKeeperProcessLog(
            TargetGroupConfig groupConfig,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory, IReplicationReliableAsyncMediator mediator, IReplicationSyncTargetProcessLog synchronizingProcessLog) {
        throw new UnsupportedOperationException();
    }

    public IReplicationReliableAsyncTargetProcessLog createReliableAsyncProcessLog(
            TargetGroupConfig groupConfig, IBacklogHandshakeRequest handshakeRequest,
            IReplicationInFacade replicationInFacade,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            String myLookupName, String groupName, String sourceLookupName,
            IReplicationReliableAsyncTargetProcessLog synchronizingProcessLog,
            IReplicationGroupHistory groupHistory, Logger specificLogger) {
        if (handshakeRequest instanceof MultiBucketSingleFileHandshakeRequest) {
            verifyReplicationModeOnDistributedTransactionProcessingConfiguration(specificLogger);

            MultiBucketSingleFileProcessLogConfig processLogConfig = new MultiBucketSingleFileProcessLogConfig();
            processLogConfig.setBucketsCount((short) ((MultiBucketSingleFileHandshakeRequest) handshakeRequest).getBucketsConfirmedKeys().length);
            TargetGroupConfig newGroupConfig = new TargetGroupConfig(groupConfig.getName(),
                    processLogConfig,
                    groupConfig.getGroupChannelType(),
                    groupConfig.getMembersLookupNames());
            newGroupConfig.setChannelCloseTimeout(groupConfig.getChannelCloseTimeout());
            newGroupConfig.setUnbounded(groupConfig.isUnbounded());
            return _multiBucketSingleFileBuilder.createReliableAsyncProcessLog(newGroupConfig,
                    handshakeRequest,
                    replicationInFacade,
                    exceptionHandler,
                    myLookupName,
                    groupName,
                    sourceLookupName,
                    synchronizingProcessLog,
                    groupHistory,
                    specificLogger);
        } else if (handshakeRequest instanceof MultiSourceSingleFileBacklogHandshakeRequest) {
            final MultiSourceSingleFileProcessLogConfig processLogConfig = new MultiSourceSingleFileProcessLogConfig();
            processLogConfig.setConsumeTimeout(_distributedTransactionProcessingConfiguration.getTimeoutBeforePartialCommit());
            processLogConfig.setPendingPacketPacketsIntervalBeforeConsumption(_distributedTransactionProcessingConfiguration.getWaitForOperationsBeforePartialCommit());
            processLogConfig.setMonitorPendingOperationsMemory(_distributedTransactionProcessingConfiguration.isMonitorPendingOperationsMemory());
            final TargetGroupConfig newGroupConfig = new TargetGroupConfig(groupConfig.getName(),
                    processLogConfig,
                    groupConfig.getGroupChannelType(),
                    groupConfig.getMembersLookupNames());
            return _multiSourceSingleFileBuilder.createReliableAsyncProcessLog(newGroupConfig,
                    handshakeRequest,
                    replicationInFacade,
                    exceptionHandler,
                    myLookupName,
                    groupName,
                    sourceLookupName,
                    synchronizingProcessLog,
                    groupHistory,
                    specificLogger);
        }

        verifyReplicationModeOnDistributedTransactionProcessingConfiguration(specificLogger);

        return _globalOrderBuilder.createReliableAsyncProcessLog(groupConfig,
                handshakeRequest,
                replicationInFacade,
                exceptionHandler,
                myLookupName,
                groupName,
                sourceLookupName,
                synchronizingProcessLog,
                groupHistory,
                specificLogger);
    }

    /**
     * Modes other than "multi-source" are calling this method for logging a warning message in a
     * case where distributed transaction processing configuration parameters has been set by the
     * user but "multi-source" was not set in the source side (space configuration).
     */
    private void verifyReplicationModeOnDistributedTransactionProcessingConfiguration(
            Logger specificLogger) {
        if (_distributedTransactionProcessingConfiguration.isOverriden()) {
            if (specificLogger.isWarnEnabled())
                specificLogger.warn(
                        "Distributed transaction processing configuration has been set but replication mode was not set to '" + MULTI_SOURCE_MODE + "' - configuration will be ignored");
        }
    }

    @Override
    public String toString() {
        return "ReliableAsyncAdaptiveProcessLogBuilder [_dataConsumer="
                + _dataConsumer + "]";
    }

}
