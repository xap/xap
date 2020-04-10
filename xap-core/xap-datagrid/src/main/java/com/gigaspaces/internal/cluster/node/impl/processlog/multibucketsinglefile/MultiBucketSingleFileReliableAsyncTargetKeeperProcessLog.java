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

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationInContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.IMultiBucketSingleFileReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.MultiBucketSingleFileReliableAsyncHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.multibucketsinglefile.MultiBucketSingleFileReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.ReliableAsyncHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.SynchronizeMissingPacketsHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.IReplicationReliableAsyncMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.ProcessLogConfig;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncKeeperTargetProcessLog;
import com.gigaspaces.internal.utils.ConditionLatch;
import com.gigaspaces.internal.utils.ConditionLatch.Predicate;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;

import java.util.List;
import java.util.concurrent.TimeUnit;


//TODO code duplication with corresponding global order class
@com.gigaspaces.api.InternalApi
public class MultiBucketSingleFileReliableAsyncTargetKeeperProcessLog
        extends MultiBucketSingleFileSyncTargetProcessLog implements IReplicationReliableAsyncKeeperTargetProcessLog {

    private final IReplicationReliableAsyncMediator _mediator;
    private final ReplicationInContext _replicationInContext;

    public MultiBucketSingleFileReliableAsyncTargetKeeperProcessLog(
            ProcessLogConfig config,
            IReplicationPacketDataConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName,
            IReplicationGroupHistory groupHistory,
            IReplicationReliableAsyncMediator mediator) {
        super(config,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                groupHistory);
        _mediator = mediator;
        _replicationInContext = createReplicationInContext();
    }

    public MultiBucketSingleFileReliableAsyncTargetKeeperProcessLog(
            ProcessLogConfig config,
            IReplicationPacketDataConsumer dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade, String name,
            String groupName, String sourceLookupName,
            long[] lastProcessedKeys, long[] lastGlobalProcessedKeys,
            boolean firstHandshakeForTarget,
            IReplicationGroupHistory groupHistory,
            IReplicationReliableAsyncMediator mediator) {
        super(config,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                lastProcessedKeys,
                lastGlobalProcessedKeys,
                firstHandshakeForTarget,
                groupHistory);
        _mediator = mediator;
        _replicationInContext = createReplicationInContext();
    }

    @Override
    public ReplicationInContext createReplicationInContext() {
        return new ReplicationInContext(getSourceLookupName(), getGroupName(), _specificLogger, contentRequiredWhileProcessing(), false, getMediator());
    }

    @Override
    public ReplicationInContext getReplicationInContext() {
        return _replicationInContext;
    }

    @Override
    protected boolean contentRequiredWhileProcessing() {
        return true;
    }

    @Override
    public IReplicationReliableAsyncMediator getMediator() {
        return _mediator;
    }

    @Override
    protected boolean shouldCloneOnFilter() {
        //We should clone data before filter since we are using that data for the reliable async
        return true;
    }

    @Override
    protected void afterSuccessfulConsumption(String sourceLookupName, IReplicationOrderedPacket packet) {
        super.afterSuccessfulConsumption(sourceLookupName, packet);

        IMultiBucketSingleFileReplicationOrderedPacket typedPacket = (IMultiBucketSingleFileReplicationOrderedPacket) packet;
        //After successful consumption we add the packet to the reliable async source group
        if (typedPacket.getReliableAsyncBeforeFilterPacket() != null)
            packet = typedPacket.getReliableAsyncBeforeFilterPacket();

        _mediator.reliableAsyncSourceAdd(sourceLookupName, packet);
    }

    @Override
    public void processHandshakeIteration(final String sourceMemberName,
                                          IHandshakeIteration handshakeIteration) {
        if (handshakeIteration instanceof SynchronizeMissingPacketsHandshakeIteration) {
            final IReplicationSourceGroup sourceGroup = getReplicationInFacade().getReplicationSourceGroup(getGroupName());
            sourceGroup.createTemporaryChannel(sourceMemberName, Boolean.FALSE);
            ConditionLatch conditionLatch = new ConditionLatch();
            conditionLatch.timeout(30, TimeUnit.SECONDS).pollingInterval(50, TimeUnit.MILLISECONDS);
            try {
                conditionLatch.waitFor(new Predicate() {
                    @Override
                    public boolean isDone() throws InterruptedException {
                        if (sourceGroup.getGroupBacklog().size(sourceMemberName) == 0)
                            return true;

                        IBacklogMemberState state = sourceGroup.getGroupBacklog().getState(sourceMemberName);
                        if (state.isBacklogDropped()) {
                            if (_specificLogger.isWarnEnabled())
                                _specificLogger.warn("failed completing missing packets from keeper to active primary - backlog is dropped for source, skipping process ");
                            return true;
                        }
                        if (!state.isExistingMember()) {
                            if (_specificLogger.isWarnEnabled())
                                _specificLogger.warn("failed completing missing packets from keeper to active primary - source does not exists in backlog, skipping process ");
                            return true;
                        }
                        if (state.isInconsistent()) {
                            if (_specificLogger.isWarnEnabled())
                                _specificLogger.warn("failed completing missing packets from keeper to active primary, skipping process", state.getInconsistencyReason());
                            return true;
                        }

                        return false;
                    }
                });
            } catch (Exception e) {
                throw new ReplicationInternalSpaceException("Timeout occurred while waiting for keeper to complete missing packets in the primary source");
            } finally {
                sourceGroup.closeTemporaryChannel(sourceMemberName);
            }
        } else {
            ReliableAsyncHandshakeIteration sharedHandshakeIteration = (ReliableAsyncHandshakeIteration) handshakeIteration;
            List<IReplicationOrderedPacket> packets = sharedHandshakeIteration.getPackets();
            for (IReplicationOrderedPacket packet : packets) {
                _mediator.reliableAsyncSourceKeep(sourceMemberName, packet);
            }
        }
    }

    @Override
    public MultiBucketSingleFileHandshakeResponse performHandshake(
            String memberName, IBacklogHandshakeRequest handshakeRequest) throws IncomingReplicationOutOfSyncException {
        MultiBucketSingleFileHandshakeResponse response = super.performHandshake(memberName, handshakeRequest);

        //Backward if source version is primary which is in older version and has no mirror or gateway it will not
        //be a reliable async replication group, we need to still work with it as if we have no reliable async replication
        if (!(handshakeRequest instanceof MultiBucketSingleFileReliableAsyncHandshakeRequest))
            throw new IllegalStateException("Trying to execute handshake with reliable async target keeper from source which is not in reliable async mode");

        MultiBucketSingleFileReliableAsyncHandshakeRequest typedHandshakeRequest = (MultiBucketSingleFileReliableAsyncHandshakeRequest) handshakeRequest;
        MultiBucketSingleFileReliableAsyncState reliableAsyncState = typedHandshakeRequest.getReliableAsyncState();
        //We need to adjust the state of the source group backlog according to the response
        _mediator.afterHandshake(response);
        //Update reliable async state
        _mediator.updateReliableAsyncState(reliableAsyncState, memberName);
        return response;
    }

    @Override
    protected boolean canResetState() {
        return false;
    }
}
