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

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.ReplicationInContext;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderReliableAsyncBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.globalorder.GlobalOrderReliableAsyncKeptDiscardedOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.IReliableAsyncState;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.ReliableAsyncHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.backlog.reliableasync.SynchronizeMissingPacketsHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroup;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.groups.reliableasync.IReplicationReliableAsyncMediator;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketDataConsumer;
import com.gigaspaces.internal.cluster.node.impl.packets.data.operations.AbstractReplicationPacketSingleEntryData;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationProcessLogExceptionHandler;
import com.gigaspaces.internal.cluster.node.impl.processlog.reliableasync.IReplicationReliableAsyncKeeperTargetProcessLog;
import com.gigaspaces.internal.server.storage.ICustomTypeDescLoader;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.utils.ConditionLatch;
import com.gigaspaces.internal.utils.ConditionLatch.Predicate;
import com.j_spaces.core.exception.internal.ReplicationInternalSpaceException;

import java.util.List;
import java.util.concurrent.TimeUnit;



@com.gigaspaces.api.InternalApi
public class GlobalOrderReliableAsyncTargetKeeperProcessLog
        extends GlobalOrderTargetProcessLog
        implements IReplicationReliableAsyncKeeperTargetProcessLog {

    private final IReplicationReliableAsyncMediator _mediator;
    private final ReplicationInContext _replicationInContext;

    public GlobalOrderReliableAsyncTargetKeeperProcessLog(
            GlobalOrderProcessLogConfig processLogConfig,
            IReplicationPacketDataConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade,
            String name,
            String groupName, String sourceLookupName, boolean oneWayMode,
            IReplicationGroupHistory groupHistory, IReplicationReliableAsyncMediator mediator) {
        super(processLogConfig,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                oneWayMode,
                groupHistory);
        _mediator = mediator;
        _replicationInContext = createReplicationInContext();
    }

    public GlobalOrderReliableAsyncTargetKeeperProcessLog(
            GlobalOrderProcessLogConfig processLogConfig,
            IReplicationPacketDataConsumer<?> dataConsumer,
            IReplicationProcessLogExceptionHandler exceptionHandler,
            IReplicationInFacade replicationInFacade,
            String name,
            String groupName, String sourceLookupName, long lastProcessedKey, boolean isFirstHandshake, boolean oneWayMode,
            IReplicationGroupHistory groupHistory, IReplicationReliableAsyncMediator mediator) {
        super(processLogConfig,
                dataConsumer,
                exceptionHandler,
                replicationInFacade,
                name,
                groupName,
                sourceLookupName,
                lastProcessedKey,
                isFirstHandshake,
                oneWayMode,
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
        // We should clone data before filter since we are using that data for
        // the reliable async
        return true;
    }

    @Override
    protected void afterSuccessfulConsumption(String sourceLookupName,
                                              IReplicationOrderedPacket packet) {
        super.afterSuccessfulConsumption(sourceLookupName, packet);

        // After successful consumption we add the packet to the reliable async
        // source group
        if (packet instanceof GlobalOrderReliableAsyncKeptDiscardedOrderedPacket)
            packet = ((GlobalOrderReliableAsyncKeptDiscardedOrderedPacket) packet).getBeforeFilterPacket();

        _mediator.reliableAsyncSourceAdd(sourceLookupName, packet);
        _mediator.performCompaction();
    }

    @Override
    public void processHandshakeIteration(final String sourceMemberName,
                                          IHandshakeIteration handshakeIteration) {
        if (handshakeIteration instanceof SynchronizeMissingPacketsHandshakeIteration) {
            final IReplicationSourceGroup sourceGroup = getReplicationInFacade().getReplicationSourceGroup(getGroupName());
            sourceGroup.createTemporaryChannel(sourceMemberName, null);
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
                loadTypeDecIfNeeded(packet);
                _mediator.reliableAsyncSourceKeep(sourceMemberName, packet);
            }
        }
    }

    private void loadTypeDecIfNeeded(IReplicationOrderedPacket packet) {
        if(packet.getData() instanceof AbstractReplicationPacketSingleEntryData){
            IEntryData mainEntryData = ((AbstractReplicationPacketSingleEntryData) packet.getData()).getMainEntryData();
            if(mainEntryData instanceof ICustomTypeDescLoader){
                ((ICustomTypeDescLoader) mainEntryData).loadTypeDescriptor(_dataConsumer.getTypeManager());
            }
        }
    }

    @Override
    public GlobalOrderProcessLogHandshakeResponse performHandshake(
            String memberName, IBacklogHandshakeRequest handshakeRequest)
            throws IncomingReplicationOutOfSyncException {
        GlobalOrderProcessLogHandshakeResponse response = super.performHandshake(memberName,
                handshakeRequest);

        final IReliableAsyncState reliableAsyncState = getReliableAsyncStateFromHandshakeRequest(handshakeRequest);
        //Backward if source version is primary which is in older version and has no mirror or gateway it will not
        //be a reliable async replication group, we need to still work with it as if we have no reliable async replication
        if (reliableAsyncState == null)
            throw new IllegalStateException("Trying to execute handshake with reliable async target keeper from source which is not in reliable async mode");

        response = new GlobalOrderReliableAsyncKeeperProcessLogHandshakeResponse(response);

        // We need to adjust the state of the source group backlog according to
        // the response
        _mediator.afterHandshake(response);
        // Update reliable async state
        _mediator.updateReliableAsyncState(reliableAsyncState, memberName);
        return response;
    }

    protected IReliableAsyncState getReliableAsyncStateFromHandshakeRequest(
            IBacklogHandshakeRequest handshakeRequest) {
        if (!(handshakeRequest instanceof GlobalOrderReliableAsyncBacklogHandshakeRequest))
            return null;

        GlobalOrderReliableAsyncBacklogHandshakeRequest typedHandshakeRequest = (GlobalOrderReliableAsyncBacklogHandshakeRequest) handshakeRequest;
        return typedHandshakeRequest.getReliableAsyncState();
    }

    @Override
    protected boolean canResetState() {
        return false;
    }

}
