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

package com.gigaspaces.internal.cluster.node.impl.groups.async;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.cluster.replication.ReplicationException;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.IIdleStateData;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationSourceChannel;
import com.gigaspaces.internal.cluster.node.impl.groups.IAsyncReplicationListener;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupOutContext;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicateFuture;
import com.gigaspaces.internal.cluster.node.impl.groups.ReplicatedDataPacketResource;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IdleStateDataReplicatedPacket;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessResult;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandler;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationOperatingMode;
import com.j_spaces.kernel.JSpaceUtilities;

import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;



@com.gigaspaces.api.InternalApi
public class AsyncReplicationSourceChannel
        extends AbstractReplicationSourceChannel {

    private final int _batchSize;
    private final long _intervalMilis;
    private final IAsyncHandlerProvider _asyncProvider;
    private final Object _pendingCountLock = new Object();
    private final AtomicLong _pendingCount = new AtomicLong();
    private final int _intervalOperations;
    private final Object _asyncDispatcherLifeCycle = new Object();
    private volatile IAsyncHandler _asyncHandler;

    public AsyncReplicationSourceChannel(
            DynamicSourceGroupConfigHolder groupConfig, String groupName,
            String memberName, IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationGroupBacklog groupBacklog,
            IReplicationOutFilter outFilter, int batchSize, long intervalMilis,
            int intervalOperations, IAsyncHandlerProvider asyncHandlerProvider,
            IReplicationChannelDataFilter dataFilter,
            IReplicationSourceGroupStateListener stateListener,
            IReplicationGroupHistory groupHistory, ReplicationMode channelType,
            Object customBacklogMetadata,
            String tag) {
        this(groupConfig,
                groupName,
                memberName,
                replicationRouter,
                connection,
                groupBacklog,
                outFilter,
                batchSize,
                intervalMilis,
                intervalOperations,
                asyncHandlerProvider,
                true,
                dataFilter,
                stateListener,
                groupHistory,
                channelType,
                customBacklogMetadata,
                tag, false);
    }

    public AsyncReplicationSourceChannel(
            DynamicSourceGroupConfigHolder groupConfig, String groupName,
            String memberName, IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationGroupBacklog groupBacklog,
            IReplicationOutFilter outFilter, int batchSize, long intervalMilis,
            int intervalOperations, IAsyncHandlerProvider asyncHandlerProvider,
            IReplicationChannelDataFilter dataFilter,
            IReplicationSourceGroupStateListener stateListener,
            IReplicationGroupHistory groupHistory, ReplicationMode channelType,
            Object customBacklogMetadata,
            String tag, boolean resetTarget) {
        this(groupConfig,
                groupName,
                memberName,
                replicationRouter,
                connection,
                groupBacklog,
                outFilter,
                batchSize,
                intervalMilis,
                intervalOperations,
                asyncHandlerProvider,
                true,
                dataFilter,
                stateListener,
                groupHistory,
                channelType,
                customBacklogMetadata,
                tag, resetTarget);
    }

    public AsyncReplicationSourceChannel(
            DynamicSourceGroupConfigHolder groupConfig, String groupName,
            String memberName, IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationGroupBacklog groupBacklog,
            IReplicationOutFilter outFilter, int batchSize, long intervalMilis,
            int intervalOperations, IAsyncHandlerProvider asyncHandlerProvider,
            boolean autoStart, IReplicationChannelDataFilter dataFilter,
            IReplicationSourceGroupStateListener stateListener,
            IReplicationGroupHistory groupHistory, ReplicationMode channelType,
            Object customBacklogMetadata,
            String tag, boolean resetTarget) {
        super(groupConfig,
                groupName,
                memberName,
                replicationRouter,
                connection,
                groupBacklog,
                outFilter,
                asyncHandlerProvider,
                dataFilter,
                stateListener,
                groupHistory,
                channelType,
                customBacklogMetadata,
                tag, resetTarget);
        _batchSize = batchSize;
        _intervalMilis = intervalMilis;
        _intervalOperations = intervalOperations;
        _asyncProvider = asyncHandlerProvider;
        // After all is initialized we can let the super class to start since it
        // will perform operations that can delegate onConnected/onDisconnected
        // events
        if (autoStart)
            start();
    }

    @Override
    protected void start() {
        super.start();

        _asyncHandler = _asyncProvider.start(new AsyncDispatcher(),
                _intervalMilis,
                "AsyncChannel-"
                        + getMyLookupName() + "."
                        + getGroupName() + "."
                        + getMemberName(),
                false);
    }

    public int getBatchSize() {
        return _batchSize;
    }

    @Override
    protected void onActiveImpl() {
        IAsyncHandler asyncHandler = _asyncHandler;
        if (asyncHandler != null)
            asyncHandler.wakeUp();
    }

    @Override
    protected void onDisconnectedImpl() {
    }

    @Override
    protected void closeImpl() {
        _asyncHandler.stop(3, TimeUnit.SECONDS);
    }

    public void execute(IReplicationGroupOutContext groupContext) {
        // Update pending count in order to see if the async handler should be
        // waken up
        // We only use this counter here and do not reset it when the async
        // handler is waken up
        // automatically do to interval elapsed time in order to simplify code
        // and reduce lock
        // worst case the handler will be work twice.
        long currentPending = _pendingCount.addAndGet(groupContext.getWeight());
        if (currentPending >= _intervalOperations) {
            synchronized (_pendingCountLock) {
                if (_pendingCount.get() == 0)
                    return;
                if (_specificLogger.isTraceEnabled())
                    _specificLogger.trace("Reached interval operations ["
                            + _pendingCount + "/" + _intervalOperations
                            + "], waking up async dispatcher");
                _pendingCount.set(0);
                _asyncHandler.wakeUp();
            }
        }

    }

    protected List<IReplicationOrderedPacket> getPendingPackets() {
        return super.getPendingPackets(getBatchSize());
    }

    /**
     * The async channel is in two states: 1) Idle (the async dispatcher has returned an
     * IDLE_CONTINUE result) 2) Sending: 2.1) The async dispatcher is running (the call method is
     * being executed), it is preparing the the packets for sending, and then dispatch them to the
     * target in asynchronous manner 2.2) The async dispatcher is suspended, waiting to be resumed
     * or waken once the async result is recieved and processed 2.3) onResult is called when the
     * async result is arrived and the result is being processed which in its turn decide whether to
     * go into idle mode or to continue to another sending cycle if there are enough packets
     * pending
     */
    public class AsyncDispatcher
            extends AsyncCallable implements IAsyncReplicationListener {

        private List<IReplicationOrderedPacket> _currentCyclePackets;
        private IIdleStateData _currentCycleIdleStateData;

        public CycleResult call() throws Exception {
            if (!isActive()) {
                if (_specificVerboseLogger.isTraceEnabled())
                    _specificVerboseLogger.trace("AsyncDispatcher idle cycle. Channel is not active");

                return CycleResult.IDLE_CONTINUE;
            }
            // we need to check if the channel is out of sync due to deletion
            // and send out of sync notice to target, this can happen if the
            // channel has limited redolog and
            // there were more than redolog size operation inserted before
            // all sent synchrounously to the backup
            IBacklogMemberState memberState = getGroupBacklog().getState(getMemberName());
            // Handle case where replication is out of sync since backlog was
            // dropped
            if (memberState.isBacklogDropped()) {
                try {
                    dispatchBacklogDropped(memberState);
                } catch (RemoteException e) {
                    if (_specificLogger.isDebugEnabled()) {
                        _specificLogger.debug(
                                "Caught remote exception while asynchronous dispatcher notifies target of backlog dropped",
                                e);
                    }
                } catch (Throwable t) {
                    if (_specificLogger.isErrorEnabled()) {
                        _specificLogger.error(
                                "Error in replication when attempting notify target of backlog dropped",
                                t);
                    }
                }
                return CycleResult.IDLE_CONTINUE;
            }
            _currentCyclePackets = getPendingPackets();

            if (_currentCyclePackets == null || _currentCyclePackets.isEmpty()) {
                if (_specificVerboseLogger.isTraceEnabled())
                    _specificVerboseLogger.trace("AsyncDispatcher idle cycle. No pending packets to replicate.");

                // If this channel is in synchronizing mode, this means it
                // is done
                if (isSynchronizing())
                    signalSynchronizingDone();

                _currentCycleIdleStateData = getGroupBacklog().getIdleStateData(getMemberName(), getTargetLogicalVersion());
                if (_currentCycleIdleStateData != null && !_currentCycleIdleStateData.isEmpty()) {
                    return replicateIdleStateData(_currentCycleIdleStateData);
                }

                return CycleResult.IDLE_CONTINUE;
            }

            if (_specificLogger.isTraceEnabled())
                _specificLogger.trace("AsyncDispatcher cycle. Replicating ["
                        + _currentCyclePackets.size() + "] pending packets.");

            //Make sure resume is not called before suspend in case async invocation ends before the suspend is returned.
            synchronized (_asyncDispatcherLifeCycle) {
                try {
                    replicateBatchDelayedAsync(_currentCyclePackets, this);

                    return CycleResult.SUSPEND;
                } catch (RemoteException e) {
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug(
                                "AsyncDispatcher cycle error.",
                                e);

                    //Free strong reference to packets
                    _currentCyclePackets = null;
                    return CycleResult.IDLE_CONTINUE;
                } catch (Throwable t) {
                    if (_specificLogger.isDebugEnabled()) {
                        _specificLogger.debug(
                                "AsyncDispatcher cycle error while replicating "
                                        + _currentCyclePackets
                                        + "."
                                        + StringUtils.NEW_LINE
                                        + getGroupBacklog().toLogMessage(getMemberName()),
                                t);
                    }

                    //Free strong reference to packets
                    _currentCyclePackets = null;
                    return CycleResult.IDLE_CONTINUE;
                }
            }
        }

        private CycleResult replicateIdleStateData(IIdleStateData idleStateData) {
            synchronized (_asyncDispatcherLifeCycle) {
                try {
                    replicateIdleStateDataAsync(idleStateData, this);

                    return CycleResult.SUSPEND;
                } catch (RemoteException e) {
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug(
                                "AsyncDispatcher idle state cycle error.",
                                e);

                    return CycleResult.IDLE_CONTINUE;
                } catch (Throwable t) {
                    if (_specificLogger.isDebugEnabled()) {
                        _specificLogger.debug(
                                "AsyncDispatcher cycle error while replicating idle state data."
                                        + StringUtils.NEW_LINE
                                        + getGroupBacklog().toLogMessage(getMemberName()),
                                t);
                    }
                    return CycleResult.IDLE_CONTINUE;
                }
            }

        }

        private void replicateIdleStateDataAsync(final IIdleStateData idleStateData, final IAsyncReplicationListener listener) throws RemoteException {
            if (_specificVerboseLogger.isTraceEnabled())
                _specificVerboseLogger.trace("replicating idle state data: "
                        + idleStateData);

            final ReplicatedDataPacketResource replicatedDataPacketResource = _packetsPool.get();
            boolean delegatedToAsync = false;
            try {
                final IdleStateDataReplicatedPacket idleStateDataPacket = replicatedDataPacketResource.getIdleStateDataPacket();
                idleStateDataPacket.setIdleStateData(idleStateData);

                final AsyncFuture<Object> processResultFuture = getConnection().dispatchAsync(idleStateDataPacket);
                final ReplicateFuture resultFuture = new ReplicateFuture();
                processResultFuture.setListener(new AsyncFutureListener<Object>() {
                    public void onResult(AsyncResult<Object> wiredResult) {
                        Throwable error = null;
                        IProcessResult processResult = null;
                        try {
                            Exception exception = wiredResult.getException();
                            if (exception != null)
                                throw exception;
                            processResult = getGroupBacklog().fromWireForm(wiredResult.getResult());

                            getGroupBacklog().processIdleStateDataResult(getMemberName(), processResult, idleStateData);
                            resultFuture.releaseOk();
                        } catch (ReplicationException e) {
                            error = e;
                            resultFuture.releaseError(e);
                        } catch (Throwable t) {
                            error = t;
                            trackPendingErrorIfNeeded(t, idleStateData);
                            resultFuture.releaseError(t);
                        } finally {
                            replicatedDataPacketResource.release();
                            if (listener != null) {
                                if (error != null)
                                    listener.onReplicateFailed(error);
                                else
                                    listener.onReplicateSucceeded(processResult);
                            }
                        }
                    }

                });
                delegatedToAsync = true;
            } finally {
                if (!delegatedToAsync)
                    replicatedDataPacketResource.release();
            }
        }

        private void trackPendingErrorIfNeeded(Throwable t,
                                               IIdleStateData idleStateData) {
            if (t instanceof ReplicationException) {
                getGroupBacklog().setPendingError(getMemberName(), t, idleStateData);
            }
            if (t instanceof RuntimeException) {
                getGroupBacklog().setPendingError(getMemberName(), t, idleStateData);
            }
            if (t instanceof Error) {
                getGroupBacklog().setPendingError(getMemberName(), t, idleStateData);
            }
        }

        @Override
        public void onReplicateFailed(Throwable error) {
            synchronized (_asyncDispatcherLifeCycle) {
                List<IReplicationOrderedPacket> packets = _currentCyclePackets;
                IIdleStateData idleStateData = _currentCycleIdleStateData;
                //Free strong reference to packets
                _currentCyclePackets = null;
                _currentCycleIdleStateData = null;

                if (error instanceof RemoteException) {
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug(
                                "AsyncDispatcher cycle error.",
                                error);

                } else {
                    if (_specificLogger.isDebugEnabled())

                    {
                        _specificLogger.debug(
                                "AsyncDispatcher cycle error while replicating "
                                        + ((packets != null) ? packets : idleStateData)
                                        + "."
                                        + StringUtils.NEW_LINE
                                        + getGroupBacklog().toLogMessage(getMemberName()),
                                JSpaceUtilities.getRootCauseException(error));
                    }

                }
                getHandler().resume();

            }
        }

        @Override
        public void onReplicateSucceeded(IProcessResult processResult) {
            synchronized (_asyncDispatcherLifeCycle) {
                List<IReplicationOrderedPacket> packets = _currentCyclePackets;
                List<IReplicationOrderedPacket> idleStateData = _currentCyclePackets;
                //Free strong reference to packets
                _currentCyclePackets = null;
                _currentCycleIdleStateData = null;

                final long remainingWeight = getGroupBacklog().getWeight(getMemberName());
                // If there are remaining unreplicated packets more than batch
                // size, do another cycle
                // otherwise considered as idle
                if (remainingWeight >= _intervalOperations) {
                    getHandler().resumeNow();
                    return;
                }

                if (processResult != null && processResult instanceof GlobalOrderProcessResult) {
                    GlobalOrderProcessResult typedResult = (GlobalOrderProcessResult) processResult;
                    if (!typedResult.isProcessed() && typedResult.getError() == null) {
                        if (_specificLogger.isDebugEnabled())
                            _specificLogger.debug(
                                    "Replication was not fully processed, probably due to unconsolidated transactions, resuming replication.");
                        getHandler().resumeNow();
                        return;
                    }
                }

                // If this channel is in synchronizing mode, this means it
                // is done
                if (isSynchronizing()) {
                    try {
                        signalSynchronizingDone();
                    } catch (RemoteException e) {
                        if (_specificLogger.isDebugEnabled())
                            _specificLogger.debug(
                                    "AsyncDispatcher cycle error.",
                                    e);

                    } catch (Throwable t) {
                        if (_specificLogger.isErrorEnabled()) {
                            _specificLogger.error(
                                    "AsyncDispatcher cycle error while replicating "
                                            + ((packets != null) ? packets : idleStateData)
                                            + "."
                                            + StringUtils.NEW_LINE
                                            + getGroupBacklog().toLogMessage(getMemberName()),
                                    t);
                        }

                    }

                }
                getHandler().resume();
            }

        }
    }

    @Override
    public void flushPendingReplication() {
        IAsyncHandler asyncHandler = _asyncHandler;
        if (asyncHandler != null)
            asyncHandler.wakeUp();
    }

    @Override
    public ReplicationOperatingMode getChannelOpertingMode() {
        return ReplicationOperatingMode.ASYNC;
    }

    @Override
    protected void onAsyncReplicateErrorResult(Throwable t,
                                               IReplicationOrderedPacket finalPacket) {
    }

    @Override
    protected void onAsyncReplicateErrorResult(Throwable t,
                                               List<IReplicationOrderedPacket> finalPackets) {
    }
}
