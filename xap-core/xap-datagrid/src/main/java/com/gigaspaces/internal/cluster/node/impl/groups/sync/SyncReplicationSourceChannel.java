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

package com.gigaspaces.internal.cluster.node.impl.groups.sync;

import com.gigaspaces.cluster.replication.ReplicationException;
import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.AbstractReplicationSourceChannel;
import com.gigaspaces.internal.cluster.node.impl.groups.CompletedFuture;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationChannelDataFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationGroupHistory;
import com.gigaspaces.internal.cluster.node.impl.groups.IReplicationSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.processlog.ReplicationConsumeTimeoutException;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandler;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;
import com.gigaspaces.logger.LogLevel;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationOperatingMode;
import com.j_spaces.kernel.JSpaceUtilities;

import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;



@com.gigaspaces.api.InternalApi
public class SyncReplicationSourceChannel
        extends AbstractReplicationSourceChannel {

    private final IReplicationGroupBacklog _groupBacklog;
    private final IReplicationThrottleController _throttleController;
    private final IAsyncHandlerProvider _asyncHandlerProvider;
    private final long _idleDelayMilis;
    private volatile boolean _syncState;
    private final int _asyncStateBatchSize;
    private final AsyncDispatcher _asyncDispatcher;
    private final IAsyncHandler _asyncHandler;
    private final Object _operatingModeLock = new Object();
    // Not volatile on purpose
    private IMarker _asyncMinimalCompletionMarker;
    private IMarker _beginOfSyncStateMarker;
    private volatile Throwable _unresolvedError;

    public SyncReplicationSourceChannel(
            DynamicSourceGroupConfigHolder groupConfig, String groupName,
            String memberName, IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationGroupBacklog groupBacklog,
            IReplicationOutFilter outFilter,
            IReplicationThrottleController throttleController,
            IAsyncHandlerProvider asyncHandlerProvider,
            int asyncStateBatchSize, long idleDelayMilis,
            IReplicationChannelDataFilter dataFilter,
            IReplicationSourceGroupStateListener stateListener,
            IReplicationGroupHistory groupHistory, ReplicationMode channelType,
            Object customBacklogMetadata) {
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
                null /*tag*/);
        _groupBacklog = groupBacklog;
        _throttleController = throttleController;
        _asyncHandlerProvider = asyncHandlerProvider;
        _asyncStateBatchSize = asyncStateBatchSize;
        _idleDelayMilis = idleDelayMilis;
        _syncState = true;
        // After all is initialized we can let the super class to start since it
        // will perform operations that can delegate onConnected/onDisconnected
        // events
        start();

        _asyncDispatcher = new AsyncDispatcher();
        _asyncHandler = _asyncHandlerProvider.start(_asyncDispatcher,
                _idleDelayMilis,
                "SyncChannelAsyncState-"
                        + getMyLookupName()
                        + "."
                        + getGroupName()
                        + "."
                        + getMemberName(),
                false);
    }

    public Future<?> executeAsync(ISyncReplicationGroupOutContext context) {
        // If not in sync state we do not send the replication but we throttle
        // the replication rate
        if (!_syncState) {
            // Delegate to async state, if denies than we should execute sync
            if (delegateToAsyncRunner(context.size()))
                return CompletedFuture.INSTANCE;
        }
        try {
            if (context.isSinglePacket()) {
                IReplicationOrderedPacket packet = context.getSinglePacket();
                return replicateAsync(packet);
            } else {
                final List<IReplicationOrderedPacket> packets = context.getOrderedPackets();
                return replicateAsync(packets);
            }
        } catch (RemoteException e) {
            if (_specificLogger.isDebugEnabled()) {
                _specificLogger.debug(
                        "Caught remote exception during synchronous execution",
                        e);
            }
            // Do nothing, async dispatcher will send this packets
            moveToAsyncIfNeeded();
            return CompletedFuture.INSTANCE;
        } catch (RuntimeException unexpectedException) {
            if (_specificLogger.isErrorEnabled()) {
                _specificLogger.error(
                        "Error in replication while replicating ["
                                + ReplicationLogUtils.packetsToLogString(context)
                                + "], the replication will be resent until the error is resolved",
                        unexpectedException);
            }
            moveToAsyncIfNeeded();
            throw unexpectedException;
        } catch (Error unexpectedError) {
            if (_specificLogger.isErrorEnabled()) {
                _specificLogger.error(
                        "Error in replication while replicating ["
                                + ReplicationLogUtils.packetsToLogString(context)
                                + "], the replication will be resent until the error is resolved",
                        unexpectedError);
            }
            moveToAsyncIfNeeded();
            throw unexpectedError;
        }
    }

    @Override
    protected void onAsyncReplicateErrorResult(Throwable t,
                                               IReplicationOrderedPacket packet) {
        moveToAsyncIfNeeded();
        if (t instanceof RemoteException)
            return;

        if (_specificLogger.isErrorEnabled()) {
            _specificLogger.error(
                    "Error in replication while replicating ["
                            + packet + "]",
                    t);
        }

    }

    @Override
    protected void onAsyncReplicateErrorResult(Throwable t,
                                               List<IReplicationOrderedPacket> packets) {
        moveToAsyncIfNeeded();
        if (t instanceof RemoteException)
            return;

        if (_specificLogger.isErrorEnabled()) {
            _specificLogger.error(
                    "Error in replication while replication ["
                            + packets + "]",
                    t);
        }

    }

    public int execute(ISyncReplicationGroupOutContext context) {
        // If not in sync state we do not send the replication but we throttle
        // the replication rate
        if (!_syncState) {
            // Delegate to async state, if denies than we should execute sync
            if (delegateToAsyncRunner(context.size()))
                return 0;
        }
        try {
            if (context.isSinglePacket()) {
                IReplicationOrderedPacket packet = context.getSinglePacket();
                return replicate(packet);
            } else {
                final List<IReplicationOrderedPacket> packets = context.getOrderedPackets();
                return replicateBatch(packets);
            }
        } catch (RemoteException e) {
            if (_specificLogger.isDebugEnabled()) {
                _specificLogger.debug(
                        "Caught remote exception during synchronous execution",
                        e);
            }
            // Do nothing, async dispatcher will send this packets
            moveToAsyncIfNeeded();
        } catch (RuntimeException unexpectedException) {
            if (_specificLogger.isErrorEnabled()) {
                _specificLogger.error(
                        "Error in replication while replicating ["
                                + ReplicationLogUtils.packetsToLogString(context)
                                + "]",
                        unexpectedException);
            }
            moveToAsyncIfNeeded(unexpectedException);
        } catch (Error unexpectedError) {
            if (_specificLogger.isErrorEnabled()) {
                _specificLogger.error(
                        "Error in replication while replicating ["
                                + ReplicationLogUtils.packetsToLogString(context)
                                + "]",
                        unexpectedError);
            }
            moveToAsyncIfNeeded(unexpectedError);
            throw unexpectedError;
        } catch (ReplicationException e) {
            LogLevel logLevel = getExceptionLogLevel(JSpaceUtilities.getRootCauseException(e));
            if (logLevel.isEnabled(_specificLogger)) {
                logLevel.log(_specificLogger,
                        "Error in replication while replicating ["
                                + ReplicationLogUtils.packetsToLogString(context)
                                + "]",
                        e);
            }
            moveToAsyncIfNeeded(e.getCause());
        }
        return 0;
    }

    private LogLevel getExceptionLogLevel(Throwable rootCauseException) {
        if (rootCauseException instanceof ReplicationConsumeTimeoutException)
            return LogLevel.WARNING;

        return LogLevel.SEVERE;
    }

    /**
     * @return true if this operation will be executed by the async runner
     */
    private boolean delegateToAsyncRunner(int contextSize) {
        boolean throttleAsChannelActive = isActive()
                && (hasReachedAsyncMinimalCompletionMarker());

        boolean keepThrottling = _throttleController.throttle(_groupBacklog.size(getMemberName()),
                contextSize,
                throttleAsChannelActive);
        // If throttle said we should stop throttling
        // move to sync mode if needed
        if (!keepThrottling)
            return !moveToSyncIfNeeded(true);
        else {
            // Protect from race condition where the async dispatcher has empty
            // backlog
            // and decided to change to sync state, but in between the empty and
            // the change to sync state a packet was
            // inserted to the backlog and the executing thread decided he
            // should continue throttling and hence
            // would leave this packet to the async handler which will not sent
            // it since it is at sync state now
            // TODO write unit test for this scenario?
            synchronized (_operatingModeLock) {
                return !_syncState;
            }
        }
    }

    /**
     * @return true if the state after this operation is sync
     */
    private boolean moveToSyncIfNeeded(boolean fromSyncCaller) {
        synchronized (_operatingModeLock) {
            if (isActive()
                    && !_syncState
                    && (_asyncMinimalCompletionMarker == null || _asyncMinimalCompletionMarker.isMarkerReached())) {
                if (fromSyncCaller) {
                    IMarker currentMarker = _groupBacklog.getCurrentMarker(getMemberName());
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug("moved to sync state from backlog marked position "
                                + currentMarker);
                    _beginOfSyncStateMarker = currentMarker;
                } else {
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug("restored sync state");
                }
                if (_unresolvedError != null) {
                    String msg = "pending error ["
                            + _unresolvedError.getMessage()
                            + "] was resolved, channel synchronous mode is restored";
                    logEventInHistory(msg);
                    if (_specificLogger.isInfoEnabled())
                        _specificLogger.info(msg);
                } else {
                    logEventInHistory("restored sync state");
                }

                _asyncMinimalCompletionMarker = null;
                _unresolvedError = null;

                _syncState = true;
            }

            return _syncState;
        }
    }

    private boolean moveToAsyncIfNeeded() {
        return moveToAsyncIfNeeded(null);
    }

    /**
     * This is synchronized in order to avoid concurrent reconnection state
     *
     * @return true if the state after this operation is async
     */
    private boolean moveToAsyncIfNeeded(Throwable error) {
        synchronized (_operatingModeLock) {
            if (_syncState && !isClosed()) {
                int sampleTPBefore = getSampleTPBefore(10, TimeUnit.SECONDS);
                IMarker currentMarker = getGroupBacklog().getCurrentMarker(getMemberName());
                String msg = "moving to async state (measured TP before state change "
                        + sampleTPBefore
                        + ") minimal async completion marked position "
                        + currentMarker;
                logEventInHistory(msg);
                if (_specificLogger.isDebugEnabled())
                    _specificLogger.debug(msg);
                _throttleController.suggestThroughPut(sampleTPBefore);
                _asyncMinimalCompletionMarker = currentMarker;
                _unresolvedError = error;
                _beginOfSyncStateMarker = null;
                // We will not restore sync state until the async completor have at
                // least passed the current point in the backlog
                _syncState = false;
                if (error != null) {
                    msg = "channel changed to asynchronous mode until it will resolve the error ["
                            + error.getMessage() + "]";
                    logEventInHistory(msg);
                    if (_specificLogger.isInfoEnabled())
                        _specificLogger.info(msg);
                }
            }

            return !_syncState;
        }
    }

    private boolean hasReachedAsyncMinimalCompletionMarker() {
        IMarker asyncMinimalCompletionMarker = _asyncMinimalCompletionMarker;
        if (asyncMinimalCompletionMarker == null)
            return true;

        if (asyncMinimalCompletionMarker.isMarkerReached()) {
            synchronized (_operatingModeLock) {
                if (asyncMinimalCompletionMarker == _asyncMinimalCompletionMarker) {
                    _asyncMinimalCompletionMarker = null;
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    protected void onActiveImpl() {
        if (getGroupBacklog().size(getMemberName()) > 0)
            moveToAsyncIfNeeded();
        if (_asyncHandler != null)
            _asyncHandler.wakeUp();
    }

    @Override
    protected void onDisconnectedImpl() {
        moveToAsyncIfNeeded();
    }

    @Override
    protected void closeImpl() {
        if (_asyncHandler != null)
            _asyncHandler.stop(3, TimeUnit.SECONDS);
    }

    public boolean isSync() {
        return _syncState;
    }

    private List<IReplicationOrderedPacket> getPendingPackets() {
        return super.getPendingPackets(_asyncStateBatchSize);
    }

    public class AsyncDispatcher
            extends AsyncCallable {

        public CycleResult call() {
            // If the channel is disconnected complete iteration as idle
            if (!isActive())
                return CycleResult.IDLE_CONTINUE;

            // we need to check if the channel is out of sync due to
            // deletion
            // and send out of sync notice to target, this can happen if the
            // channel has limited redolog and
            // there were more than redolog size operation inserted before
            // all sent synchrounously to the backup
            IBacklogMemberState state = getGroupBacklog().getState(getMemberName());
            // Handle case where replication is out of sync since backlog
            // was
            // dropped
            if (state.isBacklogDropped())
                return handleBacklogDroppedState(state);

            synchronized (_operatingModeLock) {

                final IMarker currentMarker = _beginOfSyncStateMarker;
                if (currentMarker == null && isSync())
                    return CycleResult.IDLE_CONTINUE;

                // If reached the marker, we can terminate the async runner
                // since there are already sync
                // threads executing the following packets
                if (currentMarker != null && currentMarker.isMarkerReached()) {
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug("async state handler reached its end marker ["
                                + currentMarker + "]");
                    // If channel was synchronizing this will signal it is done
                    if (isSynchronizing()) {
                        try {
                            signalSynchronizingDone();
                            _beginOfSyncStateMarker = null;
                        } catch (RemoteException e) {
                            if (_specificLogger.isDebugEnabled())
                                _specificLogger.debug(
                                        "AsyncDispatcher error while signaling synchronization is done.",
                                        e);
                            // If we got disconnection here, the channel will
                            // restore its async state
                            // and create a new async handler which eventually
                            // will send sync complete
                            // packet since isSynchronizing will remain true
                        }
                    } else
                        _beginOfSyncStateMarker = null;
                    return CycleResult.IDLE_CONTINUE;
                }
            }

            // Get packets to replicate
            List<IReplicationOrderedPacket> packets = getPendingPackets();
            if (packets.isEmpty()) {
                synchronized (_operatingModeLock) {
                    //Double check to protect from a scenario where a packet was inserted after the previous check and a execute method is called
                    //and the caller got that there is still a need to throttle so he thinks the async dispatcher will send his packet and gave up
                    //we need to make sure that a concurrent call to delegateToAsyncRunner in this case will return false if the async handler moved to
                    //sync state or true otherwise.
                    packets = getPendingPackets();
                    if (packets.isEmpty())
                        return moveToSyncStateUponEmptyBacklog(state);
                }
            }

            try {
                // Async state needs to call before delayed replication
                // in order to allow the
                // packets to change their state due to the delay
                replicateBatchDelayed(packets);
                if (_specificLogger.isTraceEnabled())
                    _specificLogger.trace("async state handler replicated "
                            + packets.size() + " packets");
                // Keep the async runner running
                return CycleResult.CONTINUE;
            } catch (RemoteException e) {
                if (_specificLogger.isDebugEnabled())
                    _specificLogger.debug(
                            "AsyncDispatcher cycle error.",
                            e);
                // Do nothing channel is disconnected
                // Keep the async runner running
                return CycleResult.IDLE_CONTINUE;
            } catch (Throwable t) {
                if (_specificLogger.isDebugEnabled()) {
                    _specificLogger.debug(
                            "AsyncDispatcher cycle error while replicating "
                                    + packets
                                    + "."
                                    + StringUtils.NEW_LINE
                                    + getGroupBacklog().toLogMessage(getMemberName()),
                            JSpaceUtilities.getRootCauseException(t));
                }
                return CycleResult.IDLE_CONTINUE;
            }
        }

        private CycleResult moveToSyncStateUponEmptyBacklog(IBacklogMemberState state) {
            if (_specificLogger.isDebugEnabled())
                _specificLogger.debug("async state handler reached end of backlog, backlog state "
                        + state.toLogMessage());
            // No remaining packets, restore sync state
            moveToSyncIfNeeded(false);
            // If channel was synchronizing this will signal it is done
            if (isSynchronizing()) {
                try {
                    signalSynchronizingDone();
                } catch (RemoteException e) {
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug(
                                "AsyncDispatcher error while signaling synchronization is done.",
                                e);
                    // If failed to signal, restore async state
                    moveToAsyncIfNeeded();
                    // We do not know that the packet arrived its
                    // destination,
                    // therefore remain at this state
                    return CycleResult.IDLE_CONTINUE;
                }
            }
            return CycleResult.IDLE_CONTINUE;
        }

        private CycleResult handleBacklogDroppedState(IBacklogMemberState state) {
            try {
                dispatchBacklogDropped(state);
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
    }

    @Override
    public void flushPendingReplication() {
        if (_asyncHandler != null)
            _asyncHandler.wakeUp();
    }

    @Override
    public ReplicationOperatingMode getChannelOpertingMode() {
        if (_syncState)
            return ReplicationOperatingMode.SYNC;
        else
            return ReplicationOperatingMode.ASYNC;
    }

    @Override
    public String onDumpState() {
        return StringUtils.NEW_LINE + "mode ["
                + (isSync() ? "SYNC]" : "ASYNC]");
    }

}
