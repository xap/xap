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

package com.gigaspaces.internal.cluster.node.impl.groups;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.cluster.replication.OutgoingReplicationOutOfSyncException;
import com.gigaspaces.cluster.replication.ReplicationException;
import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.StatisticsHolder;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.ConnectChannelHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.ConnectChannelHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeContext;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.packets.BatchReplicatedDataPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.ChannelBacklogDroppedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.ConnectChannelPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IterativeHandshakePacket;
import com.gigaspaces.internal.cluster.node.impl.packets.PingPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.ReplicatedDataPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.UnreliableOperationPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketEntryData;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.globalorder.GlobalOrderProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.multibucketsinglefile.MultiBucketSingleFileProcessResult;
import com.gigaspaces.internal.cluster.node.impl.replica.SynchronizationDonePacket;
import com.gigaspaces.internal.cluster.node.impl.router.ConnectionState;
import com.gigaspaces.internal.cluster.node.impl.router.IConnectionStateListener;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.internal.cluster.node.replica.CannotExecuteSynchronizeReplicaException;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandler;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;
import com.gigaspaces.internal.utils.concurrent.SegmentedAtomicInteger;
import com.gigaspaces.internal.utils.threadlocal.PoolFactory;
import com.gigaspaces.internal.utils.threadlocal.ThreadLocalPool;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;
import com.gigaspaces.metrics.Gauge;
import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationOperatingMode;

import java.rmi.RemoteException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class AbstractReplicationSourceChannel
        implements IReplicationSourceChannel, IConnectionStateListener {
    public enum ChannelState {
        CONNECTED, // The channel is physically connected
        ACTIVE, // The channel is active, finished handshake process
        DISCONNECTED
        // The channel is physically disconnected
    }

    // A logger specific to this channel instance only
    protected final Logger _specificLogger;
    protected final Logger _specificVerboseLogger;

    private final String _groupName;
    private final String _memberName;
    private final IReplicationRouter _replicationRouter;
    private final String _myLookupName;
    private final IReplicationMonitoredConnection _connection;
    private final IReplicationGroupBacklog _groupBacklog;
    private final IReplicationOutFilter _outFilter;
    private final boolean _isOutFiltered;
    private final IAsyncHandlerProvider _asyncHandlerProvider;
    private final IReplicationChannelDataFilter _dataFilter;
    private final boolean _isDataFiltered;
    private final IReplicationSourceGroupStateListener _stateListener;
    private final StatisticsHolder<Integer> _throughPutStatistics;
    private volatile long _totalNumberOfReplicatedPackets;
    private final StatisticsHolder<Long> _generatedTrafficStatistics;
    private long _lastSampledGeneratedTraffic;
    private final StatisticsHolder<Long> _receivedTrafficStatistics;
    private long _lastSampledReceivedTraffic;
    private final IReplicationGroupHistory _groupHistory;
    private final long _inconsistentStateDelay;
    private final int _inconsistentStateRetries;
    private final ReplicationMode _channelType;
    private final Object _customBacklogMetadata;
    private final boolean _isNetworkCompressionEnabled;

    protected final SegmentedAtomicInteger _statisticsCounter = new SegmentedAtomicInteger();
    protected final ThreadLocalPool<ReplicatedDataPacketResource> _packetsPool;

    private volatile ChannelState _channelState = ChannelState.DISCONNECTED;
    private volatile boolean _closed;
    private volatile boolean _synchronizing;
    private volatile boolean _inconsistentDuringHandshakeState;
    private volatile Throwable _inconsistentDuringHandshakeStateReason;
    private volatile int _inconsistentDuringHandshakeStateIteration;
    private volatile ReplicationEndpointDetails _targetEndpointDetails;
    private volatile ConnectionEndpointDetails _delegatorEndpointDetails;
    private volatile boolean _wasEverActive;
    private IAsyncHandler _inconsistentDuringHandshakeStateHandler;
    private IAsyncHandler _iterativeHandshakeHandler;

    public AbstractReplicationSourceChannel(DynamicSourceGroupConfigHolder groupConfig,
                                            String groupName, String memberName,
                                            IReplicationRouter replicationRouter,
                                            IReplicationMonitoredConnection connection,
                                            IReplicationGroupBacklog groupBacklog,
                                            IReplicationOutFilter outFilter,
                                            IAsyncHandlerProvider asyncHandlerProvider,
                                            IReplicationChannelDataFilter dataFilter,
                                            IReplicationSourceGroupStateListener stateListener,
                                            IReplicationGroupHistory groupHistory, ReplicationMode channelType, Object customBacklogMetadata) {
        _groupName = groupName;
        _replicationRouter = replicationRouter;
        _memberName = memberName;
        _stateListener = stateListener;
        _groupHistory = groupHistory;
        _channelType = channelType;
        _customBacklogMetadata = customBacklogMetadata;
        _myLookupName = replicationRouter.getMyLookupName();
        _connection = connection;
        _groupBacklog = groupBacklog;
        _outFilter = outFilter;
        _asyncHandlerProvider = asyncHandlerProvider;
        _dataFilter = dataFilter;
        _isOutFiltered = _outFilter != null;
        _isDataFiltered = getDataFilter() != null;
        SourceGroupConfig config = groupConfig.getConfig();
        _inconsistentStateDelay = config.getInconsistentStateDelay();
        _inconsistentStateRetries = config.getInconsistentStateRetries();
        _specificLogger = ReplicationLogUtils.createOutgoingChannelSpecificLogger(_myLookupName, _memberName, _groupName);
        _specificVerboseLogger = ReplicationLogUtils.createChannelSpecificVerboseLogger(_myLookupName, _memberName, _groupName);
        _throughPutStatistics = new StatisticsHolder<Integer>(50, 0);
        _throughPutStatistics.addSample(SystemTime.timeMillis(), 0);
        _generatedTrafficStatistics = new StatisticsHolder<Long>(50, 0L);
        _receivedTrafficStatistics = new StatisticsHolder<Long>(50, 0L);
        _generatedTrafficStatistics.addSample(SystemTime.timeMillis(), 0L);
        _receivedTrafficStatistics.addSample(SystemTime.timeMillis(), 0L);
        _isNetworkCompressionEnabled = groupConfig.getConfig().isNetworkCompressionEnabled();

        _packetsPool = new ThreadLocalPool<ReplicatedDataPacketResource>(new PoolFactory<ReplicatedDataPacketResource>() {
            public ReplicatedDataPacketResource create() {
                return new ReplicatedDataPacketResource(getGroupName());
            }
        });
    }

    protected void start() {
        wrapConnection();
    }

    public String getGroupName() {
        return _groupName;
    }

    public String getMemberName() {
        return _memberName;
    }

    public String getMyLookupName() {
        return _myLookupName;
    }

    public IReplicationGroupBacklog getGroupBacklog() {
        return _groupBacklog;
    }

    public IReplicationMonitoredConnection getConnection() {
        return _connection;
    }

    public boolean isDataFiltered() {
        return _isDataFiltered;
    }

    public IReplicationChannelDataFilter getDataFilter() {
        return _dataFilter;
    }

    protected void logEventInHistory(String event) {
        _groupHistory.logEvent(getMemberName(), event);
    }

    protected synchronized void wrapConnection() {
        getConnection().setConnectionStateListener(this);
        if (getConnection().getState() == ConnectionState.CONNECTED)
            connectChannel();
        else
            channelDisconnected();
    }

    // Protect from state changing notification while handleIO
    public synchronized void onConnected(boolean newTarget) {
        // If already connected do nothing
        if (_channelState == ChannelState.DISCONNECTED)
            connectChannel();
    }

    // Protect from state changing notification while handleIO
    public synchronized void onDisconnected() {
        // If already disconnected do nothing
        if (_channelState != ChannelState.DISCONNECTED)
            channelDisconnected();
    }

    private synchronized void connectChannel() {
        try {
            _specificLogger.fine("Connection established");

            IBacklogMemberState memberState = getGroupBacklog().getState(getMemberName());
            // Handle case where replication is out of sync since backlog was
            // dropped
            if (memberState.isBacklogDropped()) {
                dispatchBacklogDropped(memberState);
                throw new OutgoingReplicationOutOfSyncException("Replication is out of sync, replication state "
                        + memberState.toLogMessage());
            }

            if (_specificLogger.isLoggable(Level.FINE))
                _specificLogger.fine("Performing handshake " + getConnectionDescription());
            // Create a connect channel packet with handshake details
            IBacklogHandshakeRequest backlogHandshakeRequest = getHandshakeRequest();
            ConnectChannelHandshakeRequest channelHandshakeRequest = new ConnectChannelHandshakeRequest(backlogHandshakeRequest);
            ConnectChannelPacket packet = new ConnectChannelPacket(getGroupName(),
                    _replicationRouter.getMyStubHolder(),
                    channelHandshakeRequest);

            if (_specificLogger.isLoggable(Level.FINE))
                _specificLogger.fine("Sending handshake request {" + backlogHandshakeRequest.toLogMessage() + "}");

            if (_specificLogger.isLoggable(Level.FINER))
                _specificLogger.finer("Backlog state {" + getGroupBacklog().toLogMessage(getMemberName()) + "}");

            // Dispatch the packet and process the handshake details
            Object handshakeResponse = getConnection().dispatch(packet);
            IProcessLogHandshakeResponse processLogHandshakeResponse;
            ConnectChannelHandshakeResponse connectChannelHandshakeResponse;
            //Backward pre 9.0.1
            if (handshakeResponse instanceof IProcessLogHandshakeResponse) {
                processLogHandshakeResponse = (IProcessLogHandshakeResponse) handshakeResponse;
                _targetEndpointDetails = ReplicationEndpointDetails.createBackwardEndpointDetails(getConnection().getFinalEndpointLookupName(), getConnection().getClosestEndpointUniqueId());
            } else {
                connectChannelHandshakeResponse = (ConnectChannelHandshakeResponse) handshakeResponse;
                processLogHandshakeResponse = connectChannelHandshakeResponse.getProcessLogHandshakeResponse();
                _targetEndpointDetails = connectChannelHandshakeResponse.getTargetEndpointDetails();
            }
            _delegatorEndpointDetails = getConnection().getClosestEndpointDetails();

            if (_specificLogger.isLoggable(Level.FINE))
                _specificLogger.fine("Got handshake response {" + processLogHandshakeResponse.toLogMessage() + "}");


            IHandshakeContext handshakeContext = getGroupBacklog().processHandshakeResponse(getMemberName(),
                    backlogHandshakeRequest,
                    processLogHandshakeResponse, getTargetLogicalVersion(),
                    _customBacklogMetadata);

            // Track completed handshakes
            logEventInHistory("Handshake executed:"
                    + StringUtils.NEW_LINE
                    + "\trequest: "
                    + (backlogHandshakeRequest != null ? backlogHandshakeRequest.toLogMessage()
                    : null) + StringUtils.NEW_LINE
                    + "\tresponse: "
                    + (processLogHandshakeResponse != null ? processLogHandshakeResponse.toLogMessage() : null)
                    + StringUtils.NEW_LINE + "\tprocess context: "
                    + handshakeContext.toLogMessage());

            ChannelState prevState = _channelState;
            _channelState = ChannelState.CONNECTED;
            _inconsistentDuringHandshakeState = false;
            _inconsistentDuringHandshakeStateReason = null;
            _inconsistentDuringHandshakeStateIteration = 0;

            Level logLevel = requiresHighLevelLogging() ? Level.INFO : Level.FINE;
            logConnectionStateChange(prevState, ChannelState.CONNECTED, logLevel);

            handleHandshakeResponse(handshakeContext);
        } catch (RemoteException e) {
            // Do nothing, channel remains disconnected
        } catch (Throwable e) {
            if (++_inconsistentDuringHandshakeStateIteration == _inconsistentStateRetries) {
                _inconsistentDuringHandshakeStateIteration = 0;
                dispatchInconsistentStateEvent(e);
            }
            if (_inconsistentDuringHandshakeState) {
                if (_specificLogger.isLoggable(Level.FINER))
                    _specificLogger.log(Level.FINER, "Error occurred while retrying handshake " + getConnectionDescription(), e);
                return;
            }

            if (_specificLogger.isLoggable(Level.SEVERE))
                _specificLogger.log(Level.SEVERE, "Error occurred while performing handshake, replication is disabled until the error is resolved "
                        + getConnectionDescription(), e);
            stopIterativeHandshakeProcess();
            moveToInconsistentState(e);
        }
    }

    protected void dispatchBacklogDropped(IBacklogMemberState memberState)
            throws RemoteException {
        if (_specificLogger.isLoggable(Level.FINE))
            _specificLogger.fine("Channel backlog was dropped, notifying target {" + memberState.toLogMessage() + "}");
        ChannelBacklogDroppedPacket backlogDroppedPacket = new ChannelBacklogDroppedPacket(getGroupName(),
                memberState);
        getConnection().dispatch(backlogDroppedPacket);
    }

    private void dispatchChannelActivated() {
        if (_stateListener == null)
            return;
        _stateListener.onSourceChannelActivated(getGroupName(), getMemberName());
    }

    private void dispatchInconsistentStateEvent(Throwable e) {
        if (_stateListener == null)
            return;
        if (e instanceof BrokenReplicationTopologyException)
            _stateListener.onSourceBrokenReplicationTopology(getGroupName(),
                    getMemberName(),
                    (BrokenReplicationTopologyException) e);

    }

    private void handleHandshakeResponse(IHandshakeContext handshakeContext) {
        // If handshake is considered done, we can change this channel state
        // to active
        if (handshakeContext.isDone()) {
            _specificLogger.fine("Handshake completed - channel is active");

            moveToActive();
        } else {
            if (_specificLogger.isLoggable(Level.FINE))
                _specificLogger.fine("Handshake is incomplete - iterative handshake activated " + handshakeContext.toLogMessage());

            _iterativeHandshakeHandler = _asyncHandlerProvider.start(new IterativeHandshakeHandler(handshakeContext),
                    1,
                    "ChannelHandshakeHandler-"
                            + getMyLookupName()
                            + "."
                            + getGroupName()
                            + "."
                            + getMemberName(),
                    false);
        }
    }

    private void moveToActive() {
        ChannelState prevState = _channelState;
        _channelState = ChannelState.ACTIVE;
        onActiveImpl();

        Level logLevel = (requiresHighLevelLogging() || _wasEverActive) ? Level.INFO : Level.FINE;
        logConnectionStateChange(prevState, ChannelState.ACTIVE, logLevel);

        _wasEverActive = true;

        dispatchChannelActivated();
    }

    private void moveToInconsistentState(Throwable t) {
        onDisconnectedImpl();
        _inconsistentDuringHandshakeStateReason = t;
        _inconsistentDuringHandshakeState = true;
        logEventInHistory("moved to inconsistent state - reason " + t);
        _inconsistentDuringHandshakeStateHandler = _asyncHandlerProvider.start(new InconsistentStateHandler(),
                _inconsistentStateDelay,
                "ChannelInconsistentState-"
                        + getMyLookupName()
                        + "."
                        + getGroupName()
                        + "."
                        + getMemberName(),
                false);
    }

    private void stopIterativeHandshakeProcess() {
        IAsyncHandler iterativeHandshakeHandler = _iterativeHandshakeHandler;
        if (iterativeHandshakeHandler != null) {
            iterativeHandshakeHandler.stop(5, TimeUnit.SECONDS);
            _iterativeHandshakeHandler = null;
        }
    }

    protected abstract void onActiveImpl();

    private synchronized void channelDisconnected() {
        ChannelState prevState = _channelState;
        _channelState = ChannelState.DISCONNECTED;

        stopIterativeHandshakeProcess();

        Level logLevel = (requiresHighLevelLogging() || _wasEverActive) ? Level.INFO : Level.FINE;
        logConnectionStateChange(prevState, ChannelState.DISCONNECTED, logLevel);
        onDisconnectedImpl();
    }

    protected abstract void onDisconnectedImpl();

    private IBacklogHandshakeRequest getHandshakeRequest() {
        return getGroupBacklog().getHandshakeRequest(getMemberName(), _customBacklogMetadata);
    }

    public boolean isActive() {
        return _channelState == ChannelState.ACTIVE && !isClosed();
    }

    public ChannelState getChannelState() {
        return _channelState;
    }

    public boolean pingTarget() {
        if (_channelState == ChannelState.DISCONNECTED || isClosed())
            return false;

        try {
            getConnection().dispatch(new PingPacket());
            return true;
        } catch (RemoteException e) {
            return false;
        }
    }

    public boolean isClosed() {
        return _closed;
    }

    public synchronized void close() {
        if (_closed)
            return;

        if (_specificLogger.isLoggable(Level.FINER))
            _specificLogger.finer("Closing...");

        stopIterativeHandshakeProcess();
        stopInconsistentStateHandler();

        closeImpl();

        getConnection().close();
        _closed = true;

        Level logLevel = requiresHighLevelLogging() ? Level.INFO : Level.FINE;
        if (_specificLogger.isLoggable(logLevel))
            _specificLogger.log(logLevel, "Channel is closed " + getConnectionDescription());
        _specificLogger.finer("Closed");
    }

    private void stopInconsistentStateHandler() {
        IAsyncHandler inconsistentStateHandler = _inconsistentDuringHandshakeStateHandler;
        if (inconsistentStateHandler != null) {
            inconsistentStateHandler.stop(5, TimeUnit.SECONDS);
            _inconsistentDuringHandshakeStateHandler = null;
        }
    }

    protected abstract void closeImpl();

    public synchronized void beginSynchronizing(boolean isDirectPersistencySync)
            throws CannotExecuteSynchronizeReplicaException {
        if (pingTarget())
            throw new CannotExecuteSynchronizeReplicaException("replication group ["
                    + getGroupName()
                    + "] has a connected channel to ["
                    + getMemberName() + "]");
        // TODO protect from invalid call? (call when already synchronizing
        // or
        // when connected?)
        getGroupBacklog().beginSynchronizing(getMemberName(), isDirectPersistencySync);
        _synchronizing = true;
    }

    public synchronized void beginSynchronizing()
            throws CannotExecuteSynchronizeReplicaException {
        beginSynchronizing(false);
    }

    public synchronized void stopSynchronization() {
        if (!_synchronizing)
            return;

        _synchronizing = false;
        getGroupBacklog().stopSynchronization(getMemberName());
    }

    public boolean isSynchronizing() {
        return _synchronizing;
    }

    public boolean synchronizationDataGenerated(String uid) {
        return getGroupBacklog().synchronizationDataGenerated(getMemberName(),
                uid);
    }

    public void synchronizationCopyStageDone() {
        getGroupBacklog().synchronizationCopyStageDone(getMemberName());
    }

    public synchronized void signalSynchronizingDone() throws RemoteException {
        _specificLogger.fine("Signaling to target synchronization is done");
        getConnection().dispatch(new SynchronizationDonePacket(getGroupName()));
        getGroupBacklog().synchronizationDone(getMemberName());
        _synchronizing = false;
    }

    /**
     * If this packets were sent in a delay, i.e not at sync state, this will validate this packets
     * are still relevant and need to be updated or they should be discarded
     *
     * @return a list of processed packets, containing discarded packets instead of packets that are
     * not relevant any more
     */
    private boolean beforeDelayedReplication(
            List<IReplicationOrderedPacket> packets) {
        boolean containsDiscardedPacket = false;
        ListIterator<IReplicationOrderedPacket> iterator = packets.listIterator();
        while(iterator.hasNext()){
            IReplicationOrderedPacket packet = iterator.next();
            // Check if the packet is still relevant, if not replace it with
            // discarded packet
            final boolean stillRelevant = packet.getData()
                    .beforeDelayedReplication();

            if (!stillRelevant) {
                if (_specificLogger.isLoggable(Level.FINER))
                    _specificLogger.finer("Packet [" + packet.toString() + "] discarded by the channel filter");

                iterator.set(getGroupBacklog().replaceWithDiscarded(packet, false));

                if(!containsDiscardedPacket)
                    containsDiscardedPacket = true;

            }

            else if(!containsDiscardedPacket)
                containsDiscardedPacket = packet.isDiscardedPacket();
        }

        return containsDiscardedPacket;
    }

    /**
     * Replicate given packets
     *
     * @return number of completed replications
     */
    protected int replicateBatch(List<IReplicationOrderedPacket> packets)
            throws RemoteException, ReplicationException {
        if (packets == null || packets.isEmpty())
            return 0;

        boolean containsDiscardedPacket = invokeBeforeReplicatingChannelDataFilter(packets);

        return replicateBatchAfterChannelFilter(packets, containsDiscardedPacket);
    }

    protected int replicate(IReplicationOrderedPacket packet)
            throws RemoteException, ReplicationException {
        if (packet == null)
            return 0;

        packet = invokeBeforeReplicatingChannelDataFilter(packet);

        return replicateAfterChannelFilter(packet);
    }

    protected Future replicateAsync(List<IReplicationOrderedPacket> packets)
            throws RemoteException {
        if (packets == null || packets.isEmpty())
            return CompletedFuture.INSTANCE;

        boolean containsDiscardedPacket = invokeBeforeReplicatingChannelDataFilter(packets);

        return replicateBatchAsyncAfterChannelFilter(packets, null, containsDiscardedPacket);
    }

    protected Future replicateAsync(IReplicationOrderedPacket packet)
            throws RemoteException {
        if (packet == null)
            return CompletedFuture.INSTANCE;

        packet = invokeBeforeReplicatingChannelDataFilter(packet);

        return replicateAsyncAfterChannelFilter(packet);
    }

    private int replicateBatchAfterChannelFilter(
            List<IReplicationOrderedPacket> packets, boolean containsDiscardedPacket) throws RemoteException,
            ReplicationException {
        invokeOutputFilterIfNeeded(packets, containsDiscardedPacket);
        if (_specificLogger.isLoggable(Level.FINEST))
            _specificLogger.finest("Replicating filtered packets: " + ReplicationLogUtils.packetsToLogString(packets));

        ReplicatedDataPacketResource replicatedDataPacketResource = _packetsPool.get();
        int replicatedCompleted = 0;
        try {
            replicatedCompleted = dispatchBatchReplicationPacket(packets,
                    replicatedDataPacketResource);
            // Accumulate statistics
            _statisticsCounter.add(packets.size());
        } finally {
            replicatedDataPacketResource.release();
        }
        return replicatedCompleted;
    }

    private int dispatchBatchReplicationPacket(
            List<IReplicationOrderedPacket> packets,
            ReplicatedDataPacketResource replicatedDataPacketResource)
            throws RemoteException, ReplicationException {
        try {
            BatchReplicatedDataPacket batchPacket = replicatedDataPacketResource.getBatchPacket();
            batchPacket.setBatch(packets);
            Object wiredProcessResult = getConnection().dispatch(batchPacket);
            IProcessResult processResult = _groupBacklog.fromWireForm(wiredProcessResult);

            logProcessResultReceivedIfNecessary(processResult, packets);

            _groupBacklog.processResult(_memberName, processResult, packets);
            invokeAfterReplicatedChannelDataFilter(packets);
            return isReplicationCompleted(processResult) ? 1 : 0;
        } catch (ReplicationException e) {
            throw e;
        } catch (Throwable t) {
            trackAndRethrowPendingErrorIfNeeded(t, packets);
        }
        return 0;
    }

    private int replicateAfterChannelFilter(IReplicationOrderedPacket packet)
            throws RemoteException, ReplicationException {
        int res = 0;
        packet = invokeOutputFilterIfNeeded(packet);
        if (_specificLogger.isLoggable(Level.FINEST))
            _specificLogger.finest("Replicating filtered packet: " + packet);

        ReplicatedDataPacketResource replicatedDataPacketResource = _packetsPool.get();
        try {
            res = dispatchReplicationPacket(packet, replicatedDataPacketResource);
            // Accumulate statistics
            _statisticsCounter.increment();
        } finally {
            replicatedDataPacketResource.release();
        }
        return res;
    }

    private int dispatchReplicationPacket(IReplicationOrderedPacket packet,
                                          ReplicatedDataPacketResource replicatedDataPacketResource)
            throws RemoteException, ReplicationException {
        try {
            ReplicatedDataPacket replicatedPacket = replicatedDataPacketResource.getPacket();
            replicatedPacket.setPacket(packet);

            Object wiredProcessResult = getConnection().dispatch(replicatedPacket);
            IProcessResult processResult = _groupBacklog.fromWireForm(wiredProcessResult);

            logProcessResultIfNecessary(processResult, packet);

            _groupBacklog.processResult(_memberName, processResult, packet);

            invokeAfterReplicatedChannelDataFilter(packet);
            return isReplicationCompleted(processResult) ? 1 : 0;
        } catch (ReplicationException e) {
            throw e;
        } catch (Throwable t) {
            trackAndRethrowPendingErrorIfNeeded(t, packet);
        }
        return 0;
    }

    private boolean isReplicationCompleted(IProcessResult processResult) {
        if (GlobalOrderProcessResult.OK.equals(processResult)) {
            return true;
        } else //noinspection SimplifiableIfStatement
            if (processResult instanceof MultiBucketSingleFileProcessResult) {
                return ((MultiBucketSingleFileProcessResult) processResult).isProcessed();
            } else {
                return false;
            }
    }

    private void invokeAfterReplicatedChannelDataFilter(
            IReplicationOrderedPacket packet) {
        if (!isDataFiltered())
            return;

        if (!packet.isDataPacket())
            return;

        IReplicationPacketData<?> packetData = packet.getData();

        if (packetData.isSingleEntryData()) {
            getDataFilter().filterAfterReplicatedEntryData(packetData.getSingleEntryData(),
                    getTargetLogicalVersion(),
                    getGroupBacklog().getDataProducer(),
                    _specificLogger);
        } else {
            for (IReplicationPacketEntryData entryData : packetData) {
                getDataFilter().filterAfterReplicatedEntryData(entryData,
                        getTargetLogicalVersion(),
                        getGroupBacklog().getDataProducer(),
                        _specificLogger);
            }
        }

    }

    private void invokeAfterReplicatedChannelDataFilter(
            List<IReplicationOrderedPacket> packets) {

        if (!isDataFiltered())
            return;

        for (IReplicationOrderedPacket packet : packets) {
            invokeAfterReplicatedChannelDataFilter(packet);
        }
    }

    private void trackPendingErrorIfNeeded(Throwable t, IReplicationOrderedPacket replicatedPacket) {
        if (t instanceof ReplicationException) {
            setPendingError(t, replicatedPacket);
        }
        if (t instanceof RuntimeException) {
            setPendingError(t, replicatedPacket);
        }
        if (t instanceof Error) {
            setPendingError(t, replicatedPacket);
        }
    }

    private void trackPendingErrorIfNeeded(Throwable t, List<IReplicationOrderedPacket> replicatedPackets) {
        if (t instanceof ReplicationException) {
            setPendingError(t, replicatedPackets);
        }
        if (t instanceof RuntimeException) {
            setPendingError(t, replicatedPackets);
        }
        if (t instanceof Error) {
            setPendingError(t, replicatedPackets);
        }
    }

    private void trackAndRethrowPendingErrorIfNeeded(Throwable t,
                                                     IReplicationOrderedPacket replicatedPacket) throws RemoteException,
            ReplicationException {
        trackPendingErrorIfNeeded(t, replicatedPacket);
        if (t instanceof RemoteException) {
            throw (RemoteException) t;
        }
        if (t instanceof ReplicationException) {
            throw (ReplicationException) t;
        }
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
    }

    private void trackAndRethrowPendingErrorIfNeeded(Throwable t,
                                                     List<IReplicationOrderedPacket> replicatedPackets) throws RemoteException,
            ReplicationException {
        trackPendingErrorIfNeeded(t, replicatedPackets);
        if (t instanceof RemoteException) {
            throw (RemoteException) t;
        }
        if (t instanceof ReplicationException) {
            throw (ReplicationException) t;
        }
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
    }

    private Future replicateBatchAsyncAfterChannelFilter(
            List<IReplicationOrderedPacket> packets, final IAsyncReplicationListener listener, boolean containsDiscardedPacket) throws RemoteException {
        final boolean containsDiscarded = invokeOutputFilterIfNeeded(packets, containsDiscardedPacket);
        final List<IReplicationOrderedPacket> finalPackets = packets;

        if (_specificLogger.isLoggable(Level.FINEST))
            _specificLogger.finest("Replicating filtered packets: "
                    + ReplicationLogUtils.packetsToLogString(finalPackets));

        final ReplicatedDataPacketResource replicatedDataPacketResource = _packetsPool.get();
        boolean delegatedToAsync = false;
        try {
            BatchReplicatedDataPacket batchPacket = replicatedDataPacketResource.getBatchPacket();

            batchPacket.setBatch(finalPackets);

            if(_isNetworkCompressionEnabled) {

                final int originalSize = finalPackets.size();

                if(_specificLogger.isLoggable(Level.FINEST)){
                    _specificLogger.finest("Compressing batch packets: "
                            + ReplicationLogUtils.packetsToLogString(finalPackets));
                }

                batchPacket.compressBatch(containsDiscarded);

                if (_specificLogger.isLoggable(Level.FINEST)) {

                    double compressionRatio = (double) batchPacket.getBatch().size() / originalSize;

                    String prefix = batchPacket.isCompressed() ? "Finished batch compression." : "Batch contains no discarded packets.";

                    String msg =  prefix + " compressionRatio=" + new DecimalFormat("#.##").format(compressionRatio);

                    _specificLogger.finest(msg);

                }
            }

            else {
                if(_specificLogger.isLoggable(Level.FINEST)){
                    _specificLogger.finest("Discarded packets network compression is disabled");
                }
            }

            AsyncFuture<Object> processResultFuture = getConnection().dispatchAsync(batchPacket);
            final ReplicateFuture resultFuture = new ReplicateFuture();
            processResultFuture.setListener(new AsyncFutureListener<Object>() {
                public void onResult(AsyncResult<Object> wiredResult) {
                    Throwable error = null;
                    IProcessResult processResult = null;
                    try {
                        Exception exception = wiredResult.getException();
                        if (exception != null)
                            throw exception;
                        processResult = _groupBacklog.fromWireForm(wiredResult.getResult());
                        logProcessResultReceivedIfNecessary(processResult, finalPackets);
                        _groupBacklog.processResult(_memberName,
                                processResult,
                                finalPackets);
                        // Accumulate statistics
                        invokeAfterReplicatedChannelDataFilter(finalPackets);
                        _statisticsCounter.add(finalPackets.size());
                        resultFuture.releaseOk();
                    } catch (ReplicationException e) {
                        error = e;
                        onAsyncReplicateErrorResult(e, finalPackets);
                        resultFuture.releaseError(e);
                    } catch (Throwable t) {
                        error = t;
                        trackPendingErrorIfNeeded(t, finalPackets);
                        onAsyncReplicateErrorResult(t, finalPackets);
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
            return resultFuture;
        } finally {
            if (!delegatedToAsync)
                replicatedDataPacketResource.release();
        }
    }

    private void logProcessResultIfNecessary(IProcessResult processResult, IReplicationOrderedPacket packet) {
        if (_specificVerboseLogger.isLoggable(Level.FINEST))
            _specificVerboseLogger.finest("Received " + processResult.toString() + " for packet replication [packetKey=" + packet.getKey() + "]");
    }

    private void logProcessResultReceivedIfNecessary(IProcessResult processResult, final List<IReplicationOrderedPacket> packets) {
        if (_specificVerboseLogger.isLoggable(Level.FINEST)) {
            String packetsBatchText = packets.isEmpty() ? "EMPTY"
                    : "firstPacketKey="
                    + packets.get(0)
                    .getKey()
                    + ", lastPacketKey="
                    + packets.get(packets.size() - 1)
                    .getEndKey();
            _specificVerboseLogger.finest("Received " + processResult.toString() + " for packets batch replication [" + packetsBatchText + "]");
        }
    }

    private Future replicateAsyncAfterChannelFilter(
            IReplicationOrderedPacket packet) throws RemoteException {
        final IReplicationOrderedPacket finalPacket = invokeOutputFilterIfNeeded(packet);
        if (_specificLogger.isLoggable(Level.FINEST))
            _specificLogger.finest("Replicating filtered packet: "
                    + finalPacket);
        final ReplicatedDataPacketResource replicatedDataPacketResource = _packetsPool.get();
        boolean delegatedToAsync = false;
        try {
            ReplicatedDataPacket replicatedPacket = replicatedDataPacketResource.getPacket();
            replicatedPacket.setPacket(finalPacket);

            AsyncFuture<Object> processResultFuture = getConnection().dispatchAsync(replicatedPacket);
            final ReplicateFuture resultFuture = new ReplicateFuture();
            processResultFuture.setListener(new AsyncFutureListener<Object>() {
                public void onResult(AsyncResult<Object> wiredResult) {
                    try {
                        Exception exception = wiredResult.getException();
                        if (exception != null)
                            throw exception;

                        IProcessResult processResult = _groupBacklog.fromWireForm(wiredResult.getResult());

                        logProcessResultIfNecessary(processResult, finalPacket);

                        _groupBacklog.processResult(_memberName,
                                processResult,
                                finalPacket);
                        // Accumulate statistics
                        invokeAfterReplicatedChannelDataFilter(finalPacket);
                        _statisticsCounter.increment();
                        resultFuture.releaseOk();
                    } catch (Throwable t) {
                        trackPendingErrorIfNeeded(t, finalPacket);
                        onAsyncReplicateErrorResult(t, finalPacket);
                        resultFuture.releaseError(t);
                    } finally {
                        replicatedDataPacketResource.release();
                    }
                }

            });
            delegatedToAsync = true;
            return resultFuture;
        } finally {
            if (!delegatedToAsync)
                replicatedDataPacketResource.release();
        }
    }

    protected abstract void onAsyncReplicateErrorResult(Throwable t,
                                                        List<IReplicationOrderedPacket> finalPackets);

    protected abstract void onAsyncReplicateErrorResult(Throwable t,
                                                        IReplicationOrderedPacket finalPacket);

    /**
     * This method should be called when replicating packets that were generated at a considerable
     * time before the actual replication (i.e async replication), this will replicate only packets
     * that are not obsolete (i.e {@link IReplicationPacketData#beforeDelayedReplication() equals
     * true})
     */
    protected void replicateBatchDelayed(List<IReplicationOrderedPacket> packets)
            throws RemoteException, ReplicationException {
        // Execute before delayed first to filter obsolete packets
        boolean containsDiscardedPacket = beforeDelayedReplication(packets);
        // Replicate packets
        replicateBatchAfterChannelFilter(packets, containsDiscardedPacket);
    }

    protected void replicateBatchDelayedAsync(List<IReplicationOrderedPacket> packets, IAsyncReplicationListener listener)
            throws RemoteException {
        // Execute before delayed first to filter obsolete packets
        boolean containsDiscardedPacket = beforeDelayedReplication(packets);
        // Replicate packets in async manner
        replicateBatchAsyncAfterChannelFilter(packets, listener, containsDiscardedPacket);
    }

    private boolean invokeOutputFilterIfNeeded(
            List<IReplicationOrderedPacket> packets, boolean containsDiscardedPacket) {

        if (!_isOutFiltered)
            return containsDiscardedPacket;

        ListIterator<IReplicationOrderedPacket> it = packets.listIterator();

        while(it.hasNext()) {
            IReplicationOrderedPacket packet = it.next();

            if (packet.getData().supportsReplicationFilter()) {
                it.set(invokeOutputFilter(packet));
                if(!containsDiscardedPacket)
                    containsDiscardedPacket = packet.isDiscardedPacket();
            }
            else {
                it.set(packet);
            }

        }

        return containsDiscardedPacket;
    }

    private IReplicationOrderedPacket invokeOutputFilterIfNeeded(
            IReplicationOrderedPacket packet) {
        if (!_isOutFiltered || !packet.getData().supportsReplicationFilter())
            return packet;

        return invokeOutputFilter(packet);
    }

    private IReplicationOrderedPacket invokeOutputFilter(
            IReplicationOrderedPacket packet) {
        final IReplicationOrderedPacket clonedPacket = packet.clone();
        IReplicationOrderedPacket result = clonedPacket;
        final IReplicationPacketData<?> data = clonedPacket.getData();
        // Each packet can become more than one filter entry (transaction)
        Iterable<IReplicationFilterEntry> filterEntries = getGroupBacklog().getDataProducer()
                .toFilterEntries(data);
        // Invoke filter on entries
        for (IReplicationFilterEntry filterEntry : filterEntries) {
            try {
                _outFilter.filterOut(filterEntry, getMemberName(), getGroupName());
            } catch (Exception e) {
                if (_specificLogger.isLoggable(Level.WARNING))
                    _specificLogger.log(Level.WARNING, "Replication filter caused an exception when filtering entry [" + filterEntry + "]", e);
            }
        }
        // If filtered the entire data we can replace it with discarded
        // packet
        if (data.isEmpty()) {
            if (_specificLogger.isLoggable(Level.FINEST))
                _specificLogger.finest("Packet [" + packet.toString()
                        + "] discarded by replication output filter.");
            result = getGroupBacklog().replaceWithDiscarded(clonedPacket, false);
        }
        return result;
    }

    private synchronized void setPendingError(Throwable error,
                                              List<IReplicationOrderedPacket> replicatedPackets) {
        _groupBacklog.setPendingError(getMemberName(), error, replicatedPackets);
    }

    private synchronized void setPendingError(Throwable error,
                                              IReplicationOrderedPacket replicatedPacket) {
        _groupBacklog.setPendingError(getMemberName(), error, replicatedPacket);
    }

    private void logConnectionStateChange(ChannelState oldState, ChannelState newState, Level logLevel) {
        String msg = oldState != newState
                ? "Channel state changed from " + oldState + " to " + newState
                : "Channel state is " + newState;
        msg += " " + getConnectionDescription();
        logEventInHistory(msg);
        if (_specificLogger.isLoggable(logLevel))
            _specificLogger.log(logLevel, msg);
    }

    private String getConnectionDescription() {
        return "[" +
                "target=" + ReplicationLogUtils.toShortLookupName(getMemberName()) + ", " +
                "target url=" + getConnection().getConnectionUrl() + ", " +
                "target machine connection url=" + getConnection().getClosestEndpointAddress() +
                "]";
    }

    /**
     * Get a list of packets that are waiting to be replicated
     */
    protected List<IReplicationOrderedPacket> getPendingPackets(int batchSize) {
        return _groupBacklog.getPackets(getMemberName(),
                batchSize,
                getDataFilter(),
                getTargetLogicalVersion(),
                _specificLogger);
    }

    private boolean invokeBeforeReplicatingChannelDataFilter(
            List<IReplicationOrderedPacket> packets) {
        boolean containsDiscardedPacket = false;

        if (!isDataFiltered())
            return containsDiscardedPacket;


        ListIterator<IReplicationOrderedPacket> iterator = packets.listIterator();
        IReplicationOrderedPacket previousDiscardedPacket = null;

        while(iterator.hasNext()) {
            IReplicationOrderedPacket packet = iterator.next();

            IReplicationOrderedPacket filterPacket = ReplicationChannelDataFilterHelper.filterPacket(getDataFilter(),
                    getTargetLogicalVersion(),
                    packet,
                    getGroupBacklog().getDataProducer(),
                    getGroupBacklog(),
                    previousDiscardedPacket,
                    _specificLogger,
                    _memberName);

            //current packet was discarded and merged into the previous discarded packet
            if (previousDiscardedPacket == filterPacket)
                iterator.remove();

            //The previous packet is not a discarded one
            if (previousDiscardedPacket == null) {
                iterator.set(filterPacket);
                // Mark the previous as discarded for next iteration
                if (filterPacket.isDiscardedPacket()) {
                    previousDiscardedPacket = filterPacket;
                }
            }
            //The previous is discarded but the current was not merged into it
            else if (previousDiscardedPacket != filterPacket) {
                iterator.set(filterPacket);
                //If the current is a newly discarded packet, keep it as the previous discarded, otherwise reset previous
                //discarded state to null.
                previousDiscardedPacket = filterPacket.isDiscardedPacket() ? filterPacket : null;
            }

            if(!containsDiscardedPacket)
                containsDiscardedPacket = filterPacket.isDiscardedPacket();

        }
        return containsDiscardedPacket;
    }

    private IReplicationOrderedPacket invokeBeforeReplicatingChannelDataFilter(
            IReplicationOrderedPacket packet) {
        if (!isDataFiltered())
            return packet;

        IReplicationOrderedPacket filterPacket = ReplicationChannelDataFilterHelper.filterPacket(getDataFilter(),
                getTargetLogicalVersion(),
                packet,
                getGroupBacklog().getDataProducer(),
                getGroupBacklog(),
                null, _specificLogger, _memberName);
        return filterPacket;
    }

    /**
     * Samples the channel through put, TP is measured between sample calls.
     */
    public void sampleStatistics() {
        long currentTime = SystemTime.timeMillis();
        int operationsDone = _statisticsCounter.get();
        _statisticsCounter.reset();

        long intervalFromLastSample = currentTime
                - _throughPutStatistics.getLastTimeStamp();
        if (intervalFromLastSample == 0)
            return;
        // Sample TP
        int sampleTP = (int) (((float) operationsDone / intervalFromLastSample) * 1000);
        _throughPutStatistics.addSample(currentTime, sampleTP);
        // Sample generated traffic
        long generatedTraffic = getConnection().getGeneratedTraffic();
        long generatedTrafficTP = -1;
        //After disconnection if the underlying stub is replaced or some connections are closed, the generated traffic may reset or reduce on the LRMI layer, we need to reset the traffic here as well otherwise
        //we will get negative traffic.
        if (generatedTraffic >= _lastSampledGeneratedTraffic) {
            generatedTrafficTP = (long) (((float) (generatedTraffic - _lastSampledGeneratedTraffic) / intervalFromLastSample) * 1000);
            // Sample received traffic
            _generatedTrafficStatistics.addSample(currentTime, generatedTrafficTP);
        }
        _lastSampledGeneratedTraffic = generatedTraffic;
        long receivedTraffic = getConnection().getReceivedTraffic();
        long receivedTrafficTP = -1;
        //After disconnection if the underlying stub is replaced or some connections are closed, the generated traffic may reset or reduce on the LRMI layer, we need to reset the traffic here as well otherwise
        //we will get negative traffic.
        if (receivedTraffic >= _lastSampledReceivedTraffic) {
            receivedTrafficTP = (long) (((float) (receivedTraffic - _lastSampledReceivedTraffic) / intervalFromLastSample) * 1000);
            _receivedTrafficStatistics.addSample(currentTime, receivedTrafficTP);
        }
        _lastSampledReceivedTraffic = receivedTraffic;
        _totalNumberOfReplicatedPackets += operationsDone;
        if (_specificVerboseLogger.isLoggable(Level.FINEST))
            _specificVerboseLogger.finest("sampled packets throughput [" + sampleTP
                    + "] generated traffic throughput [" + generatedTrafficTP
                    + "] received traffic throughput [" + receivedTrafficTP
                    + "]");
    }

    protected int getLastSampledTP() {
        return _throughPutStatistics.getLastSample();
    }

    protected int getSampleTPBefore(long timeBefore, TimeUnit unit) {
        return _throughPutStatistics.getSampleBefore(timeBefore, unit);
    }

    public boolean isInconsistent() {
        return _inconsistentDuringHandshakeState || _groupBacklog.getState(getMemberName()).isInconsistent();
    }

    protected Throwable getInconsistencyReason() {
        if (_inconsistentDuringHandshakeStateReason != null)
            return _inconsistentDuringHandshakeStateReason;

        IBacklogMemberState memberState = _groupBacklog.getState(getMemberName());
        if (!memberState.isInconsistent())
            return null;

        return memberState.getInconsistencyReason();
    }

    private class InconsistentStateHandler
            extends AsyncCallable {

        public CycleResult call() throws Exception {
            synchronized (AbstractReplicationSourceChannel.this) {
                if (isClosed())
                    return CycleResult.TERMINATE;
                _specificLogger.finest("Inconsistent state handler waken up");
                // If we are now not at inconsistent, the connection
                // could have been disconnected and successfully reconnected
                // + handshake
                if (!_inconsistentDuringHandshakeState)
                    return CycleResult.TERMINATE;
                // Try to execute connect channel
                connectChannel();
                // If we are now no longer at inconsistent we can break
                if (!_inconsistentDuringHandshakeState)
                    return CycleResult.TERMINATE;
                // We are still at inconsistent state
                _specificLogger.finest("Channel is still at inconsistent state, waiting for next retry");
                return CycleResult.IDLE_CONTINUE;
            }
        }
    }

    private class IterativeHandshakeHandler
            extends AsyncCallable {

        private final IHandshakeContext _handshakeContext;

        public IterativeHandshakeHandler(IHandshakeContext handshakeContext) {
            _handshakeContext = handshakeContext;
        }

        public CycleResult call() throws Exception {
            synchronized (AbstractReplicationSourceChannel.this) {
                if (isClosed())
                    return CycleResult.TERMINATE;
                if (_channelState == ChannelState.DISCONNECTED) {
                    _iterativeHandshakeHandler = null;
                    return CycleResult.TERMINATE;
                }
                IHandshakeIteration nextHandshakeIteration = getGroupBacklog().getNextHandshakeIteration(getMemberName(),
                        _handshakeContext);
                try {
                    getConnection().dispatch(new IterativeHandshakePacket(getGroupName(),
                            nextHandshakeIteration));
                    if (!_handshakeContext.isDone()) {
                        if (_specificLogger.isLoggable(Level.FINER))
                            _specificLogger.finer("Handshake is incomplete: " + _handshakeContext.toLogMessage());

                        return CycleResult.CONTINUE;
                    }

                    _specificLogger.fine("Handshake completed - channel is active");

                    moveToActive();
                    _iterativeHandshakeHandler = null;
                    return CycleResult.TERMINATE;
                } catch (RemoteException e) {
                    _iterativeHandshakeHandler = null;
                    return CycleResult.TERMINATE;
                } catch (Throwable t) {
                    if (_specificLogger.isLoggable(Level.SEVERE))
                        _specificLogger.log(Level.SEVERE, "Error occurred while executing iterative handshake, replication is disabled until the error is resolved "
                                        + getConnectionDescription(),
                                t);
                    moveToInconsistentState(t);
                    return CycleResult.TERMINATE;
                }
            }
        }

    }

    public IReplicationSourceChannelStatistics getStatistics() {
        long lastConfirmedKey = getGroupBacklog().getState(getMemberName())
                .getLastConfirmedKey();

        long generatedTraffic = getConnection().getGeneratedTraffic();
        long receivedTraffic = getConnection().getReceivedTraffic();
        long generatedTrafficTP = _generatedTrafficStatistics.getLastSample();
        long receivedTrafficTP = _receivedTrafficStatistics.getLastSample();
        int lastSampledTP = getLastSampledTP();
        long generatedTrafficPerPacket = lastSampledTP == 0 ? 0
                : (long) ((double) generatedTrafficTP
                / lastSampledTP);
        ReplicationEndpointDetails endpointDetails = getTargetReplicationEndpointDetails();
        ConnectionEndpointDetails delegatorDetails = getConnection().getClosestEndpointDetails();
        //It could be that the connection is currently disconnected, in that case we try to use the last known endpoint details which were saved
        //during handshake. This is just a heuristic, to try and get the latest details upon concurrent changes, we could always return
        //the details which were saved during handshake but we may get inaccurate results sometimes.
        if (delegatorDetails == null)
            delegatorDetails = _delegatorEndpointDetails;
        if (delegatorDetails != null && endpointDetails != null) {
            if (delegatorDetails.equals(endpointDetails.getConnectionDetails()))
                delegatorDetails = null;
        }

        return new ReplicationSourceChannelStatistics(getMemberName(),
                _channelType,
                getConnection().getState(),
                isActive(),
                lastConfirmedKey,
                lastSampledTP,
                _totalNumberOfReplicatedPackets,
                isInconsistent() ? getInconsistencyReason()
                        : null,
                generatedTraffic,
                receivedTraffic,
                generatedTrafficTP,
                receivedTrafficTP,
                generatedTrafficPerPacket,
                getGroupBacklog().size(getMemberName()),
                getChannelOpertingMode(),
                endpointDetails,
                delegatorDetails);
    }

    public void registerWith(MetricRegistrator metricRegister) {
        metricRegister.register("total-replicated-packets", new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return _totalNumberOfReplicatedPackets;
            }
        });
        metricRegister.register("generated-traffic-bytes", new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return getConnection().getGeneratedTraffic();
            }
        });
        metricRegister.register("received-traffic-bytes", new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return getConnection().getReceivedTraffic();
            }
        });
        metricRegister.register("retained-size-packets", new Gauge<Long>() {
            @Override
            public Long getValue() throws Exception {
                return getGroupBacklog().size(getMemberName());
            }
        });
    }

    public abstract ReplicationOperatingMode getChannelOpertingMode();

    public abstract void flushPendingReplication();

    public void replicate(IReplicationUnreliableOperation operation) {
        if (operation == null)
            return;

        if (isActive()) {
            if (isDataFiltered()) {
                if (!getDataFilter().filterBeforeReplicatingUnreliableOperation(operation, getTargetLogicalVersion())) {
                    if (_specificLogger.isLoggable(Level.FINEST))
                        _specificLogger.finest("Filtered unreliable operation: " + operation);
                    return;
                }
            }
            if (_specificLogger.isLoggable(Level.FINEST))
                _specificLogger.finest("Replicating unreliable operation: " + operation);

            try {
                getConnection().dispatch(new UnreliableOperationPacket(getGroupName(),
                        operation));
            } catch (RemoteException e) {
                // Silently fail if got remote exception
            }
        }
        // Silently fail if not active
    }

    public String dumpState() {
        return "Channel [" + getMemberName() + "] state [" + _channelState
                + "]:" + StringUtils.NEW_LINE + "Type ["
                + this.getClass().getName() + "]" + StringUtils.NEW_LINE
                + "last measured send packets throughput ["
                + getLastSampledTP() + "]" + StringUtils.NEW_LINE
                + "inconsistent [" + isInconsistent() + "]"
                + StringUtils.NEW_LINE + "synchronizing [" + isSynchronizing()
                + "]" + onDumpState() + StringUtils.NEW_LINE
                + getConnection().dumpState() + StringUtils.NEW_LINE
                + "latest history:" + StringUtils.NEW_LINE + dumpHistory();
    }

    protected String onDumpState() {
        // Nothing on default
        return "";
    }

    private String dumpHistory() {
        return _groupHistory.outputDescendingEvents(getMemberName());
    }

    public ReplicationEndpointDetails getTargetReplicationEndpointDetails() {
        return _targetEndpointDetails;
    }

    protected PlatformLogicalVersion getTargetLogicalVersion() {
        PlatformLogicalVersion result = null;
        ReplicationEndpointDetails targetEndpointDetails = _targetEndpointDetails;
        if (targetEndpointDetails != null)
            result = targetEndpointDetails.getPlatformLogicalVersion();

        //Workaround to backward issue that may arise since in gateway older then 9.0.1 we don't know the final target version we
        //take the worst case scenario and put 8.0.3 which is the version when gateway was introduced.
        //if (result == null && getMemberName().startsWith(GatewayPolicy.GATEWAY_NAME_PREFIX))
        //    result = PlatformLogicalVersion.v8_0_3;

        PlatformLogicalVersion connectionTargetLogicalVersion = _connection.getClosestEndpointLogicalVersion();
        if (result == null)
            return connectionTargetLogicalVersion;

        //We need this to support scenario where we passed via a delegator which is older than 9.0 since it didn't do this check on its own.
        //We want to return the minimal version between the final endpoint and the connection endpoint (i.e Delegator connected
        //to a remote gateway)
        return PlatformLogicalVersion.minimum(connectionTargetLogicalVersion, result);
    }

    private boolean requiresHighLevelLogging() {
        //Safety code, should not occur
        if (_channelType == null)
            return true;
        switch (_channelType) {
            case ACTIVE_SPACE:
            case BACKUP_SPACE:
            case MIRROR:
            case GATEWAY:
                return true;
            case DURABLE_NOTIFICATION:
            case LOCAL_VIEW:
                return false;
            default:
                return true;
        }
    }
}
