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

import com.gigaspaces.internal.cluster.node.handlers.IReplicationInFacade;
import com.gigaspaces.internal.cluster.node.impl.EventsTracer;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.IIdleStateData;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.ConnectChannelHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.ConnectChannelHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.replica.SpaceReplicaState;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.internal.cluster.node.impl.router.RouterStubHolder;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.exception.ClosedResourceException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class AbstractReplicationTargetGroup
        implements IReplicationTargetGroup, IReplicationGroupHistory {
    protected final static Logger _loggerReplica = Logger.getLogger(Constants.LOGGER_REPLICATION_REPLICA);
    protected final Logger _specificLogger;

    private final IReplicationRouter _replicationRouter;
    private final Map<String, AbstractReplicationTargetChannel> _channels;
    private final TargetGroupConfig _groupConfig;
    private final IReplicationInFacade _replicationInFacade;
    private final IReplicationInFilter _inFilter;
    protected final int _channelCloseTimeout;
    private final Map<String, SpaceReplicaState> _spaceReplicaStates;
    private final IReplicationTargetGroupStateListener _listener;
    private final Map<String, EventsTracer<String>> _groupChannelHistory;
    private final EventsTracer<String> _groupHistory;
    private final Object _historyLock = new Object();
    private boolean _closed;

    public AbstractReplicationTargetGroup(TargetGroupConfig groupConfig,
                                          IReplicationRouter replicationRouter,
                                          IReplicationInFacade replicationInFacade,
                                          IReplicationInFilter inFilter,
                                          IReplicationTargetGroupStateListener stateListener) {
        _groupConfig = groupConfig;
        _replicationRouter = replicationRouter;
        _replicationInFacade = replicationInFacade;
        _inFilter = inFilter;
        _listener = stateListener;
        _channelCloseTimeout = _groupConfig.getChannelCloseTimeout();
        _channels = new CopyOnUpdateMap<String, AbstractReplicationTargetChannel>();
        _spaceReplicaStates = new HashMap<String, SpaceReplicaState>();
        _groupChannelHistory = createGroupHistory();
        _groupHistory = new EventsTracer<String>(_groupConfig.getHistoryLength());
        _specificLogger = Logger.getLogger(Constants.LOGGER_REPLICATION_GROUP
                + "." + _groupConfig.getName());
    }

    private Map<String, EventsTracer<String>> createGroupHistory() {
        HashMap<String, EventsTracer<String>> history = new HashMap<String, EventsTracer<String>>();
        //If the group is not bounded, we do not know its members so we cannot pre allocate history event tracers
        if (!_groupConfig.isUnbounded() && !_groupConfig.isSupportDynamicMembers()) {
            String[] membersLookupNames = _groupConfig.getMembersLookupNames();
            for (String memberName : membersLookupNames) {
                history.put(memberName, new EventsTracer<String>(_groupConfig.getHistoryLength()));
            }
        }
        return history;
    }

    public void logEvent(String memberName, String event) {
        EventsTracer<String> eventsTracer = getEventsTracer(memberName);

        eventsTracer.logEvent(event);
    }

    @Override
    public void logGroupEvent(String event) {
        _groupHistory.logEvent(event);
    }

    private EventsTracer<String> getEventsTracer(String memberName) {
        synchronized (_historyLock) {
            EventsTracer<String> eventsTracer = _groupChannelHistory.get(memberName);
            if (eventsTracer == null)
                throw new IllegalArgumentException("Unknown member name " + memberName + ", valid members are " + Arrays.toString(_groupConfig.getMembersLookupNames()));

            return eventsTracer;
        }
    }

    public String outputDescendingEvents(String memberName) {
        return getEventsTracer(memberName).outputDescendingEvents();
    }

    protected IReplicationRouter getReplicationRouter() {
        return _replicationRouter;
    }

    @Override
    public synchronized IReplicationTargetChannel getChannel(String sourceMemberLookupName) {
        validNotClosed();

        return _channels.get(sourceMemberLookupName);
    }

    public synchronized ConnectChannelHandshakeResponse connectChannel(
            RouterStubHolder sourceRouterStubHolder,
            ConnectChannelHandshakeRequest handshakeRequest) {
        validNotClosed();

        AbstractReplicationTargetChannel channel = _channels.get(sourceRouterStubHolder.getMyEndpointDetails().getLookupName());
        if (channel == null
                || !channel.getSourceUniqueId()
                .equals(sourceRouterStubHolder.getMyEndpointDetails().getUniqueId()))
            channel = createNewChannel(handshakeRequest.getBacklogHandshakeRequest(), sourceRouterStubHolder);

        IProcessLogHandshakeResponse processLogHandshakeResponse = channel.performHandshake(handshakeRequest.getBacklogHandshakeRequest());
        return new ConnectChannelHandshakeResponse(_replicationRouter.getMyEndpointDetails(), processLogHandshakeResponse);
    }

    public synchronized void onChannelBacklogDropped(
            String sourceMemberLookupName, Object sourceUniqueId,
            IBacklogMemberState memberState) {
        validNotClosed();

        AbstractReplicationTargetChannel channel = _channels.get(sourceMemberLookupName);
        if (channel != null
                && channel.getSourceUniqueId().equals(sourceUniqueId)) {
            IReplicationTargetGroupStateListener listener = _listener;
            channel.onChannelBacklogDropped(getGroupName(),
                    memberState,
                    listener);
        } else if (_specificLogger.isLoggable(Level.WARNING))
            _specificLogger.warning("Received channel backlog dropped notification from a wrong source ["
                    + sourceMemberLookupName
                    + ", "
                    + sourceUniqueId
                    + "], ignoring it.");
    }

    public void processHandshakeIteration(String sourceMemberLookupName,
                                          Object sourceUniqueId, IHandshakeIteration handshakeIteration) {
        validNotClosed();

        AbstractReplicationTargetChannel channel = getCorrespondingChannel(sourceMemberLookupName, sourceUniqueId);

        channel.processHandshakeIteration(handshakeIteration);
    }

    private AbstractReplicationTargetChannel createNewChannel(
            IBacklogHandshakeRequest handshakeRequest, RouterStubHolder sourceRouterStubHolder) {
        // First validate if this channel can be accepted (if not it
        // throws an exception)
        validateConnectChannel(sourceRouterStubHolder.getMyEndpointDetails().getLookupName(),
                sourceRouterStubHolder.getMyEndpointDetails().getUniqueId());
        // Attempt to create a backward connection the this specific source, if
        // failed
        // this channel connection attempt will fail
        IReplicationMonitoredConnection sourceConnection = createConnectionToSource(sourceRouterStubHolder);

        // Create a new replication channel
        AbstractReplicationTargetChannel channel = createNewChannelImpl(sourceRouterStubHolder.getMyEndpointDetails(),
                sourceConnection,
                handshakeRequest,
                this);
        AbstractReplicationTargetChannel previousChannel = _channels.put(sourceRouterStubHolder.getMyEndpointDetails().getLookupName(),
                channel);
        // When a group is unbounded, we may need to create history tracer if this is the first time this channel
        // is established
        createHistoryTracerIfNeeded(sourceRouterStubHolder.getMyEndpointDetails().getLookupName());

        // Close previous channel if exists
        if (previousChannel != null)
            previousChannel.close(_channelCloseTimeout, TimeUnit.SECONDS);
        // Handshake is successful
        // Failed handshake should throw an exception
        return channel;
    }

    private void createHistoryTracerIfNeeded(String memberLookupName) {
        //If this group is bounded, history tracers are already created at construction time
        if (!_groupConfig.isUnbounded() && !_groupConfig.isSupportDynamicMembers())
            return;

        synchronized (_historyLock) {
            if (_groupChannelHistory.containsKey(memberLookupName))
                return;

            //If group is unbounded we should create an event tracer for this member
            EventsTracer<String> eventsTracer = new EventsTracer<String>(_groupConfig.getHistoryLength());
            _groupChannelHistory.put(memberLookupName, eventsTracer);
        }
    }

    protected abstract AbstractReplicationTargetChannel createNewChannelImpl(
            ReplicationEndpointDetails sourceEndpointDetails, IReplicationMonitoredConnection sourceConnection,
            IBacklogHandshakeRequest handshakeRequest, IReplicationGroupHistory groupHistory);

    private void validateConnectChannel(String sourceMemberName,
                                        Object sourceUniqueId) {
        //Disable unknown member protection when the group is not bounded
        if (!_groupConfig.isUnbounded())
            validateUnknownMember(sourceMemberName);
        validateDuplicateChannel(sourceMemberName, sourceUniqueId);
        validateConnectChannelImpl(sourceMemberName);
        validateConnectionPlugable(sourceMemberName, sourceUniqueId);
        // All ok, no exception thrown
    }

    private void validateConnectionPlugable(String sourceMemberName,
                                            Object sourceUniqueId) {
        if (_listener != null)
            _listener.onTargetChannelCreationValidation(getGroupName(), sourceMemberName, sourceUniqueId);
    }

    private void validateUnknownMember(String sourceMemberName) {
        if (_groupConfig.isSupportDynamicMembers()) {
            List<String> list = Arrays.asList(_groupConfig.getMembersLookupNames());
            for (String memberName : list) {
                if (sourceMemberName.matches(memberName))
                    return;
            }
            throw new UnknownReplicationSourceException("Replication group "
                    + _groupConfig.getName() + " does not contain matching dynamic member definition of "
                    + sourceMemberName + ", known dynamic member definitions are "
                    + Arrays.toString(_groupConfig.getMembersLookupNames()));
        } else if (!Arrays.asList(_groupConfig.getMembersLookupNames())
                .contains(sourceMemberName)) {
            throw new UnknownReplicationSourceException("Replication group "
                    + _groupConfig.getName()
                    + " does not contain member "
                    + sourceMemberName
                    + ", known member are "
                    + Arrays.toString(_groupConfig.getMembersLookupNames()));
        }
    }

    private void validateDuplicateChannel(String sourceMemberName,
                                          Object sourceUniqueId)
            throws ReplicationSourceAlreadyAttachedException {
        AbstractReplicationTargetChannel channel = _channels.get(sourceMemberName);
        // This is a reconnection attempt
        if (channel != null
                && channel.getSourceUniqueId().equals(sourceUniqueId))
            return;

        if (channel != null && channel.isSourceAttached())
            throw new ReplicationSourceAlreadyAttachedException("Replication group "
                    + _groupConfig.getName()
                    + " member "
                    + sourceMemberName
                    + " source is already attached [" + channel.getSourceEndpointAddress() + "]");
    }

    private IReplicationMonitoredConnection createConnectionToSource(
            RouterStubHolder sourceRouterStubHolder) {
        IReplicationMonitoredConnection sourceConnection = _replicationRouter.getDirectConnection(sourceRouterStubHolder);
        return sourceConnection;
    }

    public Object processBatch(String sourceMemberLookupName,
                               Object sourceUniqueId, List<IReplicationOrderedPacket> packets) {
        validNotClosed();

        AbstractReplicationTargetChannel channel = getCorrespondingChannel(sourceMemberLookupName,
                sourceUniqueId);

        return channel.processBatch(packets);
    }

    @Override
    public Object processIdleStateData(String sourceMemberLookupName,
                                       Object sourceUniqueId, IIdleStateData idleStateData) {
        validNotClosed();

        AbstractReplicationTargetChannel channel = getCorrespondingChannel(sourceMemberLookupName,
                sourceUniqueId);

        return channel.processIdleStateData(idleStateData);
    }

    private void validNotClosed() {
        if (_closed)
            throw new ClosedResourceException("Replication group "
                    + getGroupName() + " is closed");
    }

    public Object process(String sourceMemberLookupName,
                          Object sourceUniqueId, IReplicationOrderedPacket packet) {
        validNotClosed();

        AbstractReplicationTargetChannel channel = getCorrespondingChannel(sourceMemberLookupName,
                sourceUniqueId);

        return channel.process(packet);
    }

    public void processUnreliableOperation(String sourceMemberLookupName,
                                           Object sourceUniqueId, IReplicationUnreliableOperation operation) {
        validNotClosed();

        getCorrespondingChannel(sourceMemberLookupName, sourceUniqueId);

        try {
            if (_specificLogger.isLoggable(Level.FINEST))
                _specificLogger.finest("incoming replication of unreliable operation: " + operation);

            operation.execute(sourceMemberLookupName, getReplicationInFacade());
        } catch (Exception e) {
            if (_specificLogger.isLoggable(Level.WARNING))
                _specificLogger.log(Level.WARNING,
                        "error occurred while processing operation "
                                + operation,
                        e);
        }
    }

    private AbstractReplicationTargetChannel getCorrespondingChannel(
            String sourceMemberLookupName, Object sourceUniqueId) {
        // space
        AbstractReplicationTargetChannel channel = _channels.get(sourceMemberLookupName);

        if (channel == null)
            throw new IncompatibleReplicationSourceException("Replication group "
                    + getGroupName()
                    + " received process invocation from "
                    + sourceMemberLookupName
                    + " when there is no open channel from that source");

        if (!channel.getSourceUniqueId().equals(sourceUniqueId))
            throw new IncompatibleReplicationSourceException("Replication group "
                    + getGroupName()
                    + " received process invocation from "
                    + sourceMemberLookupName
                    + " with incompatible id, received "
                    + sourceUniqueId
                    + " expecting " + channel.getSourceUniqueId());
        return channel;
    }

    public String getGroupName() {
        return _groupConfig.getName();
    }

    public IReplicationInFilter getInFilter() {
        return _inFilter;
    }

    public TargetGroupConfig getGroupConfig() {
        return _groupConfig;
    }

    protected void closeChannel(AbstractReplicationTargetChannel channel) {
        if (channel != null) {
            _channels.remove(channel.getSourceLookupName());
            channel.close(_channelCloseTimeout, TimeUnit.SECONDS);
        }
    }

    public synchronized void close() {
        if (_closed)
            return;

        _closed = true;

        for (AbstractReplicationTargetChannel channel : _channels.values())
            closeChannel(channel);
    }

    @Override
    public long getLastProcessTimeStamp(String replicaSourceLookupName) {
        AbstractReplicationTargetChannel channel = _channels.get(replicaSourceLookupName);
        if (channel != null)
            return channel.getLastProcessTimeStamp();
        return -1;
    }

    public synchronized void addSynchronizeState(String sourceMemberLookupName,
                                                 SpaceReplicaState spaceReplicaState) {
        _spaceReplicaStates.put(sourceMemberLookupName, spaceReplicaState);
    }

    public synchronized void synchronizationDone(String sourceMemberLookupName, Object sourceUniqueId) {
        SpaceReplicaState spaceReplicaState = _spaceReplicaStates.remove(sourceMemberLookupName);
        if (spaceReplicaState != null) {
            AbstractReplicationTargetChannel channel = _channels.get(sourceMemberLookupName);

            if (channel == null) {
                if (_loggerReplica.isLoggable(Level.WARNING)) {
                    _loggerReplica.warning(_replicationRouter.getMyLookupName()
                            + " received synchronization done signal from source with no open channel ["
                            + sourceMemberLookupName + "]");
                }
            } else if (!channel.getSourceUniqueId().equals(sourceUniqueId)) {
                if (_loggerReplica.isLoggable(Level.WARNING))
                    _loggerReplica.warning(_replicationRouter.getMyLookupName()
                            + " received synchronization done signal from source with wrong id ["
                            + sourceMemberLookupName + "] expected id [" + channel.getSourceUniqueId() + "] actual [" + sourceUniqueId + "]");
            }

            if (_loggerReplica.isLoggable(Level.FINE))
                _loggerReplica.fine(_replicationRouter.getMyLookupName()
                        + " synchronization done with source ["
                        + sourceMemberLookupName + "]");
            spaceReplicaState.updateSynchronizationDone();
        } else {
            if (_loggerReplica.isLoggable(Level.WARNING))
                _loggerReplica.warning(_replicationRouter.getMyLookupName()
                        + " received synchronization done signal when there's no pending synchronization in progress ["
                        + sourceMemberLookupName + "]");
        }
    }

    public boolean isFiltered() {
        return _inFilter != null;
    }

    public IReplicationTargetGroupStateListener getStateListener() {
        return _listener;
    }

    public void setActive() {
        // Do nothing by default
    }

    public void setPassive() {
        // Do nothing by default
    }

    /**
     * Validate if can accept the channel request, no exception means this channel request can be
     * accepted
     */
    protected abstract void validateConnectChannelImpl(
            String sourceMemberLookupName);

    public IReplicationInFacade getReplicationInFacade() {
        return _replicationInFacade;
    }

    public String dumpState() {
        StringBuilder dump = new StringBuilder("Replication target Group [");
        dump.append(getGroupName());
        dump.append("]");
        dump.append(StringUtils.NEW_LINE + "{");
        dump.append(StringUtils.NEW_LINE);
        dump.append("Type [");
        dump.append(this.getClass().getName());
        dump.append("]");
        dump.append(StringUtils.NEW_LINE);
        dump.append("Channels:");
        for (AbstractReplicationTargetChannel channel : _channels.values()) {
            dump.append(StringUtils.NEW_LINE);
            dump.append(channel.dumpState());
        }
        dump.append(StringUtils.NEW_LINE);
        dump.append("Group History:");
        dump.append(StringUtils.NEW_LINE);
        dump.append(_groupHistory.outputDescendingEvents());
        dump.append(StringUtils.NEW_LINE);
        dump.append("}");
        return dump.toString();
    }

}
