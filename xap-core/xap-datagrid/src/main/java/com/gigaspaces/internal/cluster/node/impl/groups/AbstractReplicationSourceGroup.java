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

import com.gigaspaces.cluster.replication.ConsistencyLevelViolationException;
import com.gigaspaces.internal.cluster.node.impl.EventsTracer;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig;
import com.gigaspaces.internal.cluster.node.impl.backlog.BacklogConfig.LimitReachedPolicy;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.IReplicationGroupBacklog;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder;
import com.gigaspaces.internal.cluster.node.impl.config.DynamicSourceGroupConfigHolder.IDynamicSourceGroupStateListener;
import com.gigaspaces.internal.cluster.node.impl.config.MemberAddedEvent;
import com.gigaspaces.internal.cluster.node.impl.config.SourceChannelConfig;
import com.gigaspaces.internal.cluster.node.impl.config.SourceGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationOutFilter;
import com.gigaspaces.internal.cluster.node.impl.groups.consistencylevel.GroupConsistencyLevelPolicy;
import com.gigaspaces.internal.cluster.node.impl.router.ConnectionState;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateMap;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.metrics.MetricRegistrator;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.kernel.JSpaceUtilities;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractReplicationSourceGroup<T extends SourceGroupConfig>
        implements IReplicationSourceGroup, IReplicationGroupHistory, IDynamicSourceGroupStateListener {
    protected final Logger _specificLogger;
    private final IReplicationRouter _replicationRouter;
    private final DynamicSourceGroupConfigHolder _groupConfigHolder;
    private final Map<String, AbstractReplicationSourceChannel> _channels;
    private final IReplicationGroupBacklog _groupBacklog;
    private final String _myLookupName;
    private final IReplicationOutFilter _outFilter;
    protected final Object _channelCreationLock = new Object();
    private final IAsyncHandlerProvider _asyncHandlerProvider;
    private final IReplicationSourceGroupStateListener _stateListener;
    private final ConcurrentMap<String, EventsTracer<String>> _groupChannelsHistory;
    private final EventsTracer<String> _groupHistory;
    private volatile boolean _closed;

    public AbstractReplicationSourceGroup(DynamicSourceGroupConfigHolder groupConfigHolder,
                                          IReplicationRouter replicationRouter,
                                          IReplicationGroupBacklog groupBacklog, String myLookupName,
                                          IReplicationOutFilter outFilter,
                                          IAsyncHandlerProvider asyncHandlerProvider, IReplicationSourceGroupStateListener stateListener) {
        validateGroupConsistencyLevelPolicy(groupConfigHolder.getConfig().getGroupConsistencyLevelPolicy(), (T) groupConfigHolder.getConfig());

        _groupConfigHolder = groupConfigHolder;
        _replicationRouter = replicationRouter;
        _groupBacklog = groupBacklog;
        _myLookupName = myLookupName;
        _outFilter = outFilter;
        _asyncHandlerProvider = asyncHandlerProvider;
        _stateListener = stateListener;
        _channels = new CopyOnUpdateMap<String, AbstractReplicationSourceChannel>();
        _groupChannelsHistory = new CopyOnUpdateMap<String, EventsTracer<String>>();
        _groupHistory = new EventsTracer<String>(_groupConfigHolder.getConfig().getHistoryLength());

        _specificLogger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_GROUP + "." + _groupConfigHolder.getConfig().getName());
        _groupBacklog.setGroupHistory(this);
        //It is important that the channel will register after the backlog have registered for this notifications
        _groupConfigHolder.addListener(this);
    }

    protected void validateGroupConsistencyLevelPolicy(GroupConsistencyLevelPolicy groupConsistencyLevelPolicy, T sourceGroupConfig) {
        if (GroupConsistencyLevelPolicy.isEmptyPolicy(groupConsistencyLevelPolicy))
            return;

        throw new IllegalArgumentException("Replication group of type ["
                + getClass() + "] does not support consistency level policy ["
                + groupConsistencyLevelPolicy.getClass() + "]");
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
        EventsTracer<String> eventsTracer = _groupChannelsHistory.get(memberName);
        if (eventsTracer == null) {
            eventsTracer = new EventsTracer<String>(getConfigHolder().getConfig().getHistoryLength());
            EventsTracer<String> previous = _groupChannelsHistory.putIfAbsent(memberName, eventsTracer);
            if (previous != null)
                return previous;
        }
        return eventsTracer;
    }

    public String outputDescendingEvents(String memberName) {
        return getEventsTracer(memberName).outputDescendingEvents();
    }

    public EventsTracer<String> getChannelHistory(String memberName) {
        return _groupChannelsHistory.get(memberName);
    }

    public String getMyLookupName() {
        return _myLookupName;
    }

    public IReplicationOutFilter getOutFilter() {
        return _outFilter;
    }

    public IReplicationSourceGroupStateListener getStateListener() {
        return _stateListener;
    }

    public IAsyncHandlerProvider getAsyncHandlerProvider() {
        return _asyncHandlerProvider;
    }

    protected void createReplicationChannels() {
        synchronized (_channelCreationLock) {
            SourceGroupConfig config = _groupConfigHolder.getConfig();
            for (String memberLookupName : config.getMembersLookupNames()) {
                createChannel(memberLookupName, false, config, false, null, false);
                if (_specificLogger.isTraceEnabled())
                    _specificLogger.trace(getLogPrefix() + "created channel to "
                            + memberLookupName);
            }
            if (_specificLogger.isDebugEnabled())
                _specificLogger.trace(getLogPrefix() + "created all channels");
        }
    }

    protected void closeReplicationChannels() {
        synchronized (_channelCreationLock) {
            for (AbstractReplicationSourceChannel channel : _channels.values()) {
                channel.close();
                if (_specificLogger.isTraceEnabled())
                    _specificLogger.trace(getLogPrefix() + "closed channel to "
                            + channel.getMemberName());
            }
            _channels.clear();
            if (_specificLogger.isDebugEnabled())
                _specificLogger.trace(getLogPrefix() + "closed all channels");
        }
    }


    @Override
    public void memberAdded(MemberAddedEvent memberAddedEvent, SourceGroupConfig newConfig) {
        synchronized (_channelCreationLock) {
            _groupBacklog.memberAdded(memberAddedEvent, newConfig);
            if (_specificLogger.isDebugEnabled())
                _specificLogger.debug("adding new member [" + memberAddedEvent.getMemberName() + "] to replication group");

            onMemberAdded(memberAddedEvent, newConfig);

            logGroupEvent("new member [" + memberAddedEvent.getMemberName() + "] added to group");
        }
    }

    @Override
    public void memberRemoved(String memberName, SourceGroupConfig newConfig) {
        synchronized (_channelCreationLock) {
            if (_specificLogger.isDebugEnabled())
                _specificLogger.debug("removing member [" + memberName + "] from replication group");
            AbstractReplicationSourceChannel channel = _channels.remove(memberName);
            if (channel != null)
                channel.close();
            _groupBacklog.memberRemoved(memberName, newConfig);
            onMemberRemoved(memberName, newConfig);

            logGroupEvent("removed member [" + memberName + "] from group");
        }
    }

    protected void onMemberRemoved(String memberName, SourceGroupConfig newConfig) {
    }

    protected void onMemberAdded(MemberAddedEvent memberAddedEvent,
                                 SourceGroupConfig newConfig) {
        createChannel(memberAddedEvent.getMemberName(), true, newConfig, false/*connect synchronously*/, null, memberAddedEvent.isResetTarget());
    }

    @Override
    public void createTemporaryChannel(String memberName, Object customBacklogMetadata) {
        synchronized (_channelCreationLock) {
            createChannel(memberName, false, getConfigHolder().getConfig(), true, customBacklogMetadata, false);
        }
    }

    @Override
    public void closeTemporaryChannel(String sourceMemberName) {
        synchronized (_channelCreationLock) {
            AbstractReplicationSourceChannel channel = _channels.remove(sourceMemberName);
            if (channel != null) {
                channel.close();
                onCloseTemporaryChannel(sourceMemberName, channel);
            }
        }
    }

    protected abstract void onCloseTemporaryChannel(String sourceMemberName, AbstractReplicationSourceChannel channel);

    protected void createChannel(String memberLookupName,
                                 boolean dynamicMember, SourceGroupConfig config,
                                 boolean connectSynchronously, Object customBacklogMetadata, boolean resetTarget) {
        IReplicationMonitoredConnection connection = connectSynchronously ? _replicationRouter.getMemberConnection(memberLookupName) : _replicationRouter.getMemberConnectionAsync(memberLookupName);
        AbstractReplicationSourceChannel channel = createChannel(memberLookupName,
                _replicationRouter,
                connection,
                config.getFilter(memberLookupName),
                this,
                dynamicMember,
                config,
                customBacklogMetadata, resetTarget);
        _channels.put(memberLookupName, channel);
    }

    public int execute(IReplicationGroupOutContext groupContext) {
        int completed = executeImpl(groupContext);

        groupContext.clear();

        return completed;
    }


    protected abstract int executeImpl(IReplicationGroupOutContext groupContext);

    public void execute(IReplicationUnreliableOperation operation) {
        // TODO optimize to array save memory garbage allocation
        // TODO switch to parallel thread pool or async invocation
        for (AbstractReplicationSourceChannel channel : _channels.values())
            channel.replicate(operation);
    }

    protected abstract AbstractReplicationSourceChannel createChannel(
            String memberLookupName, IReplicationRouter replicationRouter,
            IReplicationMonitoredConnection connection,
            IReplicationChannelDataFilter dataFilter,
            IReplicationGroupHistory groupHistory, boolean dynamicMember,
            SourceGroupConfig groupConfig, Object customBacklogMetadata, boolean resetTarget);

    public String getGroupName() {
        return _groupConfigHolder.getConfig().getName();
    }

    @Override
    public DynamicSourceGroupConfigHolder getConfigHolder() {
        return _groupConfigHolder;
    }

    protected AbstractReplicationSourceChannel getChannel(
            String memberLookupName) {
        return _channels.get(memberLookupName);
    }

    public void beginSynchronizing(String synchronizingMemberLookupName, Object synchronizingSourceUniqueId, boolean isDirectPersistencySync) {
        AbstractReplicationSourceChannel channel = getChannelSafe(synchronizingMemberLookupName);
        channel.beginSynchronizing(isDirectPersistencySync);
    }


    public void beginSynchronizing(String synchronizingMemberLookupName,
                                   Object synchronizingSourceUniqueId) {
        beginSynchronizing(synchronizingMemberLookupName, synchronizingSourceUniqueId, false);
    }

    protected AbstractReplicationSourceChannel getChannelSafe(
            String synchronizingMemberLookupName) {
        AbstractReplicationSourceChannel channel = _channels.get(synchronizingMemberLookupName);
        if (channel == null)
            throw new IllegalArgumentException("No channel to specified member exists ["
                    + synchronizingMemberLookupName
                    + "], existing channels are "
                    + Arrays.toString(_channels.keySet().toArray()));
        return channel;
    }

    public boolean synchronizationDataGenerated(
            String synchronizingMemberLookupName, String uid) {
        AbstractReplicationSourceChannel channel = getChannelSafe(synchronizingMemberLookupName);
        return channel.synchronizationDataGenerated(uid);
    }

    @Override
    public void synchronizationCopyStageDone(
            String synchronizingMemberLookupName) {
        AbstractReplicationSourceChannel channel = getChannelSafe(synchronizingMemberLookupName);
        channel.synchronizationCopyStageDone();
    }

    public void stopSynchronization(
            String synchronizingMemberLookupName) {
        AbstractReplicationSourceChannel channel = _channels.get(synchronizingMemberLookupName);
        //Maybe the group state has changed and it no longer has this channel
        if (channel != null)
            channel.stopSynchronization();
    }

    public boolean checkChannelConnected(String sourceMemberLookupName) {
        AbstractReplicationSourceChannel channel = getChannelSafe(sourceMemberLookupName);
        return channel.pingTarget();
    }

    public ReplicationEndpointDetails getChannelEndpointDetails(String sourceMemberLookupName) {
        AbstractReplicationSourceChannel channel = _channels.get(sourceMemberLookupName);
        return (channel != null) ? channel.getTargetReplicationEndpointDetails() : null;
    }

    public IReplicationGroupBacklog getGroupBacklog() {
        return _groupBacklog;
    }

    public void close() {
        if (_closed)
            return;

        onClose();

        if (_specificLogger.isDebugEnabled())
            _specificLogger.debug("closing replication group");
        for (AbstractReplicationSourceChannel channel : _channels.values())
            channel.close();

        getGroupBacklog().close();

        _closed = true;

        if (_specificLogger.isDebugEnabled())
            _specificLogger.debug("replication group closed");
    }

    protected void onClose() {
        // Do nothing by default
    }

    public Map<String, Boolean> getChannelsStatus() {
        Map<String, Boolean> result = new HashMap<String, Boolean>();
        for (Map.Entry<String, AbstractReplicationSourceChannel> entry : _channels.entrySet()) {
            result.put(entry.getKey(), entry.getValue().isActive());
        }
        return result;
    }

    protected String getLogPrefix() {
        return "Replication [" + _myLookupName + "]: ";
    }

    public void setActive(MetricRegistrator metricRegistrator) {
        // Do nothing, only specific groups should treat this
    }

    public void setPassive(boolean closeProxy) {
        // Do nothing, only specific groups should treat this
    }

    public Map<String, AbstractReplicationSourceChannel> getChannels() {
        return _channels;
    }

    public void sampleStatistics() {
        for (AbstractReplicationSourceChannel channel : _channels.values()) {
            channel.sampleStatistics();
        }
    }

    public IReplicationSourceGroupStatistics getStatistics() {
        List<IReplicationSourceChannelStatistics> channelStats = new LinkedList<IReplicationSourceChannelStatistics>();
        for (AbstractReplicationSourceChannel channel : _channels.values())
            channelStats.add(channel.getStatistics());
        return new ReplicationSourceGroupStatistics(channelStats);
    }

    @Override
    public void registerWith(MetricRegistrator metricRegister) {
        for (AbstractReplicationSourceChannel channel : _channels.values()) {
            channel.registerWith(metricRegister.extend(channel.getMemberName()));
        }
    }

    @Override
    public void monitorConsistencyLevel() throws ConsistencyLevelViolationException {

    }

    public boolean flushPendingReplication(long timeout, TimeUnit units) {
        if (_specificLogger.isDebugEnabled())
            _specificLogger.debug(getLogPrefix() + "flushing pending replication");
        final Collection<AbstractReplicationSourceChannel> channels = _channels.values();
        for (AbstractReplicationSourceChannel sourceChannel : channels) {
            sourceChannel.flushPendingReplication();
        }
        long remainingTime = units.toMillis(timeout);
        final int sleepTime = 50;
        long runningTime = 0;
        while (remainingTime >= 0) {
            boolean done = true;
            long weightToReplicate = 0;
            for (AbstractReplicationSourceChannel sourceChannel : channels) {
                long channelBacklogWeight = getGroupBacklog().getWeight(sourceChannel.getMemberName());
                if (sourceChannel.isActive() && channelBacklogWeight > 0) {
                    done = false;
                    weightToReplicate = Math.max(channelBacklogWeight, weightToReplicate);
                }
            }
            if (done) {
                if (_specificLogger.isDebugEnabled())
                    _specificLogger.debug(getLogPrefix() + "flushing pending replication completed");
                return true;
            }
            if(remainingTime == 0){
                if (_specificLogger.isWarnEnabled())
                    _specificLogger.warn(getLogPrefix() + "failed flush of pending replication due to incomplete replication:" + buildingPendingReplicationMsg());
                return false;
            }
            if (remainingTime > 0)
                try {
                    if (runningTime % 5000 == 0 && _specificLogger.isInfoEnabled())
                        _specificLogger.info(getLogPrefix() + "waiting for replication of " +
                                weightToReplicate + " weight to complete [Time remaining "
                                + JSpaceUtilities.formatMillis(remainingTime) + "]:" +
                                buildingPendingReplicationMsg());
                    Thread.sleep(sleepTime);
                    runningTime += sleepTime;
                    remainingTime -= sleepTime;
                } catch (InterruptedException e) {
                    if (_specificLogger.isWarnEnabled())
                        _specificLogger.warn(getLogPrefix() + "failed replication orderly shutdown", e);
                    Thread.currentThread().interrupt();
                    return false;
                }
        }
        if (_specificLogger.isWarnEnabled())
            _specificLogger.warn(getLogPrefix() + "failed flush of pending replication due to incomplete replication:" + buildingPendingReplicationMsg());
        return false;
    }

    protected void scanAndRemoveDroppedMembers() {
        SourceGroupConfig config = getConfigHolder().getConfig();
        BacklogConfig backlogConfig = config.getBacklogConfig();
        for (String memberName : getPotentialRemovedMembers(config)) {
            try {
                if (backlogConfig.isLimited(memberName)
                        && config.getBacklogConfig()
                        .getLimitReachedPolicy(memberName) == LimitReachedPolicy.DROP_MEMBER) {
                    IBacklogMemberState state = _groupBacklog.getState(memberName);
                    if (state.isBacklogDropped()) {
                        String msg = "member ["
                                + memberName
                                + "] backlog is dropped due to capacity limitations, this member will be removed from the group";
                        if (_specificLogger.isInfoEnabled())
                            _specificLogger.info(msg);

                        logGroupEvent(msg);
                        getConfigHolder().removeMember(memberName);
                        _replicationRouter.getAdmin().removeRemoteStubHolder(memberName);
                        continue;
                    }
                }
                //Check for long disconnected channels
                SourceChannelConfig channelConfig = config.getChannelConfig(memberName);
                if (channelConfig != null && channelConfig.getMaxAllowedDisconnectionTimeBeforeDrop() != SourceChannelConfig.UNLIMITED) {
                    AbstractReplicationSourceChannel channel = getChannel(memberName);
                    //Concurrent removal of channel
                    if (channel != null && channel.getConnection().getState() == ConnectionState.DISCONNECTED) {
                        final Long timeOfDisconnection = channel.getConnection().getTimeOfDisconnection();
                        //In case the channel is reconnected between the last check in this line
                        if (timeOfDisconnection == null)
                            continue;
                        long timeDisconnected = SystemTime.timeMillis() - timeOfDisconnection;
                        if (timeDisconnected > channelConfig.getMaxAllowedDisconnectionTimeBeforeDrop()) {
                            String msg = "member ["
                                    + memberName
                                    + "] is disconnected for [" + timeDisconnected + "ms] and is being dropped due to disconnection time limitations [" + channelConfig.getMaxAllowedDisconnectionTimeBeforeDrop() + "ms], this member will be removed from the group";
                            if (_specificLogger.isInfoEnabled())
                                _specificLogger.info(msg);

                            logGroupEvent(msg);
                            getConfigHolder().removeMember(memberName);
                            _replicationRouter.getAdmin().removeRemoteStubHolder(memberName);
                        }
                    }
                }
            } catch (Exception e) {
                if (_specificLogger.isWarnEnabled())
                    _specificLogger.warn("Caught exception when scanning for dropped members [" + memberName + "]", e);
            }
        }
    }

    protected String[] getPotentialRemovedMembers(SourceGroupConfig config) {
        return new String[0];
    }

    private String buildingPendingReplicationMsg() {
        StringBuilder msg = new StringBuilder();
        for (AbstractReplicationSourceChannel channel : _channels.values()) {
            if (!channel.isActive())
                continue;
            long missingPackets = getGroupBacklog().size(channel.getMemberName());
            if (missingPackets > 0) {
                msg.append(StringUtils.NEW_LINE);
                msg.append("\ttarget: ");
                msg.append(channel.getMemberName());
                msg.append(" has ");
                msg.append(missingPackets);
                //TODO we should print proper statistics directly from the backlog per active channel and not externally
                //This is currently missing the last packet in backlog unlike old replication module that does print it
                msg.append(" replication packets remaining. Last confirmed key [" + channel.getStatistics().getLastConfirmedKey() + "]");
            }
        }
        return msg.toString();
    }

    public String dumpState() {
        StringBuilder dump = new StringBuilder("Replication Source Group [");
        dump.append(getGroupName());
        dump.append("]");
        dump.append(StringUtils.NEW_LINE + "{");
        dump.append(StringUtils.NEW_LINE);
        dump.append("Type [");
        dump.append(this.getClass().getName());
        dump.append("]");
        dump.append(StringUtils.NEW_LINE);
        dump.append("GroupConfig [");
        dump.append(getConfigHolder().getConfig());
        dump.append("]");
        dump.append(StringUtils.NEW_LINE);
        dump.append("Channels:");
        Collection<AbstractReplicationSourceChannel> channels = _channels.values();
        for (AbstractReplicationSourceChannel channel : channels) {
            dump.append(StringUtils.NEW_LINE);
            dump.append(channel.dumpState());
        }
        dump.append(StringUtils.NEW_LINE);
        dump.append("Backlog:");
        dump.append(StringUtils.NEW_LINE);
        dump.append(getGroupBacklog().dumpState());
        dump.append(StringUtils.NEW_LINE);
        dump.append("Group History:");
        dump.append(StringUtils.NEW_LINE);
        dump.append(_groupHistory.outputDescendingEvents());
        dump.append(StringUtils.NEW_LINE);
        dump.append("}");
        return dump.toString();
    }

}
