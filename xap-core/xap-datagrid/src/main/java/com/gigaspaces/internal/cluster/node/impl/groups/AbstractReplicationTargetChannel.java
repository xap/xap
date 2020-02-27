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

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogHandshakeRequest;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.backlog.IIdleStateData;
import com.gigaspaces.internal.cluster.node.impl.config.TargetGroupConfig;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilter;
import com.gigaspaces.internal.cluster.node.impl.filters.IReplicationInFilterCallback;
import com.gigaspaces.internal.cluster.node.impl.groups.handshake.IHandshakeIteration;
import com.gigaspaces.internal.cluster.node.impl.packets.CheckSourceChannelPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.IReplicationOrderedPacket;
import com.gigaspaces.internal.cluster.node.impl.packets.data.IReplicationPacketData;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessLogHandshakeResponse;
import com.gigaspaces.internal.cluster.node.impl.processlog.IProcessResult;
import com.gigaspaces.internal.cluster.node.impl.processlog.IReplicationTargetProcessLog;
import com.gigaspaces.internal.cluster.node.impl.router.IConnectionStateListener;
import com.gigaspaces.internal.cluster.node.impl.router.IConnectivityCheckListener;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationEndpointDetails;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.LogLevel;
import com.gigaspaces.time.SystemTime;
import com.j_spaces.core.cluster.IReplicationFilterEntry;
import com.j_spaces.core.filters.ReplicationStatistics.ReplicationMode;

import java.rmi.RemoteException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class AbstractReplicationTargetChannel
        implements IReplicationInFilterCallback, IReplicationTargetChannel, IConnectionStateListener, IConnectivityCheckListener {
    // A logger specific to this channel instance only
    protected final Logger _specificLogger;
    protected final Logger _specificVerboseLogger;

    private final String _myLookupName;
    private final ReplicationEndpointDetails _sourceEndpointDetails;
    private final IReplicationTargetProcessLog _processLog;
    private final IReplicationInFilter _inFilter;
    private final String _groupName;
    private final boolean _isInFiltered;
    private final IReplicationMonitoredConnection _sourceConnection;
    private final Object _myUniqueId;
    private final IReplicationTargetGroupStateListener _groupStateListener;
    private final IReplicationGroupHistory _groupHistory;
    private final ReplicationMode _channelType;
    private final Object _channelStateLock = new Object();
    private final Object _lastProcessSampleLock = new Object();

    private boolean _connected;
    private boolean _wasEverActive;
    private volatile boolean _outOfSync;
    //Not volatile to not cause volatile write on every incoming process packet, the odd for repeatedly
    //having packets being processes but the time stamp is not flushed to memory is extremely low.
    //Worse case the upper layer will think there was no process progress and restart recovery process
    private long _lastProcessTimeStamp = -1;


    public AbstractReplicationTargetChannel(TargetGroupConfig groupConfig,
                                            String myLookupName, Object myUniqueId, ReplicationEndpointDetails sourceEndpointDetails,
                                            IReplicationMonitoredConnection sourceConnection, String groupName,
                                            IReplicationTargetProcessLog processLog, IReplicationInFilter inFilter,
                                            IReplicationTargetGroupStateListener groupStateListener,
                                            IReplicationGroupHistory groupHistory,
                                            boolean wasEverActive) {
        _myLookupName = myLookupName;
        _myUniqueId = myUniqueId;
        _sourceEndpointDetails = sourceEndpointDetails;
        _sourceConnection = sourceConnection;
        _groupName = groupName;
        _processLog = processLog;
        _inFilter = inFilter;
        _groupStateListener = groupStateListener;
        _groupHistory = groupHistory;
        _wasEverActive = wasEverActive;
        _channelType = groupConfig.getGroupChannelType();
        _isInFiltered = _inFilter != null;
        _specificLogger = ReplicationLogUtils.createIncomingChannelSpecificLogger(getSourceLookupName(), _myLookupName, _groupName);
        _specificVerboseLogger = ReplicationLogUtils.createChannelSpecificVerboseLogger(getSourceLookupName(), _myLookupName, _groupName);

        _sourceConnection.setConnectionStateListener(this);
        //This is not entirely safe as the final target version could be less than 8.0.5, we only pass source endpoint full details from 9.0.1
        //so we cannot really know the final source version if it is smaller than 9.0.1, so we'll keep this validation, at the worse case we will fire
        //false backwards check 
        if (_sourceConnection.supportsConnectivityCheckEvents())
            _sourceConnection.setConnectivityCheckListener(this);
    }

    @Override
    public IReplicationTargetProcessLog getProcessLog() {
        return _processLog;
    }

    public String getSourceLookupName() {
        return _sourceEndpointDetails.getLookupName();
    }

    public String getMyLookupName() {
        return _myLookupName;
    }

    public Object getSourceUniqueId() {
        return _sourceEndpointDetails.getUniqueId();
    }

    public Object processBatch(List<IReplicationOrderedPacket> packets) {
        if (_specificLogger.isLoggable(Level.FINEST))
            _specificLogger.finest("Incoming packets: " + ReplicationLogUtils.packetsToLogString(packets));

        updateLastProcessTimeStamp();

        // Process packets
        IProcessResult processResult = _processLog.processBatch(getSourceLookupName(),
                packets,
                _isInFiltered ? this
                        : null);
        logProcessResultIfNecessary(processResult, packets);
        return _processLog.toWireForm(processResult);
    }

    public Object processIdleStateData(IIdleStateData idleStateData) {
        if (_specificVerboseLogger.isLoggable(Level.FINEST))
            _specificVerboseLogger.finest("incoming idle state data replication: "
                    + idleStateData);

        updateLastProcessTimeStamp();

        // Process idle state data
        IProcessResult processResult = _processLog.processIdleStateData(getSourceLookupName(),
                idleStateData,
                _isInFiltered ? this
                        : null);
        logProcessResultIfNecessary(processResult, Collections.<IReplicationOrderedPacket>emptyList());
        return _processLog.toWireForm(processResult);
    }

    private void logProcessResultIfNecessary(
            IProcessResult processResult, List<IReplicationOrderedPacket> packets) {
        if (_specificVerboseLogger.isLoggable(Level.FINEST)) {
            String packetsBatchText = packets.isEmpty() ? "IDLE-STATE"
                    : "firstPacketKey="
                    + packets.get(0)
                    .getKey()
                    + ", lastPacketKey="
                    + packets.get(packets.size() - 1)
                    .getEndKey();
            _specificVerboseLogger.finest("Returning " + processResult.toString() + " for received packets batch [" + packetsBatchText + "]");
        }
    }

    private void updateLastProcessTimeStamp() {
        _lastProcessTimeStamp = SystemTime.timeMillis();
    }

    public Object process(IReplicationOrderedPacket packet) {
        if (_specificLogger.isLoggable(Level.FINEST))
            _specificLogger.finest("Incoming packet: " + packet);

        updateLastProcessTimeStamp();

        // Process packets
        IProcessResult processResult = _processLog.process(getSourceLookupName(),
                packet,
                _isInFiltered ? this
                        : null);
        if (_specificVerboseLogger.isLoggable(Level.FINEST))
            _specificVerboseLogger.finest("Returning " + processResult.toString() + " for received packet [packetKey=" + packet.getKey() + "]");

        return _processLog.toWireForm(processResult);
    }

    /*
     * @see
     * com.gigaspaces.internal.cluster.node.impl.groups.IReplicationInFilterCallback
     * #invokeInFilter(com.gigaspaces.internal.cluster.node.impl.packets.data.
     * IReplicationPacketData)
     */
    public void invokeInFilter(IReplicationInContext context,
                               IReplicationPacketData<?> data) {
        if (!data.supportsReplicationFilter())
            return;

        // Each packet can become more than one filter entry
        // (transaction)
        Iterable<IReplicationFilterEntry> filterEntries = _processLog.getDataConsumer()
                .toFilterEntries(context, data);
        // Invoke filter on entries
        for (IReplicationFilterEntry filterEntry : filterEntries) {
            _inFilter.filterIn(filterEntry, getSourceLookupName(), _groupName);
        }

        if (data.isEmpty()) {
            if (_specificLogger.isLoggable(Level.FINER))
                _specificLogger.finer("Input filter discarded packet " + data);
        }
    }

    public IProcessLogHandshakeResponse performHandshake(
            IBacklogHandshakeRequest handshakeRequest) {
        synchronized (_channelStateLock) {
            if (_specificLogger.isLoggable(Level.FINE))
                _specificLogger.fine("Received handshake request {" + handshakeRequest.toLogMessage() + "}");

            IProcessLogHandshakeResponse handshakeResponse;
            try {
                handshakeResponse = _processLog.performHandshake(getSourceLookupName(),
                        handshakeRequest);
            } catch (IncomingReplicationOutOfSyncException e) {
                String msg = "Channel is out of sync:"
                        + StringUtils.NEW_LINE + "[  source space: "
                        + getSourceLookupName() + "  ] " + StringUtils.NEW_LINE
                        + "[  target space: " + _myLookupName + "  ] "
                        + StringUtils.NEW_LINE + "[  replication group name: "
                        + _groupName + " ]" + StringUtils.NEW_LINE
                        + "Handshake request details ["
                        + handshakeRequest.toLogMessage() + "]"
                        + StringUtils.NEW_LINE + "ProcessLog state ["
                        + _processLog.toLogMessage() + "]";

                if (_groupStateListener != null
                        && _groupStateListener.onTargetChannelOutOfSync(_groupName,
                        getSourceLookupName(),
                        e)) {
                    handshakeResponse = _processLog.resync(handshakeRequest);
                    logEventInHistory(msg
                            + StringUtils.NEW_LINE
                            + "Reason - "
                            + e
                            + StringUtils.NEW_LINE
                            + "Outcome: Resynchronized channel and dropped missing packets");
                    if (_specificLogger.isLoggable(Level.SEVERE))
                        _specificLogger.log(Level.SEVERE,
                                msg + StringUtils.NEW_LINE +
                                        "Outcome: Resynchronized channel and dropped missing packets",
                                e);
                } else {
                    logEventInHistory(msg + StringUtils.NEW_LINE + "Reason - " + e);
                    _specificLogger.log(Level.SEVERE, msg, e);
                    throw new ReplicationChannelOutOfSync(e.getMessage(), e);
                }
            }

            if (_specificLogger.isLoggable(Level.FINE))
                _specificLogger.fine("Handshake response {" + handshakeResponse.toLogMessage() + "}");

            // Track completed handshakes
            logEventInHistory("Handshake executed:"
                    + StringUtils.NEW_LINE
                    + "\trequest: "
                    + (handshakeRequest != null ? handshakeRequest.toLogMessage()
                    : null)
                    + StringUtils.NEW_LINE
                    + "\tresponse: "
                    + (handshakeResponse != null ? handshakeResponse.toLogMessage()
                    : null));

            String msg = "Channel established " + getConnectionDescription();

            logEventInHistory(msg);
            LogLevel logLevel = requiresHighLevelLogging() || _wasEverActive ? LogLevel.INFO : LogLevel.DEBUG;
            logLevel.log(_specificLogger, msg);

            _wasEverActive = true;

            if (!_connected) {
                _connected = true;
                if (_groupStateListener != null)
                    _groupStateListener.onTargetChannelConnected(_groupName, getSourceLookupName(), getSourceUniqueId());
            }
            return handshakeResponse;
        }
    }

    public void processHandshakeIteration(IHandshakeIteration handshakeIteration) {
        boolean detailed = _specificLogger.isLoggable(Level.FINEST);
        if (_specificLogger.isLoggable(Level.FINER))
            _specificLogger.finer("Received handshake iteration {"
                    + handshakeIteration.toLogMessage(detailed) + "}");

        updateLastProcessTimeStamp();

        _processLog.processHandshakeIteration(getSourceLookupName(),
                handshakeIteration);
    }

    public void close(long channelCloseTimeout, TimeUnit unit) {
        try {
            if (!_processLog.close(channelCloseTimeout, unit)
                    && _specificLogger.isLoggable(Level.WARNING))
                _specificLogger.warning("Channel was not closed gracefully, close operation timed-out");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            _sourceConnection.close();
        }
    }

    public boolean isSourceAttached() {
        boolean sourceAttached = false;
        try {
            // Check if the already known source as this member is still
            // considered the source
            sourceAttached = _sourceConnection.dispatch(new CheckSourceChannelPacket(_groupName,
                    _myUniqueId,
                    false));
        } catch (RemoteException e) {
            // isSource is still false
        }
        return sourceAttached;
    }

    public void onChannelBacklogDropped(String groupName,
                                        IBacklogMemberState memberState,
                                        IReplicationTargetGroupStateListener listener) {
        if (_outOfSync)
            return;

        _outOfSync = true;
        String msg = "Channel is out of sync, backlog was dropped by source {" + memberState.toLogMessage() + "}";
        logEventInHistory(msg);
        if (listener != null) {
            _specificLogger.severe(msg);
            listener.onTargetChannelBacklogDropped(groupName,
                    getSourceLookupName(),
                    memberState);
        }

    }

    protected String onDumpState() {
        return "";
    }

    public String dumpState() {
        return "Channel [" + getSourceLookupName() + ", " + getSourceUniqueId()
                + "]" + StringUtils.NEW_LINE + "Type ["
                + this.getClass().getName() + "]" + StringUtils.NEW_LINE
                + "Process log: " + _processLog.dumpState() + onDumpState()
                + StringUtils.NEW_LINE + "latest history:"
                + StringUtils.NEW_LINE
                + _groupHistory.outputDescendingEvents(getSourceLookupName());
    }

    protected void logEventInHistory(String event) {
        _groupHistory.logEvent(getSourceLookupName(), event);
    }

    @Override
    public void onConnected(boolean newTarget) {
        synchronized (_channelStateLock) {
            if (_connected)
                return;

            try {
                // After a disconnection the source may have dropped this target due to backlog limitations (local view..)
                // However, the source is physically available to this target. We want to prevent this channel from becoming
                // connected and therefore verifying the source still considers this target as a replication target.
                if (!_sourceConnection.dispatch(new CheckSourceChannelPacket(_groupName, _myUniqueId, true)))
                    return;
            } catch (RemoteException e) {
                return;
            }

            _connected = true;

            String msg = "Channel reestablished connection to source " + getConnectionDescription();
            logEventInHistory(msg);
            LogLevel logLevel = requiresHighLevelLogging() || _wasEverActive ? LogLevel.INFO : LogLevel.DEBUG;
            logLevel.log(_specificLogger, msg);

            if (_groupStateListener != null)
                _groupStateListener.onTargetChannelConnected(_groupName, getSourceLookupName(), getSourceUniqueId());
        }
    }

    @Override
    public void afterSuccessfulConnectivityCheck() {
        try {
            // After every successful connectivity check (which only checks physical connection) verify logically
            // that the source has a replication channel connected to this target.
            if (!_sourceConnection.dispatch(new CheckSourceChannelPacket(_groupName, _myUniqueId, true)))
                onDisconnected();
            else
                onConnected(false);
        } catch (RemoteException e) {
        }
    }

    @Override
    public void onDisconnected() {
        synchronized (_channelStateLock) {
            if (!_connected)
                return;

            _connected = false;

            String msg = "Channel lost connection to source " + getConnectionDescription();
            logEventInHistory(msg);
            _specificLogger.info(msg);

            if (_groupStateListener != null)
                _groupStateListener.onTargetChannelSourceDisconnected(_groupName, getSourceLookupName(), getSourceUniqueId());
        }
    }

    private String getConnectionDescription() {
        return "[" +
                "source=" + ReplicationLogUtils.toShortLookupName(getSourceLookupName()) + ", " +
                "source url=" + _sourceConnection.getConnectionUrl() + ", " +
                "source machine connection url=" + _sourceConnection.getClosestEndpointAddress() +
                "]";
    }

    public Object getSourceEndpointAddress() {
        return _sourceConnection.getClosestEndpointAddress();
    }

    public long getLastProcessTimeStamp() {
        //Force memory barrier
        synchronized (_lastProcessSampleLock) {
            return _lastProcessTimeStamp;
        }
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