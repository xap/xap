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

package com.gigaspaces.internal.events.durable;

import com.gigaspaces.cluster.replication.IncomingReplicationOutOfSyncException;
import com.gigaspaces.events.EventSessionConfig;
import com.gigaspaces.internal.cluster.node.IReplicationNodeStateListener;
import com.gigaspaces.internal.cluster.node.impl.backlog.IBacklogMemberState;
import com.gigaspaces.internal.cluster.node.impl.config.ReplicationNodeMode;
import com.gigaspaces.internal.cluster.node.impl.groups.BrokenReplicationTopologyException;
import com.gigaspaces.internal.remoting.routing.partitioned.PartitionedClusterUtils;
import com.gigaspaces.internal.utils.concurrent.AsyncCallable;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandler;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider;
import com.gigaspaces.internal.utils.concurrent.IAsyncHandlerProvider.CycleResult;
import com.gigaspaces.time.SystemTime;

import net.jini.lease.LeaseListener;
import net.jini.lease.LeaseRenewalEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Dan Kilman
 * @since 9.0
 */
@com.gigaspaces.api.InternalApi
public class DurableNotificationReplicationNodeStateListener
        implements IReplicationNodeStateListener {

    private final Logger _logger;

    private final EventSessionConfig _config;
    private final DurableNotificationLease _lease;
    private final LeaseListener _leaseListener;
    private final IAsyncHandlerProvider _asyncProvider;
    private final int _numOfPartitions;
    private final int _partitionId;

    private final Object _lock = new Object();

    // Guarded by _lock
    private final Map<Integer, Long> _disconnectedPartitions;
    private long _disconnectTime;
    private boolean _disconnectionMonitorInProgress;
    private boolean _channelClosed;
    private IAsyncHandler _asyncHandler;

    public DurableNotificationReplicationNodeStateListener(
            EventSessionConfig config,
            DurableNotificationLease lease,
            IAsyncHandlerProvider asyncProvider, int numOfPartitions, int partitionId) {
        _logger = lease.getLogger();
        _lease = lease;
        _config = config;
        _asyncProvider = asyncProvider;
        _numOfPartitions = numOfPartitions;
        _partitionId = partitionId;
        _leaseListener = _config.getLeaseListener();
        _disconnectedPartitions = populateDisconnectedPartitions();
    }

    public void startDisconnectionMonitoring() {
        synchronized (_lock) {
            if (_disconnectionMonitorInProgress)
                return;

            if (_logger.isDebugEnabled())
                _logger.debug("Starting disconnection monitoring");

            _disconnectionMonitorInProgress = true;
            _asyncHandler = _asyncProvider.start(new DisconnectionMonitorAsyncCallable(), 1000, "DurableNotificationDisconnectionMonitor", false);

        }
    }

    @Override
    public boolean onTargetChannelOutOfSync(String groupName,
                                            String channelSourceLookupName,
                                            IncomingReplicationOutOfSyncException outOfSyncReason) {
        synchronized (_lock) {
            if (_channelClosed)
                return true;
        }

        notifyClientListenerAndCloseStateListener();
        return true;
    }

    @Override
    public void onTargetChannelBacklogDropped(String groupName,
                                              String channelSourceLooString, IBacklogMemberState memberState) {
        synchronized (_lock) {
            if (_channelClosed)
                return;
        }

        notifyClientListenerAndCloseStateListener();
    }

    @Override
    public void onTargetChannelSourceDisconnected(String groupName,
                                                  String sourceMemberName, Object sourceUniqueId) {
        synchronized (_lock) {
            if (_channelClosed)
                return;

            if (_logger.isDebugEnabled())
                _logger.debug("Target channel source disconnected, member name: " + sourceMemberName);

            // Get current time and partition:
            final long currTime = SystemTime.timeMillis();
            final int partitionId = extractPartitionId(sourceMemberName);

            // Register partition disconnect time:
            _disconnectedPartitions.put(partitionId, currTime);

            _disconnectTime = updateDisconnectTime();

            if (_config.isAutoRenew() && _config.getLeaseListener() != null)
                startDisconnectionMonitoring();
        }

    }

    @Override
    public void onTargetChannelConnected(String groupName,
                                         String sourceMemberName, Object sourceUniqueId) {
        synchronized (_lock) {
            if (_channelClosed)
                return;

            if (_logger.isDebugEnabled())
                _logger.debug("Target channel connected, member name: " + sourceMemberName);

            final int partitionId = extractPartitionId(sourceMemberName);
            _disconnectedPartitions.remove(partitionId);
            _disconnectTime = updateDisconnectTime();
        }

    }

    @Override
    public void onReplicationNodeModeChange(ReplicationNodeMode newMode) {
    }

    public void close() {
        synchronized (_lock) {
            if (_channelClosed)
                return;

            if (_asyncHandler != null)
                _asyncHandler.stop(3, TimeUnit.SECONDS);

            _asyncHandler = null;

            _channelClosed = true;
        }
    }

    private Map<Integer, Long> populateDisconnectedPartitions() {
        Map<Integer, Long> result = new HashMap<Integer, Long>();

        int startIndex = _partitionId == PartitionedClusterUtils.NO_PARTITION ? 0 : _partitionId;
        int endIndex = _partitionId == PartitionedClusterUtils.NO_PARTITION ? (_numOfPartitions - 1) : _partitionId;

        for (int i = startIndex; i <= endIndex; i++) {
            long currentTime = SystemTime.timeMillis();
            result.put(i, currentTime);
        }

        return result;
    }

    private void notifyClientListenerAndCloseStateListener() {
        synchronized (_lock) {
            if (_channelClosed)
                return;

            _channelClosed = true;
        }

        if (_logger.isDebugEnabled())
            _logger.debug("Sending disconnection notification and closing listener");

        // We spawn a new thread because the lease cancel is mapped to closing the enpoint which in turn will unregister
        // this replication target which will close the async hanlder at the server side which belongs to this channel
        // and it is currently waiting for the response from this invocation
        new Thread(new Runnable() {
            public void run() {
                _lease.cancel();
            }
        }).start();

        if (_leaseListener != null)
            _leaseListener.notify(new LeaseRenewalEvent(this, _lease, _lease.getExpiration(), null));

    }

    private boolean isHealthy() {
        final long disconnectTime = _disconnectTime;
        // If no disconnect is logged, this target is healthy:
        if (isAllConnected())
            return true;
        // Calculate disconnect duration:
        final long disconnectDuration = SystemTime.timeMillis() - disconnectTime;
        return disconnectDuration < _config.getRenewDuration();
    }

    private boolean isAllConnected() {
        return _disconnectTime == 0;
    }


    private long updateDisconnectTime() {
        long oldestDisconnectTime = 0;

        for (Long disconnectTime : _disconnectedPartitions.values())
            if (oldestDisconnectTime == 0 || oldestDisconnectTime > disconnectTime)
                oldestDisconnectTime = disconnectTime;

        return oldestDisconnectTime;
    }

    private static int extractPartitionId(String memberName) {
        return PartitionedClusterUtils.extractPartitionIdFromSpaceName(memberName);
    }

    public void onTargetChannelCreationValidation(String groupName, String sourceMemberName, Object sourceUniqueId) {
    }

    public void onSourceBrokenReplicationTopology(String groupName, String channelTargetMemberName,
                                                  BrokenReplicationTopologyException error) {
    }

    public void onSourceChannelActivated(String groupName, String memberName) {
    }

    public void onNewReplicaRequest(String groupName, String channelName, boolean isSynchronizeRequest) {
    }

    private class DisconnectionMonitorAsyncCallable extends AsyncCallable {

        public CycleResult call() throws Exception {
            synchronized (_lock) {
                _disconnectTime = updateDisconnectTime();

                if (isAllConnected()) {
                    if (_logger.isDebugEnabled())
                        _logger.debug("From monitor: all partitions connected, stable state restored");

                    _disconnectionMonitorInProgress = false;
                    return CycleResult.TERMINATE;
                } else if (isHealthy()) {
                    if (_logger.isDebugEnabled())
                        _logger.debug("From monitor: Not all partitions are alive yet, rechecking");

                    return CycleResult.IDLE_CONTINUE;
                } else {
                    if (_logger.isDebugEnabled())
                        _logger.debug("From monitor: Maximum disconnection time elapsed, closing registration");

                    notifyClientListenerAndCloseStateListener();

                    _disconnectionMonitorInProgress = false;
                    return CycleResult.TERMINATE;
                }
            }
        }

    }

}
