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

package com.gigaspaces.internal.cluster.node.impl.router.spacefinder.failuredetector;

import com.gigaspaces.cluster.ClusterFailureDetector;
import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractProxyBasedReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.ConnectionState;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IConnectionMonitor;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IReplicationConnectionProxy;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.ISpaceProxyProvider;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.RemoteSpaceResult;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.collections.CopyOnUpdateSet;
import com.gigaspaces.internal.utils.concurrent.GSThreadFactory;
import com.gigaspaces.logger.Constants;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.cluster.startup.FaultDetectionListener;

import net.jini.core.lookup.ServiceID;
import net.jini.id.Uuid;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link ClusterFailureDetector} based implementation of the {@link IConnectionMonitor}
 * interface. Used an internal pool to monitor dead connections and the ClusterFailureDetector to
 * detect live connections failure
 *
 * @author eitany
 * @since 8.0
 */
@SuppressWarnings("deprecation")
@com.gigaspaces.api.InternalApi
public class FailureDetectorConnectionMonitor
        implements IConnectionMonitor<IRemoteSpace, SpaceURL>,
        FaultDetectionListener {

    private final ScheduledExecutorService _pool;
    private final long _monitorDisconnectedDelay;
    private final TimeUnit _timeUnit;
    private final Map<AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>, ScheduledFuture<?>> _monitoredDisconnectedFutures = new ConcurrentHashMap<AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>, ScheduledFuture<?>>();
    private final ConcurrentMap<ServiceID, Set<AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>>> _livenessMonitored = new ConcurrentHashMap<ServiceID, Set<AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>>>();
    private final ISpaceProxyProvider _proxyProvider;
    private final String _myLookupName;
    private final IFailureDetector _failureDetector;
    private final Logger _specificLogger;
    private volatile boolean _closed;

    public FailureDetectorConnectionMonitor(String myLookupName,
                                            int corePoolSize, long monitorDisconnectedDelay, TimeUnit timeUnit,
                                            ISpaceProxyProvider proxyProvider, IFailureDetector failureDetector) {
        _monitorDisconnectedDelay = monitorDisconnectedDelay;
        _timeUnit = timeUnit;
        _proxyProvider = proxyProvider;
        _myLookupName = myLookupName;
        _failureDetector = failureDetector;
        _specificLogger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_ROUTER
                + "." + ReplicationLogUtils.toShortLookupName(_myLookupName));
        _pool = new ScheduledThreadPoolExecutor(corePoolSize,
                new GSThreadFactory(null, true));
    }

    public void monitor(
            AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection) {
        synchronized (connection.getStateLock()) {
            ConnectionState connectionState = connection.getState();
            if (connectionState == ConnectionState.CONNECTED) {
                try {
                    monitorConnected(connection);
                } catch (Exception e) {
                    updateDisconnected(connection, e);
                }
            } else if (connectionState == ConnectionState.DISCONNECTED) {
                monitorDisconnected(connection);
            }
        }
    }

    private String getLogPrefix() {
        return "Failure Monitor [" + _myLookupName + "]: ";
    }

    // Should be called under state lock
    private void monitorDisconnected(
            AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection) {
        if (_monitoredDisconnectedFutures.containsKey(connection))
            throw new IllegalArgumentException("Provided connection "
                    + connection + " is already monitored");

        if (_specificLogger.isTraceEnabled())
            _specificLogger.trace(getLogPrefix()
                    + "monitoring disconnected connection "
                    + connection.getTargetLookupName());

        MonitorDisconnectedConnectionTask monitorTask = new MonitorDisconnectedConnectionTask(connection,
                this);
        try {
            ScheduledFuture<?> future = _pool.scheduleWithFixedDelay(monitorTask,
                    0,
                    _monitorDisconnectedDelay,
                    _timeUnit);
            _monitoredDisconnectedFutures.put(connection, future);
        } catch (RejectedExecutionException e) {
            if (!_closed)
                throw e;
        }
    }

    // Should be called under state lock
    private void monitorConnected(
            AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection)
            throws Exception {
        IRemoteSpace spaceProxy = connection.getTag();
        ServiceID serviceId = connection.getServiceId();

        if (_specificLogger.isTraceEnabled())
            _specificLogger.trace(getLogPrefix() + "monitoring connected connection "
                    + connection.getTargetLookupName() + StringUtils.NEW_LINE
                    + "ServiceID=" + serviceId);

        boolean firstSubscriber;
        synchronized (_livenessMonitored) {
            Set<AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>> monitoredConnections = _livenessMonitored.get(serviceId);
            if (monitoredConnections == null) {
                firstSubscriber = true;
                monitoredConnections = new CopyOnUpdateSet<AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>>();
                _livenessMonitored.put(serviceId, monitoredConnections);
            } else
                firstSubscriber = false;

            monitoredConnections.add(connection);
        }
        if (firstSubscriber)
            _failureDetector.registerRemoteSpace(this, serviceId, spaceProxy);
    }

    public void stopMonitoring(
            AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection) {
        synchronized (connection.getStateLock()) {
            ScheduledFuture<?> future = _monitoredDisconnectedFutures.remove(connection);
            if (future != null)
                future.cancel(false);

            ServiceID serviceId = connection.getServiceId();
            if (serviceId != null) {
                // Warning, potential deadlock due to nested locks. change code
                // with extreme care.
                synchronized (_livenessMonitored) {
                    Set<AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>> connectionSet = _livenessMonitored.get(serviceId);
                    if (connectionSet != null) {
                        connectionSet.remove(connection);
                        if (connectionSet.isEmpty())
                            _livenessMonitored.remove(serviceId);
                    }
                }
            }
        }
    }

    private RemoteSpaceResult getSpaceProxy(SpaceURL spaceURL)
            throws FinderException {
        return _proxyProvider.getSpaceProxy(spaceURL);
    }

    public void updateDisconnected(
            AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection,
            Exception reason) {
        synchronized (connection.getStateLock()) {
            if (_monitoredDisconnectedFutures.containsKey(connection)) {
                // This can occur if we got both serviceFailure event and a
                // dispatch invocation caught remote exception
                // The service failure called updateDisconnected and after that
                // the dispatch invocation
                // called updateDisconnected.
                return;
            }
            if (_specificLogger.isDebugEnabled())
                _specificLogger.debug(getLogPrefix()
                        + "connection disconnection detected "
                        + connection.getTargetLookupName() + reason != null ? " reason - "
                        + reason
                        : "");
            stopMonitoring(connection);
            connection.setDisconnected(reason);
            monitorDisconnected(connection);
        }
    }

    public int getMonitoredCount() {
        return _monitoredDisconnectedFutures.size() + _livenessMonitored.size();
    }

    public void serviceFailure(Object service, Object serviceID) {
        if (_specificLogger.isDebugEnabled())
            _specificLogger.debug(getLogPrefix() + "received failure notice for "
                    + service + ", " + serviceID);
        Set<AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>> connectionSet = _livenessMonitored.get(serviceID);
        if (connectionSet != null) {
            for (AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection : connectionSet) {
                synchronized (connection.getStateLock()) {
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug(getLogPrefix()
                                + "monitoring failure detected via service failure "
                                + connection.getTargetLookupName());
                    updateDisconnected(connection, null);
                }
            }
        }
    }

    public void close() {
        if (_closed)
            return;

        _closed = true;

        for (ScheduledFuture<?> future : _monitoredDisconnectedFutures.values()) {
            future.cancel(true);
        }
        _livenessMonitored.clear();
        _pool.shutdownNow();
    }

    public String dumpState() {
        StringBuilder dump = new StringBuilder("Disconnected monitored:");
        dump.append(StringUtils.NEW_LINE);
        for (AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection : _monitoredDisconnectedFutures.keySet()) {
            dump.append(connection.dumpState());
            dump.append(StringUtils.NEW_LINE);
        }
        dump.append("Connected monitored:");
        dump.append(StringUtils.NEW_LINE);
        for (Entry<ServiceID, Set<AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>>> entry : _livenessMonitored.entrySet()) {
            dump.append("ServiceID [" + entry.getKey() + "]");
            dump.append(StringUtils.NEW_LINE);
            dump.append("Connections: ");
            dump.append(StringUtils.NEW_LINE);
            for (AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection : entry.getValue()) {
                dump.append(connection.dumpState());
                dump.append(StringUtils.NEW_LINE);
            }
        }
        return dump.toString();
    }

    public class MonitorDisconnectedConnectionTask
            implements Runnable {

        private final AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> _connection;
        private final FailureDetectorConnectionMonitor _monitor;

        public MonitorDisconnectedConnectionTask(
                AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection,
                FailureDetectorConnectionMonitor monitor) {
            _connection = connection;
            _monitor = monitor;
        }

        public void run() {
            try {
                if (_specificLogger.isDebugEnabled())
                    _specificLogger.debug(getLogPrefix()
                            + "trying to establish connection with "
                            + _connection.getTargetLookupName() + " ["
                            + _connection.getFinderURL() + "]");
                RemoteSpaceResult result = _monitor.getSpaceProxy(_connection.getFinderURL());
                IRemoteSpace remoteSpace = result.getRemoteSpace();
                if (remoteSpace != null) {
                    String endpointLookupName = result.getEndpointLookupName();
                    IReplicationConnectionProxy connectionProxy = null;
                    Uuid proxyId = null;
                    try {
                        proxyId = remoteSpace.getSpaceUuid();
                        // if (!_connection.isUniqueId() ||
                        // proxyId.equals(_connection.getProxyId()))
                        connectionProxy = remoteSpace.getReplicationRouterConnectionProxy();
                    } catch (RemoteException e) {
                        // Proxy disconnected, keep this disconnected monitor
                        // task alive
                        _connection.setLastDisconnectionReason(e);
                    }
                    if (connectionProxy != null) {
                        synchronized (_connection.getStateLock()) {
                            if (_specificLogger.isDebugEnabled())
                                _specificLogger.debug(getLogPrefix()
                                        + "established connection with "
                                        + _connection.getTargetLookupName()
                                        + " [" + _connection.getFinderURL()
                                        + "]");
                            _connection.setConnected(connectionProxy,
                                    remoteSpace,
                                    endpointLookupName,
                                    proxyId);
                            // Stop this monitoring of the connection
                            _monitor.stopMonitoring(_connection);
                            _monitor.monitor(_connection);
                        }
                    } else {
                        if (_specificLogger.isWarnEnabled())
                            _specificLogger.warn(getLogPrefix()
                                    + "failed to establish connection with "
                                    + _connection.getTargetLookupName() + " ["
                                    + _connection.getFinderURL() + "]"
                                    + StringUtils.NEW_LINE
                                    + "no connection proxy available");
                    }
                }
            } catch (FinderException e) {
                if (_specificLogger.isTraceEnabled())
                    _specificLogger.trace(getLogPrefix()
                            + "failed to establish connection with "
                            + _connection.getTargetLookupName() + " ["
                            + _connection.getFinderURL() + "]"
                            + StringUtils.NEW_LINE + e);
                // Proxy still not available do nothing
                _connection.setLastDisconnectionReason(e);
            }

        }
    }

}
