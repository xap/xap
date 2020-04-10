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

package com.gigaspaces.internal.cluster.node.impl.router;

import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IConnectionMonitor;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.internal.utils.concurrent.GSThreadFactory;
import com.gigaspaces.logger.Constants;

import java.rmi.RemoteException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractScheduledPoolConnectionMonitor<T, L>
        implements IConnectionMonitor<T, L> {
    protected final Logger _specificLogger;
    private final ScheduledExecutorService _pool;
    private final long _monitorConnectedDelay;
    private final long _monitorDisconnectedDelay;
    private final TimeUnit _timeUnit;
    private final Map<AbstractProxyBasedReplicationMonitoredConnection<T, L>, ScheduledFuture<?>> _monitoredDisconnectedFutures = new ConcurrentHashMap<AbstractProxyBasedReplicationMonitoredConnection<T, L>, ScheduledFuture<?>>();
    private final Map<AbstractProxyBasedReplicationMonitoredConnection<T, L>, ScheduledFuture<?>> _monitoredConnectedFutures = new ConcurrentHashMap<AbstractProxyBasedReplicationMonitoredConnection<T, L>, ScheduledFuture<?>>();
    private final String _myLookupName;

    private volatile boolean _closed;

    /* determines if the thread executing a future 'connection monitoring' task should be interrupted;
       otherwise, in-progress tasks are allowed to complete. Default is false. */
    public final boolean mayInterruptIfRunning = Boolean.getBoolean("com.gs.replication.connection-monitor.mayInterruptIfRunning");


    public AbstractScheduledPoolConnectionMonitor(String myLookupName,
                                                  int corePoolSize, long monitorConnectedDelay,
                                                  long monitorDisconnectedDelay, TimeUnit timeUnit) {
        _myLookupName = myLookupName;
        _monitorConnectedDelay = monitorConnectedDelay;
        _monitorDisconnectedDelay = monitorDisconnectedDelay;
        _timeUnit = timeUnit;
        _specificLogger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_ROUTER
                + "." + ReplicationLogUtils.toShortLookupName(_myLookupName));
        _pool = new ScheduledThreadPoolExecutor(corePoolSize,
                new GSThreadFactory("connection-monitor-thread", true));
    }

    protected String getLogPrefix() {
        return "Failure Monitor [" + _myLookupName + "]: ";
    }

    public void monitor(
            AbstractProxyBasedReplicationMonitoredConnection<T, L> connection) {
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

    // Should be called under state lock
    private void monitorDisconnected(
            AbstractProxyBasedReplicationMonitoredConnection<T, L> connection) {
        if (_monitoredDisconnectedFutures.containsKey(connection))
            throw new IllegalArgumentException("Provided connection "
                    + connection + " is already monitored");

        if (_specificLogger.isTraceEnabled())
            _specificLogger.trace(getLogPrefix()
                    + "monitoring disconnected connection "
                    + connection.getTargetLookupName());

        Runnable monitorTask = createMonitorDisconnectedTask(connection);
        try {
            ScheduledFuture<?> future = _pool.scheduleWithFixedDelay(monitorTask,
                    _monitorDisconnectedDelay,
                    _monitorDisconnectedDelay,
                    _timeUnit);
            _monitoredDisconnectedFutures.put(connection, future);
        } catch (RejectedExecutionException e) {
            if (!_closed)
                throw e;
        }
    }

    protected abstract Runnable createMonitorDisconnectedTask(
            AbstractProxyBasedReplicationMonitoredConnection<T, L> connection);

    // Should be called under state lock
    private void monitorConnected(
            AbstractProxyBasedReplicationMonitoredConnection<T, L> connection) {
        if (_monitoredConnectedFutures.containsKey(connection))
            throw new IllegalArgumentException("Provided connection "
                    + connection + " is already monitored");

        if (_specificLogger.isTraceEnabled())
            _specificLogger.trace(getLogPrefix() + "monitoring connected connection "
                    + connection.getTargetLookupName() + StringUtils.NEW_LINE
                    + "ServiceID=" + connection.getServiceId());

        MonitorConnectedConnectionTask monitorTask = new MonitorConnectedConnectionTask(connection);
        try {
            ScheduledFuture<?> future = _pool.scheduleWithFixedDelay(monitorTask,
                    _monitorConnectedDelay,
                    _monitorConnectedDelay,
                    _timeUnit);
            _monitoredConnectedFutures.put(connection, future);
        } catch (RejectedExecutionException e) {
            if (!_closed)
                throw e;
        }
    }

    public void stopMonitoring(
            AbstractProxyBasedReplicationMonitoredConnection<T, L> connection) {
        synchronized (connection.getStateLock()) {
            ScheduledFuture<?> future = _monitoredConnectedFutures.remove(connection);
            if (future != null) {
                future.cancel(mayInterruptIfRunning);
            }
            future = _monitoredDisconnectedFutures.remove(connection);
            if (future != null) {
                future.cancel(mayInterruptIfRunning);
            }
        }
    }

    public void updateDisconnected(
            AbstractProxyBasedReplicationMonitoredConnection<T, L> connection,
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

            if (!connection.isConnectionClosed()) {
                monitorDisconnected(connection);
            }
        }
    }

    public int getMonitoredCount() {
        return _monitoredConnectedFutures.size()
                + _monitoredDisconnectedFutures.size();
    }

    public void close() {
        //Aquire lock on pool so it wont be executed concurrently
        synchronized (_pool) {
            if (_closed)
                return;
            _closed = true;
            for (ScheduledFuture<?> future : _monitoredConnectedFutures.values()) {
                future.cancel(true);
            }
            for (ScheduledFuture<?> future : _monitoredDisconnectedFutures.values()) {
                future.cancel(true);
            }
            _pool.shutdownNow();
        }
    }

    public String dumpState() {
        StringBuilder dump = new StringBuilder("Disconnected monitored:");
        dump.append(StringUtils.NEW_LINE);
        for (AbstractProxyBasedReplicationMonitoredConnection<T, L> connection : _monitoredDisconnectedFutures.keySet()) {
            dump.append(connection.dumpState());
            dump.append(StringUtils.NEW_LINE);
        }
        dump.append("Connected monitored:");
        dump.append(StringUtils.NEW_LINE);
        for (AbstractProxyBasedReplicationMonitoredConnection<T, L> connection : _monitoredConnectedFutures.keySet()) {
            dump.append(connection.dumpState());
            dump.append(StringUtils.NEW_LINE);
        }
        return dump.toString();
    }

    public class MonitorConnectedConnectionTask
            implements Runnable {

        private final AbstractProxyBasedReplicationMonitoredConnection<T, L> _connection;

        public MonitorConnectedConnectionTask(
                AbstractProxyBasedReplicationMonitoredConnection<T, L> connection) {
            _connection = connection;
        }

        public void run() {
            try {
                _connection.getRouter().pingStub(_connection.getConnectionProxy());
                // TODO OPT: instead of sending two packets, send a ping packet with a piggy bag of packets to be executed
                _connection.triggerSuccessfulConnectivityCheckEvent();
            } catch (RemoteException e) {
                updateDisconnected(_connection, e);
            }
        }
    }

}
