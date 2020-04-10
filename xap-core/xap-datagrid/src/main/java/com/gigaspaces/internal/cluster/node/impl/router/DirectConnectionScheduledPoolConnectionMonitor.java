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

import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IReplicationConnectionProxy;

import java.util.concurrent.TimeUnit;


/**
 * A replication connection monitors for connection that are established with a direct connection
 * proxy. Such connection can only reestablish a connection using the same stub it was initially
 * used for creation, this means no failover is supported for such connections {@link
 * IReplicationConnectionProxy}
 *
 * @author eitany
 * @since 8.0.5
 */
@com.gigaspaces.api.InternalApi
public class DirectConnectionScheduledPoolConnectionMonitor
        extends AbstractScheduledPoolConnectionMonitor<IReplicationConnectionProxy, Object> {

    public DirectConnectionScheduledPoolConnectionMonitor(String myLookupName,
                                                          int corePoolSize, long monitorConnectedDelay,
                                                          long monitorDisconnectedDelay, TimeUnit timeUnit) {
        super(myLookupName,
                corePoolSize,
                monitorConnectedDelay,
                monitorDisconnectedDelay,
                timeUnit);
    }

    @Override
    protected Runnable createMonitorDisconnectedTask(
            AbstractProxyBasedReplicationMonitoredConnection<IReplicationConnectionProxy, Object> connection) {
        return new MonitorDisconnectedConnectionTask(connection);
    }

    private class MonitorDisconnectedConnectionTask
            implements Runnable {

        private final AbstractProxyBasedReplicationMonitoredConnection<IReplicationConnectionProxy, Object> _connection;

        public MonitorDisconnectedConnectionTask(
                AbstractProxyBasedReplicationMonitoredConnection<IReplicationConnectionProxy, Object> connection) {
            _connection = connection;
        }

        @Override
        public void run() {
            if (_specificLogger.isDebugEnabled())
                _specificLogger.debug(getLogPrefix()
                        + "trying to establish connection with "
                        + _connection.getTargetLookupName() + " ["
                        + _connection.getFinderURL() + "]");

            try {
                IReplicationConnectionProxy directConnectionProxy = _connection.getTag();
                _connection.getRouter().pingStub(directConnectionProxy);
                synchronized (_connection.getStateLock()) {
                    if (_specificLogger.isDebugEnabled())
                        _specificLogger.debug(getLogPrefix()
                                + "established connection with "
                                + _connection.getTargetLookupName() + " ["
                                + _connection.getFinderURL() + "]");
                    _connection.setConnected(directConnectionProxy,
                            directConnectionProxy,
                            _connection.getFinalEndpointLookupName(),
                            _connection.getProxyId());
                    // Stop this monitoring of the connection
                    stopMonitoring(_connection);
                    monitor(_connection);
                }
            } catch (Exception e) {
                // Proxy disconnected, keep this disconnected monitor
                // task alive
                _connection.setLastDisconnectionReason(e);
            }

        }

    }

}
