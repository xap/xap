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

package com.gigaspaces.internal.cluster.node.impl.router.spacefinder;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractProxyBasedReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractReplicationPacket;
import com.gigaspaces.internal.cluster.node.impl.router.ConnectionState;
import com.gigaspaces.internal.cluster.node.impl.router.IConnectionStateListener;
import com.gigaspaces.internal.cluster.node.impl.router.IConnectivityCheckListener;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.management.transport.ConnectionEndpointDetails;

import net.jini.id.Uuid;

import java.rmi.RemoteException;

@com.gigaspaces.api.InternalApi
public class ConnectionReference<T, L>
        implements IReplicationMonitoredConnection {

    private final AbstractProxyBasedReplicationMonitoredConnection<T, L> _connection;
    private IConnectionStateListener _listener;
    private IConnectivityCheckListener _connectivityCheckListener;
    private boolean _closed;

    public ConnectionReference(AbstractProxyBasedReplicationMonitoredConnection<T, L> connection) {
        if(true){
            new Exception().printStackTrace();
        }
        System.out.println("ZZZZZZZZZZZZZZZZZZZZZZZZZZZz" + connection.getConnectionUrl());
        _connection = connection;
        _connection.addReference();
    }

    public synchronized void close() {
        if (_closed)
            return;
        _closed = true;
        if (_listener != null)
            _connection.removeStateListener(_listener);
        if (_connectivityCheckListener != null)
            _connection.removeConnectivityCheckListener(_connectivityCheckListener);
        _connection.removeReference();
    }

    public <T> T dispatch(AbstractReplicationPacket<T> packet) throws RemoteException {
        return _connection.dispatch(packet);
    }

    public <T> AsyncFuture<T> dispatchAsync(AbstractReplicationPacket<T> packet) throws RemoteException {
        return _connection.dispatchAsync(packet);
    }

    public ConnectionState getState() {
        return _connection.getState();
    }

    public synchronized void setConnectionStateListener(IConnectionStateListener listener) {
        if (_listener != null)
            _connection.removeStateListener(_listener);
        _listener = listener;
        _connection.addStateListener(_listener);
    }

    @Override
    public synchronized void setConnectivityCheckListener(IConnectivityCheckListener listener) {
        if (_connectivityCheckListener != null)
            _connection.removeConnectivityCheckListener(_connectivityCheckListener);
        _connectivityCheckListener = listener;
        _connection.addConnectivityCheckListener(_connectivityCheckListener);
    }

    public AbstractProxyBasedReplicationMonitoredConnection<T, L> getUnderlyingConnection() {
        return _connection;
    }

    public Exception getLastDisconnectionReason() {
        return _connection.getLastDisconnectionReason();
    }

    public String getFinalEndpointLookupName() {
        return _connection.getFinalEndpointLookupName();
    }

    public Uuid getClosestEndpointUniqueId() {
        return _connection.getProxyId();
    }

    public Object getClosestEndpointAddress() {
        return _connection.getClosestEndpointAddress();
    }

    @Override
    public ConnectionEndpointDetails getClosestEndpointDetails() {
        return _connection.getClosestEndpointDetails();
    }

    @Override
    protected void finalize() throws Throwable {
        close();

        super.finalize();
    }

    public Object getConnectionUrl() {
        return _connection.getConnectionUrl();
    }

    public long getGeneratedTraffic() {
        return _connection.getGeneratedTraffic();
    }

    public long getReceivedTraffic() {
        return _connection.getReceivedTraffic();
    }

    @Override
    public PlatformLogicalVersion getClosestEndpointLogicalVersion() {
        return _connection.getClosestEndpointLogicalVersion();
    }

    @Override
    public boolean supportsConnectivityCheckEvents() {
        return _connection.supportsConnectivityCheckEvents();
    }

    @Override
    public Long getTimeOfDisconnection() {
        return _connection.getTimeOfDisconnection();
    }

    @Override
    public String dumpState() {
        return _connection.dumpState();
    }

}
