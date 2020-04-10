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

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.internal.cluster.node.impl.IServiceExporter;
import com.gigaspaces.internal.cluster.node.impl.ReplicationLogUtils;
import com.gigaspaces.internal.cluster.node.impl.packets.PingPacket;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.ConnectionReference;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IConnectionMonitor;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IReplicationConnectionProxy;
import com.gigaspaces.internal.utils.StringUtils;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.ILRMIService;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.exception.ClosedResourceException;

import net.jini.id.Uuid;

import java.rmi.RemoteException;
import java.rmi.server.ExportException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A base class for replication routers that are using {@link IReplicationConnectionProxy} to
 * communicate with each other.
 *
 * @author eitany
 * @since 8.0.3
 */
public abstract class AbstractConnectionProxyBasedReplicationRouter<T, L>
        implements IReplicationRouter, IReplicationRouterAdmin {

    protected final Logger _specificLogger;

    private final String _myLookupName;
    private final Map<String, AbstractProxyBasedReplicationMonitoredConnection<T, L>> _connections;
    private final Map<String, RouterStubHolder> _directStubs;
    private final IConnectionMonitor<T, L> _connectionMonitor;
    private final IConnectionMonitor<IReplicationConnectionProxy, Object> _directConnectionMonitor;
    private final IServiceExporter _serviceExporter;
    private final Uuid _uuid;
    private final IIncomingReplicationHandler _incomingReplicationHandler;
    private final ConnectionEndpoint _connectionEndPoint;
    private final IAsyncContextProvider _asyncContextProvider;
    private final boolean _setMyIdBeforeDispatch;
    private final ReplicationEndpointDetails _myReplicationEndpointDetails;
    private final IReplicationConnectionProxy _stub;

    private boolean _incommingCommunicationEnabled;
    private boolean _closed;

    protected AbstractConnectionProxyBasedReplicationRouter(
            String myLookupName, Uuid uuid,
            IConnectionMonitor<T, L> connectionMonitor,
            IServiceExporter serviceExporter,
            IIncomingReplicationHandler incomingReplicationHandler,
            IAsyncContextProvider asyncContextProvider,
            boolean setMyIdBeforeDispatch,
            int replicationMonitorThreadPoolSize) {
        _myLookupName = myLookupName;
        _uuid = uuid;
        _connectionMonitor = connectionMonitor;
        _serviceExporter = serviceExporter;
        _incomingReplicationHandler = incomingReplicationHandler;
        _asyncContextProvider = asyncContextProvider;
        _setMyIdBeforeDispatch = setMyIdBeforeDispatch;

        _connections = new HashMap<String, AbstractProxyBasedReplicationMonitoredConnection<T, L>>();
        _directStubs = new HashMap<String, RouterStubHolder>();
        _connectionEndPoint = new ConnectionEndpoint();
        _specificLogger = LoggerFactory.getLogger(Constants.LOGGER_REPLICATION_ROUTER
                + "." + ReplicationLogUtils.toShortLookupName(_myLookupName));

        //TODO configurable
        _directConnectionMonitor = new DirectConnectionScheduledPoolConnectionMonitor(_myLookupName,
                replicationMonitorThreadPoolSize, 3, 1, TimeUnit.SECONDS);
        _stub = createStub();
        _myReplicationEndpointDetails = ReplicationEndpointDetails.createMyEndpointDetails(_myLookupName, _uuid);
    }

    protected AbstractConnectionProxyBasedReplicationRouter(
            String myLookupName, Uuid uuid,
            IConnectionMonitor<T, L> connectionMonitor,
            IServiceExporter serviceExporter,
            IIncomingReplicationHandler incomingReplicationHandler,
            IAsyncContextProvider asyncContextProvider,
            boolean setMyIdBeforeDispatch) {
        this(myLookupName, uuid, connectionMonitor, serviceExporter, incomingReplicationHandler, asyncContextProvider,
                setMyIdBeforeDispatch, ReplicationPolicy.DEFAULT_CONNECTION_MONITOR_THREAD_POOL_SIZE);
    }

    public IReplicationMonitoredConnection getMemberConnection(
            String lookupName) {
        return getMemberConnection(lookupName, true);
    }

    public IReplicationMonitoredConnection getMemberConnectionAsync(
            String lookupName) {
        return getMemberConnection(lookupName, false);
    }

    private synchronized IReplicationMonitoredConnection getMemberConnection(String lookupName, boolean connectSynchronously) {
        if (isClosed())
            throw new ClosedResourceException("Replication Router ["
                    + getMyLookupName() + "] is closed");
        // Check for preexisting connection
        AbstractProxyBasedReplicationMonitoredConnection<T, L> connection = _connections.get(lookupName);
        if (connection != null)
            return new ConnectionReference<T, L>(connection);

        if (_directStubs.containsKey(lookupName)) {
            // Create direct connection
            return createDirectConnection(lookupName, connectSynchronously);
        }

        // Create connection
        if (_specificLogger.isDebugEnabled())
            _specificLogger.debug("creating new member connection to ["
                    + lookupName + "], connect synchronously [" + connectSynchronously + "]");
        connection = createNewMemberConnection(lookupName, connectSynchronously);
        _connections.put(lookupName, connection);
        return new ConnectionReference<T, L>(connection);
    }

    protected IReplicationMonitoredConnection createDirectConnection(
            String lookupName, boolean connectSynchronously) {
        if (_specificLogger.isDebugEnabled())
            _specificLogger.debug("creating new direct connection to ["
                    + lookupName + "], connect synchronously [" + connectSynchronously + "]");
        RouterStubHolder routerStubHolder = _directStubs.get(lookupName);

        AbstractProxyBasedReplicationMonitoredConnection<IReplicationConnectionProxy, Object> directConnection = wrapDirectStubWithMonitoredConnection(connectSynchronously,
                routerStubHolder);

        return new ConnectionReference<IReplicationConnectionProxy, Object>(directConnection);
    }

    protected AbstractProxyBasedReplicationMonitoredConnection<IReplicationConnectionProxy, Object> wrapDirectStubWithMonitoredConnection(
            boolean connectSynchronously,
            RouterStubHolder routerStubHolder) {
        String lookupName = routerStubHolder.getMyEndpointDetails().getLookupName();
        IReplicationConnectionProxy stub = (IReplicationConnectionProxy) routerStubHolder.getStub();
        AbstractProxyBasedReplicationMonitoredConnection<IReplicationConnectionProxy, Object> directConnection;
        ConnectionState state = ConnectionState.DISCONNECTED;
        Exception disconnectedReason = new RemoteException("Connection not established yet");
        if (connectSynchronously) {
            try {
                pingStub(stub);
                state = ConnectionState.CONNECTED;
                disconnectedReason = null;
            } catch (RemoteException e) {
                disconnectedReason = e;
            }
        }

        directConnection = new StubBasedReplicationMonitoredConnection(this,
                lookupName,
                stub,
                _directConnectionMonitor,
                state,
                disconnectedReason,
                (Uuid) routerStubHolder.getMyEndpointDetails().getUniqueId(),
                _asyncContextProvider);
        return directConnection;
    }

    protected abstract AbstractProxyBasedReplicationMonitoredConnection<T, L> createNewMemberConnection(
            String lookupName, boolean connectSynchronously);

    public void addRemoteRouterStub(RouterStubHolder routerStubHolder) {
        pingIfPossible(routerStubHolder);
        // we don't synchronize the whole method to prevent blocking on remote ping
        synchronized (this) {
            _directStubs.put(routerStubHolder.getMyEndpointDetails().getLookupName(), routerStubHolder);
        }
    }

    private void pingIfPossible(RouterStubHolder routerStubHolder) {
        if (routerStubHolder.getStub() instanceof CallbackVerifier) {
            if (_specificLogger.isDebugEnabled())
                _specificLogger.debug("Verifying connection using a remote stub " + routerStubHolder.toString());
            try {
                ((CallbackVerifier) routerStubHolder.getStub()).verify();
            } catch (RemoteException e) {
                throw new RuntimeException("Failure verifying connection using a remote stub " + routerStubHolder.toString(), e);
            }
        }
    }

    @Override
    public synchronized void removeRemoteStubHolder(String routerLookupName) {
        _directStubs.remove(routerLookupName);
    }

    @Override
    public IReplicationMonitoredConnection getDirectConnection(
            RouterStubHolder remoteStubHolder) {
        AbstractProxyBasedReplicationMonitoredConnection<IReplicationConnectionProxy, Object> directConnection =
                wrapDirectStubWithMonitoredConnection(true, remoteStubHolder);

        return new ConnectionReference<IReplicationConnectionProxy, Object>(directConnection);
    }

    public synchronized void close() {
        if (_closed)
            return;

        if (_specificLogger.isDebugEnabled())
            _specificLogger.debug("closing replication router");

        if (_specificLogger.isTraceEnabled())
            _specificLogger.trace("unexporting stub");
        _serviceExporter.unexport(_connectionEndPoint);

        if (_specificLogger.isTraceEnabled())
            _specificLogger.trace("closing connection monitor");
        getConnectionMonitor().close();
        if (_specificLogger.isTraceEnabled())
            _specificLogger.trace("closing direct connection monitor");
        _directConnectionMonitor.close();
        Exception reason = new ClosedResourceException("Replication Router ["
                + getMyLookupName() + "] is closed");
        if (_specificLogger.isTraceEnabled())
            _specificLogger.trace("disconnecting connections");
        for (AbstractProxyBasedReplicationMonitoredConnection<T, L> connection : _connections.values()) {
            connection.setDisconnected(reason);
        }
        _connections.clear();
        _closed = true;

        if (_specificLogger.isDebugEnabled())
            _specificLogger.debug("replication router closed");
    }

    public String getMyLookupName() {
        return _myLookupName;
    }

    public Uuid getMyUniqueId() {
        return _uuid;
    }

    public boolean isSetMyIdBeforeDispatch() {
        return _setMyIdBeforeDispatch;
    }

    public RouterStubHolder getMyStubHolder() {
        return new RouterStubHolder(getStub(),
                getMyEndpointDetails());
    }

    @Override
    public ReplicationEndpointDetails getMyEndpointDetails() {
        return _myReplicationEndpointDetails;
    }

    public synchronized String dumpState() {
        StringBuilder dump = new StringBuilder("UniqueId [" + getMyUniqueId()
                + "]");
        dump.append(StringUtils.NEW_LINE);
        for (AbstractProxyBasedReplicationMonitoredConnection<T, L> connection : _connections.values()) {
            dump.append(connection.dumpState());
            dump.append(StringUtils.NEW_LINE);
        }
        dump.append("Direct Stubs:");
        dump.append(StringUtils.NEW_LINE);
        for (Entry<String, RouterStubHolder> entry : _directStubs.entrySet()) {
            dump.append("Id [");
            dump.append(entry.getKey());
            dump.append("Id ]");
            dump.append(StringUtils.NEW_LINE);
            dump.append(entry.getValue());
            dump.append(StringUtils.NEW_LINE);
        }
        dump.append("Connection Monitor: " + StringUtils.NEW_LINE
                + getConnectionMonitor().dumpState());
        dump.append("Direct connection Monitor: " + StringUtils.NEW_LINE
                + _directConnectionMonitor.dumpState());
        return dump.toString();
    }

    public void pingStub(IReplicationConnectionProxy proxy) throws RemoteException {
        PingPacket packet = new PingPacket();
        packet.setSourceEndpointDetails(getMyEndpointDetails());
        Object result = proxy.dispatchAsync(packet);
        AsyncFuture<Object> future = _asyncContextProvider.getFutureContext(result, proxy);
        try {
            future.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RemoteException(e.getMessage(), e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RemoteException)
                throw (RemoteException) e.getCause();

            if (_specificLogger.isDebugEnabled())
                _specificLogger.debug("pinging of stub has failed [" + e.getCause() + "], considering it as failure");
            throw new RemoteException(e.getMessage(), e.getCause());
        } catch (TimeoutException e) {
            if (_specificLogger.isDebugEnabled())
                _specificLogger.debug("pinging of stub has timed out, considering it as failure");
            throw new RemoteException(e.getMessage(), e);
        }
    }

    public IReplicationConnectionProxy createStub() {
        try {
            return _serviceExporter.export(_connectionEndPoint);
        } catch (ExportException e) {
            throw new RuntimeException("Failed to export replication router stub",
                    e);
        }
    }

    public IReplicationConnectionProxy getStub() {
        return _stub;
    }

    public synchronized boolean isClosed() {
        return _closed;
    }

    public synchronized void removeUrlConnection(
            UrlProxyBasedReplicationMonitoredConnection<T, L> connection) {
        getConnectionMonitor().stopMonitoring(connection);
    }

    public synchronized void removeMemberConnection(
            MemberProxyBasedReplicationMonitoredConnection<T, L> connection) {
        _connections.remove(connection.getTargetLookupName());

        getConnectionMonitor().stopMonitoring(connection);
    }

    public synchronized void removeDirectConnection(
            StubBasedReplicationMonitoredConnection connection) {
        _directConnectionMonitor.stopMonitoring(connection);
    }

    protected IConnectionMonitor<T, L> getConnectionMonitor() {
        return _connectionMonitor;
    }

    public IAsyncContextProvider getAsyncContextProvider() {
        return _asyncContextProvider;
    }

    public synchronized boolean hasExistingConnection(String lookupName) {
        return _connections.containsKey(lookupName);
    }

    @Override
    public IReplicationRouterAdmin getAdmin() {
        return this;
    }

    @Override
    public RouterStubHolder getMyRouterStubHolder() {
        return getMyStubHolder();
    }

    @Override
    public synchronized RouterStubHolder getRemoteRouterStub(String routerLookupName) {
        return _directStubs.get(routerLookupName);
    }

    //Flush to main memory
    @Override
    public synchronized void enableIncomingCommunication() {
        if (_specificLogger.isDebugEnabled())
            _specificLogger.debug("Incoming communication enabled");
        _incommingCommunicationEnabled = true;
    }

    public class ConnectionEndpoint
            implements IReplicationConnectionProxy, ILRMIService, CallbackVerifier {

        public ConnectionEndpoint() {
        }

        public <TR> TR dispatch(AbstractReplicationPacket<TR> packet)
                throws RemoteException {
            if (!_incommingCommunicationEnabled)
                throw new RemoteException("Incoming communication is not enabled");
            return _incomingReplicationHandler.onReplication(packet);
        }

        public <TR> TR dispatchAsync(AbstractReplicationPacket<TR> packet)
                throws RemoteException {
            if (!_incommingCommunicationEnabled)
                throw new RemoteException("Incoming communication is not enabled");
            return _incomingReplicationHandler.onReplication(packet);
        }

        public void close() throws RemoteException {

        }

        @Override
        public String getServiceName() {
            return _myLookupName;
        }

        @Override
        public void verify() throws RemoteException {

        }
    }

}
