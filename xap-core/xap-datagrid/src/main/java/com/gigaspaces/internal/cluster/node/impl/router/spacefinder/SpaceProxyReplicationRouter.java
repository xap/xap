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

import com.gigaspaces.internal.cluster.node.impl.IServiceExporter;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractConnectionProxyBasedReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.AbstractProxyBasedReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.ConnectionState;
import com.gigaspaces.internal.cluster.node.impl.router.IIncomingReplicationHandler;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.IReplicationRouter;
import com.gigaspaces.internal.cluster.node.impl.router.MemberProxyBasedReplicationMonitoredConnection;
import com.gigaspaces.internal.cluster.node.impl.router.ReplicationRouterBuilder;
import com.gigaspaces.internal.cluster.node.impl.router.UrlProxyBasedReplicationMonitoredConnection;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.j_spaces.core.client.FinderException;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.cluster.ReplicationPolicy;
import com.j_spaces.core.exception.ClosedResourceException;

import net.jini.id.Uuid;

import java.rmi.RemoteException;


/**
 * An implementation of the {@link IReplicationRouter} interface that uses a space proxy to locate a
 * replication connection proxy as its communication facade
 *
 * @author eitany
 * @since 8.0
 */
@SuppressWarnings("deprecation")
@com.gigaspaces.api.InternalApi
public class SpaceProxyReplicationRouter
        extends
        AbstractConnectionProxyBasedReplicationRouter<IRemoteSpace, SpaceURL>
        implements IReplicationRouter {
    private final ISpaceProxyProvider _spaceProxyProvider;
    private final ISpaceUrlConverter _spaceUrlConverter;

    public SpaceProxyReplicationRouter(String myLookupName, Uuid uuid,
                                       IConnectionMonitor<IRemoteSpace, SpaceURL> connectionMonitor,
                                       IServiceExporter serviceExporter,
                                       IIncomingReplicationHandler incomingReplicationHandler,
                                       ISpaceProxyProvider spaceProxyProvider,
                                       ISpaceUrlConverter spaceUrlConverter,
                                       int replicationMonitorThreadPoolSize) {
        super(myLookupName,
                uuid,
                connectionMonitor,
                serviceExporter,
                incomingReplicationHandler,
                spaceProxyProvider,
                true, replicationMonitorThreadPoolSize);
        _spaceProxyProvider = spaceProxyProvider;
        _spaceUrlConverter = spaceUrlConverter;
    }

    public SpaceProxyReplicationRouter(String myLookupName, Uuid uuid,
                                       IConnectionMonitor<IRemoteSpace, SpaceURL> connectionMonitor,
                                       IServiceExporter serviceExporter,
                                       IIncomingReplicationHandler incomingReplicationHandler,
                                       ISpaceProxyProvider spaceProxyProvider,
                                       ISpaceUrlConverter spaceUrlConverter) {
        super(myLookupName,
                uuid,
                connectionMonitor,
                serviceExporter,
                incomingReplicationHandler,
                spaceProxyProvider,
                true);
        _spaceProxyProvider = spaceProxyProvider;
        _spaceUrlConverter = spaceUrlConverter;
    }

    @Override
    protected AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> createNewMemberConnection(
            String lookupName, boolean connectSynchronously) {
        // Create connection
        SpaceURL url = convertNameToLookupParameters(lookupName);

        if (connectSynchronously)
            return createConnectionAndConnect(lookupName, url, true);
        return createConnection(lookupName, url);
    }

    private SpaceURL convertNameToLookupParameters(String lookupName) {
        SpaceURL url = _spaceUrlConverter.convertMemberName(lookupName);
        return url;
    }

    private MemberProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> createConnection(
            String lookupName, SpaceURL url) {
        ConnectionState connectionState = ConnectionState.DISCONNECTED;
        Uuid proxyId = null;
        IReplicationConnectionProxy connectionProxy = new DisconnectionProxy();
        String endpointLookupName = null;
        IRemoteSpace remoteSpace = null;
        Exception lastDisconnectionReason = null;

        return new MemberProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>(this,
                lookupName,
                connectionProxy,
                remoteSpace,
                endpointLookupName,
                getConnectionMonitor(),
                url,
                connectionState,
                lastDisconnectionReason,
                proxyId,
                _spaceProxyProvider,
                false);
    }

    private AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> createConnectionAndConnect(
            String lookupName, SpaceURL url, boolean memberConnection) {
        AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection;
        ConnectionState connectionState = ConnectionState.DISCONNECTED;
        Uuid proxyId = null;
        IReplicationConnectionProxy connectionProxy = new DisconnectionProxy();
        String endpointLookupName = null;
        IRemoteSpace remoteSpace = null;
        Exception lastDisconnectionReason = null;
        try {
            RemoteSpaceResult result = _spaceProxyProvider.getSpaceProxy(url);
            remoteSpace = result.getRemoteSpace();
            connectionProxy = remoteSpace.getReplicationRouterConnectionProxy();
            if (connectionProxy == null)
                throw new IllegalStateException("Got null connection proxy");
            connectionState = ConnectionState.CONNECTED;
            proxyId = remoteSpace.getSpaceUuid();
            endpointLookupName = result.getEndpointLookupName();
        } catch (FinderException e) {
            // Cannot find target space right now, return a disconnection
            // connection
            lastDisconnectionReason = e;
        } catch (RemoteException e) {
            // Cannot find target space right now, return a disconnection
            // connection
            lastDisconnectionReason = e;
        }

        if (memberConnection)
            connection = new MemberProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>(this,
                    lookupName,
                    connectionProxy,
                    remoteSpace,
                    endpointLookupName,
                    getConnectionMonitor(),
                    url,
                    connectionState,
                    lastDisconnectionReason,
                    proxyId,
                    _spaceProxyProvider,
                    false);
        else
            connection = new UrlProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL>(this,
                    lookupName,
                    connectionProxy,
                    remoteSpace,
                    endpointLookupName,
                    getConnectionMonitor(),
                    url,
                    connectionState,
                    lastDisconnectionReason,
                    proxyId,
                    _spaceProxyProvider);
        return connection;
    }

    @SuppressWarnings("deprecation")
    public synchronized IReplicationMonitoredConnection getUrlConnection(
            Object customUrl) {
        System.out.println("IReplicationMonitoredConnection:getUrlConnection ZZZZZZZZZZZZZZZZZZZZZZZZZZZz" + customUrl);
        if (isClosed())
            throw new ClosedResourceException("Replication Router ["
                    + getMyLookupName() + "] is closed");
        SpaceURL url = (SpaceURL) customUrl;
        final String urlString = url.toString();
        // Create connection
        AbstractProxyBasedReplicationMonitoredConnection<IRemoteSpace, SpaceURL> connection = createConnectionAndConnect(urlString,
                url,
                false);
        return new ConnectionReference<IRemoteSpace, SpaceURL>(connection);
    }

    public static class Builder extends ReplicationRouterBuilder<SpaceProxyReplicationRouter> {
        private final String _myLookupName;
        private final ISpaceProxyProvider _spaceProxyProvider;
        private final ISpaceUrlConverter _spaceUrlConverter;
        private final IConnectionMonitor _connectionMonitor;
        private final IServiceExporter _serviceExporter;
        private final Uuid _uuid;
        private final int _connectionMonitorThreadPoolSize;

        public Builder(String myLookupName, Uuid uuid, ISpaceProxyProvider spaceProxyProvider,
                       ISpaceUrlConverter spaceUrlConverter, IConnectionMonitor connectionMonitor,
                       IServiceExporter serviceExporter) {
            this(myLookupName, uuid, spaceProxyProvider, spaceUrlConverter, connectionMonitor, serviceExporter, ReplicationPolicy.DEFAULT_CONNECTION_MONITOR_THREAD_POOL_SIZE);
        }

        public Builder(String myLookupName, Uuid uuid, ISpaceProxyProvider spaceProxyProvider, ISpaceUrlConverter spaceUrlConverter, IConnectionMonitor connectionMonitor, IServiceExporter serviceExporter, int connectionMonitorThreadPoolSize) {
            _myLookupName = myLookupName;
            _uuid = uuid;
            _spaceProxyProvider = spaceProxyProvider;
            _spaceUrlConverter = spaceUrlConverter;
            _connectionMonitor = connectionMonitor;
            _serviceExporter = serviceExporter;
            _connectionMonitorThreadPoolSize = connectionMonitorThreadPoolSize;
        }

        public SpaceProxyReplicationRouter create(IIncomingReplicationHandler handler) {
            return new SpaceProxyReplicationRouter(_myLookupName, _uuid, _connectionMonitor, _serviceExporter, handler, _spaceProxyProvider, _spaceUrlConverter, _connectionMonitorThreadPoolSize);
        }

        @Override
        public String toString() {
            return "SpaceProxyReplicationRouter.Builder [_myLookupName="
                    + _myLookupName + ", _spaceProxyProvider="
                    + _spaceProxyProvider + ", _spaceUrlConverter="
                    + _spaceUrlConverter + ", _connectionMonitor="
                    + _connectionMonitor + ", _stubHandler=" + _serviceExporter
                    + ", _uuid=" + _uuid + "]";
        }
    }

}
