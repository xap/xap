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

package com.gigaspaces.lrmi;

import com.gigaspaces.config.lrmi.ITransportConfig;
import com.gigaspaces.internal.backport.java.util.concurrent.atomic.LongAdder;
import com.gigaspaces.internal.lrmi.ConnectionUrlDescriptor;
import com.gigaspaces.internal.lrmi.LRMIProxyMonitoringDetailsImpl;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.j_spaces.kernel.SystemProperties;
import com.j_spaces.kernel.pool.BlockingResourcePool;
import com.j_spaces.kernel.pool.IResourcePool;
import com.j_spaces.kernel.pool.IResourceProcedure;

import java.net.MalformedURLException;
import java.rmi.RemoteException;


/**
 * ConnectionPool for reuse of LRMI Connections (an LRMI Connection is abstracted by a Client Peer).
 * A Connection Pool facilitates the reuse of connections.
 *
 * An Pooled Client Proxy makes use of a Connection Pool.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
//
@com.gigaspaces.api.InternalApi
public class ConnectionPool {
    private static final LongAdder activeConnections = new LongAdder();
    private final static boolean WAIT_INDEFINITELY_FOR_CONNECTION = !Boolean.valueOf(System.getProperty(SystemProperties.LRMI_ASYNC_THROW_RESOURCE_NOT_AVAILABLE, String.valueOf(SystemProperties.LRMI_ASYNC_THROW_RESOURCE_NOT_AVAILABLE_DEFAULT)));
    private final IResourcePool<ConnectionResource> _peersPool;
    private final String _connectionURL;
    private final String _serviceDetails;
    private final PlatformLogicalVersion _serviceVersion;
    private volatile boolean _disabled;
    private volatile boolean _closed;

    public static LongAdder getActiveConnectionsCounter() {
        return activeConnections;
    }

    /**
     * Creates a new Connection Pool with the specified Protocol Adapter, connection URL and max
     * connections.
     */
    public ConnectionPool(ProtocolAdapter protocolAdapter, ITransportConfig config, String connectionURL, PlatformLogicalVersion serviceVersion) {
        int maxConns = config.getConnectionPoolSize();

        if (maxConns <= 0)
            throw new IllegalArgumentException("Max connection pool can't be less or equals zero.");

        this._connectionURL = connectionURL;
        this._serviceVersion = serviceVersion;
        //this._peersPool = new ResourcePool<ClientPeer>(new ConnectionFactory(protocolAdapter, config), 0, maxConns);
        this._peersPool = new BlockingResourcePool<ConnectionResource>(new ConnectionFactory(protocolAdapter, config, serviceVersion), 0, maxConns);
        this._serviceDetails = extractServiceDetailsFromConnectionUrl(_connectionURL);
    }

    private static String extractServiceDetailsFromConnectionUrl(String connectionUrl) {
        if (connectionUrl == null)
            return "";
        String serviceDetails = ConnectionUrlDescriptor.fromUrl(connectionUrl).getServiceDetails();
        return serviceDetails != null ? serviceDetails : "";
    }

    /**
     * Returns a connected Client Peer from the pool. If there is an un-used connection in the pool,
     * it is returned; otherwise, a new connection is created and added to the pool. If the pool is
     * full, the caller will be blocked until a free connection is available.
     */
    public ConnectionResource getConnection(LRMIMethod lrmiMethod) throws RemoteException, MalformedURLException {
        boolean waitForConnection = true;
        if (lrmiMethod.isAsync){
            waitForConnection = WAIT_INDEFINITELY_FOR_CONNECTION;
        }
        ConnectionResource conn = _peersPool.getResource(waitForConnection);
        try {
            if (_closed) {
                //Concurrent close, maybe we created a new resource from getResource here and the close process did not find it,
                //we need to disconnect and close it.
                conn.disconnect();
                conn.close();
                DynamicSmartStub.throwProxyClosedExeption(_connectionURL);
            }

            if (!conn.isConnected()) {
                if (_disabled) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw new RemoteException("LRMI force disconnection enabled for this stub");
                }
                conn.connect(_connectionURL, lrmiMethod);
            }
        } catch (RemoteException ex) {
            freeConnection(conn);
            throw ex;
        } catch (MalformedURLException ex) {
            freeConnection(conn);
            throw ex;
        } catch (RuntimeException ex) {
            freeConnection(conn);
            throw ex;
        }
        activeConnections.increment();
        return conn;
    }

    /**
     * Free a connection (return it to pool).
     */
    public void freeConnection(ConnectionResource clientPeer) {
        activeConnections.decrement();
        _peersPool.freeResource(clientPeer);
    }

    public long getGeneratedTraffic() {
        GeneratedTrafficProcedure procedure = new GeneratedTrafficProcedure();
        _peersPool.forAllResources(procedure);
        return procedure.getGeneratedTraffic();
    }

    public long getReceivedTraffic() {
        ReceivedTrafficProcedure procedure = new ReceivedTrafficProcedure();
        _peersPool.forAllResources(procedure);
        return procedure.getReceivedTraffic();
    }

    public void disable() {
        _disabled = true;
        DisableStubProcedure procedure = new DisableStubProcedure();
        _peersPool.forAllResources(procedure);
    }

    public void enable() {
        _disabled = false;
    }

    public LRMIProxyMonitoringDetailsImpl getMonitoringDetails() {
        final LRMIProxyMonitoringDetailsImpl monitoringDetails = new LRMIProxyMonitoringDetailsImpl(_connectionURL, _serviceDetails, _serviceVersion);
        _peersPool.forAllResources(new IResourceProcedure<ConnectionResource>() {
            @Override
            public void invoke(ConnectionResource resource) {
                monitoringDetails.addConnectionResource(resource);
            }
        });
        return monitoringDetails;
    }

    public void close() {
        if (_closed)
            return;

        _closed = true;
        _peersPool.forAllResources(new IResourceProcedure<ConnectionResource>() {
            @Override
            public void invoke(
                    ConnectionResource resource) {
                boolean acquired = resource.acquire();
                try {
                    if (acquired)
                        resource.disconnect();
                } finally {
                    resource.close();
                    if (acquired)
                        resource.release();
                }

            }
        });
    }

}