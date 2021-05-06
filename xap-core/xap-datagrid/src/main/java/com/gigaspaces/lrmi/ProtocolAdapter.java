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
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.lrmi.classloading.IClassProvider;
import com.gigaspaces.management.transport.ITransportConnection;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;

/**
 * A Protocol Adapter abstracts a specific communication protocol. <p/> Each protocol has a unique
 * name in an LRMI environment, and this name serves as the key to the LRMI Registry. <p/> The
 * protocol adapter implementation is a factory of ClientPeer and ServerPeer instances. A pair of a
 * ClientPeer and ServerPeer represent a logical LRMI connection. Implementations may share physical
 * connections. <p/> A protocol is initialized by a set of properties, which serve as the basic
 * configuration mechanism.
 *
 * @author Igor Goldenberg
 * @since 4.0
 */
public interface ProtocolAdapter<C extends ClientPeer> {
    enum Side {CLIENT, SERVER}


    /**
     * Initializes the protocol adapter according to the supplied properties.
     *
     * @throws RemoteException if init. failed.
     * @props properties under protocol-specific conventions.
     */
    void init(ITransportConfig config, Side side) throws IOException;

    /**
     * Returns the unique name of this protocol adapter.
     *
     * @return the unique name of this protocol adapter.
     */
    String getName();


    /**
     * Returns an un-connected client peer.
     */
    C getClientPeer(PlatformLogicalVersion serviceVersion);

    /**
     * Construct and returns a server peer for the specified object id.
     *
     * @param objectId          The exported remote objectId.
     * @param objectClassLoader the exported object class loader
     */
    ServerPeer newServerPeer(long objectId, ClassLoader objectClassLoader, String serviceDetails);

    /**
     * @param objectId The exported remote objectId.
     * @return the Transport connection info list for the supplied remote objectID.
     */
    List<ITransportConnection> getRemoteObjectConnectionsList(long objectId);

    int countRemoteObjectConnections(long objectId);

    IClassProvider getClassProvider();

    int getPort();

    String getHostName();

    /**
     * Shutdown this Protocol Adapter, freeing excess resources.
     */
    void shutdown();

    /**
     * Returns the remote method invocation handler {@link ClientPeerInvocationHandler} which
     * represents remote object specified by the supplied c and connection URL. The returned {@link
     * ClientPeerInvocationHandler} maintains a pool of connections(sockets) to the remote object.
     *
     * @param connectionURL the connectionURL to the remoteObj provided by {@link ServerPeer}.
     * @param config        the client configuration.
     * @return the remote method invocation handler.
     **/
    ClientPeerInvocationHandler getClientInvocationHandler(String connectionURL, ITransportConfig config, PlatformLogicalVersion serviceVersion);

    LRMIMonitoringDetails getMonitoringDetails();
}