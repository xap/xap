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

package com.gigaspaces.internal.client.spaceproxy;

import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.client.DirectSpaceProxyFactory;
import com.gigaspaces.internal.client.spaceproxy.events.SpaceProxyDataEventsManager;
import com.gigaspaces.internal.client.spaceproxy.metadata.ISpaceProxyTypeManager;
import com.gigaspaces.internal.client.spaceproxy.router.SpaceProxyRouter;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.demote.DemoteException;
import com.gigaspaces.query.ISpaceQuery;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.IJSpaceContainer;
import com.j_spaces.core.IStubHandler;
import com.j_spaces.core.admin.ContainerConfig;
import com.j_spaces.core.client.IProxySecurityManager;
import com.j_spaces.core.client.ProxySettings;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.client.sql.IQueryManager;

import java.rmi.RemoteException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A direct space proxy working on an actual space.
 *
 * @author GigaSpaces
 */
public interface IDirectSpaceProxy extends ISpaceProxy {
    DirectSpaceProxyFactory getFactory();

    ProxySettings getProxySettings();

    SpaceClusterInfo getSpaceClusterInfo();

    IProxySecurityManager getSecurityManager();

    ISpaceProxyTypeManager getTypeManager();

    IQueryManager getQueryManager();

    SpaceProxyRouter getProxyRouter();

    SpaceProxyDataEventsManager getDataEventsManager();

    /**
     * Returns the StubHandler of this space.
     *
     * @return StubHandler of this space.
     * @deprecated Since 8.0 - This method is reserved for internal usage.
     **/
    @Deprecated
    IStubHandler getStubHandler();

    /**
     * Returns the container proxy this space resides in. <br> The container holds information and
     * attributes of neighboring spaces, and exposes API for creating and destroying of spaces.
     *
     * @return Returns the container proxy.
     * @deprecated Since 8.0 - This method is reserved for internal usage.
     **/
    @Deprecated
    public IJSpaceContainer getContainer();


    /**
     * Returns a proxy of the specified space. When the proxy is not part of a cluster, it returns
     * itself as the only member.
     *
     * @return IJSpace space proxy.
     * @since 6.0.2
     */
    IJSpace getNonClusteredProxy();

    /**
     * Returns a clustered view of this proxy. When the proxy is not part of a cluster, it returns
     * itself as the clustered view.
     *
     * @return IJSpace space proxy.
     * @since 6.0.2
     */
    IJSpace getClusteredProxy();

    long getClientID();

    /**
     * Returns the reference to initial remote space
     */
    IRemoteSpace getRemoteJSpace();

    String getRemoteMemberName();

    SpaceImpl getSpaceImplIfEmbedded();

    ExecutorService getThreadPool();

    void directClean();

    ITypeDesc getTypeDescFromServer(String typeName);

    ITypeDesc registerTypeDescInServers(ITypeDesc typeDesc);

    boolean isGatewayProxy();

    void shutdown() throws RemoteException;

    ContainerConfig getContainerConfig() throws RemoteException;

    SpaceURL getContainerURL() throws RemoteException;

    void setQuiesceToken(QuiesceToken token);

    ISpaceQuery prepareTemplate(Object template);

    void demoteToBackup(int timeout, TimeUnit unit) throws DemoteException;

}
