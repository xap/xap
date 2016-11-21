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

package com.j_spaces.core.admin;

import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.internal.client.spaceproxy.IDirectSpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.jvm.JVMDetails;
import com.gigaspaces.internal.jvm.JVMStatistics;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.os.OSDetails;
import com.gigaspaces.internal.os.OSStatistics;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.lrmi.LRMIMonitoringDetails;
import com.gigaspaces.lrmi.nio.info.NIODetails;
import com.gigaspaces.lrmi.nio.info.NIOStatistics;
import com.gigaspaces.management.space.LocalCacheDetails;
import com.gigaspaces.management.space.LocalViewDetails;
import com.gigaspaces.management.transport.ITransportConnection;
import com.j_spaces.core.DropClassException;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceCopyStatus;
import com.j_spaces.core.client.BasicTypeInfo;
import com.j_spaces.core.client.SpaceURL;
import com.j_spaces.core.client.SpaceURLParser;
import com.j_spaces.core.client.TransactionInfo;
import com.j_spaces.core.client.UnderTxnLockedObject;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.exception.SpaceAlreadyStartedException;
import com.j_spaces.core.exception.SpaceAlreadyStoppedException;
import com.j_spaces.core.filters.StatisticsContext;
import com.j_spaces.core.filters.StatisticsHolder;
import com.j_spaces.core.service.ServiceAdmin;
import com.j_spaces.core.service.ServiceAdminProxy;

import net.jini.core.transaction.Transaction;
import net.jini.id.Uuid;
import org.jini.rio.boot.SpaceInstanceRemoteClassLoaderInfo;

import java.net.MalformedURLException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/*******************************************************************************
 * Copyright (c) 2010 GigaSpaces Technologies Ltd. All rights reserved
 *
 * The software source code is proprietary and confidential information of GigaSpaces. You may use
 * the software source code solely under the terms and limitations of The license agreement granted
 * to you by GigaSpaces.
 *******************************************************************************/
@com.gigaspaces.api.InternalApi
public class JSpaceAdminProxy
        extends ServiceAdminProxy
        implements IInternalRemoteJSpaceAdmin, StatisticsAdmin {
    private static final long serialVersionUID = 3258413919704856887L;

    /**
     * reference to the space-proxy in order to use IJSpace methods implementation
     */
    transient private IJSpace _spaceProxy;

    public JSpaceAdminProxy(ServiceAdmin admin, Uuid uuid) {
        super(admin, uuid);
    }

    /**
     * set space proxy for this Admin proxy, useally calls by IJSpace.getAdmin() method
     */
    public void setSpace(IJSpace space) {
        _spaceProxy = space;
    }

    /**
     * return the defined spaceProxy for this spaceAdmin, <code>null</code> if setSpace hasn't never
     * invoked
     */
    public IJSpace getSpace() {
        return _spaceProxy;
    }

    /**
     * IRemoteJSpaceAdmin interface
     */
    public void restart() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) adminImpl).restart();
    }

    @Override
    public SpaceInstanceRemoteClassLoaderInfo getSpaceInstanceRemoteClassLoaderInfo() throws RemoteException {
        return ((IRemoteJSpaceAdmin) adminImpl).getSpaceInstanceRemoteClassLoaderInfo();
    }

    public String getName() throws RemoteException {
        return ((IRemoteJSpaceAdmin) adminImpl).getName();
    }

    public SpaceRuntimeInfo getRuntimeInfo() throws RemoteException {
        return ((IRemoteJSpaceAdmin) adminImpl).getRuntimeInfo();
    }

    public SpaceRuntimeInfo getRuntimeInfo(String className) throws RemoteException {
        return ((IRemoteJSpaceAdmin) adminImpl).getRuntimeInfo(className);
    }

    public SpaceConfig getConfig() throws RemoteException {
        return ((IRemoteJSpaceAdmin) adminImpl).getConfig();
    }

    public void ping() throws RemoteException {
        ((IRemoteJSpaceAdmin) adminImpl).ping();
    }

    public void disableStub() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) adminImpl).disableStub();
    }

    public Object[] getReplicationStatus() throws RemoteException {
        return ((IRemoteJSpaceAdmin) adminImpl).getReplicationStatus();
    }

    public RuntimeHolder getRuntimeHolder() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getRuntimeHolder();
    }

    public boolean isStartedWithinGSC() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).isStartedWithinGSC();
    }

    public ITypeDesc getClassDescriptor(String className)
            throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getClassDescriptor(className);
    }

    public BasicTypeInfo getClassTypeInfo(String className) throws RemoteException {
        return ((IRemoteJSpaceAdmin) adminImpl).getClassTypeInfo(className);
    }

    public ClusterPolicy getClusterPolicy() throws RemoteException {
        return ((IRemoteJSpaceAdmin) adminImpl).getClusterPolicy();
    }


    public void dropClass(String className, SpaceContext sc)
            throws RemoteException, DropClassException {
        ((IRemoteJSpaceAdmin) adminImpl).dropClass(className, sc);
    }

    public void start() throws RemoteException, SpaceAlreadyStartedException {
        ((IInternalRemoteJSpaceAdmin) adminImpl).start();
    }

    public void stop() throws RemoteException, SpaceAlreadyStoppedException {
        ((IInternalRemoteJSpaceAdmin) adminImpl).stop();
    }

    /*    public void pause() throws RemoteException, SpaceAlreadyPausedException {
        ((IRemoteJSpaceAdmin)adminImpl).pause();
    }
    public void resume() throws RemoteException, SpaceAlreadyStartedException {
        ((IRemoteJSpaceAdmin)adminImpl).resume();
    }
     */

    /**
     * @see IRemoteJSpaceAdmin
     */
    public TransactionInfo[] getTransactionsInfo(int type, int status) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getTransactionsInfo(type, status);
    }

    public int countTransactions(int type, int status) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).countTransactions(type, status);
    }

    /**
     * @see com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin#getTemplatesInfo(java.lang.String)
     */
    public List<TemplateInfo> getTemplatesInfo(String className) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getTemplatesInfo(className);
    }

    @Override
    public List<ITransportConnection> getConnectionsInfo() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getConnectionsInfo();
    }

    @Override
    public int countIncomingConnections() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).countIncomingConnections();
    }

    public List<UnderTxnLockedObject> getLockedObjects(Transaction txn) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getLockedObjects(txn);
    }


    public int getState() throws RemoteException {
        return ((IRemoteJSpaceAdmin) adminImpl).getState();
    }


    public boolean isStatisticsAvailable() throws RemoteException {
        return ((StatisticsAdmin) adminImpl).isStatisticsAvailable();
    }

    public String[] getStatisticsStringArray() throws RemoteException {
        return ((StatisticsAdmin) adminImpl).getStatisticsStringArray();
    }

    public void setStatisticsSamplingRate(long rate) throws RemoteException {
        ((StatisticsAdmin) adminImpl).setStatisticsSamplingRate(rate);
    }

    public long getStatisticsSamplingRate() throws RemoteException {
        return ((StatisticsAdmin) adminImpl).getStatisticsSamplingRate();
    }

    public StatisticsContext getStatistics(int operationCode) throws RemoteException {
        return ((StatisticsAdmin) adminImpl).getStatistics(operationCode);
    }

    public StatisticsHolder getHolder() throws RemoteException {
        return ((StatisticsAdmin) adminImpl).getHolder();
    }

    public Map<Integer, StatisticsContext> getStatistics(Integer[] operationCodes)
            throws RemoteException {
        return ((StatisticsAdmin) adminImpl).getStatistics(operationCodes);
    }

    public Map<Integer, StatisticsContext> getStatistics() throws RemoteException {
        return ((StatisticsAdmin) adminImpl).getStatistics();
    }


    public byte[] getClusterConfigFile() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getClusterConfigFile();
    }

    public void setClusterConfigFile(byte[] config) throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) adminImpl).setClusterConfigFile(config);
    }

    public SpaceCopyStatus spaceCopy(String remoteUrl, Object template, boolean includeNotifyTemplates, int chunkSize) throws RemoteException {
        IDirectSpaceProxy directProxy = (IDirectSpaceProxy) _spaceProxy;
        final ObjectType objectType = ObjectType.fromObject(template);
        final ITemplatePacket templPacket = directProxy.getTypeManager().getTemplatePacketFromObject(template, objectType);
        return spaceCopy(remoteUrl, templPacket, includeNotifyTemplates, chunkSize);
    }

    public SpaceCopyStatus spaceCopy(String remoteUrl, ITemplatePacket templPacket, boolean includeNotifyTemplates, int chunkSize) throws RemoteException {
        SpaceContext localSpaceContext = ((IDirectSpaceProxy) _spaceProxy).getSecurityManager().acquireContext(((IDirectSpaceProxy) _spaceProxy).getRemoteJSpace());
        return ((IInternalRemoteJSpaceAdmin) adminImpl).spaceCopy(remoteUrl, templPacket, includeNotifyTemplates, chunkSize, localSpaceContext);
    }

    public SpaceCopyStatus spaceCopy(String remoteUrl, ITemplatePacket template,
                                     boolean includeNotifyTemplates, int chunkSize, SpaceContext sc)
            throws RemoteException {
        throw new IllegalStateException("Path not supported");
    }


    public SpaceCopyStatus spaceCopy(IRemoteSpace remoteProxy,
                                     String remoteSpaceUrl, String remoteSpaceName,
                                     ITemplatePacket template, boolean includeNotifyTemplates,
                                     int chunkSize, SpaceContext spaceContext, SpaceContext remoteSpaceContext) throws RemoteException {
        throw new IllegalStateException("Path not supported");
    }

    public SpaceCopyStatus spaceCopy(IJSpace remoteProxy, Object template, boolean includeNotifyTemplates, int chunkSize) throws RemoteException {
        // verify that user can write on the local.
        IDirectSpaceProxy directProxy = (IDirectSpaceProxy) _spaceProxy;

        SpaceContext localSpaceContext = ((IDirectSpaceProxy) _spaceProxy).getSecurityManager().acquireContext(((IDirectSpaceProxy) _spaceProxy).getRemoteJSpace());
        SpaceContext remoteSpaceContext = ((IDirectSpaceProxy) remoteProxy).getSecurityManager().acquireContext(((IDirectSpaceProxy) remoteProxy).getRemoteJSpace());

        final IRemoteSpace remoteSpace = ((ISpaceProxy) remoteProxy).getDirectProxy().getRemoteJSpace();
        final ObjectType objectType = ObjectType.fromObject(template);
        final ITemplatePacket templPacket = directProxy.getTypeManager().getTemplatePacketFromObject(template, objectType);

        String remoteUrl = "jini://*/" + remoteProxy.getContainerName() + "/" + remoteProxy.getName();

        SpaceURL parseURL;
        try {
            parseURL = SpaceURLParser.parseURL(remoteUrl);
        } catch (MalformedURLException e) {
            throw new RemoteException("", e);
        }
        String groups = ((ISpaceProxy) remoteProxy).getDirectProxy().getProxySettings().getFinderURL().getProperty(SpaceURL.GROUPS);
        if (groups != null)
            parseURL.setProperty(SpaceURL.GROUPS, groups);
        String locators = ((ISpaceProxy) remoteProxy).getDirectProxy().getProxySettings().getFinderURL().getProperty(SpaceURL.LOCATORS);
        if (locators != null)
            parseURL.setProperty(SpaceURL.LOCATORS, locators);

        String remoteName = ((ISpaceProxy) remoteProxy).getDirectProxy().getRemoteMemberName();

        return ((IInternalRemoteJSpaceAdmin) adminImpl).spaceCopy(remoteSpace, parseURL.getURL(), remoteName,
                templPacket, includeNotifyTemplates, chunkSize, localSpaceContext, remoteSpaceContext);

    }

    /*
     * @see com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin#getSpaceMode()
     */
    public SpaceMode getSpaceMode() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getSpaceMode();
    }

    /*
     * @see com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin#addSpaceModeListener(com.gigaspaces.cluster.activeelection.ISpaceModeListener)
     */
    public SpaceMode addSpaceModeListener(ISpaceModeListener listener) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).addSpaceModeListener(listener);
    }

    /*
     * @see com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin#removeSpaceModeListener(com.gigaspaces.cluster.activeelection.ISpaceModeListener)
     */
    public void removeSpaceModeListener(ISpaceModeListener listener)
            throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) adminImpl).removeSpaceModeListener(listener);
    }

    public NIODetails getNIODetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getNIODetails();
    }

    public NIOStatistics getNIOStatistics() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getNIOStatistics();
    }

    @Override
    public void enableLRMIMonitoring() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) adminImpl).enableLRMIMonitoring();
    }

    @Override
    public void disableLRMIMonitoring() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) adminImpl).disableLRMIMonitoring();
    }

    @Override
    public LRMIMonitoringDetails fetchLRMIMonitoringDetails()
            throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).fetchLRMIMonitoringDetails();
    }

    public long getCurrentTimestamp() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getCurrentTimestamp();
    }

    public OSDetails getOSDetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getOSDetails();
    }

    public OSStatistics getOSStatistics() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getOSStatistics();
    }

    public JVMDetails getJVMDetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getJVMDetails();
    }

    public JVMStatistics getJVMStatistics() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getJVMStatistics();
    }

    public void runGc() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) adminImpl).runGc();
    }

    public String[] getZones() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getZones();
    }

    public void forceMoveToPrimary() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) adminImpl).forceMoveToPrimary();
    }

    public String getReplicationDump() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getReplicationDump();
    }

    @Override
    public Map<String, LocalCacheDetails> getLocalCacheDetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getLocalCacheDetails();
    }

    @Override
    public Map<String, LocalViewDetails> getLocalViewDetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) adminImpl).getLocalViewDetails();
    }
}