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
import com.j_spaces.core.client.TransactionInfo;
import com.j_spaces.core.client.UnderTxnLockedObject;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.exception.SpaceAlreadyStartedException;
import com.j_spaces.core.exception.SpaceAlreadyStoppedException;
import com.j_spaces.core.filters.StatisticsContext;
import com.j_spaces.core.filters.StatisticsHolder;
import com.j_spaces.core.service.AbstractService;
import com.j_spaces.core.service.ServiceAdmin;
import com.j_spaces.core.service.ServiceAdminImpl;

import net.jini.core.transaction.Transaction;
import net.jini.export.Exporter;
import org.jini.rio.boot.SpaceInstanceRemoteClassLoaderInfo;

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
public class JSpaceAdminImpl extends ServiceAdminImpl
        implements IInternalRemoteJSpaceAdmin, StatisticsAdmin {
    /**
     * @param service
     * @param exporter
     * @throws RemoteException
     */
    public JSpaceAdminImpl(AbstractService service, Exporter exporter)
            throws RemoteException {
        super(service, exporter);
    }

    /**
     * Overload method;
     *
     * @see com.j_spaces.core.service.ServiceAdminImpl#getProxy()
     */
    @Override
    public ServiceAdmin getProxy() {
        if (m_adminProxy == null)
            m_adminProxy = new JSpaceAdminProxy(m_thisRemoteRef, m_service.getUuid());

        return m_adminProxy;
    }


    public void restart() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) m_service).restart();
    }

    @Override
    public SpaceInstanceRemoteClassLoaderInfo getSpaceInstanceRemoteClassLoaderInfo() throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_service).getSpaceInstanceRemoteClassLoaderInfo();
    }

    public String getName() throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_service).getName();
    }

    public SpaceRuntimeInfo getRuntimeInfo() throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_service).getRuntimeInfo();
    }

    public SpaceRuntimeInfo getRuntimeInfo(String className) throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_service).getRuntimeInfo(className);
    }

    public SpaceConfig getConfig() throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_service).getConfig();
    }

    public void ping() throws RemoteException {
        ((IRemoteJSpaceAdmin) m_service).ping();
    }

    public void disableStub() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) m_service).disableStub();
    }

    public Object[] getReplicationStatus() throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_service).getReplicationStatus();
    }

    public RuntimeHolder getRuntimeHolder() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getRuntimeHolder();
    }

    public boolean isStartedWithinGSC() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).isStartedWithinGSC();
    }

    public ITypeDesc getClassDescriptor(String className)
            throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getClassDescriptor(className);
    }

    public BasicTypeInfo getClassTypeInfo(String className) throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_service).getClassTypeInfo(className);
    }

    public ClusterPolicy getClusterPolicy() throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_service).getClusterPolicy();
    }


    public void dropClass(String className, SpaceContext sc)
            throws RemoteException, DropClassException {
        ((IRemoteJSpaceAdmin) m_service).dropClass(className, sc);
    }

    public void start() throws RemoteException, SpaceAlreadyStartedException {
        ((IInternalRemoteJSpaceAdmin) m_service).start();
    }

    public void stop() throws RemoteException, SpaceAlreadyStoppedException {
        ((IInternalRemoteJSpaceAdmin) m_service).stop();
    }

    /**
     * @see IRemoteJSpaceAdmin
     */
    public TransactionInfo[] getTransactionsInfo(int type, int status) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getTransactionsInfo(type, status);
    }

    public int countTransactions(int type, int status) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).countTransactions(type, status);
    }

    /**
     * @see com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin#getTemplatesInfo(java.lang.String)
     */
    public List<TemplateInfo> getTemplatesInfo(String className) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getTemplatesInfo(className);
    }

    @Override
    public List<ITransportConnection> getConnectionsInfo() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getConnectionsInfo();
    }

    @Override
    public int countIncomingConnections() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).countIncomingConnections();
    }

    public List<UnderTxnLockedObject> getLockedObjects(Transaction txn) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getLockedObjects(txn);
    }

    public int getState() throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_service).getState();
    }


    public boolean isStatisticsAvailable() throws RemoteException {
        return ((StatisticsAdmin) m_service).isStatisticsAvailable();
    }

    public String[] getStatisticsStringArray() throws RemoteException {
        return ((StatisticsAdmin) m_service).getStatisticsStringArray();
    }

    public void setStatisticsSamplingRate(long rate) throws RemoteException {
        ((StatisticsAdmin) m_service).setStatisticsSamplingRate(rate);
    }

    public long getStatisticsSamplingRate() throws RemoteException {
        return ((StatisticsAdmin) m_service).getStatisticsSamplingRate();
    }

    public StatisticsContext getStatistics(int operationCode) throws RemoteException {
        return ((StatisticsAdmin) m_service).getStatistics(operationCode);
    }

    public StatisticsHolder getHolder() throws RemoteException {
        return ((StatisticsAdmin) m_service).getHolder();
    }

    public Map<Integer, StatisticsContext> getStatistics(Integer[] operationCodes)
            throws RemoteException {
        return ((StatisticsAdmin) m_service).getStatistics(operationCodes);
    }

    public Map<Integer, StatisticsContext> getStatistics() throws RemoteException {
        return ((StatisticsAdmin) m_service).getStatistics();
    }


    public byte[] getClusterConfigFile() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getClusterConfigFile();
    }

    public void setClusterConfigFile(byte[] config) throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) m_service).setClusterConfigFile(config);
    }

    /**
     * Igor.G 13/3/2005 ver 5.01. This method will never invoked! We have an abstraction problem
     * with IRemoteJSpaceAdmin, JSpaceAdminProxy and SpaceInternal, so added method only to make
     * compiler happy :(
     **/
    public SpaceCopyStatus spaceCopy(String remoteUrl, Object template, boolean includeNotifyTemplates, int chunkSize)
            throws RemoteException {
        return ((IRemoteJSpaceAdmin) m_service).spaceCopy(remoteUrl, template, includeNotifyTemplates, chunkSize);
    }

    public SpaceCopyStatus spaceCopy(String remoteUrl, ITemplatePacket template, boolean includeNotifyTemplates, int chunkSize)
            throws RemoteException {
        throw new IllegalArgumentException();
    }

    public SpaceCopyStatus spaceCopy(String remoteUrl, ITemplatePacket template, boolean includeNotifyTemplates, int chunkSize, SpaceContext sc)
            throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).spaceCopy(remoteUrl, template, includeNotifyTemplates, chunkSize, sc);
    }

    public SpaceCopyStatus spaceCopy(IRemoteSpace remoteProxy, String remoteSpaceUrl, String remoteSpaceName, ITemplatePacket template, boolean includeNotifyTemplates, int chunkSize, SpaceContext sc, SpaceContext remoteSpaceContext) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).spaceCopy(remoteProxy, remoteSpaceUrl, remoteSpaceName, template, includeNotifyTemplates, chunkSize, sc, remoteSpaceContext);
    }


    public SpaceCopyStatus spaceCopy(IJSpace remoteSpace, Object template, boolean includeNotifyTemplates, int chunkSize) throws RemoteException {
        throw new UnsupportedOperationException("use the remote one");
    }

    /*
     * @see com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin#getSpaceMode()
     */
    public SpaceMode getSpaceMode() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getSpaceMode();
    }

    /*
     * @see com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin#addSpaceModeListener(com.gigaspaces.cluster.activeelection.ISpaceModeListener)
     */
    public SpaceMode addSpaceModeListener(ISpaceModeListener listener) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).addSpaceModeListener(listener);
    }

    /*
     * @see com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin#removeSpaceModeListener(com.gigaspaces.cluster.activeelection.ISpaceModeListener)
     */
    public void removeSpaceModeListener(ISpaceModeListener listener)
            throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) m_service).removeSpaceModeListener(listener);
    }

    public NIODetails getNIODetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getNIODetails();
    }

    public NIOStatistics getNIOStatistics() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getNIOStatistics();
    }

    @Override
    public void enableLRMIMonitoring() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) m_service).enableLRMIMonitoring();
    }

    @Override
    public void disableLRMIMonitoring() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) m_service).disableLRMIMonitoring();
    }

    @Override
    public LRMIMonitoringDetails fetchLRMIMonitoringDetails()
            throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).fetchLRMIMonitoringDetails();
    }

    public long getCurrentTimestamp() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getCurrentTimestamp();
    }

    public OSDetails getOSDetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getOSDetails();
    }

    public OSStatistics getOSStatistics() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getOSStatistics();
    }

    public JVMDetails getJVMDetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getJVMDetails();
    }

    public JVMStatistics getJVMStatistics() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getJVMStatistics();
    }

    public void runGc() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) m_service).runGc();
    }

    public String[] getZones() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getZones();
    }

    public void forceMoveToPrimary() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) m_service).forceMoveToPrimary();
    }

    public String getReplicationDump() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getReplicationDump();
    }

    @Override
    public Map<String, LocalCacheDetails> getLocalCacheDetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getLocalCacheDetails();
    }

    @Override
    public Map<String, LocalViewDetails> getLocalViewDetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) m_service).getLocalViewDetails();
    }
}
