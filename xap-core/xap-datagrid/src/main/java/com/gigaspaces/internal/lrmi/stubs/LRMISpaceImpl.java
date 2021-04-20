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

/*
 * @(#)LRMISpaceImpl.java 1.0 13/10/2002 14:57PM
 */

package com.gigaspaces.internal.lrmi.stubs;

import com.gigaspaces.cluster.activeelection.ISpaceModeListener;
import com.gigaspaces.cluster.activeelection.SpaceMode;
import com.gigaspaces.grid.zone.GridZoneProvider;
import com.gigaspaces.internal.client.spaceproxy.DirectSpaceProxyFactoryImpl;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntrySpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.SpaceConnectRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.SpaceConnectResult;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncListBatch;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IReplicationConnectionProxy;
import com.gigaspaces.internal.jvm.JVMDetails;
import com.gigaspaces.internal.jvm.JVMInfoProvider;
import com.gigaspaces.internal.jvm.JVMStatistics;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.os.OSDetails;
import com.gigaspaces.internal.os.OSInfoProvider;
import com.gigaspaces.internal.os.OSStatistics;
import com.gigaspaces.internal.remoting.RemoteOperationRequest;
import com.gigaspaces.internal.remoting.RemoteOperationResult;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.admin.demote.DemoteFailedException;
import com.gigaspaces.internal.server.space.suspend.SuspendTypeChangedInternalListener;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.lrmi.LRMIMonitoringDetails;
import com.gigaspaces.lrmi.RemoteStub;
import com.gigaspaces.lrmi.nio.info.NIODetails;
import com.gigaspaces.lrmi.nio.info.NIOInfoProvider;
import com.gigaspaces.lrmi.nio.info.NIOStatistics;
import com.gigaspaces.management.space.LocalCacheDetails;
import com.gigaspaces.management.space.LocalViewDetails;
import com.gigaspaces.management.transport.ITransportConnection;
import com.gigaspaces.security.service.RemoteSecuredService;
import com.gigaspaces.server.space.suspend.SuspendType;
import com.gigaspaces.transport.NioChannel;
import com.gigaspaces.transport.PocSettings;
import com.gigaspaces.transport.client.*;
import com.j_spaces.core.DropClassException;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceCopyStatus;
import com.j_spaces.core.SpaceHealthStatus;
import com.j_spaces.core.admin.*;
import com.j_spaces.core.client.BasicTypeInfo;
import com.j_spaces.core.client.SpaceSettings;
import com.j_spaces.core.client.TransactionInfo;
import com.j_spaces.core.client.UnderTxnLockedObject;
import com.j_spaces.core.cluster.ClusterPolicy;
import com.j_spaces.core.service.Service;
import com.j_spaces.jdbc.IQueryProcessor;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.TransactionManager;
import net.jini.id.Uuid;

import org.jini.rio.boot.SpaceInstanceRemoteClassLoaderInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * LRMI Stub implementation of <code>IRemoteJSpaceImpl</code> interface.
 *
 * @author Igor Goldenberg
 * @version 2.5
 */
@com.gigaspaces.api.InternalApi
public class LRMISpaceImpl extends RemoteStub<IRemoteSpace>
        implements IRemoteSpace, IInternalRemoteJSpaceAdmin, Service {
    static final long serialVersionUID = 2L;

    private final NioConnectionPool connectionPool = initConnectionPool(PocSettings.clientConnectionPoolType);

    private static NioConnectionPool initConnectionPool(String type) {
        switch (type) {
            case "singleton": return new NioConnectionPoolSingleton();
            case "thread-local": return new NioConnectionPoolThreadLocal();
            case "fixed": return new NioConnectionPoolFixed();
            case "dynamic": return new NioConnectionPoolDynamic();
            default: throw new IllegalArgumentException("Unsupported connection pool type: " + type);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(LRMISpaceImpl.class);

    private transient Uuid _spaceUuid; //cache at client side to avoid remote calls

    private boolean nioEnabled;

    // NOTE, here just for externalizable
    public LRMISpaceImpl() {
    }

    /**
     * constructor to initialize space stub
     */
    public LRMISpaceImpl(IRemoteSpace directRefObj, IRemoteSpace dynamicProxy) {
        super(directRefObj, dynamicProxy);
        initialize();
        System.out.println("NIV_DEBUG: LRMISpaceImpl ctor #2");
    }

    private void initialize() {
        this.nioEnabled = !isDirect() && PocSettings.isEnabled();
        if (nioEnabled)
            logger.info("Created (nio enabled, {})", PocSettings.dump());
        else
            logger.info("Created (nio disabled)");
    }

    @Override
    public boolean isEmbedded() {
        return isDirect();
    }

    @Override
    public Object[] getReplicationStatus() throws RemoteException {
        return ((IRemoteJSpaceAdmin) getProxy()).getReplicationStatus();
    }

    @Override
    public boolean isStartedWithinGSC() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).isStartedWithinGSC();
    }

    @Override
    public RuntimeHolder getRuntimeHolder() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getRuntimeHolder();
    }

    @Override
    public void snapshot(ITemplatePacket e) throws UnusableEntryException,
            RemoteException {
        getProxy().snapshot(e);
    }

    @Override
    public void cancel(String entryUID, String classname, int objectType)
            throws UnknownLeaseException, RemoteException {
        getProxy().cancel(entryUID, classname, objectType);
    }

    @Override
    public long renew(String entryUID, String classname, int objectType,
                      long duration) throws LeaseDeniedException, UnknownLeaseException,
            RemoteException {
        return getProxy().renew(entryUID, classname, objectType, duration);
    }

    @Override
    public Exception[] cancelAll(String[] entryUIDs, String[] classnames,
                                 int[] objectTypes) throws RemoteException {
        return getProxy().cancelAll(entryUIDs, classnames, objectTypes);
    }

    @Override
    public Object[] renewAll(String[] entryUIDs, String[] classnames,
                             int[] objectTypes, long[] durations) throws RemoteException {
        return getProxy().renewAll(entryUIDs, classnames, objectTypes, durations);
    }

    @Override
    public void restart() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) getProxy()).restart();
    }

    @Override
    public SuspendType addSpaceSuspendTypeListener(SuspendTypeChangedInternalListener listener) {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).addSpaceSuspendTypeListener(listener);
    }

    @Override
    public void removeSpaceSuspendTypeListener(SuspendTypeChangedInternalListener listener) {
        ((IInternalRemoteJSpaceAdmin) getProxy()).removeSpaceSuspendTypeListener(listener);
    }

    @Override
    public SpaceInstanceRemoteClassLoaderInfo getSpaceInstanceRemoteClassLoaderInfo() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getSpaceInstanceRemoteClassLoaderInfo();
    }

    @Override
    public void ping() throws RemoteException {
        getProxy().ping();
    }

    @Override
    public DirectPersistencySyncListBatch getSynchronizationListBatch() throws RemoteException {
        return getProxy().getSynchronizationListBatch();
    }

    @Override
    public String getName() throws RemoteException {
        return getProxy().getName();
    }

    @Override
    public SpaceRuntimeInfo getRuntimeInfo() throws RemoteException {
        return ((IRemoteJSpaceAdmin) getProxy()).getRuntimeInfo();
    }

    @Override
    public SpaceRuntimeInfo getRuntimeInfo(String className)
            throws RemoteException {
        return ((IRemoteJSpaceAdmin) getProxy()).getRuntimeInfo(className);
    }

    @Override
    public SpaceConfig getConfig() throws RemoteException {
        return ((IRemoteJSpaceAdmin) getProxy()).getConfig();
    }

    @Override
    public void disableStub() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) getProxy()).disableStub();
    }

    @Override
    public ITypeDesc getClassDescriptor(String className) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getClassDescriptor(className);
    }

    @Override
    public ITypeDesc getClassDescriptor(String className, boolean asVirtualType) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getClassDescriptor(className, asVirtualType);
    }

    @Override
    public BasicTypeInfo getClassTypeInfo(String className) throws RemoteException {
        return ((IRemoteJSpaceAdmin) getProxy()).getClassTypeInfo(className);
    }

    @Override
    public void abort(TransactionManager parm1, Object parm2)
            throws net.jini.core.transaction.UnknownTransactionException,
            java.rmi.RemoteException {

        getProxy().abort(parm1, parm2);
    }

    @Override
    public void abort(TransactionManager parm1, long parm2)
            throws net.jini.core.transaction.UnknownTransactionException,
            java.rmi.RemoteException {
        abort(parm1, new Long(parm2));
    }

    @Override
    public void commit(TransactionManager parm1, Object parm2)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        getProxy().commit(parm1, parm2);
    }

    @Override
    public void commit(TransactionManager parm1, long parm2)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        commit(parm1, new Long(parm2));
    }

    @Override
    public void commit(TransactionManager parm1, long parm2, int numOfParticipants)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        getProxy().commit(parm1, new Long(parm2), numOfParticipants);
    }

    @Override
    public void commit(TransactionManager parm1, Object parm2, int numOfParticipants)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        getProxy().commit(parm1, parm2, numOfParticipants);
    }

    @Override
    public int prepare(TransactionManager parm1, Object parm2)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        return getProxy().prepare(parm1, parm2);
    }

    @Override
    public int prepare(TransactionManager parm1, long parm2)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        return prepare(parm1, new Long(parm2));
    }

    @Override
    public int prepare(TransactionManager parm1, Object parm2, int numOfParticipants)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        return getProxy().prepare(parm1, parm2, numOfParticipants);
    }

    @Override
    public int prepareAndCommit(TransactionManager parm1, Object parm2)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        return getProxy().prepareAndCommit(parm1, parm2);
    }

    @Override
    public Object prepare(TransactionManager parm1, long parm2, boolean parm3)
            throws UnknownTransactionException, RemoteException {
        return getProxy().prepare(parm1, parm2, parm3);
    }

    @Override
    public Object prepare(TransactionManager parm1, Object parm2, boolean parm3)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        return getProxy().prepare(parm1, parm2, parm3);
    }

    @Override
    public Object prepare(TransactionManager parm1, Object parm2, int parm3, boolean parm4)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        return getProxy().prepare(parm1, parm2, parm3, parm4);
    }

    @Override
    public int prepareAndCommit(TransactionManager parm1, long parm2)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException {
        return prepareAndCommit(parm1, new Long(parm2));
    }

    @Override
    public void dropClass(String className, SpaceContext sc) throws RemoteException, DropClassException {
        ((IRemoteJSpaceAdmin) getProxy()).dropClass(className, sc);
    }

    /**
     * Returns cluster policy.
     */
    @Override
    public ClusterPolicy getClusterPolicy() throws RemoteException {
        return ((IRemoteJSpaceAdmin) getProxy()).getClusterPolicy();
    }

    @Override
    public Object getAdmin() throws RemoteException {
        return ((Service) getProxy()).getAdmin();
    }

    @Override
    public String getUniqueID() throws RemoteException {
        return getProxy().getUniqueID();
    }

    @Override
    public Uuid getSpaceUuid() throws RemoteException {
        if (_spaceUuid != null) {
            return _spaceUuid;
        }
        Uuid localSpaceUuid = getProxy().getSpaceUuid();
        _spaceUuid = localSpaceUuid;
        return localSpaceUuid;
    }

    @Override
    public TransactionInfo[] getTransactionsInfo(int type, int status)
            throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getTransactionsInfo(type, status);
    }

    @Override
    public int countTransactions(int type, int status)
            throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).countTransactions(type, status);
    }

    @Override
    public List<TemplateInfo> getTemplatesInfo(String className) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getTemplatesInfo(className);
    }

    @Override
    public List<ITransportConnection> getConnectionsInfo() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getConnectionsInfo();
    }

    @Override
    public int countIncomingConnections() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).countIncomingConnections();
    }

    @Override
    public List<UnderTxnLockedObject> getLockedObjects(Transaction txn) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getLockedObjects(txn);
    }

    @Override
    public SpaceMode getSpaceMode() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getSpaceMode();
    }

    @Override
    public SpaceMode addSpaceModeListener(ISpaceModeListener listener) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).addSpaceModeListener(listener);
    }

    @Override
    public void removeSpaceModeListener(ISpaceModeListener listener) throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) getProxy()).removeSpaceModeListener(listener);
    }

    @Override
    public void start() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) getProxy()).start();
    }

    @Override
    public void stop() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) getProxy()).stop();
    }

    @Override
    public int getState() throws RemoteException {
        return ((IRemoteJSpaceAdmin) getProxy()).getState();
    }

    private transient NIODetails nioDetails;

    @Override
    public NIODetails getNIODetails() throws RemoteException {
        if (nioDetails != null) {
            return nioDetails;
        }
        nioDetails = ((NIOInfoProvider) getProxy()).getNIODetails();
        return nioDetails;
    }

    @Override
    public NIOStatistics getNIOStatistics() throws RemoteException {
        return ((NIOInfoProvider) getProxy()).getNIOStatistics();
    }

    @Override
    public void enableLRMIMonitoring() throws RemoteException {
        ((NIOInfoProvider) getProxy()).enableLRMIMonitoring();
    }

    @Override
    public void disableLRMIMonitoring() throws RemoteException {
        ((NIOInfoProvider) getProxy()).disableLRMIMonitoring();
    }

    @Override
    public LRMIMonitoringDetails fetchLRMIMonitoringDetails()
            throws RemoteException {
        return ((NIOInfoProvider) getProxy()).fetchLRMIMonitoringDetails();
    }

    private transient OSDetails osDetails;

    @Override
    public long getCurrentTimestamp() throws RemoteException {
        return ((OSInfoProvider) getProxy()).getCurrentTimestamp();
    }

    @Override
    public OSDetails getOSDetails() throws RemoteException {
        if (osDetails != null) {
            return osDetails;
        }
        osDetails = ((OSInfoProvider) getProxy()).getOSDetails();
        return osDetails;
    }

    @Override
    public OSStatistics getOSStatistics() throws RemoteException {
        return ((OSInfoProvider) getProxy()).getOSStatistics();
    }

    private transient JVMDetails jvmDetails;

    @Override
    public JVMDetails getJVMDetails() throws RemoteException {
        if (jvmDetails != null) {
            return jvmDetails;
        }
        jvmDetails = ((JVMInfoProvider) getProxy()).getJVMDetails();
        return jvmDetails;
    }

    @Override
    public JVMStatistics getJVMStatistics() throws RemoteException {
        return ((JVMInfoProvider) getProxy()).getJVMStatistics();
    }

    @Override
    public void runGc() throws RemoteException {
        ((JVMInfoProvider) getProxy()).runGc();
    }

    // zones are static, cache them
    private transient String[] zones;

    @Override
    public String[] getZones() throws RemoteException {
        if (zones != null) {
            return zones;
        }
        zones = ((GridZoneProvider) getProxy()).getZones();
        return zones;
    }

    @Override
    public byte[] getClusterConfigFile() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getClusterConfigFile();
    }

    @Override
    public void setClusterConfigFile(byte[] config) throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) getProxy()).setClusterConfigFile(config);
    }

    /**
     * Igor.G 13/3/2005 ver 5.01. This method will never invoked! We have an abstraction problem
     * with IRemoteJSpaceAdmin, JSpaceAdminProxy and SpaceInternal, so added this non-impl method
     * only to make compiler happy :(
     */
    @Override
    public SpaceCopyStatus spaceCopy(String remoteUrl, Object template,
                                     boolean includeNotifyTemplates, int chunkSize) throws RemoteException {
        return ((IRemoteJSpaceAdmin) getProxy()).spaceCopy(remoteUrl, template, includeNotifyTemplates, chunkSize);
    }

    @Override
    public SpaceCopyStatus spaceCopy(String remoteUrl, ITemplatePacket template,
                                     boolean includeNotifyTemplates, int chunkSize) throws RemoteException {

        throw new IllegalArgumentException();
    }

    @Override
    public SpaceCopyStatus spaceCopy(String remoteUrl, ITemplatePacket template,
                                     boolean includeNotifyTemplates, int chunkSize, SpaceContext sc)
            throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).spaceCopy(remoteUrl, template, includeNotifyTemplates, chunkSize, sc);
    }


    @Override
    public SpaceCopyStatus spaceCopy(IJSpace remoteSpace, Object template, boolean includeNotifyTemplates, int chunkSize) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).spaceCopy(remoteSpace, template, includeNotifyTemplates, chunkSize);
    }

    @Override
    public SpaceCopyStatus spaceCopy(IRemoteSpace remoteProxy,
                                     String remoteSpaceUrl, String remoteSpaceName,
                                     ITemplatePacket template, boolean includeNotifyTemplates,
                                     int chunkSize, SpaceContext spaceContext, SpaceContext remoteSpaceContext) throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).spaceCopy(remoteProxy, remoteSpaceUrl, remoteSpaceName, template, includeNotifyTemplates, chunkSize, spaceContext, remoteSpaceContext);
    }

    @Override
    public IQueryProcessor getQueryProcessor() throws RemoteException {
        return getProxy().getQueryProcessor();
    }

    @Override
    public void renewLease(TransactionManager mgr, long id, long time)
            throws LeaseDeniedException, UnknownLeaseException, RemoteException {
        getProxy().renewLease(mgr, id, time);
    }

    @Override
    public SpaceHealthStatus getSpaceHealthStatus() throws RemoteException {
        return getProxy().getSpaceHealthStatus();
    }

    @Override
    public com.gigaspaces.security.service.SecurityContext login(
            com.gigaspaces.security.service.SecurityContext securityContext) throws RemoteException {
        return ((RemoteSecuredService) getProxy()).login(securityContext);
    }

    @Override
    public Object getServiceProxy() throws java.rmi.RemoteException {
        //return getProxy().getServiceProxy();
        final SpaceConnectResult result = connect(new SpaceConnectRequest());
        final SpaceSettings spaceSettings = result.getSpaceSettings();
        if (spaceSettings.getSpaceConfig().getClusterPolicy() != null)
            throw new IllegalStateException("Cluster policy should be null");
        DirectSpaceProxyFactoryImpl factory = new DirectSpaceProxyFactoryImpl(this, spaceSettings, spaceSettings.getSpaceConfig().getClusterInfo().isClustered());
        return factory.createSpaceProxy();
    }

    @Override
    public SpaceConnectResult connect(SpaceConnectRequest request) throws RemoteException {
        return getProxy().connect(request);
    }

    @Override
    public void forceMoveToPrimary() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) getProxy()).forceMoveToPrimary();
    }

    @Override
    public IReplicationConnectionProxy getReplicationRouterConnectionProxy() throws RemoteException {
        return getProxy().getReplicationRouterConnectionProxy();
    }

    @Override
    public String getReplicationDump() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getReplicationDump();
    }

    @Override
    public Class<?> loadRemoteClass(String className) throws RemoteException, ClassNotFoundException {
        return getProxy().loadRemoteClass(className);
    }

    @Override
    public <T extends RemoteOperationResult> T executeOperation(RemoteOperationRequest<T> request)
            throws RemoteException {
        if (nioEnabled) {
            //logger.debug("Proxy is executing {}", request);
            NioChannel connection = null;
            try {
                connection = connectionPool.acquire();
                ByteBuffer requestBuffer;
                byte[] cachedRequest = connection.getCachedRequest();
                if (cachedRequest != null)
                    requestBuffer = ByteBuffer.wrap(cachedRequest);
                else {
                    requestBuffer = connection.serialize(request);
                    if (PocSettings.cacheRequest && request instanceof ReadTakeEntrySpaceOperationRequest) {
                        cachedRequest = toByteArray(connection.serialize(request));
                        connection.setCachedRequest(cachedRequest);
                        logger.info("Cached request for future executions - length: {}", cachedRequest.length);
                    }
                }
                connection.writeBlocking(requestBuffer);
                ByteBuffer reponseBuffer = connection.readBlocking();
                T response = (T) connection.deserialize(reponseBuffer);
                return response;
            } catch (IOException | ClassNotFoundException e) {
                logger.error("Failed to execute request", e);
                System.exit(1);
                throw new RemoteException("Failed to execute request", e);
            } finally {
                if (connection != null)
                    connectionPool.release(connection);
            }
        } else {
            return getProxy().executeOperation(request);
        }
    }

    private byte[] toByteArray(ByteBuffer buffer) {
        byte[] result = new byte[buffer.limit()];
        buffer.get(result);
        return result;
    }

    @Override
    public <T extends RemoteOperationResult> T executeOperationAsync(
            RemoteOperationRequest<T> request) throws RemoteException {
        if (isDirect())
            throw new IllegalStateException("Attempt to execute an async operation on a direct LRMISpaceImpl.");

        getProxy().executeOperationAsync(request);
        return null;
    }

    @Override
    public void executeOperationOneway(RemoteOperationRequest<?> request)
            throws RemoteException {
        getProxy().executeOperationOneway(request);
    }

    @Override
    public boolean isActive() throws RemoteException {
        return getProxy().isActive();
    }

    @Override
    public boolean isActiveAndNotSuspended() throws RemoteException {
        return getProxy().isActiveAndNotSuspended();
    }

    @Override
    public ClusterTopology checkChunkMapGeneration(int clientGeneration) {
        return getProxy().checkChunkMapGeneration(clientGeneration);
    }

    @Override
    public Boolean isActiveAsync() throws RemoteException {
        return getProxy().isActiveAsync();
    }

    @Override
    public Map<String, LocalCacheDetails> getLocalCacheDetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getLocalCacheDetails();
    }

    @Override
    public Map<String, LocalViewDetails> getLocalViewDetails() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getProxy()).getLocalViewDetails();
    }

    @Override
    public void demote(long maxSuspendTime, TimeUnit unit, SpaceContext sc) throws DemoteFailedException, RemoteException {
        getProxy().demote(maxSuspendTime, unit, sc);
    }

    @Override
    public void updateClusterInfo() throws RemoteException {
        ((IInternalRemoteJSpaceAdmin) getProxy()).updateClusterInfo();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        initialize();
    }
}