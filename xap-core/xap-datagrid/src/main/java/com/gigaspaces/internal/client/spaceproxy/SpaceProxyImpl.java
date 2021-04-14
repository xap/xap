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

import com.gigaspaces.admin.demote.DemoteFailedException;
import com.gigaspaces.admin.quiesce.QuiesceToken;
import com.gigaspaces.client.DirectSpaceProxyFactory;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.CommonProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.SnapshotProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actions.AbstractSpaceProxyActionManager;
import com.gigaspaces.internal.client.spaceproxy.actions.SpaceProxyImplActionManager;
import com.gigaspaces.internal.client.spaceproxy.events.SpaceProxyDataEventsManager;
import com.gigaspaces.internal.client.spaceproxy.executors.SystemTask;
import com.gigaspaces.internal.client.spaceproxy.metadata.ISpaceProxyTypeManager;
import com.gigaspaces.internal.client.spaceproxy.metadata.ObjectType;
import com.gigaspaces.internal.client.spaceproxy.metadata.SpaceProxyTypeManager;
import com.gigaspaces.internal.client.spaceproxy.operations.ExecuteTaskSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.GetEntryTypeDescriptorSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.RegisterEntryTypeDescriptorSpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.SpaceOperationRequest;
import com.gigaspaces.internal.client.spaceproxy.router.SpaceProxyRouter;
import com.gigaspaces.internal.client.spaceproxy.transaction.SpaceProxyTransactionManager;
import com.gigaspaces.internal.cluster.ClusterTopology;
import com.gigaspaces.internal.cluster.SpaceClusterInfo;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.IClusterInfoChangedListener;
import com.gigaspaces.internal.server.space.IRemoteSpace;
import com.gigaspaces.internal.server.space.SpaceImpl;
import com.gigaspaces.internal.server.space.tiered_storage.error.TieredStorageMetadataException;
import com.gigaspaces.internal.server.space.tiered_storage.error.TieredStorageOperationException;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.version.PlatformLogicalVersion;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.lrmi.LRMIInvocationContext;
import com.gigaspaces.lrmi.LRMIRuntime;
import com.gigaspaces.metadata.SpaceMetadataException;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.security.directory.CredentialsProvider;
import com.j_spaces.core.*;
import com.j_spaces.core.admin.ContainerConfig;
import com.j_spaces.core.admin.IInternalRemoteJSpaceAdmin;
import com.j_spaces.core.admin.IJSpaceContainerAdmin;
import com.j_spaces.core.admin.JSpaceAdminProxy;
import com.j_spaces.core.client.*;
import com.j_spaces.core.client.sql.IQueryManager;
import com.j_spaces.core.client.sql.QueryManager;
import com.j_spaces.jdbc.builder.SQLQueryTemplatePacket;
import com.j_spaces.kernel.SystemProperties;
import com.sun.jini.proxy.DefaultProxyPivot;
import com.sun.jini.proxy.MarshalPivot;
import com.sun.jini.proxy.MarshalPivotProvider;
import net.jini.admin.Administrable;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.core.transaction.server.ServerTransaction;
import net.jini.id.Uuid;
import net.jini.lookup.SameProxyVersionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.rmi.RemoteException;
import java.security.SecureRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides the functionality of clustered proxy.
 *
 * @version 1.3
 * @since 2.0
 */
@com.gigaspaces.api.InternalApi
public class SpaceProxyImpl extends AbstractDirectSpaceProxy implements SameProxyVersionProvider, MarshalPivotProvider, IClusterInfoChangedListener {
    private static final long serialVersionUID = 1L;

    private static final Logger _clientLogger = LoggerFactory.getLogger(Constants.LOGGER_CLIENT);

    private final DirectSpaceProxyFactoryImpl _factory;
    private final long _clientID;
    private final AtomicLong _operationID;
    private final Object _spaceInitializeLock;
    private final SpaceProxyTransactionManager _transactionManager;
    private final IProxySecurityManager _securityManager;
    private final ISpaceProxyTypeManager _typeManager;
    private final IQueryManager _queryManager;
    private final SpaceProxyDataEventsManager _dataEventsManager;

    private boolean _initializedNewRouter;
    private volatile SpaceProxyRouter _proxyRouter;
    private volatile boolean closed = false;

    public SpaceProxyImpl(DirectSpaceProxyFactoryImpl factory, ProxySettings proxySettings) {
        super(proxySettings);
        this._factory = factory;
        final SecureRandom random = new SecureRandom();
        this._clientID = random.nextLong();
        this._operationID = new AtomicLong();
        this._spaceInitializeLock = new Object();
        this._transactionManager = new SpaceProxyTransactionManager();

        this._securityManager = _proxySettings.isSecuredService() ? new SpaceProxySecurityManager(this) : new NullProxySecurityManager(this);
        this._queryManager = new QueryManager(this);
        this._typeManager = new SpaceProxyTypeManager(this);
        this._dataEventsManager = new SpaceProxyDataEventsManager(this, _proxySettings.getExportedTransportConfig());

        // set properties attached to space url
        getURL().setPropertiesForSpaceProxy(this);

        // initialize ITimeProvider provider
        try {
            if (_proxySettings.getTimeProvider() != null)
                System.setProperty(SystemProperties.SYSTEM_TIME_PROVIDER, _proxySettings.getTimeProvider());
        } catch (SecurityException se) {
            _clientLogger.info("Failed to set time provider system property", se);
        }

        if (getSpaceImplIfEmbedded() != null) {
            this.getSpaceImplIfEmbedded().registerToClusterInfoChangedEvent(this);
        }
    }

    private SpaceProxyImpl getOrCreateProxy(boolean isClustered) {
        if (isClustered() == isClustered)
            return this;
        SpaceProxyImpl copy = new SpaceProxyImpl(_factory.createCopy(isClustered), _proxySettings);
        if (this.isSecured())
            copy.getSecurityManager().initialize(this.getSecurityManager().getCredentialsProvider());
        return copy;
    }

    @Override
    public ExecutorService getThreadPool() {
        return LRMIRuntime.getRuntime().getThreadPool();
    }

    @Override
    public IDirectSpaceProxy getDirectProxy() {
        return this;
    }

    @Override
    public String getCacheTypeName() {
        return "space";
    }

    @Override
    public IDirectSpaceProxy getNotificationsDirectProxy() {
        return this;
    }

    @Override
    public IQueryManager getQueryManager() {
        return _queryManager;
    }

    @Override
    public SpaceProxyDataEventsManager getDataEventsManager() {
        return _dataEventsManager;
    }

    @Override
    public IProxySecurityManager getSecurityManager() {
        return _securityManager;
    }

    @Override
    public ISpaceProxyTypeManager getTypeManager() {
        return _typeManager;
    }

    @Override
    public void setActionListener(ActionListener actionListener) {
        _transactionManager.setActionListener(actionListener);
    }

    @Override
    public Transaction.Created getContextTransaction() {
        return _transactionManager.getContextTransaction();
    }

    @Override
    public Transaction.Created replaceContextTransaction(Transaction.Created txn) {
        return _transactionManager.replaceContextTransaction(txn);
    }

    @Override
    public Transaction.Created replaceContextTransaction(Transaction.Created transaction, ActionListener actionListener, boolean delegatedXa) {
        return _transactionManager.replaceContextTransaction(transaction, actionListener, delegatedXa);
    }

    @Override
    public Uuid getReferentUuid() {
        return _proxySettings.getUuid();
    }

    @Override
    public String getName() {
        return _proxySettings.getSpaceName();
    }

    @Override
    public boolean isEmbedded() {
        return _proxySettings.isCollocated();
    }

    @Override
    public boolean isSecured() {
        return _proxySettings.isSecuredService();
    }

    @Override
    public boolean isServiceSecured() throws RemoteException {
        return _proxySettings.isSecuredService();
    }

    @Override
    public boolean isStartedWithinGSC() throws RemoteException {
        return ((IInternalRemoteJSpaceAdmin) getAdmin()).isStartedWithinGSC();
    }

    @Override
    public Object getAdmin() throws RemoteException {
        IRemoteSpace rj = getRemoteJSpace();
        if (rj == null)
            throw new RemoteException("Admin object is not available. Space: " + _proxySettings.getMemberName() + " might be down.");

        Object admin;
        try {
            admin = ((Administrable) rj).getAdmin();
        } catch (RemoteException e) {
            // New since 9.5 - if original space is down look for an alternative.
            rj = getProxyRouter().getAnyAvailableSpace();
            if (rj == null)
                throw e;
            admin = ((Administrable) rj).getAdmin();
        }

        /** set this spaceProxy for space-admin object  */
        if (admin instanceof JSpaceAdminProxy)
            ((JSpaceAdminProxy) admin).setSpace(this);

        return admin;
    }

    /**
     * Get remote admin object for this space. No security checks are performed since this is for
     * internal use only
     */
    @Override
    public Object getPrivilegedAdmin() throws RemoteException {
        return getAdmin();
    }

    @Override
    public IJSpace getNonClusteredProxy() {
        return getOrCreateProxy(false);
    }

    @Override
    public IJSpace getClusteredProxy() {
        return getOrCreateProxy(true);
    }

    @Override
    public IStubHandler getStubHandler() {
        return _proxySettings.getStubHandler();
    }

    @Override
    public IJSpaceContainer getContainer() {
        return _proxySettings.getContainerProxy();
    }

    @Override
    public void shutdown() throws RemoteException {
        _proxySettings.getContainerProxy().shutdown();
    }

    @Override
    public ContainerConfig getContainerConfig() throws RemoteException {
        return ((IJSpaceContainerAdmin) _proxySettings.getContainerProxy()).getConfig();
    }

    @Override
    public SpaceURL getContainerURL() throws RemoteException {
        return _proxySettings.getContainerProxy().getURL();
    }

    @Override
    public SpaceURL getFinderURL() {
        return _proxySettings.getFinderURL();
    }

    public void setFinderURL(SpaceURL _finderurl) {
        _proxySettings.setFinderURL(_finderurl);
    }

    @Override
    public SpaceURL getURL() {
        return _proxySettings.getSpaceURL();
    }

    @Override
    public boolean isOptimisticLockingEnabled() {
        return _proxySettings.isVersioned();
    }

    @Override
    public void setOptimisticLocking(boolean enabled) {
        _proxySettings.setVersioned(enabled);
    }

    @Override
    public boolean isFifo() {
        return _proxySettings.isFifo();
    }

    @Override
    public void setFifo(boolean enabled) {
        _proxySettings.setFifo(enabled);
    }

    @Override
    public int getReadModifiers() {
        return _proxySettings.getReadModifiers();
    }

    @Override
    public int setReadModifiers(int readModifiers) {
        int originalMode = _proxySettings.getReadModifiers();
        _proxySettings.setReadModifiers(readModifiers);
        return originalMode;
    }

    @Override
    public int getUpdateModifiers() {
        return _proxySettings.getUpdateModifiers();
    }

    @Override
    public int setUpdateModifiers(int newModifiers) {
        if (UpdateModifiers.isUpdateOrWrite(newModifiers) && UpdateModifiers.isPartialUpdate(newModifiers))
            throw new RuntimeException("setUpdateMode:invalid update modifiers- PARTIAL Update cannot be specified with UPDATE_OR_WRITE ");

        int originalMode = _proxySettings.getUpdateModifiers();
        _proxySettings.setUpdateModifiers(newModifiers);

        return originalMode;
    }

    @Override
    public boolean isGatewayProxy() {
        return _proxySettings.isGatewayProxy();
    }

    public void setGatewayProxy() {
        this._proxySettings.setGatewayProxy(true);
    }

    @Override
    public Object getVersion() {
        return _proxySettings.getUuid();
    }

    @Override
    protected AbstractSpaceProxyActionManager<SpaceProxyImpl> createActionManager() {
        return new SpaceProxyImplActionManager(this);
    }

    @Override
    public OperationID createNewOperationID() {
        return new OperationID(_clientID, _operationID.getAndIncrement());
    }

    @Override
    public long getClientID() {
        return _clientID;
    }

    @Override
    public void directClean() {
        getTypeManager().deleteAllTypeDescs();
        getQueryManager().clean();
    }

    public void directDropClass(String className) {
        //finished OK - now delete class definition from proxy
        getTypeManager().deleteTypeDesc(className);
        // Potential optimization: consider somehow clearing only queries of dropped type.
        getQueryManager().clean();
    }

    @Override
    public com.gigaspaces.security.service.SecurityContext login(CredentialsProvider credentialsProvider) throws RemoteException {
        return getSecurityManager().login(credentialsProvider);
    }

    public Transaction beforeSpaceAction() {
        return _transactionManager.beforeSpaceAction(null);
    }

    public void beforeSpaceAction(CommonProxyActionInfo action) {
        if(this.getSpaceClusterInfo().isTieredStorage() && action.txn != null){
           throw new TieredStorageOperationException("Transactions are not supported with tiered storage at this stage");
        }
        action.txn = _transactionManager.beforeSpaceAction(action.txn);
    }

    public Transaction beforeSpaceAction(Transaction txn) {
        return _transactionManager.beforeSpaceAction(txn);
    }

    /**
     * overrides implementation of equals() method in AbstractSpaceProxy
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SpaceProxyImpl)
            return super.equals(obj);

        return false;
    }

    // BEGIN INTERNAL CLASSES - CANDIDATES FOR REFACTOR OUT
    // ----------------------------------------------------

    /**
     * This class is only used by the engine to verify that the  proxy is alive.
     */
    final public static class Listener implements IJSpaceProxyListener {
        Listener() {
        }

        /**
         * For callbacks on JSpace Proxies (non-cluster and clustered) to verify that proxy is still
         * alive before performing non-transactional take.
         */
        public boolean isProxyAlive() {
            return true;
        }
    } /* end listener */

    // END INTERNAL CLASSES - CANDIDATES FOR REFACTOR OUT
    // ----------------------------------------------------

    @Override
    public String getRemoteMemberName() {
        return _proxySettings.getMemberName();
    }

    @Override
    public IRemoteSpace getRemoteJSpace() {
        return _proxySettings.getRemoteSpace();
    }

    @Override
    public SpaceImpl getSpaceImplIfEmbedded() {
        return _proxySettings.getSpaceImplIfEmbedded();
    }

    @Override
    public boolean isClustered() {
        return _factory.isClustered();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }

    @Override
    public synchronized void close() {
        if (closed)
            return;

        closed = true;

        if (_dataEventsManager != null)
            _dataEventsManager.close();

        if (_typeManager != null)
            _typeManager.close();

        if (_proxyRouter != null)
            _proxyRouter.close();

        // close the lookup finder (cleans it). Will not be closed when running within the GSC since we want to share it
        LookupFinder.close();

        if(this.getSpaceImplIfEmbedded() != null){
            this.getSpaceImplIfEmbedded().removeClusterInfoChangedListener(this);
        }
    }


    @Override
    public MarshalPivot getMarshalPivot() throws RemoteException {
        return new DefaultProxyPivot(getRemoteJSpace());
    }

    @Override
    public ITypeDesc getTypeDescFromServer(String typeName) {
        try {
            GetEntryTypeDescriptorSpaceOperationRequest request = new GetEntryTypeDescriptorSpaceOperationRequest(typeName);
            getProxyRouter().execute(request);
            return request.getFinalResult();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SpaceMetadataException("Error in getTypeDescFromServer() remote task execution. TypeName='" + typeName + "'.", e);
        } catch (Throwable e) {
            throw new SpaceMetadataException("Error in getTypeDescFromServer() remote task execution. TypeName='" + typeName + "'.", e);
        }
    }

    @Override
    public ITypeDesc registerTypeDescInServers(ITypeDesc typeDesc) {
        try {
            RegisterEntryTypeDescriptorSpaceOperationRequest request = new RegisterEntryTypeDescriptorSpaceOperationRequest()
                    .setTypeDesc(typeDesc)
                    .setGatewayProxy(isGatewayProxy());
            getProxyRouter().execute(request);
            request.processExecutionException();
            // TODO: Return type descriptor from server instead.
            return typeDesc;
        } catch (TieredStorageMetadataException e){
            throw e;
        } catch (Throwable e) {
            throw new SpaceMetadataException("Error in registerTypeDescInServers() remote task execution. TypeName=" + typeDesc.getTypeName(), e);
        }
    }

    @Override
    public Class<?> loadRemoteClass(String className) throws ClassNotFoundException {
        try {
            //We only try to load it from the direct remote space right now
            //If we would broadcast we would create class loader per partition and load this
            //class into many class loaders
            //TODO iterate over all cluster one by one and look for the first
            //occurrence of the class.
            return getRemoteJSpace().loadRemoteClass(className);
        } catch (Exception e) {
            if (e instanceof ClassNotFoundException)
                throw (ClassNotFoundException) e;
            throw new ClassNotFoundException("Could not locate class " + className, e);
        }
    }

    @Override
    public int initWriteModifiers(int modifiers) {
        if (modifiers == Modifiers.NONE)
            modifiers = UpdateModifiers.UPDATE_OR_WRITE;
        else if (Modifiers.contains(modifiers, UpdateModifiers.UPDATE_ONLY)) {
            if (Modifiers.contains(modifiers, UpdateModifiers.WRITE_ONLY))
                throw new IllegalArgumentException("Illegal modifiers - cannot use UPDATE_ONLY with WRITE_ONLY.");
            if (Modifiers.contains(modifiers, UpdateModifiers.UPDATE_OR_WRITE))
                throw new IllegalArgumentException("Illegal modifiers - cannot use UPDATE_ONLY with UPDATE_OR_WRITE.");
        } else if (Modifiers.contains(modifiers, UpdateModifiers.WRITE_ONLY)) {
            if (Modifiers.contains(modifiers, UpdateModifiers.UPDATE_OR_WRITE))
                throw new IllegalArgumentException("Illegal modifiers - cannot use WRITE_ONLY with UPDATE_OR_WRITE.");
            if (Modifiers.contains(modifiers, UpdateModifiers.PARTIAL_UPDATE))
                throw new IllegalArgumentException("Illegal modifiers - cannot use WRITE_ONLY with PARTIAL_UPDATE.");
        } else if (Modifiers.contains(modifiers, UpdateModifiers.UPDATE_OR_WRITE)) {
            if (Modifiers.contains(modifiers, UpdateModifiers.PARTIAL_UPDATE))
                throw new IllegalArgumentException("Illegal modifiers - cannot use WRITE_ONLY with PARTIAL_UPDATE.");
        } else if (Modifiers.contains(modifiers, UpdateModifiers.PARTIAL_UPDATE)) {
            modifiers = Modifiers.add(modifiers, UpdateModifiers.UPDATE_ONLY);
        } else {
            modifiers = Modifiers.add(modifiers, UpdateModifiers.UPDATE_OR_WRITE);
        }

        if (UpdateModifiers.isNoReturnValue(modifiers))
            modifiers |= Modifiers.NO_WRITE_LEASE;

        return modifiers;
    }

    @Override
    public DirectSpaceProxyFactory getFactory() {
        return _factory;
    }

    @Override
    public SpaceClusterInfo getSpaceClusterInfo() {
        return _proxySettings.getSpaceClusterInfo(isClustered());
    }

    @Override
    public SpaceProxyRouter getProxyRouter() {
        // TODO: Remove lazyness complexity when possible.
        if (_initializedNewRouter)
            return _proxyRouter;

        synchronized (_spaceInitializeLock) {
            if (_initializedNewRouter)
                return _proxyRouter;
            _proxyRouter = new SpaceProxyRouter(this);
            _initializedNewRouter = true;
            return _proxyRouter;
        }
    }

    /***
     * Used in client side to update all proxy components with the new map
     * @param chunksMap
     * @return
     */
    public void updateProxyRouter(SpaceProxyRouter oldRouter, ClusterTopology chunksMap){
        if(this._proxyRouter != oldRouter){
            return;
        }
        synchronized (_spaceInitializeLock) {
            if(this._proxyRouter != oldRouter){
                return;
            }
            this._proxySettings = this._proxySettings.cloneAndUpdate(chunksMap);
            this._proxyRouter = new SpaceProxyRouter(this);
        }
    }

    @Override
    public void afterClusterInfoChange(SpaceClusterInfo clusterInfo) {
        this.updateProxyRouter(this._proxyRouter, clusterInfo);
    }

    /***
     * Used in server side to update all collocated proxies
     * @param newClusterInfo
     * @return
     */
    private void updateProxyRouter(SpaceProxyRouter oldRouter, SpaceClusterInfo newClusterInfo){
        if(this._proxyRouter != oldRouter){
            return;
        }
        synchronized (_spaceInitializeLock) {
            if(this._proxyRouter != oldRouter){
                return;
            }
            this._proxySettings = this._proxySettings.cloneAndUpdate(newClusterInfo);
            this._proxyRouter = new SpaceProxyRouter(this);
        }
    }

    private static final int SERIAL_VERSION = 1;
    private static final byte FLAG_ISCLUSTER = 1 << 1;
    //private static final byte FLAG_LBHANDLER					= 1 << 2;
    //private static final byte FLAG_DISABLEPARALLELSCATTERING	= 1 << 3;
    //private static final byte FLAG_VERIFYPROXYALIVEBEFORETAKE	= 1 << 4;
    private static final byte FLAG_REMOTESPACEPROXY = 1 << 5;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (LRMIInvocationContext.getEndpointLogicalVersion().greaterOrEquals(PlatformLogicalVersion.v11_0_0))
            throw new IllegalStateException("Class " + this.getClass().getName() + " should no longer be serialized");
        super.writeExternal(out);

        out.writeInt(SERIAL_VERSION);
        final byte flags = buildFlags();
        out.writeByte(flags);
        out.writeObject(_proxySettings.getOldProxyHolder());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new IllegalStateException("This class should no longer be serialized");
    }

    private byte buildFlags() {
        byte flags = FLAG_REMOTESPACEPROXY;
        if (isClustered())
            flags |= FLAG_ISCLUSTER;
        return flags;
    }

    public boolean beforeExecute(SpaceOperationRequest<?> spaceRequest, IRemoteSpace targetSpace, int partitionId, String clusterName, boolean isEmbedded)
            throws RemoteException {
        try {
            SpaceContext spaceContext = getProxyRouter().getDefaultSpaceContext();
            if (spaceRequest.supportsSecurity()) {
                QuiesceToken token = spaceContext != null ? spaceContext.getQuiesceToken() : null;
                spaceContext = getSecurityManager().acquireContext(targetSpace);
                if (spaceContext != null)
                    spaceContext.setQuiesceToken(token);
            }
            spaceRequest.setSpaceContext(spaceContext);

            // for broadcast table operation we need to set SpaceContext inside SpaceRequestInfo before task is
            // distributed to other partitions.
            if(spaceRequest instanceof ExecuteTaskSpaceOperationRequest) {
                SpaceTask<?> task = ((ExecuteTaskSpaceOperationRequest) spaceRequest).getTask();
                if (task instanceof SystemTask) {
                    ((SystemTask<?>) task).getSpaceRequestInfo().setSpaceContext(spaceContext);
                }
            }
        } catch (com.gigaspaces.security.SecurityException e) {
            spaceRequest.setRemoteOperationExecutionError(e);
            return false;
        }

        if (spaceRequest.getTransaction() != null) {
            final ServerTransaction transaction = (ServerTransaction) spaceRequest.getTransaction();
            try {
                if (transaction.joinIfNeededAndEmbedded(targetSpace, partitionId, clusterName, this)) {
                    // TODO: Log joined transaction.
                    //	            if (_logger.isTraceEnabled())
                    //	                _logger.trace("Joined transaction [" + proxy.toLogMessage(request) + "]");
                }
            } catch (TransactionException e) {
                spaceRequest.setRemoteOperationExecutionError(e);
                return false;
            }
        }

        return spaceRequest.beforeOperationExecution(isEmbedded);
    }
    public static void afterExecute(SpaceOperationRequest<?> spaceRequest, IRemoteSpace targetSpace, int partitionId,
                                    boolean isRejected) {
        // afterOperationExecution is called first on the request for keeping the execution order
        // (beforeOperationExecution is called last on the request)
        spaceRequest.afterOperationExecution(partitionId);

        // disjoin transaction on failure or operation didn't lock any entries in space (transaction)
        if (spaceRequest.getTransaction() != null && (isRejected || !spaceRequest.hasLockedResources())) {
            final ServerTransaction transaction = (ServerTransaction) spaceRequest.getTransaction();
            if (transaction.isEmbeddedMgrInProxy()) {
                try {
                    // TODO: Log joined transaction.
                    //if (_logger.isTraceEnabled())
                    //    _logger.trace("Detaching transaction [" + proxy.toLogMessage(request) + "]");
                    transaction.mgr.disJoin(transaction.id, targetSpace);
                } catch (Exception e) {
                    // Ignore..
                }
            }
        }
    }

    @Override
    public void setQuiesceToken(QuiesceToken token) {
        getProxyRouter().setQuiesceToken(token);
    }

    @Override
    public ISpaceQuery prepareTemplate(Object template) {

        SnapshotProxyActionInfo actionInfo = new SnapshotProxyActionInfo(
                this, template);

        ITemplatePacket queryPacket = actionInfo.queryPacket;

        if (actionInfo.isSqlQuery) {
            queryPacket = getQueryManager().getSQLTemplate((SQLQueryTemplatePacket) actionInfo.queryPacket, null);
            // fill the SQLQuery generated template with the metadata
            queryPacket = getTypeManager().getTemplatePacketFromObject(queryPacket, ObjectType.TEMPLATE_PACKET);
        }
        queryPacket.setSerializeTypeDesc(false);
        return new EntrySnapshot(queryPacket);
    }

    @Override
    public void demote(long maxSuspendTime, TimeUnit timeUnit, CredentialsProvider credentialsProvider) throws DemoteFailedException, RemoteException {
        final IRemoteSpace remoteJSpace = getRemoteJSpace();

        SpaceContext spaceContext = getSecurityManager().acquireContext(remoteJSpace, credentialsProvider);
        remoteJSpace.demote(maxSuspendTime, timeUnit, spaceContext);
    }


    public SpaceProxyImpl cloneProxy (){
        SpaceProxyImpl copy = new SpaceProxyImpl(_factory.createCopy(false), _proxySettings);
        if (this.isSecured())
            copy.getSecurityManager().initialize(this.getSecurityManager().getCredentialsProvider());
        return copy;

    }

}
