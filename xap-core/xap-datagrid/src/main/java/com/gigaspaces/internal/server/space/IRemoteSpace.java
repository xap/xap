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

package com.gigaspaces.internal.server.space;

import com.gigaspaces.annotation.lrmi.LivenessPriority;
import com.gigaspaces.internal.client.spaceproxy.operations.SpaceConnectRequest;
import com.gigaspaces.internal.client.spaceproxy.operations.SpaceConnectResult;
import com.gigaspaces.internal.cluster.node.impl.directPersistency.DirectPersistencySyncListBatch;
import com.gigaspaces.internal.cluster.node.impl.router.spacefinder.IReplicationConnectionProxy;
import com.gigaspaces.internal.remoting.RemoteOperationsExecutor;
import com.gigaspaces.internal.server.space.demote.DemoteFailedException;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.security.service.RemoteSecuredService;
import com.j_spaces.core.SpaceContext;
import com.j_spaces.core.SpaceHealthStatus;
import com.j_spaces.jdbc.IQueryProcessor;
import com.sun.jini.start.ServiceProxyAccessor;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.lease.LeaseDeniedException;
import net.jini.core.lease.UnknownLeaseException;
import net.jini.core.transaction.UnknownTransactionException;
import net.jini.core.transaction.server.TransactionManager;
import net.jini.core.transaction.server.TransactionParticipant;
import net.jini.export.UseStubCache;
import net.jini.id.Uuid;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;

/**
 * The GigaSpaces remote interface.
 */
public interface IRemoteSpace
        extends Remote, TransactionParticipant, RemoteSecuredService, ServiceProxyAccessor, RemoteOperationsExecutor {
    ////////////////////////////////////////
    // Space Properties
    ////////////////////////////////////////

    String getName() throws RemoteException;

    /**
     * returns the universal unique identifier of the remote space
     */
    Uuid getSpaceUuid() throws RemoteException;

    boolean isEmbedded() throws RemoteException;

    ////////////////////////////////////////
    // Admin Operations
    ////////////////////////////////////////

    @LivenessPriority
    void ping() throws RemoteException;

    SpaceHealthStatus getSpaceHealthStatus() throws RemoteException;

    IQueryProcessor getQueryProcessor() throws RemoteException;

    /**
     * Generates and returns a unique id
     */
    String getUniqueID() throws RemoteException;

    Class<?> loadRemoteClass(String className)
            throws RemoteException, ClassNotFoundException;



    /**
     * Demote leadership and become a backup Space, in the specified operation timeout.
     *
     * @param maxSuspendTime The maximum time allowed to be in suspended state, after which the operation is aborted.
     * @param unit The unit of time for the maxSuspendTime parameter.
     * @param sc The SpaceContext for security check in the server side.
     * @throws DemoteFailedException
     * @throws RemoteException
     * @since 14.0.0
     */
    void demote(long maxSuspendTime, TimeUnit unit, SpaceContext sc) throws DemoteFailedException, RemoteException;

    ////////////////////////////////////////
    // CRUD entry Operations
    ////////////////////////////////////////

    void snapshot(ITemplatePacket e)
            throws UnusableEntryException, RemoteException;

    void cancel(String entryUID, String classname, int objectType)
            throws UnknownLeaseException, RemoteException;

    long renew(String entryUID, String classname, int objectType, long duration)
            throws LeaseDeniedException, UnknownLeaseException, RemoteException;

    Exception[] cancelAll(String[] entryUIDs, String[] classnames, int[] objectTypes)
            throws RemoteException;

    Object[] renewAll(String[] entryUIDs, String[] classnames, int[] objectTypes, long[] durations)
            throws RemoteException;

    ////////////////////////////////////////
    // Transaction Operations
    ////////////////////////////////////////

    @UseStubCache
    public void abort(TransactionManager parm1, Object parm2)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException;

    @UseStubCache
    public void commit(TransactionManager parm1, Object parm2)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException;

    @UseStubCache
    public void commit(TransactionManager parm1, Object parm2, int numOfParticipants)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException;

    @UseStubCache
    public int prepare(TransactionManager parm1, Object parm2)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException;

    @UseStubCache
    public int prepare(TransactionManager parm1, Object parm2, int numOfParticipants)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException;

    @UseStubCache
    public Object prepare(TransactionManager mgr, long id, boolean needClusteredProxy)
            throws UnknownTransactionException, RemoteException;


    @UseStubCache
    public Object prepare(TransactionManager parm1, Object parm2, boolean needClusteredProxy)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException;

    @UseStubCache
    public Object prepare(TransactionManager parm1, Object parm2, int numOfParticipants, boolean needClusteredProxy)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException;

    @UseStubCache
    public int prepareAndCommit(TransactionManager parm1, Object parm2)
            throws net.jini.core.transaction.UnknownTransactionException, java.rmi.RemoteException;

    ////////////////////////////////////////
    // Replication Operations
    ////////////////////////////////////////

    IReplicationConnectionProxy getReplicationRouterConnectionProxy() throws RemoteException;

    SpaceConnectResult connect(SpaceConnectRequest request) throws RemoteException;

    DirectPersistencySyncListBatch getSynchronizationListBatch() throws RemoteException;
}