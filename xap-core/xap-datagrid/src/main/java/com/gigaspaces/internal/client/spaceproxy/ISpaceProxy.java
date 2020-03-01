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

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.ReadTakeEntriesUidsResult;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeByIdsProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeProxyActionInfo;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.metadata.index.AddTypeIndexesResult;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.gigaspaces.security.service.SecuredService;
import com.gigaspaces.serialization.pbs.IDotnetProxyAssociated;
import com.j_spaces.core.*;
import com.j_spaces.core.client.ActionListener;
import com.j_spaces.core.client.ActionMaker;
import com.j_spaces.core.client.ReadModifiers;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * A space proxy that extends the {@link com.j_spaces.core.IJSpace} interface. Operations that need
 * to be provided by a space proxy but are not exposed as an API through the {@link
 * com.j_spaces.core.IJSpace} interface should go here.
 *
 * @author GigaSpaces
 */
public interface ISpaceProxy extends IJSpace, ActionMaker, SecuredService, IDotnetProxyAssociated, net.jini.space.JavaSpace {
    /**
     * Returns the transaction associated with the current context, if any.
     */
    Transaction.Created getContextTransaction();

    Transaction.Created replaceContextTransaction(Transaction.Created transaction);

    Transaction.Created replaceContextTransaction(Transaction.Created transaction, ActionListener actionListener, boolean delegatedXa);

    /**
     * returns true if the proxy is Clustered
     *
     * @return true when the proxy is Clustered
     */
    boolean isClustered();

    /**
     * returns itself in case of a direct proxy or the real proxy in case of a local cache or a
     * view.
     */
    IDirectSpaceProxy getNotificationsDirectProxy();

    /**
     * @see #getAdmin()
     * @deprecated since 7.0.1. Is not part of current security model.
     */
    @Deprecated
    Object getPrivilegedAdmin() throws java.rmi.RemoteException;

    /**
     * Executes a task on a single space.
     *
     * @param task     task (non-null) to execute on single space, according to the routing field.
     * @param tx       transaction (if any) under which to work.
     * @param listener event listener to execute result.
     * @return future to retrieve the result in an asynchronous way
     * @throws TransactionException if a transaction error occurs
     * @throws RemoteException      if a communication error occurs
     */
    AsyncFuture execute(SpaceTask task, Object routing, Transaction tx, AsyncFutureListener listener)
            throws TransactionException, RemoteException;

    /**
     * Close the proxy and execute necessary cleanup logic.
     */
    void close();

    /**
     * Internal methods only. Take any matching entry from the space, unblocking. Returns an
     * AsyncFuture which returns null if the timeout expires.
     *
     * @param template    The template used for matching. Matching is done against template with
     *                    null fields being wildcards ("match anything") other fields being values
     *                    ("match exactly on the serialized form").
     * @param transaction The template used for matching. Matching is done against template with
     *                    null fields being wildcards ("match anything") other fields being values
     *                    ("match exactly on the serialized form").
     * @param timeout     How long the client is willing to wait for a transactionally proper
     *                    matching entry. A timeout of NO_WAIT means to wait no time at all; this is
     *                    equivalent to a wait of zero.
     * @param modifiers   one or a union of {@link ReadModifiers}.
     * @param listener    event listener to execute result.
     * @return object from the space
     * @throws UnusableEntryException - if any serialized field of the entry being read cannot be
     *                                deserialized for any reason TransactionException - if a
     *                                transaction error occurs InterruptedException - if the thread
     *                                in which the read occurs is interrupted RemoteException - if a
     *                                communication error occurs IllegalArgumentException - if a
     *                                negative timeout value is used
     * @since 6.6
     */
    AsyncFuture asyncTake(Object template, Transaction transaction, long timeout,
                          int modifiers, AsyncFutureListener listener) throws RemoteException;

    /**
     * Internal methods only. Read any matching entry from the space, unblocking. Returns an
     * AsyncFuture which returns null if the timeout expires.
     *
     * @param template    The template used for matching. Matching is done against template with
     *                    null fields being wildcards ("match anything") other fields being values
     *                    ("match exactly on the serialized form").
     * @param transaction The template used for matching. Matching is done against template with
     *                    null fields being wildcards ("match anything") other fields being values
     *                    ("match exactly on the serialized form").
     * @param timeout     How long the client is willing to wait for a transactionally proper
     *                    matching entry. A timeout of NO_WAIT means to wait no time at all; this is
     *                    equivalent to a wait of zero.
     * @param modifiers   one or a union of {@link ReadModifiers}.
     * @param listener    event listener to execute result.
     * @return object from the space
     * @throws UnusableEntryException - if any serialized field of the entry being read cannot be
     *                                deserialized for any reason TransactionException - if a
     *                                transaction error occurs InterruptedException - if the thread
     *                                in which the read occurs is interrupted RemoteException - if a
     *                                communication error occurs IllegalArgumentException - if a
     *                                negative timeout value is used
     * @since 6.6
     */
    AsyncFuture asyncRead(Object template, Transaction transaction, long timeout,
                          int modifiers, AsyncFutureListener listener) throws RemoteException;

    Object read(Object template, Transaction txn, long timeout, int modifiers, boolean ifExists)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException;

    Object take(Object template, Transaction txn, long timeout, int modifiers, boolean ifExists)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException;

    Object[] readMultiple(Object template, Transaction transaction, int limit, int modifiers, boolean returnOnlyUids)
            throws TransactionException, UnusableEntryException, RemoteException;

    Object[] readMultiple(Object template, Transaction transaction, long timeout, int limit, int minEntriesToWaitFor, int modifiers, boolean returnOnlyUids, boolean ifExist)
            throws TransactionException, UnusableEntryException, RemoteException;


    Object[] takeMultiple(Object template, Transaction transaction, int limit, int modifiers, boolean returnOnlyUids)
            throws TransactionException, UnusableEntryException, RemoteException;

    Object[] takeMultiple(Object template, Transaction transaction, long timeout, int limit, int minEntriesToWaitFor, int modifiers, boolean returnOnlyUids, boolean ifExist)
            throws TransactionException, UnusableEntryException, RemoteException;

    Object readById(ReadTakeProxyActionInfo actionInfo) throws RemoteException, UnusableEntryException, TransactionException, InterruptedException;

    Object takeById(ReadTakeProxyActionInfo actionInfo) throws RemoteException, UnusableEntryException, TransactionException, InterruptedException;

    Object readById(String className, Object id, Object routing, Transaction txn, long timeout, int modifiers, boolean ifExists, QueryResultTypeInternal resultType, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException;

    Object takeById(String className, Object id, Object routing, int version, Transaction txn, long timeout, int modifiers, boolean ifExists, QueryResultTypeInternal resultType, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException;

    Object readByUid(String uid, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPacket)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException;

    Object takeByUid(String uid, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPacket)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException;

    Object[] readByIds(String className, Object[] ids, Object routing, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException;

    Object[] readByIds(String className, Object[] ids, Object[] routings, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException;

    Object[] readByIds(ReadTakeByIdsProxyActionInfo actionInfo, boolean returnPackets) throws RemoteException, TransactionException, InterruptedException, UnusableEntryException;

    Object[] takeByIds(String className, Object[] ids, Object routing, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException;

    Object[] takeByIds(String className, Object[] ids, Object[] routings, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException;

    Object[] takeByIds(ReadTakeByIdsProxyActionInfo actionInfo, boolean returnPackets) throws RemoteException, TransactionException, InterruptedException, UnusableEntryException;

    LeaseContext[] writeMultiple(Object[] objects, Transaction txn, long lease, long[] leases, long timeout, int modifiers) throws TransactionException, RemoteException;

    /**
     * Gets an object representing the space health status
     *
     * @return object representing the space health status
     */
    SpaceHealthStatus getSpaceHealthStatus()
            throws RemoteException;

    ITypeDesc getTypeDescriptor(String typeName) throws RemoteException;

    void registerTypeDescriptor(ITypeDesc typeDesc) throws RemoteException;

    ITypeDesc registerTypeDescriptor(Class<?> type) throws RemoteException;

    AsyncFuture<AddTypeIndexesResult> asyncAddIndexes(String typeName, SpaceIndex[] indexes, AsyncFutureListener<AddTypeIndexesResult> listener) throws RemoteException;

    /**
     * Loads a remote class from the space, the class will be loaded locally into a corresponding
     * LRMIClassLoader
     *
     * @since 8.0.1
     */
    Class<?> loadRemoteClass(String className) throws ClassNotFoundException;

    void applyNotifyInfoDefaults(NotifyInfo notifyInfo);

    boolean checkIfConnected();

    /**
     * Generate a unique operation ID
     *
     * @return Unique operation ID
     */
    OperationID createNewOperationID();

    int initWriteModifiers(int modifiers);

    /**
     * Read entries UIDs from space.
     *
     * @since 9.0
     */
    ReadTakeEntriesUidsResult readEntriesUids(ITemplatePacket template, Transaction transaction, int entriesLimit, int modifiers) throws RemoteException, TransactionException, UnusableEntryException;

    /**
     * Change entry in space
     *
     * @since 9.1
     */
    <T> ChangeResult<T> change(Object template, ChangeSet changeSet, Transaction txn, long timeout, ChangeModifiers modifiers) throws RemoteException, TransactionException;

    <T> Future<ChangeResult<T>> asyncChange(Object template, ChangeSet changeSet, Transaction txn, long timeout, ChangeModifiers modifiers, AsyncFutureListener<ChangeResult<T>> listener) throws RemoteException;

    AggregationResult aggregate(Object template, AggregationSet aggregationSet, Transaction txn, int readModifiers) throws RemoteException, TransactionException, InterruptedException;

    void closeSpaceIterator(UUID uuid) throws RemoteException, InterruptedException;
}
