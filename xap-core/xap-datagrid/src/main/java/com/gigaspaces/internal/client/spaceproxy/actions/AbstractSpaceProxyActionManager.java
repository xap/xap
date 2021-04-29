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

package com.gigaspaces.internal.client.spaceproxy.actions;

import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.async.AsyncFutureListener;
import com.gigaspaces.client.ChangeModifiers;
import com.gigaspaces.client.ChangeResult;
import com.gigaspaces.client.ChangeSet;
import com.gigaspaces.client.ClearException;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.ReadTakeEntriesUidsResult;
import com.gigaspaces.internal.client.spaceproxy.ISpaceProxy;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.*;
import com.gigaspaces.internal.client.spaceproxy.executors.TypeDescriptorActionsProxyExecutor;
import com.gigaspaces.internal.client.spaceproxy.operations.ReadTakeEntriesUidsSpaceOperationRequest;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.space.actions.GetTypeDescriptorActionInfo;
import com.gigaspaces.internal.space.requests.AddTypeIndexesRequestInfo;
import com.gigaspaces.internal.space.requests.RegisterTypeDescriptorRequestInfo;
import com.gigaspaces.internal.space.requests.UnregisterTypeDescriptorRequestInfo;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.metadata.index.AddTypeIndexesResult;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.j_spaces.core.DropClassException;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.SpaceHealthStatus;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.exception.internal.InterruptedSpaceException;
import com.j_spaces.core.exception.internal.ProxyInternalSpaceException;
import com.j_spaces.kernel.JSpaceUtilities;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.concurrent.Future;

/**
 * An action manager, holding all the action listeners used by different space proxies.
 *
 * @author GigaSpaces
 */
public abstract class AbstractSpaceProxyActionManager<TSpaceProxy extends ISpaceProxy> {
    private final TSpaceProxy _spaceProxy;
    private final TypeDescriptorActionsProxyExecutor<TSpaceProxy> _typeDescriptorActionsExecutor;
    private final AdminProxyAction<TSpaceProxy> _adminAction;
    private final CountClearProxyAction<TSpaceProxy> _countClearAction;
    private final SnapshotProxyAction<TSpaceProxy> _snapshotAction;
    private final ReadTakeProxyAction<TSpaceProxy> _readTakeAction;
    private final ReadTakeMultipleProxyAction<TSpaceProxy> _readTakeMultipleAction;
    private final ReadTakeByIdsProxyAction<TSpaceProxy> _readTakeByIdsAction;
    private final WriteProxyAction<TSpaceProxy> _writeAction;
    private final ReadTakeEntriesUidsProxyAction<TSpaceProxy> _readTakeEntriesUidsAction;
    private final ChangeProxyAction<TSpaceProxy> _changeAction;
    private final AggregateProxyAction<TSpaceProxy> _aggregationAction;

    private static final Logger _devLogger = LoggerFactory.getLogger(Constants.LOGGER_DEV);

    protected AbstractSpaceProxyActionManager(TSpaceProxy spaceProxy) {
        _spaceProxy = spaceProxy;
        _typeDescriptorActionsExecutor = createTypeDescriptorActionsExecutor();
        _adminAction = createAdminProxyAction();
        _countClearAction = createCountClearProxyAction();
        _snapshotAction = createSnapshotProxyAction();
        _readTakeAction = createReadTakeProxyAction();
        _readTakeMultipleAction = createReadTakeMultipleProxyAction();
        _readTakeByIdsAction = createReadTakeByIdsProxyAction();
        _readTakeEntriesUidsAction = createReadTakeEntriesUidsProxyAction();
        _writeAction = createWriteProxyAction();
        _changeAction = createChangeProxyAction();
        _aggregationAction = createAggregateAction();
    }

    public ITypeDesc getTypeDescriptor(String typeName) throws RemoteException {
        GetTypeDescriptorActionInfo actionInfo = new GetTypeDescriptorActionInfo(typeName);
        return _typeDescriptorActionsExecutor.getTypeDescriptor(_spaceProxy, actionInfo);
    }

    public void registerTypeDescriptor(ITypeDesc typeDesc) throws RemoteException {
        RegisterTypeDescriptorRequestInfo actionInfo = new RegisterTypeDescriptorRequestInfo(typeDesc);
        _typeDescriptorActionsExecutor.registerTypeDescriptor(_spaceProxy, actionInfo);
    }

    public ITypeDesc registerTypeDescriptor(Class<?> type) throws RemoteException {
        return _typeDescriptorActionsExecutor.registerTypeDescriptor(_spaceProxy, type);
    }

    public AsyncFuture<AddTypeIndexesResult> asyncAddIndexes(String typeName, SpaceIndex[] indexes,
                                                             AsyncFutureListener<AddTypeIndexesResult> listener) throws RemoteException {
        AddTypeIndexesRequestInfo requestInfo = new AddTypeIndexesRequestInfo(typeName, indexes, listener);
        if (_spaceProxy.getDirectProxy().isGatewayProxy())
            requestInfo.setFromGateway(true);
        return _typeDescriptorActionsExecutor.asyncAddIndexes(_spaceProxy, requestInfo);
    }

    public void dropClass(String className)
            throws RemoteException, DropClassException {
        _adminAction.dropClass(_spaceProxy, className);
    }

    public void unregisterTypeDescriptor(String typeName) throws RemoteException {
        UnregisterTypeDescriptorRequestInfo actionInfo = new UnregisterTypeDescriptorRequestInfo(typeName);
        _typeDescriptorActionsExecutor.unregisterTypeDescriptor(_spaceProxy, actionInfo);
    }

    public int clear(Object template, Transaction txn, int modifiers)
            throws ClearException {
        CountClearProxyActionInfo actionInfo = new CountClearProxyActionInfo(
                _spaceProxy, template, txn, modifiers, true, true);
        try {
            return _countClearAction.execute(_spaceProxy, actionInfo);
        } catch (ClearException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ClearException(ex);
        }
    }

    public int count(Object template, Transaction txn, int modifiers)
            throws RemoteException, TransactionException, UnusableEntryException {
        CountClearProxyActionInfo actionInfo = new CountClearProxyActionInfo(
                _spaceProxy, template, txn, modifiers, false);
        return _countClearAction.execute(_spaceProxy, actionInfo);
    }

    public AsyncFuture executeTask(SpaceTask task, Object routing, Transaction tx, AsyncFutureListener listener)
            throws RemoteException, TransactionException {
        return _adminAction.execute(_spaceProxy, task, routing, tx, listener);
    }

    public void ping() throws RemoteException {
        _adminAction.ping(_spaceProxy);
    }

    public SpaceHealthStatus getSpaceHealthStatus() throws RemoteException {
        return _adminAction.getSpaceHealthStatus(_spaceProxy);
    }

    public Object read(Object template, Transaction txn, long timeout, int modifiers, boolean ifExists)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        long start = System.currentTimeMillis();
        ReadTakeProxyActionInfo actionInfo = new ReadTakeProxyActionInfo(
                _spaceProxy, template, txn, timeout, modifiers, ifExists, false);
        _devLogger.info( "> READ, create of actionInfo took " + ( System.currentTimeMillis() - start ) + " msec." );
        return read(actionInfo);
    }

    public Object readById(String className, Object id, Object routing, Transaction txn, long timeout, int modifiers, boolean ifExists, QueryResultTypeInternal resultType, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        ReadTakeProxyActionInfo actionInfo = new ReadTakeProxyActionInfo(
                _spaceProxy, className, id, routing, 0, txn, timeout, modifiers, resultType, ifExists, false, projections, null);
        return read(actionInfo);
    }

    public Object readByUid(String uid, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPacket)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        ReadTakeProxyActionInfo actionInfo = new ReadTakeProxyActionInfo(
                _spaceProxy, uid, txn, modifiers, resultType, returnPacket, false, false);
        return read(actionInfo);
    }

    public Object read(ReadTakeProxyActionInfo actionInfo)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        try {
            return _readTakeAction.read(_spaceProxy, actionInfo);
        } catch (UnusableEntryException e) {
            throw e;
        } catch (TransactionException e) {
            throw e;
        } catch (InterruptedException e) {
            throw e;
        } catch (RemoteException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw processInternalException(e);
        }
    }

    public AsyncFuture<?> asyncRead(Object template, Transaction txn, long timeout, int modifiers, AsyncFutureListener<?> listener)
            throws RemoteException {
        try {
            ReadTakeAsyncProxyActionInfo actionInfo = new ReadTakeAsyncProxyActionInfo(
                    _spaceProxy, template, txn, timeout, modifiers, false, listener);
            return _readTakeAction.asyncRead(_spaceProxy, actionInfo);
        } catch (RemoteException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw processInternalException(e);
        }
    }

    public Object[] readMultiple(Object template, Transaction txn, long timeout, int maxEntries, int minEntriesToWaitFor, int modifiers, boolean returnOnlyUids, boolean ifExist)
            throws TransactionException, UnusableEntryException, RemoteException {
        if (txn == null && Modifiers.contains(modifiers, Modifiers.EXCLUSIVE_READ_LOCK))
            throw new IllegalArgumentException("Using EXCLUSIVE_READ_LOCK modifier without a transaction is illegal.");

        ReadTakeMultipleProxyActionInfo actionInfo = new ReadTakeMultipleProxyActionInfo(
                _spaceProxy, template, txn, timeout, maxEntries, minEntriesToWaitFor, modifiers, returnOnlyUids, false, ifExist);

        try {
            return _readTakeMultipleAction.readMultiple(_spaceProxy, actionInfo);
        } catch (TransactionException e) {
            throw e;
        } catch (UnusableEntryException e) {
            throw e;
        } catch (RemoteException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw processInternalException(e);
        }
    }

    public Object[] readByIds(String className, Object[] ids, Object routing, Transaction txn, int modifiers,
                              QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        ReadTakeByIdsProxyActionInfo actionInfo = new ReadTakeByIdsProxyActionInfo(
                _spaceProxy, className, ids, routing, null, txn, false, modifiers, resultType, projections, null);
        return _readTakeByIdsAction.readByIds(_spaceProxy, actionInfo, returnPackets);
    }

    public Object[] readByIds(String className, Object[] ids, Object[] routings, Transaction txn, int modifiers,
                              QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        ReadTakeByIdsProxyActionInfo actionInfo = new ReadTakeByIdsProxyActionInfo(
                _spaceProxy, className, ids, null, routings, txn, false, modifiers, resultType, projections, null);
        return _readTakeByIdsAction.readByIds(_spaceProxy, actionInfo, returnPackets);
    }

    public Object[] readByIds(ReadTakeByIdsProxyActionInfo actionInfo, boolean returnPackets) throws RemoteException, TransactionException, InterruptedException, UnusableEntryException {
        return _readTakeByIdsAction.readByIds(_spaceProxy, actionInfo, returnPackets);
    }

    public ReadTakeEntriesUidsResult readEntriesUids(ITemplatePacket template, Transaction transaction, int entriesLimit,
                                                     int modifiers) throws RemoteException, TransactionException, UnusableEntryException {
        final ReadTakeEntriesUidsSpaceOperationRequest request = new ReadTakeEntriesUidsSpaceOperationRequest(template,
                transaction,
                entriesLimit,
                modifiers);
        return _readTakeEntriesUidsAction.readTakeEntriesUids(_spaceProxy, request);
    }

    public Object[] takeByIds(String className, Object[] ids, Object routing, Transaction txn, int modifiers,
                              QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        ReadTakeByIdsProxyActionInfo actionInfo = new ReadTakeByIdsProxyActionInfo(
                _spaceProxy, className, ids, routing, null, txn, true, modifiers, resultType, projections, null);
        return _readTakeByIdsAction.takeByIds(_spaceProxy, actionInfo, returnPackets);
    }

    public Object[] takeByIds(String className, Object[] ids, Object[] routings, Transaction txn, int modifiers,
                              QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        ReadTakeByIdsProxyActionInfo actionInfo = new ReadTakeByIdsProxyActionInfo(
                _spaceProxy, className, ids, null, routings, txn, true, modifiers, resultType, projections, null);
        return _readTakeByIdsAction.takeByIds(_spaceProxy, actionInfo, returnPackets);
    }

    public Object[] takeByIds(ReadTakeByIdsProxyActionInfo actionInfo, boolean returnPackets) throws RemoteException, TransactionException, InterruptedException, UnusableEntryException {
        return _readTakeByIdsAction.takeByIds(_spaceProxy, actionInfo, returnPackets);
    }

    public <T> ISpaceQuery<T> snapshot(Object object)
            throws RemoteException {
        try {
            SnapshotProxyActionInfo actionInfo = new SnapshotProxyActionInfo(
                    _spaceProxy, object);
            return _snapshotAction.snapshot(_spaceProxy, actionInfo);
        } catch (UnusableEntryException e) {
            throw processInternalException(e);
        }
    }

    public Object take(Object template, Transaction txn, long timeout, int modifiers, boolean ifExists)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        ReadTakeProxyActionInfo actionInfo = new ReadTakeProxyActionInfo(
                _spaceProxy, template, txn, timeout, modifiers, ifExists, true);
        return take(actionInfo);
    }

    public Object takeById(String className, Object id, Object routing, int version, Transaction txn, long timeout, int modifiers, boolean ifExists, QueryResultTypeInternal resultType, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        ReadTakeProxyActionInfo actionInfo = new ReadTakeProxyActionInfo(
                _spaceProxy, className, id, routing, version, txn, timeout, modifiers, resultType, ifExists, true, projections, null);
        return take(actionInfo);
    }

    public Object takeByUid(String uid, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPacket)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        ReadTakeProxyActionInfo actionInfo = new ReadTakeProxyActionInfo(
                _spaceProxy, uid, txn, modifiers, resultType, returnPacket, false, true);
        return take(actionInfo);
    }

    public Object take(ReadTakeProxyActionInfo actionInfo)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        try {
            return _readTakeAction.take(_spaceProxy, actionInfo);
        } catch (UnusableEntryException e) {
            throw e;
        } catch (TransactionException e) {
            throw e;
        } catch (InterruptedException e) {
            throw e;
        } catch (RemoteException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw processInternalException(e);
        }
    }

    public AsyncFuture<?> asyncTake(Object template, Transaction txn, long timeout, int modifiers, AsyncFutureListener<?> listener)
            throws RemoteException {
        try {
            ReadTakeAsyncProxyActionInfo actionInfo = new ReadTakeAsyncProxyActionInfo(
                    _spaceProxy, template, txn, timeout, modifiers, true, listener);
            return _readTakeAction.asyncTake(_spaceProxy, actionInfo);
        } catch (RemoteException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw processInternalException(e);
        }
    }

    public Object[] takeMultiple(Object template, Transaction txn, long timeout, int maxEntries, int minEntriesToWaitFor, int modifiers, boolean returnOnlyUids, boolean ifExist)
            throws TransactionException, UnusableEntryException, RemoteException {
        ReadTakeMultipleProxyActionInfo actionInfo = new ReadTakeMultipleProxyActionInfo(
                _spaceProxy, template, txn, timeout, maxEntries, minEntriesToWaitFor, modifiers, returnOnlyUids, true, ifExist);
        try {
            return _readTakeMultipleAction.takeMultiple(_spaceProxy, actionInfo);
        } catch (TransactionException e) {
            throw e;
        } catch (UnusableEntryException e) {
            throw e;
        } catch (RemoteException e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw processInternalException(e);
        }
    }

    public LeaseContext<?> write(Object entry, Transaction txn, long lease, long timeout, int modifiers)
            throws TransactionException, RemoteException {
        WriteProxyActionInfo actionInfo = new WriteProxyActionInfo(
                _spaceProxy, entry, txn, lease, timeout, modifiers);
        try {
            return _writeAction.write(_spaceProxy, actionInfo);
        } catch (InterruptedException e) {
            throw new InterruptedSpaceException(e);
        }
    }

    public LeaseContext<?>[] writeMultiple(Object[] objects, Transaction txn, long lease, long[] leases, long timeout, int modifiers)
            throws TransactionException, RemoteException {
        WriteMultipleProxyActionInfo actionInfo = new WriteMultipleProxyActionInfo(
                _spaceProxy, objects, txn, lease, leases, timeout, modifiers);
        try {
            return _writeAction.writeMultiple(_spaceProxy, actionInfo);
        } catch (InterruptedException e) {
            throw new InterruptedSpaceException(e);
        }
    }

    public <T> ChangeResult<T> change(Object template, ChangeSet changeSet,
                                      Transaction txn, long timeout, ChangeModifiers modifiers) throws RemoteException, TransactionException {
        ChangeProxyActionInfo actionInfo = new ChangeProxyActionInfo(_spaceProxy, template, changeSet, txn, timeout, modifiers);
        try {
            //noinspection unchecked
            return (ChangeResult<T>) _changeAction.change(_spaceProxy, actionInfo);
        } catch (InterruptedException e) {
            throw new InterruptedSpaceException(e);
        }
    }

    public <T> Future<ChangeResult<T>> asyncChange(Object template, ChangeSet changeSet,
                                                   Transaction txn, long timeout, ChangeModifiers modifiers, AsyncFutureListener<ChangeResult<T>> listener)
            throws RemoteException {
        ChangeProxyActionInfo actionInfo = new ChangeProxyActionInfo(_spaceProxy,
                template,
                changeSet,
                txn,
                timeout,
                modifiers);
        return _changeAction.asyncChange(_spaceProxy, actionInfo, listener);
    }

    public AggregationResult aggregate(Object query, AggregationSet aggregationSet, Transaction txn, int modifiers)
            throws RemoteException, TransactionException, InterruptedException {
        AggregateProxyActionInfo actionInfo = new AggregateProxyActionInfo(_spaceProxy,
                query, aggregationSet, txn, modifiers);
        return _aggregationAction.aggregate(_spaceProxy, actionInfo);
    }

    protected abstract TypeDescriptorActionsProxyExecutor<TSpaceProxy> createTypeDescriptorActionsExecutor();

    protected abstract AdminProxyAction<TSpaceProxy> createAdminProxyAction();

    protected abstract CountClearProxyAction<TSpaceProxy> createCountClearProxyAction();

    protected abstract ReadTakeProxyAction<TSpaceProxy> createReadTakeProxyAction();

    protected abstract ReadTakeMultipleProxyAction<TSpaceProxy> createReadTakeMultipleProxyAction();

    protected abstract ReadTakeByIdsProxyAction<TSpaceProxy> createReadTakeByIdsProxyAction();

    protected abstract ReadTakeEntriesUidsProxyAction<TSpaceProxy> createReadTakeEntriesUidsProxyAction();

    protected abstract SnapshotProxyAction<TSpaceProxy> createSnapshotProxyAction();

    protected abstract WriteProxyAction<TSpaceProxy> createWriteProxyAction();

    protected abstract ChangeProxyAction<TSpaceProxy> createChangeProxyAction();

    protected abstract AggregateProxyAction<TSpaceProxy> createAggregateAction();

    private ProxyInternalSpaceException processInternalException(Exception e) {
        return JSpaceUtilities.createProxyInternalSpaceException(e);
    }
}
