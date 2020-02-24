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
import com.gigaspaces.client.WriteMultipleException;
import com.gigaspaces.client.WriteMultipleException.IWriteResult;
import com.gigaspaces.events.NotifyActionType;
import com.gigaspaces.events.NotifyInfo;
import com.gigaspaces.executor.SpaceTask;
import com.gigaspaces.internal.client.SpaceIteratorBatchResult;
import com.gigaspaces.internal.client.QueryResultTypeInternal;
import com.gigaspaces.internal.client.ReadTakeEntriesUidsResult;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeByIdsProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actioninfo.ReadTakeProxyActionInfo;
import com.gigaspaces.internal.client.spaceproxy.actions.AbstractSpaceProxyActionManager;
import com.gigaspaces.internal.client.spaceproxy.operations.CloseIteratorSpaceOperationResult;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.metadata.index.AddTypeIndexesResult;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.query.ISpaceQuery;
import com.gigaspaces.query.aggregators.AggregationResult;
import com.gigaspaces.query.aggregators.AggregationSet;
import com.j_spaces.core.DropClassException;
import com.j_spaces.core.LeaseContext;
import com.j_spaces.core.SpaceHealthStatus;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.core.client.EntryVersionConflictException;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.OperationTimeoutException;
import com.j_spaces.core.client.UpdateModifiers;
import com.j_spaces.core.exception.internal.InterruptedSpaceException;

import net.jini.core.entry.Entry;
import net.jini.core.entry.UnusableEntryException;
import net.jini.core.event.EventRegistration;
import net.jini.core.event.RemoteEventListener;
import net.jini.core.lease.Lease;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;
import net.jini.space.JavaSpace;

import java.rmi.MarshalledObject;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A common base class for most space proxies. <p/> Implements all the space operations and calling
 * the appropriate Listener using the {@link AbstractSpaceProxyActionManager}. Extended classes
 * should initialize the listeners in the action manager.
 *
 * @author GigaSpaces
 */
public abstract class AbstractSpaceProxy implements ISpaceProxy {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = -7437421509223305572L;
    private static final Logger _logger = Logger.getLogger(com.gigaspaces.logger.Constants.LOGGER_CLIENT);


    private final transient AbstractSpaceProxyActionManager<?> _actionManager;
    private transient int _associatedAppDomainId;
    private transient boolean _isAppDomainAssociated;
    private transient long _dotnetProxyHandleId;
    private transient boolean _isTargetOfADotnetProxy;

    protected AbstractSpaceProxy() {
        this._actionManager = createActionManager();
    }

    protected abstract AbstractSpaceProxyActionManager<?> createActionManager();

    @Override
    public String getContainerName() {
        return getDirectProxy().getProxySettings().getContainerName();
    }


    // -----------------------------------------
    // -------- JAVA SPACE OPERATIONS ----------
    // -----------------------------------------

    @Override
    public Lease write(Entry entry, Transaction txn, long lease)
            throws TransactionException, RemoteException {
        return _actionManager.write(entry, txn, lease, JavaSpace.NO_WAIT, getUpdateModifiers());
    }

    @Override
    public Entry read(Entry template, Transaction txn, long timeout)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return (Entry) _actionManager.read(template, txn, timeout, getReadModifiers(), false);
    }

    @Override
    public Entry readIfExists(Entry template, Transaction txn, long timeout)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return (Entry) _actionManager.read(template, txn, timeout, getReadModifiers(), true);
    }

    @Override
    public Entry take(Entry template, Transaction txn, long timeout)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return (Entry) _actionManager.take(template, txn, timeout, getReadModifiers(), false);
    }

    @Override
    public Entry takeIfExists(Entry template, Transaction txn, long timeout)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return (Entry) _actionManager.take(template, txn, timeout, getReadModifiers(), true);
    }

    @Override
    public EventRegistration notify(Entry template, Transaction txn, RemoteEventListener listener, long lease, MarshalledObject handback)
            throws TransactionException, RemoteException {
        if (txn != null)
            throw new IllegalArgumentException("Notification registration with transaction is not supported.");

        NotifyInfo notifyInfo = new NotifyInfo(listener, NotifyActionType.NOTIFY_WRITE.getModifier()
                , false, handback);
        return getDirectProxy().getDataEventsManager().addListener(template, lease, notifyInfo, false);
    }

    @Override
    public Entry snapshot(Entry e) throws RemoteException {
        return (Entry) _actionManager.snapshot(e);
    }

    // ------------------------------------
    // -------- SPACE OPERATIONS ----------
    // ------------------------------------

    public ITypeDesc getTypeDescriptor(String typeName) throws RemoteException {
        return _actionManager.getTypeDescriptor(typeName);
    }

    public void registerTypeDescriptor(ITypeDesc typeDesc) throws RemoteException {
        _actionManager.registerTypeDescriptor(typeDesc);
    }

    public ITypeDesc registerTypeDescriptor(Class<?> type) throws RemoteException {
        return _actionManager.registerTypeDescriptor(type);
    }

    public AsyncFuture<AddTypeIndexesResult> asyncAddIndexes(String typeName, SpaceIndex[] indexes,
                                                             AsyncFutureListener<AddTypeIndexesResult> listener) throws RemoteException {
        return _actionManager.asyncAddIndexes(typeName, indexes, listener);
    }

    @Override
    public void dropClass(String className) throws RemoteException, DropClassException {
        _actionManager.dropClass(className);
    }

    @SuppressWarnings("deprecation")
    public void clear(Object template, Transaction txn)
            throws RemoteException, TransactionException, UnusableEntryException {
        _actionManager.clear(template, txn, getReadModifiers());
    }

    @SuppressWarnings("deprecation")
    public int clear(Object template, Transaction txn, int modifiers)
            throws RemoteException, TransactionException, UnusableEntryException {
        return _actionManager.clear(template, txn, modifiers);
    }

    @SuppressWarnings("deprecation")
    public int count(Object template, Transaction txn)
            throws RemoteException, TransactionException, UnusableEntryException {
        return _actionManager.count(template, txn, getReadModifiers());
    }

    @SuppressWarnings("deprecation")
    public int count(Object template, Transaction txn, int modifiers)
            throws RemoteException, TransactionException, UnusableEntryException {
        return _actionManager.count(template, txn, modifiers);
    }

    @Override
    public AsyncFuture execute(SpaceTask task, Object routing, Transaction tx, AsyncFutureListener listener)
            throws TransactionException, RemoteException {
        return _actionManager.executeTask(task, routing, tx, listener);
    }

    public void ping()
            throws RemoteException {
        _actionManager.ping();
    }

    public SpaceHealthStatus getSpaceHealthStatus()
            throws RemoteException {
        return _actionManager.getSpaceHealthStatus();
    }

    @SuppressWarnings("deprecation")
    public Object read(Object template, Transaction txn, long timeout)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return _actionManager.read(template, txn, timeout, getReadModifiers(), false);
    }

    @SuppressWarnings("deprecation")
    public Object read(Object template, Transaction txn, long timeout, int modifiers)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return _actionManager.read(template, txn, timeout, modifiers, false);
    }

    public Object read(Object template, Transaction txn, long timeout, int modifiers, boolean ifExists)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return _actionManager.read(template, txn, timeout, modifiers, ifExists);
    }

    public AsyncFuture<?> asyncRead(Object template, Transaction txn, long timeout, int modifiers, AsyncFutureListener listener)
            throws RemoteException {
        return _actionManager.asyncRead(template, txn, timeout, modifiers, listener);
    }

    public Object readById(String className, Object id, Object routing, Transaction txn, long timeout, int modifiers, boolean ifExists, QueryResultTypeInternal resultType, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return _actionManager.readById(className, id, routing, txn, timeout, modifiers, ifExists, resultType,
                projections);
    }

    @Override
    public Object readById(ReadTakeProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return _actionManager.read(actionInfo);
    }

    @Override
    public Object takeById(ReadTakeProxyActionInfo actionInfo)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return _actionManager.take(actionInfo);
    }

    public Object readByUid(String uid, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPacket)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return _actionManager.readByUid(uid, txn, modifiers, resultType, returnPacket);
    }

    @SuppressWarnings("deprecation")
    public Object readIfExists(Object template, Transaction txn, long timeout)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return _actionManager.read(template, txn, timeout, getReadModifiers(), true);
    }

    @SuppressWarnings("deprecation")
    public Object readIfExists(Object template, Transaction txn, long timeout, int modifiers)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return _actionManager.read(template, txn, timeout, modifiers, true);
    }

    @SuppressWarnings("deprecation")
    public Object[] readMultiple(Object template, Transaction txn, int maxEntries)
            throws TransactionException, UnusableEntryException, RemoteException {
        return _actionManager.readMultiple(template, txn, JavaSpace.NO_WAIT, maxEntries, maxEntries /*minEntriesToWaitFor*/, getReadModifiers(), false, false /*ifExist*/);
    }

    @SuppressWarnings("deprecation")
    public Object[] readMultiple(Object template, Transaction txn, int maxEntries, int modifiers)
            throws TransactionException, UnusableEntryException, RemoteException {
        return _actionManager.readMultiple(template, txn, JavaSpace.NO_WAIT, maxEntries, maxEntries /*minEntriesToWaitFor*/,
                modifiers, false, false /*ifExist*/);
    }

    public Object[] readMultiple(Object template, Transaction txn, int maxEntries, int modifiers, boolean returnOnlyUids)
            throws TransactionException, UnusableEntryException, RemoteException {
        return _actionManager.readMultiple(template, txn, JavaSpace.NO_WAIT, maxEntries, maxEntries /*minEntriesToWaitFor*/,
                modifiers, returnOnlyUids, false /*ifExist*/);
    }

    public Object[] readMultiple(Object template, Transaction txn, long timeout, int maxEntries, int minEntriesToWaitFor, int modifiers, boolean returnOnlyUids, boolean ifExist)
            throws TransactionException, UnusableEntryException, RemoteException {
        return _actionManager.readMultiple(template, txn, timeout, maxEntries, minEntriesToWaitFor, modifiers, returnOnlyUids, ifExist);
    }

    @SuppressWarnings("deprecation")
    public <T> ISpaceQuery<T> snapshot(Object template)
            throws RemoteException {
        return _actionManager.snapshot(template);
    }

    @SuppressWarnings("deprecation")
    public Object take(Object template, Transaction txn, long timeout)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return _actionManager.take(template, txn, timeout, getReadModifiers(), false);
    }

    @SuppressWarnings("deprecation")
    public Object take(Object template, Transaction txn, long timeout, int modifiers)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return _actionManager.take(template, txn, timeout, modifiers, false);
    }

    public Object take(Object template, Transaction txn, long timeout, int modifiers, boolean ifExists)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return _actionManager.take(template, txn, timeout, modifiers, ifExists);
    }

    public AsyncFuture<?> asyncTake(Object template, Transaction txn, long timeout, int modifiers, AsyncFutureListener listener)
            throws RemoteException {
        return _actionManager.asyncTake(template, txn, timeout, modifiers, listener);
    }

    public Object takeById(String className, Object id, Object routing, int version, Transaction txn, long timeout, int modifiers, boolean ifExists, QueryResultTypeInternal resultType, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return _actionManager.takeById(className, id, routing, version, txn, timeout, modifiers, ifExists, resultType, projections);
    }

    public Object takeByUid(String uid, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPacket)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return _actionManager.takeByUid(uid, txn, modifiers, resultType, returnPacket);
    }

    @SuppressWarnings("deprecation")
    public Object takeIfExists(Object template, Transaction txn, long timeout)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return _actionManager.take(template, txn, timeout, getReadModifiers(), true);
    }

    public Object takeIfExists(Object template, Transaction txn, long timeout, int modifiers)
            throws UnusableEntryException, TransactionException, InterruptedException, RemoteException {
        return _actionManager.take(template, txn, timeout, modifiers, true);
    }

    @SuppressWarnings("deprecation")
    public Object[] takeMultiple(Object template, Transaction txn, int maxEntries) throws TransactionException, UnusableEntryException, RemoteException {
        return _actionManager.takeMultiple(template, txn, JavaSpace.NO_WAIT, maxEntries, maxEntries /*minEntriesToWaitFor*/,
                getReadModifiers(), false, false /*ifExist*/);
    }

    @SuppressWarnings("deprecation")
    public Object[] takeMultiple(Object template, Transaction txn, int maxEntries, int modifiers)
            throws TransactionException, UnusableEntryException, RemoteException {
        return _actionManager.takeMultiple(template, txn, JavaSpace.NO_WAIT, maxEntries, maxEntries /*minEntriesToWaitFor*/,
                modifiers, false, false /*ifExist*/);
    }

    public Object[] takeMultiple(Object template, Transaction txn, int maxEntries, int modifiers, boolean returnOnlyUids)
            throws TransactionException, UnusableEntryException, RemoteException {
        return _actionManager.takeMultiple(template, txn, JavaSpace.NO_WAIT, maxEntries, maxEntries /*minEntriesToWaitFor*/,
                modifiers, returnOnlyUids, false /*ifExist*/);
    }

    public Object[] takeMultiple(Object template, Transaction txn, long timeout, int maxEntries, int minEntriesToWaitFor, int modifiers, boolean returnOnlyUids, boolean ifExist)
            throws TransactionException, UnusableEntryException, RemoteException {
        return _actionManager.takeMultiple(template, txn, timeout, maxEntries, minEntriesToWaitFor, modifiers,
                returnOnlyUids, ifExist);

    }


    @SuppressWarnings("deprecation")
    public Object update(Object updatedEntry, Transaction transaction, long lease, long timeout)
            throws TransactionException, UnusableEntryException, RemoteException, InterruptedException {
        return update(updatedEntry, transaction, lease, timeout, getUpdateModifiers());
    }

    @SuppressWarnings("deprecation")
    public Object update(Object entry, Transaction txn, long lease, long timeout, int modifiers)
            throws TransactionException, UnusableEntryException, RemoteException, InterruptedException {
        if (!UpdateModifiers.isWriteOnly(modifiers) && !UpdateModifiers.isUpdateOrWrite(modifiers))
            modifiers = Modifiers.add(modifiers, UpdateModifiers.UPDATE_ONLY);
        if (!UpdateModifiers.isNoReturnValue(modifiers))
            modifiers = Modifiers.add(modifiers, UpdateModifiers.RETURN_PREV_ON_UPDATE);
        try {
            LeaseContext<?> result = _actionManager.write(entry, txn, lease, timeout, modifiers);
            return result == null ? null : result.getObject();
        } catch (RemoteException e) {
            Throwable cause = e.getCause();
            if (cause instanceof EntryNotInSpaceException)
                throw (EntryNotInSpaceException) cause;
            if (cause instanceof EntryAlreadyInSpaceException)
                throw (EntryAlreadyInSpaceException) cause;
            if (cause instanceof EntryVersionConflictException)
                throw (EntryVersionConflictException) cause;
            throw e;
        } catch (InterruptedSpaceException e) {
            Throwable cause = e.getCause();
            if (cause instanceof InterruptedException)
                throw (InterruptedException) cause;
            throw e;
        }
    }

    @SuppressWarnings("deprecation")
    public Object[] updateMultiple(Object[] entries, Transaction txn, long[] leases)
            throws UnusableEntryException, TransactionException, RemoteException {
        return updateMultiple(entries, txn, leases, getUpdateModifiers());
    }

    @SuppressWarnings("deprecation")
    public Object[] updateMultiple(Object[] entries, Transaction txn, long[] leases, int modifiers)
            throws UnusableEntryException, TransactionException, RemoteException {
        Object[] results = new Object[entries.length];

        if (!UpdateModifiers.isWriteOnly(modifiers) && !(UpdateModifiers.isUpdateOrWrite(modifiers)))
            modifiers = Modifiers.add(modifiers, UpdateModifiers.UPDATE_ONLY);
        if (!UpdateModifiers.isNoReturnValue(modifiers))
            modifiers = Modifiers.add(modifiers, UpdateModifiers.RETURN_PREV_ON_UPDATE);

        try {
            LeaseContext<?>[] writeResult = _actionManager.writeMultiple(entries, txn, Long.MAX_VALUE, leases, 0, modifiers);
            if (writeResult != null)
                for (int i = 0; i < results.length; i++)
                    results[i] = writeResult[i] == null ? null : writeResult[i].getObject();
        } catch (WriteMultipleException e) {
            for (int i = 0; i < results.length; i++) {
                IWriteResult result = e.getResults()[i];
                if (result.isError()) {
                    results[i] = result.getError();
                    if (results[i] instanceof OperationTimeoutException)
                        results[i] = null;
                } else {
                    results[i] = result.getLeaseContext().getObject();
                }
            }
        }
        return results;
    }

    @SuppressWarnings("deprecation")
    public LeaseContext<?> write(Object entry, Transaction txn, long lease)
            throws TransactionException, RemoteException {
        return _actionManager.write(entry, txn, lease, JavaSpace.NO_WAIT, getUpdateModifiers());
    }

    @SuppressWarnings("deprecation")
    public LeaseContext<?> write(Object entry, Transaction txn, long lease, long timeout, int modifiers)
            throws TransactionException, RemoteException {
        return _actionManager.write(entry, txn, lease, timeout, modifiers);
    }

    @SuppressWarnings("deprecation")
    public LeaseContext<?>[] writeMultiple(Object[] objects, Transaction txn, long lease) throws TransactionException, RemoteException {
        return _actionManager.writeMultiple(objects, txn, lease, null, 0, getUpdateModifiers());
    }

    @SuppressWarnings("deprecation")
    public LeaseContext<?>[] writeMultiple(Object[] objects, Transaction txn, long lease, int modifiers) throws TransactionException, RemoteException {
        return _actionManager.writeMultiple(objects, txn, lease, null, 0, modifiers);
    }

    @SuppressWarnings("deprecation")
    public LeaseContext<?>[] writeMultiple(Object[] objects, Transaction txn, long[] leases, int modifiers) throws TransactionException, RemoteException {
        return _actionManager.writeMultiple(objects, txn, Long.MIN_VALUE, leases, 0, modifiers);
    }

    public LeaseContext<?>[] writeMultiple(Object[] objects, Transaction txn, long lease, long[] leases, int modifiers) throws TransactionException, RemoteException {
        return _actionManager.writeMultiple(objects, txn, lease, leases, 0, modifiers);
    }

    public LeaseContext<?>[] writeMultiple(Object[] objects, Transaction txn, long lease, long[] leases, long timeout, int modifiers) throws TransactionException, RemoteException {
        return _actionManager.writeMultiple(objects, txn, lease, leases, timeout, modifiers);
    }

    public Object[] readByIds(String className, Object[] ids, Object routing, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return _actionManager.readByIds(className, ids, routing, txn, modifiers, resultType, returnPackets, projections);
    }

    public Object[] readByIds(String className, Object[] ids, Object[] routings, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return _actionManager.readByIds(className, ids, routings, txn, modifiers, resultType, returnPackets, projections);
    }

    @Override
    public Object[] readByIds(ReadTakeByIdsProxyActionInfo actionInfo, boolean returnPackets) throws RemoteException, TransactionException, InterruptedException, UnusableEntryException {
        return _actionManager.readByIds(actionInfo, returnPackets);
    }

    public Object[] takeByIds(String className, Object[] ids, Object routing, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return _actionManager.takeByIds(className, ids, routing, txn, modifiers, resultType, returnPackets, projections);
    }

    public Object[] takeByIds(String className, Object[] ids, Object[] routings, Transaction txn, int modifiers, QueryResultTypeInternal resultType, boolean returnPackets, String[] projections)
            throws RemoteException, UnusableEntryException, TransactionException, InterruptedException {
        return _actionManager.takeByIds(className, ids, routings, txn, modifiers, resultType, returnPackets, projections);
    }

    @Override
    public Object[] takeByIds(ReadTakeByIdsProxyActionInfo actionInfo, boolean returnPackets) throws RemoteException, TransactionException, InterruptedException, UnusableEntryException {
        return _actionManager.takeByIds(actionInfo, returnPackets);
    }

    @Override
    public ReadTakeEntriesUidsResult readEntriesUids(ITemplatePacket template, Transaction transaction, int entriesLimit, int modifiers) throws RemoteException, TransactionException, UnusableEntryException {
        return _actionManager.readEntriesUids(template, transaction, entriesLimit, modifiers);
    }

    @Override
    public <T> ChangeResult<T> change(Object template, ChangeSet changeSet, Transaction txn, long timeout, com.gigaspaces.client.ChangeModifiers modifiers) throws RemoteException, TransactionException {
        return _actionManager.change(template, changeSet, txn, timeout, modifiers);
    }

    @Override
    public <T> Future<ChangeResult<T>> asyncChange(Object template,
                                                   ChangeSet changeSet, Transaction txn, long timeout,
                                                   ChangeModifiers modifiers,
                                                   AsyncFutureListener<ChangeResult<T>> listener)
            throws RemoteException {
        return _actionManager.asyncChange(template, changeSet, txn, timeout, modifiers, listener);
    }

    @Override
    public AggregationResult aggregate(Object template, AggregationSet aggregationSet, Transaction txn, int readModifiers)
            throws RemoteException, TransactionException, InterruptedException {
        return _actionManager.aggregate(template, aggregationSet, txn, readModifiers);
    }

    //Flush to main memory
    public synchronized void setAppDomainId(int appDomainId) {
        _associatedAppDomainId = appDomainId;
        _isAppDomainAssociated = true;
    }

    public boolean hasAssociatedAppDomain() {
        return _isAppDomainAssociated;
    }

    public int getAppDomainId() {
        if (!_isAppDomainAssociated)
            throw new IllegalStateException("Cannot get associated AppDomain id if the object is not associated to any AppDomain");
        return _associatedAppDomainId;
    }

    //Flush to main memory
    public void setDotnetProxyHandleId(long handleId) {
        _dotnetProxyHandleId = handleId;
        _isTargetOfADotnetProxy = true;
    }

    public boolean isTargetOfADotnetProxy() {
        return _isTargetOfADotnetProxy;
    }

    public long getDotnetProxyHandleId() {
        if (!_isTargetOfADotnetProxy)
            throw new IllegalStateException("Cannot get .NET target proxy id if the object is not a target of a .NET proxy");
        return _dotnetProxyHandleId;
    }

    @Override
    public void applyNotifyInfoDefaults(NotifyInfo notifyInfo) {
    }

    @Override
    public boolean checkIfConnected() {
        try {
            ping();
            return true;
        } catch (RemoteException e) {
            return false;
        } catch (Exception e) {
            if (_logger.isLoggable(Level.WARNING))
                _logger.log(Level.WARNING, " Unexpected exception during checkIfConnected.", e);
            return false;
        }
    }

    public boolean isCacheContainer() {
        return false;
    }

    public IDirectSpaceProxy getLocalSpace() {
        return null;
    }

    public IDirectSpaceProxy getRemoteSpace() {
        return null;
    }

    public boolean isLocalCacheCacheContainer() {
        return false;
    }

    public boolean isLocalViewContainer() {
        return false;
    }

    @Override
    public SpaceIteratorBatchResult getBatchForIterator(Object template, int batchSize, int batchNumber, int modifiers, UUID uuid) throws TransactionException, UnusableEntryException, RemoteException{
        return _actionManager.getNextBatchFromServerIterator(template, batchSize, batchNumber, modifiers, uuid);
    }

    @Override
    public void closeSpaceIterator(UUID uuid) throws RemoteException, InterruptedException {
        _actionManager.closeServerIterator(uuid);
    }
}
