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

package com.gigaspaces.internal.cluster.node.impl.handlers;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.cluster.node.handlers.AbstractReplicationEntryEventHandler;
import com.gigaspaces.internal.cluster.node.handlers.IReplicationInEvictEntryHandler;
import com.gigaspaces.internal.metadata.ITypeDesc;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.logger.LogLevel;
import com.j_spaces.core.AnswerHolder;
import com.j_spaces.core.AnswerPacket;
import com.j_spaces.core.ExtendedAnswerHolder;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.client.EntryAlreadyInSpaceException;
import com.j_spaces.core.client.EntryNotInSpaceException;
import com.j_spaces.core.client.EntryVersionConflictException;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.OperationTimeoutException;
import com.j_spaces.core.client.ReadModifiers;
import com.j_spaces.core.client.TakeModifiers;
import com.j_spaces.core.client.UpdateModifiers;
import com.j_spaces.core.cluster.ConflictingOperationPolicy;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.Collection;



public abstract class AbstractSpaceReplicationEntryEventHandler
        extends AbstractReplicationEntryEventHandler
        implements IReplicationInEvictEntryHandler {
    protected final SpaceEngine _engine;
    protected final ReplicationInExceptionHandler _exceptionHandler;

    public AbstractSpaceReplicationEntryEventHandler(SpaceEngine engine,
                                                     ReplicationInExceptionHandler exceptionHandler) {
        _engine = engine;
        _exceptionHandler = exceptionHandler;
    }

    protected ConflictingOperationPolicy getConflictingOperationPolicy() {
        return _engine.getConflictingOperationPolicy();
    }

    protected boolean ignoreOperation(boolean isTransient) {
        return _engine.getCacheManager().isCacheExternalDB() && !isTransient
                && _engine.getCacheManager().isCentralDB()
                && _engine.getCacheManager().isEvictableCachePolicy()
                && !_engine.hasMirror();
    }

    @Override
    public void writeEntry(IReplicationInContext context, Transaction txn,
                           boolean twoPhaseCommit, IEntryPacket entry) throws Exception {
        _engine.getTypeManager().loadServerTypeDesc(entry);
        long lease = entry.getTTL();

        // discard write operation arriving from replication in case of
        // central data-source
        // this is done to avoid stale entries on backup after failover
        if (ignoreOperation(entry.isTransient()))
            return;

        writeEntryIntoSpace(context, txn, entry, lease, twoPhaseCommit);

    }

    @Override
    public void updateEntry(IReplicationInContext context, Transaction txn,
                            boolean twoPhaseCommit, IEntryPacket entry,
                            IEntryPacket previousEntry, boolean partialUpdate, boolean overrideVersion) throws Exception {
        _engine.getTypeManager().loadServerTypeDesc(entry);
        long lease = entry.getTTL();

        if (!shouldUpdateEntryInSpace(context, txn, twoPhaseCommit, entry))
            return;

        updateEntryInSpace(context,
                txn,
                entry,
                previousEntry,
                partialUpdate,
                overrideVersion,
                lease,
                twoPhaseCommit);

    }

    @Override
    public void removeEntry(IReplicationInContext context, Transaction txn,
                            boolean twoPhaseCommit, IEntryPacket entry) throws Exception {
        _engine.getTypeManager().loadServerTypeDesc(entry);
        ITemplatePacket template;
        // Entry may have arrived from an old server (before 9.0)
        // reconversion will cause loss of dynamic properties
        if (entry instanceof ITemplatePacket)
            template = (ITemplatePacket) entry;
        else
            template = TemplatePacketFactory.createFullPacket(entry);

        if (!shouldRemoveEntryFromSpace(entry.isTransient()))
            return;

        removeEntryFromSpace(context, txn, template, twoPhaseCommit);
    }

    @Override
    public void removeEntryByUid(IReplicationInContext context, Transaction transaction, boolean twoPhaseCommit, String uid, boolean isTransient, OperationID operationID, String typeName)
            throws Exception {
        if (!shouldRemoveEntryFromSpace(isTransient))
            return;

        ITemplatePacket entryPacket;
        if(_engine.isTieredStorage()){
            ITypeDesc typeDesc = this._engine.getTypeManager().getTypeDesc(typeName);
            entryPacket = TemplatePacketFactory.createUidPacket(typeDesc, uid, 0);
        } else {
            entryPacket = TemplatePacketFactory.createUidPacket(uid, 0, false);
        }

        entryPacket.setOperationID(operationID);

        removeEntryFromSpace(context, transaction, entryPacket, twoPhaseCommit);
    }

    @Override
    public void changeEntry(IReplicationInContext context, Transaction txn,
                            boolean twoPhaseCommit, String typeName, String uid,
                            Object id, int routingHash,
                            int version, int previousVersion, long timeToLive, Collection<SpaceEntryMutator> mutators, boolean isTransient, OperationID operationID, IEntryData previousEntry) throws Exception {
        if (!shouldChangeEntryInSpace(context, txn, twoPhaseCommit, typeName, uid, isTransient, version))
            return;

        changeEntryInSpace(context,
                txn,
                uid,
                version,
                mutators,
                isTransient,
                operationID,
                previousEntry,
                timeToLive,
                twoPhaseCommit);
    }

    protected void changeEntryInSpace(IReplicationInContext context,
                                      Transaction txn, String uid, int version,
                                      Collection<SpaceEntryMutator> mutators, boolean isTransient,
                                      OperationID operationID, IEntryData previousEntry, long timeToLive,
                                      boolean twoPhaseCommit) throws Exception {
        ITemplatePacket template = TemplatePacketFactory.createUidPacket(uid, version);
        int operationModifiers = 0;
        try {
            ExtendedAnswerHolder answerHolder = executeChangeOperation(context,
                    txn,
                    mutators,
                    timeToLive,
                    template,
                    operationModifiers);
            IEntryData previousEntryData = answerHolder.getPreviousEntryData();
            IEntryData modifiedEntryData = answerHolder.getModifiedEntryData();
            if (modifiedEntryData == null) {
                IEntryData rejectedEntry = answerHolder.getRejectedEntry();
                Exception exception = answerHolder.getException();
                if (rejectedEntry == null) {
                    if (exception != null)
                        throw exception;
                    _exceptionHandler.handleEntryNotInSpaceOnChange(context.getContextLogger(), template);
                } else {
                    if (exception instanceof OperationTimeoutException)
                        _exceptionHandler.handleEntryLockedByTransactionOnChange(context.getContextLogger(), template);
                    else if (exception instanceof EntryVersionConflictException)
                        _exceptionHandler.handleEntryVersionConflictOnChange(context.getContextLogger(), template, (EntryVersionConflictException) exception);
                    else if (exception != null)
                        throw exception;
                    else
                        throw new IllegalStateException("rejected entry upon change operation without any exception [" + template + "]");
                }

            }
            postChangeExecution(context, previousEntryData, modifiedEntryData);
        } catch (InterruptedException e) {
            // can't get here cause the call is NO_WAIT
            // restore interrupt state just in case it does get here
            Thread.currentThread().interrupt();
        }

    }

    private ExtendedAnswerHolder executeChangeOperation(IReplicationInContext context,
                                                        Transaction txn,
                                                        Collection<SpaceEntryMutator> mutators, long timeToLive,
                                                        ITemplatePacket template, int operationModifiers)
            throws UnusableEntryException, UnknownTypeException,
            TransactionException, RemoteException, InterruptedException {
        ExtendedAnswerHolder answerHolder = _engine.change(template,
                txn,
                timeToLive,
                0 /* timeout */,
                null /* SpaceContext */,
                true /* fromReplication */,
                false /* origin */,
                mutators,
                operationModifiers,
                false);
        IEntryData modifiedEntryData = answerHolder.getModifiedEntryData();
        if (modifiedEntryData == null) {
            Exception exception = answerHolder.getException();
            if (answerHolder.getRejectedEntry() != null && exception instanceof EntryVersionConflictException) {
                //Re-attempt with version 0 if override version was specified
                if (getConflictingOperationPolicy().isOverride()) {
                    _exceptionHandler.handleEntryVersionConflictOnChange(context.getContextLogger(),
                            template,
                            (EntryVersionConflictException) exception,
                            LogLevel.INFO);
                    template.setVersion(0);

                    return _engine.change(template,
                            txn,
                            timeToLive,
                            0 /* timeout */,
                            null /* SpaceContext */,
                            true /* fromReplication */,
                            false /* origin */,
                            mutators,
                            operationModifiers,
                            false);
                }
            }
        }
        return answerHolder;
    }


    protected void postChangeExecution(IReplicationInContext context,
                                       IEntryData previousEntryData, IEntryData modifiedEntryData) {
        // does nothing
    }

    @Override
    public void inEvictEntry(IReplicationInContext context, String uid, boolean isTransient, OperationID operationID)
            throws Exception {
        try {
            if (!_engine.getCacheManager().isEvictableCachePolicy())
                return;
            if (!shouldEvictEntryFromSpace(isTransient))
                return;

            ITemplatePacket template = TemplatePacketFactory.createUidPacket(uid, 0, false);
            template.setOperationID(operationID);

            _engine.read(template, null /* xtn */, 0 /* timeout */,
                    false/* ifExists */, true /* isTake */, null/* context */,
                    false/* returnUID */, true/* fromRepl */, false,
                    ReadModifiers.MATCH_BY_ID | TakeModifiers.EVICT_ONLY/* modifiers */);
        } catch (InterruptedException e) {
            // can't get here cause the call is NO_WAIT
            // restore interrupt state just in case it does get here
            Thread.currentThread().interrupt();
        } catch (Exception e) {
        }
    }

    protected void writeEntryIntoSpace(IReplicationInContext context,
                                       Transaction txn, IEntryPacket entry, long lease,
                                       boolean twoPhaseCommit) throws Exception {
        try {
            /** failed to consume */

            if (getConflictingOperationPolicy().isOverride()) {
                updateOrWrite(context, txn, entry, lease, Modifiers.NONE);
            } else {
                _engine.write(entry,
                        txn,
                        lease,
                        0 /* modifiers */,
                        true /* fromRepl */,
                        false,
                        null);
            }
        } catch (EntryAlreadyInSpaceException ex) {
            _exceptionHandler.handleEntryAlreadyInSpaceOnWrite(context, entry);
        }
    }

    protected void updateEntryInSpace(IReplicationInContext context,
                                      Transaction txn, IEntryPacket entry, IEntryPacket previousEntry, boolean partialUpdate,
                                      boolean overrideVersion, long lease, boolean twoPhaseCommit) throws UnusableEntryException,
            UnknownTypeException, TransactionException, RemoteException {
        try {
            int updateModifiers = overrideVersion ? Modifiers.OVERRIDE_VERSION : Modifiers.NONE;
            ExtendedAnswerHolder aHolder = executeUpdateOperation(context,
                    txn,
                    entry,
                    previousEntry,
                    partialUpdate,
                    overrideVersion,
                    lease,
                    twoPhaseCommit,
                    updateModifiers);

            IEntryData previousEntryData = aHolder != null ? aHolder.getPreviousEntryData() : null;
            IEntryData currentEntryData = aHolder != null ? aHolder.getModifiedEntryData() : null;

            postUpdateExecution(context, previousEntryData, currentEntryData);

            if (aHolder != null &&
                    aHolder.m_AnswerPacket != null &&
                    aHolder.m_AnswerPacket.m_EntryPacket == null &&
                    aHolder.m_AnswerPacket.m_leaseProxy == null) {
                _exceptionHandler.handleEntryLockedByTransactionOnUpdate(context.getContextLogger(),
                        entry);
            }
        } catch (EntryVersionConflictException ex) {
            _exceptionHandler.handleEntryVersionConflictOnUpdate(context.getContextLogger(),
                    entry, ex);
            /** failed to consume */
        } catch (EntryNotInSpaceException ex) {
            _exceptionHandler.handleEntryNotInSpaceOnUpdate(context.getContextLogger(),
                    entry, ex);
        } catch (InterruptedException e) {
            // can't get here cause the call is NO_WAIT
            // restore interrupt state just in case it does get here
            Thread.currentThread().interrupt();
        }
    }

    protected ExtendedAnswerHolder executeUpdateOperation(IReplicationInContext context,
                                                          Transaction txn, IEntryPacket entry, IEntryPacket previousEntry, boolean partialUpdate,
                                                          boolean overrideVersion, long lease, boolean twoPhaseCommit, int updateModifiers) throws UnusableEntryException,
            UnknownTypeException, TransactionException, RemoteException,
            InterruptedException {
        if (partialUpdate)
            return _engine.update(entry,
                    txn,
                    lease,
                    0 /* timeout */,
                    null /* SpaceContext */,
                    true /* fromReplication */,
                    false,
                    false /* newRouter */,
                    updateModifiers | UpdateModifiers.PARTIAL_UPDATE);
        else
            return updateOrWrite(context, txn, entry, lease, updateModifiers);
    }

    protected void postUpdateExecution(IReplicationInContext context, IEntryData previousEntryData, IEntryData currentEntryData) {
        // do nothing
    }

    protected void removeEntryFromSpace(IReplicationInContext context,
                                        Transaction txn, ITemplatePacket template, boolean twoPhaseCommit)
            throws TransactionException, UnusableEntryException,
            UnknownTypeException, RemoteException {

        try {
            //reset the version if override is allowed - so it doesn't throw VersionConflictException
            if (getConflictingOperationPolicy().isOverride())
                template.setVersion(0);

            // TODO replace with takeById
            AnswerHolder answerHolder = _engine.read(template,
                    txn /* xtn */,
                    0 /* timeout */,
                    true/* ifExists */,
                    true /* isTake */,
                    null/* context */,
                    false/* returnUID */,
                    true/* fromRepl */,
                    false /* origin */,
                    ReadModifiers.MATCH_BY_ID /* modifiers */);

            AnswerPacket aPacket = answerHolder != null ? answerHolder.getAnswerPacket() : null;

            postRemoveExecution(context, aPacket);

            if (aPacket != null && aPacket.m_EntryPacket == null) {
                _exceptionHandler.handleEntryLockedByTransactionOnTake(context.getContextLogger(), template);
            }
        } catch (EntryVersionConflictException ex) {
            _exceptionHandler.handleEntryVersionConflictOnTake(context.getContextLogger(),
                    template,
                    ex);
        } catch (EntryNotInSpaceException ex) {
            if (getConflictingOperationPolicy().isOverride())
                return;

            _exceptionHandler.handleEntryNotInSpaceOnTake(context.getContextLogger(), template);
        } catch (InterruptedException e) {
            // can't get here cause the call is NO_WAIT
            // restore interrupt state just in case it does get here
            Thread.currentThread().interrupt();
        }
    }

    protected void postRemoveExecution(IReplicationInContext context,
                                       AnswerPacket aPacket) {
        // do nothing
    }

    abstract protected ExtendedAnswerHolder updateOrWrite(
            IReplicationInContext context, Transaction txn, IEntryPacket entry,
            long lease, int modifiers) throws TransactionException,
            UnusableEntryException, UnknownTypeException, RemoteException,
            InterruptedException;

    abstract protected boolean shouldUpdateEntryInSpace(
            IReplicationInContext context, Transaction txn,
            boolean twoPhaseCommit, IEntryPacket entry) throws Exception;

    protected abstract boolean shouldChangeEntryInSpace(IReplicationInContext context,
                                                        Transaction txn, boolean twoPhaseCommit, String typeName,
                                                        String uid, boolean isTransient, int version) throws Exception;


    abstract protected boolean shouldRemoveEntryFromSpace(boolean isTransient);

    abstract protected boolean shouldEvictEntryFromSpace(boolean isTransient);

}
