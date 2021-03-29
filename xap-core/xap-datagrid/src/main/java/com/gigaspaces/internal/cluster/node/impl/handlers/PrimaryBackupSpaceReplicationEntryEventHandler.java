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
import com.gigaspaces.internal.cluster.node.impl.backlog.sync.IMarker;
import com.gigaspaces.internal.cluster.node.impl.view.EntryPacketServerEntryAdapter;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.logger.LogLevel;
import com.j_spaces.core.AnswerPacket;
import com.j_spaces.core.ExtendedAnswerHolder;
import com.j_spaces.core.OperationID;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.UpdateOrWriteContext;
import com.j_spaces.core.client.EntryVersionConflictException;
import com.j_spaces.core.client.Modifiers;
import com.j_spaces.core.client.UpdateModifiers;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.Collection;



@com.gigaspaces.api.InternalApi
public class PrimaryBackupSpaceReplicationEntryEventHandler
        extends AbstractSpaceReplicationEntryEventHandler {

    public PrimaryBackupSpaceReplicationEntryEventHandler(SpaceEngine engine, boolean isCentralAndExternalDB) {
        super(engine, new PrimaryBackupReplicationInExceptionHandler(engine.getFullSpaceName(), isCentralAndExternalDB));
    }

    @Override
    protected void writeEntryIntoSpace(IReplicationInContext context,
                                       Transaction txn, IEntryPacket entry, long lease, boolean twoPhaseCommit) throws Exception {
        protectEntryFromEvictionIfNeeded(context, entry.getUID(), twoPhaseCommit);
        super.writeEntryIntoSpace(context, txn, entry, lease, twoPhaseCommit);
    }

    @Override
    protected ExtendedAnswerHolder updateOrWrite(IReplicationInContext context,
                                                 Transaction transaction, IEntryPacket entryPacket, long lease,
                                                 int updateModifiers) throws TransactionException,
            UnusableEntryException, UnknownTypeException, RemoteException,
            InterruptedException {
        boolean applyOverrideVersion = !Modifiers.contains(updateModifiers, Modifiers.OVERRIDE_VERSION)
                && getConflictingOperationPolicy().isOverride();

        int originalVersion = entryPacket.getVersion();
        UpdateOrWriteContext ctx = new UpdateOrWriteContext(entryPacket,
                lease,
                0 /* timeout */,
                transaction,
                null,
                updateModifiers | UpdateModifiers.UPDATE_OR_WRITE,
                false,
                true,
                false/* fromWriteMultiple */);
        try {
            return _engine.updateOrWrite(ctx,
                    true /* fromReplication */,
                    false,
                    false);
        } catch (EntryVersionConflictException ex) {
            if (applyOverrideVersion) {
                entryPacket.setVersion(originalVersion);
                _exceptionHandler.handleEntryVersionConflictOnUpdate(context.getContextLogger(),
                        entryPacket,
                        ex,
                        LogLevel.INFO);
                updateModifiers |= Modifiers.OVERRIDE_VERSION; // if its a
                // backup-
                // override the
                // version too.
                ctx.operationModifiers = updateModifiers;
            } else if (getConflictingOperationPolicy().isOverride()) {
                _exceptionHandler.handleEntryVersionConflictOnUpdate(context.getContextLogger(),
                        entryPacket,
                        ex,
                        LogLevel.INFO);
                entryPacket.setVersion(0);
            } else {
                throw ex;
            }
            /** failed to consume */
        }
        // retry with override modifier. first try just recorded an INFO log
        // record
        return _engine.updateOrWrite(ctx,
                true /* fromReplication */,
                false,
                false);
    }


    @Override
    protected boolean shouldUpdateEntryInSpace(IReplicationInContext context, Transaction txn,
                                               boolean twoPhaseCommit, IEntryPacket entry) {
        // If external-data-source doesn't support versioning, remove entry
        // from cache;
        // Otherwise, update only in pure cache.
        return !ignoreOperation(entry.isTransient());
    }

    @Override
    protected void updateEntryInSpace(IReplicationInContext context,
                                      Transaction txn, IEntryPacket entry, IEntryPacket previousEntry, boolean partialUpdate,
                                      boolean overrideVersion, long lease, boolean twoPhaseCommit) throws UnusableEntryException,
            UnknownTypeException, TransactionException, RemoteException {
        protectEntryFromEvictionIfNeeded(context,
                entry.getUID(),
                twoPhaseCommit);
        super.updateEntryInSpace(context,
                txn,
                entry,
                previousEntry,
                partialUpdate,
                overrideVersion,
                lease,
                twoPhaseCommit);
    }

    @Override
    protected void postUpdateExecution(IReplicationInContext context, IEntryData previousEntryData, IEntryData currentEntryData) {
        if (context.getContentContext() != null) {
            if (previousEntryData != null)
                context.getContentContext().setSecondaryEntryData(previousEntryData);
            if (currentEntryData != null)
                context.getContentContext().setMainEntryData(currentEntryData);
        }
    }

    @Override
    protected void changeEntryInSpace(IReplicationInContext context,
                                      Transaction txn, String uid, int version,
                                      Collection<SpaceEntryMutator> mutators, boolean isTransient,
                                      OperationID operationID, IEntryData previousEntry, long timeToLive,
                                      boolean twoPhaseCommit, String typeName) throws Exception {
        protectEntryFromEvictionIfNeeded(context,
                uid,
                twoPhaseCommit);

        super.changeEntryInSpace(context,
                txn,
                uid,
                version,
                mutators,
                isTransient,
                operationID,
                previousEntry,
                timeToLive,
                twoPhaseCommit, typeName);
    }


    @Override
    protected void postChangeExecution(IReplicationInContext context,
                                       IEntryData previousEntryData, IEntryData modifiedEntryData) {
        //We need to set the previous state and updated into the content
        if (context.getContentContext() != null) {
            if (previousEntryData != null)
                context.getContentContext().setSecondaryEntryData(previousEntryData);
            if (modifiedEntryData != null)
                context.getContentContext().setMainEntryData(modifiedEntryData);
        }
    }

    @Override
    protected boolean shouldChangeEntryInSpace(IReplicationInContext context,
                                               Transaction txn, boolean twoPhaseCommit, String typeName,
                                               String uid, boolean isTransient, int version) {
        return !ignoreOperation(isTransient);
    }

    @Override
    protected void removeEntryFromSpace(IReplicationInContext context,
                                        Transaction txn, ITemplatePacket template, boolean twoPhaseCommit)
            throws TransactionException, UnusableEntryException,
            UnknownTypeException, RemoteException {
        protectEntryFromEvictionIfNeeded(context, template.getUID(), twoPhaseCommit);
        super.removeEntryFromSpace(context, txn, template, twoPhaseCommit);
    }

    @Override
    protected void postRemoveExecution(IReplicationInContext context, AnswerPacket aPacket) {
        if (aPacket != null &&
                aPacket.m_EntryPacket != null &&
                context.getContentContext() != null) {
            context
                    .getContentContext()
                    .setMainEntryData(new EntryPacketServerEntryAdapter(aPacket.m_EntryPacket));
        }
    }

    @Override
    protected boolean shouldRemoveEntryFromSpace(boolean isTransient) {
        //Non transient entry removal, discard since it is not in the space
        return !ignoreOperation(isTransient);
    }

    @Override
    protected boolean shouldEvictEntryFromSpace(boolean isTransient) {
        //Non transient entry eviction, discard since it is not in the space
        return !ignoreOperation(isTransient);
    }

    private void protectEntryFromEvictionIfNeeded(
            IReplicationInContext context, String uid, boolean twoPhaseCommit) {
        if (!twoPhaseCommit
                && _engine.getCacheManager()
                .requiresEvictionReplicationProtection()) {
            final IMarker marker = context.getContextMarker(_engine.getCacheManager()
                    .getEvictionPolicyReplicationMembersGroupName());
            _engine.getCacheManager()
                    .getEvictionReplicationsMarkersRepository()
                    .insert(uid, marker, false /* alreadyLocked */);
        }
    }
}
