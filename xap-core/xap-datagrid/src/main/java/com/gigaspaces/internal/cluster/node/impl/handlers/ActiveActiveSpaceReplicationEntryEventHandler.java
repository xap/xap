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

import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.server.space.SpaceEngine;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.gigaspaces.internal.transport.ITemplatePacket;
import com.gigaspaces.internal.transport.TemplatePacketFactory;
import com.gigaspaces.logger.LogLevel;
import com.j_spaces.core.ExtendedAnswerHolder;
import com.j_spaces.core.UnknownTypeException;
import com.j_spaces.core.UpdateOrWriteContext;
import com.j_spaces.core.client.EntryVersionConflictException;
import com.j_spaces.core.client.UpdateModifiers;

import net.jini.core.entry.UnusableEntryException;
import net.jini.core.transaction.Transaction;
import net.jini.core.transaction.TransactionException;

import java.rmi.RemoteException;
import java.util.logging.Level;


@com.gigaspaces.api.InternalApi
public class ActiveActiveSpaceReplicationEntryEventHandler extends AbstractSpaceReplicationEntryEventHandler {

    public ActiveActiveSpaceReplicationEntryEventHandler(SpaceEngine engine, boolean isCentralAndExternalDB) {
        super(engine, new ReplicationInExceptionHandler(engine.getFullSpaceName(), isCentralAndExternalDB));
    }

    @Override
    protected ExtendedAnswerHolder updateOrWrite(IReplicationInContext context,
                                                 Transaction transaction, IEntryPacket entryPacket, long lease,
                                                 int updateModifiers) throws TransactionException,
            UnusableEntryException, UnknownTypeException, RemoteException,
            InterruptedException {
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
            if (getConflictingOperationPolicy().isOverride()) {
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
                                               boolean twoPhaseCommit, IEntryPacket entry) throws Exception {
        // If external-data-source doesn't support versioning, remove entry
        // from cache;
        // Otherwise, update only in pure cache.
        if (ignoreOperation(entry.isTransient()) && !_engine.getCacheManager().isVersionedExternalDB()) {
            // the EntryPacket used to perform the update must be converted
            // into a templatePacket
            // to allow take.
            ITemplatePacket templatePacket = TemplatePacketFactory.createFullPacket(entry);
            removeEntry(context, txn, twoPhaseCommit, templatePacket);
            return false;
        }
        return true;
    }

    @Override
    protected boolean shouldChangeEntryInSpace(IReplicationInContext context,
                                               Transaction txn, boolean twoPhaseCommit, String typeName,
                                               String uid, boolean isTransient, int version) throws Exception {
        // If external-data-source doesn't support versioning, remove entry
        // from cache;
        // Otherwise, update only in pure cache.
        if (ignoreOperation(isTransient) && !_engine.getCacheManager().isVersionedExternalDB()) {
            // the EntryPacket used to perform the update must be converted
            // into a templatePacket
            // to allow take.

            ITemplatePacket templatePacket = TemplatePacketFactory.createUidPacket(uid, version);
            removeEntry(context, txn, twoPhaseCommit, templatePacket);
            return false;
        }
        return true;
    }

    @Override
    protected boolean shouldRemoveEntryFromSpace(boolean isTransient) {
        return true;
    }

    @Override
    protected boolean shouldEvictEntryFromSpace(boolean isTransient) {
        return true;
    }

}
