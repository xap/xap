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

package com.gigaspaces.internal.cluster.node.handlers;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.OperationID;

import net.jini.core.transaction.Transaction;

import java.util.Collection;

/**
 * @author Niv Ingberg
 * @since 8.0.5
 */
public abstract class AbstractReplicationEntryEventHandler implements IReplicationInEntryHandler {
    @Override
    public void inWriteEntry(IReplicationInContext context, IEntryPacket entry)
            throws Exception {
        writeEntry(context, null, false, entry);
    }

    @Override
    public void inUpdateEntry(IReplicationInContext context, IEntryPacket entry, IEntryPacket oldEntryPacket,
                              boolean partialUpdate, boolean overrideVersion, short flags)
            throws Exception {
        updateEntry(context, null, false, entry, oldEntryPacket, partialUpdate, overrideVersion);
    }

    @Override
    public void inRemoveEntry(IReplicationInContext context, IEntryPacket entry)
            throws Exception {
        removeEntry(context, null, false, entry);
    }

    @Override
    public void inRemoveEntryByUID(IReplicationInContext context, String typeName, String uid, boolean isTransient, OperationID operationID)
            throws Exception {
        removeEntryByUid(context, null, false, uid, isTransient, operationID, typeName);
    }

    @Override
    public void inChangeEntry(IReplicationInContext context, String typeName, String uid,
                              Object id, int version,
                              int previousVersion, int routingHash, long timeToLive, Collection<SpaceEntryMutator> mutators, boolean isTransient, OperationID operationID) throws Exception {
        changeEntry(context, null, false, typeName, uid, id, routingHash, version, previousVersion, timeToLive, mutators, isTransient, operationID, null);
    }

    @Override
    public void afterConsumption(IReplicationInContext context, boolean successful, long lastProcessedKey) {
    }

    @Override
    public void beforeConsume(IReplicationInContext context) {
    }

    public abstract void writeEntry(IReplicationInContext context, Transaction txn, boolean twoPhaseCommit, IEntryPacket entry)
            throws Exception;

    public abstract void updateEntry(IReplicationInContext context, Transaction txn, boolean twoPhaseCommit, IEntryPacket entry,
                                     IEntryPacket previousPacket, boolean partialUpdate, boolean overrideVersion) throws Exception;

    public abstract void removeEntry(IReplicationInContext context, Transaction txn, boolean twoPhaseCommit, IEntryPacket entry)
            throws Exception;

    public abstract void removeEntryByUid(IReplicationInContext context, Transaction txn, boolean twoPhaseCommit, String uid, boolean isTransient, OperationID operationID, String typeName)
            throws Exception;

    public abstract void changeEntry(IReplicationInContext context,
                                     Transaction txn, boolean twoPhaseCommit, String typeName,
                                     String uid, Object id, int routingHash, int version,
                                     int previousVersion, long timeToLive,
                                     Collection<SpaceEntryMutator> mutators, boolean isTransient,
                                     OperationID operationID, IEntryData previousEntryData) throws Exception;

}
