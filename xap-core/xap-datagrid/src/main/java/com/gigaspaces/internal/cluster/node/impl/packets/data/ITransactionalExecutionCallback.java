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

package com.gigaspaces.internal.cluster.node.impl.packets.data;

import com.gigaspaces.client.mutators.SpaceEntryMutator;
import com.gigaspaces.internal.cluster.node.IReplicationInContext;
import com.gigaspaces.internal.server.storage.IEntryData;
import com.gigaspaces.internal.transport.IEntryPacket;
import com.j_spaces.core.OperationID;

import net.jini.core.transaction.Transaction;

import java.util.Collection;

public interface ITransactionalExecutionCallback {

    void writeEntry(IReplicationInContext context, Transaction transaction, boolean twoPhaseCommit,
                    IEntryPacket entryPacket) throws Exception;

    void removeEntry(IReplicationInContext context, Transaction transaction, boolean twoPhaseCommit,
                     IEntryPacket entryPacket) throws Exception;

    void removeEntryByUID(IReplicationInContext context, Transaction transaction, boolean twoPhaseCommit,
                          String uid, boolean isTransient, OperationID operationID, String typeName) throws Exception;

    void updateEntry(IReplicationInContext context, Transaction transaction, boolean twoPhaseCommit,
                     IEntryPacket entryPacket, IEntryPacket previousEntryPacket, boolean partialUpdate, boolean overrideVersion, short flags) throws Exception;

    void changeEntry(IReplicationInContext context, Transaction transaction,
                     boolean twoPhaseCommit, String typeName, String uid, Object id,
                     int version, int previousVersion, long timeToLive, int routingHash,
                     Collection<SpaceEntryMutator> spaceEntryMutators,
                     boolean isTransient, OperationID operationID,
                     IEntryData previousEntryData) throws Exception;

}
